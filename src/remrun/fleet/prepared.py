"""Prepare immutable fleet work before it enters the durable queue."""
from __future__ import annotations

import hashlib
import math
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping

from .models import normalize_capabilities
from .task_contract import canonical_json, sha256_id, verify_id


class PreparationError(ValueError):
    """A job could not be prepared without ambiguity."""


class SourceChangedError(PreparationError):
    """A pathname no longer names the bytes frozen in a prepared job."""


RAW_COMMAND_SPEC = {
    "schema": 1,
    "kind": "command",
    "batching": "never",
    "completion": "exit-code-v1",
    "cost": "unestimated",
    "shell": False,
}
RAW_COMMAND_SPEC_ID = sha256_id(RAW_COMMAND_SPEC)


_PREPARED_BASE_FIELDS = {
    "schema", "kind", "spec_id", "payload", "task", "command", "routing",
    "output", "cost", "work_id", "prepared_id",
}
_PREPARED_V2_FIELDS = _PREPARED_BASE_FIELDS | {"limits"}
_MAX_EXPLICIT_MEMORY_LIMIT_MIB = (2**63 - 1) // (1024 * 1024)


def _resource_limits(memory_limit_mib: int) -> dict[str, Any]:
    """Freeze an operator-owned hard RSS ceiling without inventing demand."""
    if (type(memory_limit_mib) is not int or memory_limit_mib <= 0
            or memory_limit_mib > _MAX_EXPLICIT_MEMORY_LIMIT_MIB):
        raise PreparationError(
            "memory limit must be a positive whole-MiB value that fits a signed 64-bit byte count"
        )
    return {
        "process_tree_rss_mib": memory_limit_mib,
        "provenance": "submit-explicit",
    }


def prepared_memory_limit_mib(record: Mapping[str, Any]) -> int | None:
    """Return the explicit hard RSS limit; legacy PreparedJobV1 means none."""
    schema = record.get("schema")
    if schema == 1:
        return None
    if schema != 2:
        raise PreparationError("unsupported prepared job schema")
    limits = record.get("limits")
    if not isinstance(limits, Mapping):
        raise PreparationError("prepared resource limits are invalid")
    value = limits.get("process_tree_rss_mib")
    provenance = limits.get("provenance")
    if set(limits) != {"process_tree_rss_mib", "provenance"}:
        raise PreparationError("prepared resource limits have unknown or missing fields")
    if (type(value) is not int or value <= 0 or value > _MAX_EXPLICIT_MEMORY_LIMIT_MIB
            or provenance != "submit-explicit"):
        raise PreparationError("prepared explicit memory limit is invalid")
    return value


def _typed_options(definition: Mapping[str, Any], supplied: Mapping[str, Any] | None) -> dict[str, Any]:
    supplied = dict(supplied or {})
    declared = definition["options"]
    unknown = sorted(set(supplied) - set(declared))
    if unknown:
        raise PreparationError(f"unknown task option(s): {', '.join(unknown)}")
    out: dict[str, Any] = {}
    for name, spec in declared.items():
        if name in supplied:
            value = supplied[name]
        elif "default" in spec:
            value = spec["default"]
        elif spec["required"]:
            raise PreparationError(f"missing required task option {name!r}")
        else:
            continue
        kind = spec["type"]
        valid = ((kind == "string" and isinstance(value, str)) or
                 (kind == "integer" and type(value) is int) or
                 (kind == "number" and type(value) in (int, float)) or
                 (kind == "boolean" and type(value) is bool))
        if not valid:
            raise PreparationError(f"option {name!r} must have type {kind}")
        if "values" in spec and value not in spec["values"]:
            raise PreparationError(f"option {name!r} is outside its allowed values")
        out[name] = value
    return out


def parse_option_assignments(definition: Mapping[str, Any], assignments: list[str] | None) -> dict:
    """Parse compact CLI ``name=value`` values through declared scalar types."""
    raw: dict[str, Any] = {}
    for assignment in assignments or []:
        if "=" not in assignment:
            raise PreparationError(f"task option must be name=value: {assignment!r}")
        name, value = assignment.split("=", 1)
        if name in raw:
            raise PreparationError(f"task option {name!r} was supplied more than once")
        spec = definition["options"].get(name)
        if spec is None:
            raise PreparationError(f"unknown task option {name!r}")
        kind = spec["type"]
        try:
            if kind == "integer":
                parsed: Any = int(value)
            elif kind == "number":
                parsed = float(value)
            elif kind == "boolean":
                if value.lower() not in {"true", "false"}:
                    raise ValueError
                parsed = value.lower() == "true"
            else:
                parsed = value
        except ValueError as exc:
            raise PreparationError(f"task option {name!r} must have type {kind}") from exc
        raw[name] = parsed
    return _typed_options(definition, raw)


def _expand_files(raw: list[str], extensions: list[str]) -> list[Path]:
    allow_all = extensions == ["*"]
    allowed = set(extensions)
    out: list[Path] = []
    for value in raw:
        path = Path(value).expanduser()
        if not path.exists():
            raise PreparationError(f"input does not exist: {value}")
        if path.is_dir():
            for child in sorted(path.iterdir(), key=lambda item: item.name):
                if child.is_file() and (allow_all or child.suffix.lower() in allowed):
                    out.append(child)
            continue
        if not path.is_file():
            raise PreparationError(f"input is not a regular file: {value}")
        if not allow_all and path.suffix.lower() not in allowed:
            raise PreparationError(f"input extension is not allowed: {value}")
        out.append(path)
    if not out:
        raise PreparationError("no input files matched the configured extensions")
    return out


def _stable_open(path: Path):  # noqa: ANN202
    """Open one pathname and return its handle plus stable before metadata."""
    handle = path.open("rb")
    before = os.fstat(handle.fileno())
    if not before.st_ino:
        handle.close()
        raise OSError("source has no stable file identity")
    return handle, before


def _same_open_file(path: Path, before: os.stat_result, handle) -> os.stat_result:  # noqa: ANN001
    after = os.fstat(handle.fileno())
    named = path.stat()
    if ((before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
            or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
            or not named.st_ino
            or (named.st_dev, named.st_ino) != (after.st_dev, after.st_ino)):
        raise OSError("source changed while its identity was measured")
    return after


def _identity(path: Path, mode: str, index: int) -> dict[str, Any]:
    try:
        resolved = path.resolve(strict=True)
        stream, before = _stable_open(resolved)
        with stream:
            if mode == "sha256":
                digest = hashlib.sha256()
                read = 0
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
                    read += len(chunk)
            else:
                stream.read(1)
                read = before.st_size
            stat = _same_open_file(resolved, before, stream)
            if read != stat.st_size:
                raise OSError("source byte count disagrees with metadata")
    except OSError as exc:
        raise PreparationError(f"input is unreadable: {path}: {exc}") from exc
    ident: dict[str, Any] = {"mode": mode, "bytes": stat.st_size}
    if mode == "sha256":
        ident["sha256"] = "sha256:" + digest.hexdigest()
    else:
        ident["mtime_ns"] = stat.st_mtime_ns
    return {"index": index, "source_path": str(resolved), "identity": ident}


def snapshot_prepared_input(item: Mapping[str, Any]) -> Path:
    """Copy only bytes that match a frozen file identity into a private snapshot."""
    source = Path(item["source_path"])
    expected = item["identity"]
    snapshot: Path | None = None
    try:
        stream, before = _stable_open(source)
        with stream:
            with tempfile.NamedTemporaryFile("wb", delete=False) as target:
                snapshot = Path(target.name)
                digest = hashlib.sha256() if expected["mode"] == "sha256" else None
                copied = 0
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    target.write(chunk)
                    copied += len(chunk)
                    if digest is not None:
                        digest.update(chunk)
            after = _same_open_file(source, before, stream)
        matches = copied == after.st_size == expected["bytes"]
        if expected["mode"] == "metadata":
            matches = matches and after.st_mtime_ns == expected["mtime_ns"]
        else:
            matches = matches and "sha256:" + digest.hexdigest() == expected["sha256"]
        if not matches:
            raise SourceChangedError(f"source_changed: {source}")
        return snapshot
    except (OSError, SourceChangedError) as exc:
        if snapshot is not None:
            snapshot.unlink(missing_ok=True)
        if isinstance(exc, SourceChangedError):
            raise
        raise SourceChangedError(f"source_changed: {source}: {exc}") from exc


def _payload(definition: Mapping[str, Any], *, text: str | None,
             inputs: list[str] | None) -> dict[str, Any]:
    mode = definition["input"]["mode"]
    raw_inputs = list(inputs or [])
    has_text = text is not None
    has_files = bool(raw_inputs)
    if has_text and has_files:
        raise PreparationError("a submission may contain text or files, not both")
    if mode == "none":
        if has_text or has_files:
            raise PreparationError("this task accepts no payload")
        return {"mode": "none", "text": None, "items": []}
    if has_text:
        if mode not in {"text", "text-or-files"}:
            raise PreparationError("this task does not accept text")
        return {"mode": "text", "text": text, "items": []}
    if has_files:
        if mode not in {"files", "text-or-files"}:
            raise PreparationError("this task does not accept files")
        paths = _expand_files(raw_inputs, definition["input"]["extensions"])
        identity_mode = definition["input"]["file_identity"]
        return {"mode": "files", "text": None,
                "items": [_identity(path, identity_mode, index)
                          for index, path in enumerate(paths)]}
    raise PreparationError("this task requires a payload")


def _cost(definition: Mapping[str, Any], payload: Mapping[str, Any],
          options: Mapping[str, Any]) -> dict[str, Any]:
    contract = definition["cost"]
    measure = contract["measure"]
    bucket = {name: options[name] for name in contract["bucket_options"] if name in options}
    if measure == "none":
        return {"status": "unestimated", "unit": None, "value": None,
                "relative_uncertainty": None, "provenance": "none", "bucket_id": None}
    if measure == "input-bytes":
        raw = sum(item["identity"]["bytes"] for item in payload["items"])
    elif measure == "text-codepoints":
        raw = len(payload["text"] or "")
    else:
        raw = len(payload["items"])
    return {"status": "exact", "unit": contract["unit"],
            "value": raw / contract["divisor"],
            "relative_uncertainty": 0.0, "provenance": measure,
            "bucket_id": sha256_id(bucket)}


def _source_stem(path: str, index: int) -> str:
    stem = Path(path).stem
    safe = "".join(char if char.isalnum() or char in "._-" else "_" for char in stem)
    return f"{(safe or 'item')[:80]}-{index:04d}"


def prepare_task_job(spec: Mapping[str, Any], *, repo_root: Path, text: str | None = None,
                     inputs: list[str] | None = None, options: Mapping[str, Any] | None = None,
                     caller_requirements: list[str] | tuple[str, ...] = (),
                     force_device: str | None = None, allow_fallback: bool = False,
                     engine: str | None = None, output_root: str | None = None,
                     memory_limit_mib: int | None = None) -> dict[str, Any]:
    """Create one canonical prepared job using an already resolved spec.

    Jobs without an explicit limit retain the exact PreparedJobV1 shape. A
    submit-explicit hard process-tree RSS limit selects PreparedJobV2.
    """
    verify_id(spec.get("spec_id"), "spec_id")
    definition = spec["definition"]
    payload = _payload(definition, text=text, inputs=inputs)
    normalized_options = _typed_options(definition, options)
    if output_root == "":
        raise PreparationError("output-root override must not be empty")
    if output_root is not None and not definition["output"]["allow_root_override"]:
        raise PreparationError("this task forbids output-root overrides")
    configured = list(definition["routing"]["requirements"])
    for option_name, value_map in definition["routing"]["requirements_by_option"].items():
        try:
            configured.extend(value_map[normalized_options[option_name]])
        except KeyError as exc:
            raise PreparationError(
                f"routing option {option_name!r} has no resolved value") from exc
    caller = list(normalize_capabilities(caller_requirements, "caller requirements"))
    requirements = list(normalize_capabilities(
        [*configured, *caller], "prepared requirements"))
    cost = _cost(definition, payload, normalized_options)
    semantic = {
        "spec_id": spec["spec_id"], "payload": payload,
        "options": normalized_options, "requirements": requirements,
        "output_root": output_root,
    }
    work_id = sha256_id(semantic)
    reservations: list[dict[str, Any]] = []
    reservation = definition["output"]["reservation"]
    if reservation != "none":
        for item in payload["items"]:
            stem = _source_stem(item["source_path"], item["index"])
            # Both reservation policies require a globally collision-resistant
            # namespace. The readable source stem is presentation only; the full
            # semantic-work digest is the authority and remains stable on retry.
            stem = f"{stem}-{work_id.removeprefix('sha256:')}"
            reservations.append({"item_index": item["index"], "stem": stem})
    record: dict[str, Any] = {
        "schema": 1, "kind": "task", "spec_id": spec["spec_id"],
        "payload": payload,
        "task": {"name": spec["task_name"], "options": normalized_options},
        "command": None,
        "routing": {"requirements": requirements, "force_device": force_device,
                    "allow_fallback": bool(allow_fallback),
                    "engine": engine},
        "output": {"root_override": output_root, "reservations": reservations},
        "cost": cost, "work_id": work_id,
    }
    if memory_limit_mib is not None:
        record["schema"] = 2
        record["limits"] = _resource_limits(memory_limit_mib)
    record["prepared_id"] = sha256_id(record)
    return record


def prepare_task_jobs(spec: Mapping[str, Any], **kwargs: Any) -> list[dict[str, Any]]:
    """Split configured per-item submissions before preparation and enqueue."""
    definition = spec["definition"]
    inputs = list(kwargs.get("inputs") or [])
    if definition["input"]["split"] != "per-item" or not inputs:
        return [prepare_task_job(spec, **kwargs)]
    expanded = _expand_files(inputs, definition["input"]["extensions"])
    if len(expanded) <= 1:
        return [prepare_task_job(spec, **kwargs)]
    jobs = []
    for item in expanded:
        item_kwargs = dict(kwargs)
        item_kwargs["inputs"] = [str(item)]
        jobs.append(prepare_task_job(spec, **item_kwargs))
    return jobs


def prepare_raw_command(argv: list[str], *, device: str, inputs: list[str] | None = None,
                        allow_fallback: bool = False,
                        memory_limit_mib: int | None = None) -> dict[str, Any]:
    if not device:
        raise PreparationError("queued raw commands require an explicit device")
    if not isinstance(argv, list) or not argv or any(
            not isinstance(value, str) or "\x00" in value for value in argv):
        raise PreparationError("raw command argv must be a non-empty exact string list")
    raw_inputs = list(inputs or [])
    items = ([_identity(path, "sha256", index)
              for index, path in enumerate(_expand_files(raw_inputs, ["*"]))]
             if raw_inputs else [])
    payload = {"mode": "files" if items else "none", "text": None, "items": items}
    semantic = {"spec_id": RAW_COMMAND_SPEC_ID, "payload": payload, "argv": argv,
                "device": device, "allow_fallback": bool(allow_fallback)}
    work_id = sha256_id(semantic)
    record: dict[str, Any] = {
        "schema": 1, "kind": "command", "spec_id": RAW_COMMAND_SPEC_ID,
        "payload": payload, "task": None, "command": {"argv": list(argv)},
        "routing": {"requirements": [], "force_device": device,
                    "allow_fallback": bool(allow_fallback),
                    "engine": None},
        "output": {"root_override": None, "reservations": []},
        "cost": {"status": "unestimated", "unit": None, "value": None,
                 "relative_uncertainty": None, "provenance": "none", "bucket_id": None},
        "work_id": work_id,
    }
    if memory_limit_mib is not None:
        record["schema"] = 2
        record["limits"] = _resource_limits(memory_limit_mib)
    record["prepared_id"] = sha256_id(record)
    return record


def validate_prepared_job(record: Mapping[str, Any]) -> None:
    """Integrity check used when reading a prepared row from durable storage."""
    if not isinstance(record, Mapping):
        raise PreparationError("prepared job must be an object")
    schema = record.get("schema")
    fields = _PREPARED_BASE_FIELDS if schema == 1 else _PREPARED_V2_FIELDS if schema == 2 else None
    if fields is None or set(record) != fields:
        raise PreparationError("prepared job has unknown or missing fields")
    if record.get("kind") not in {"task", "command"}:
        raise PreparationError("unsupported prepared job schema or kind")
    expected = sha256_id({key: value for key, value in record.items() if key != "prepared_id"})
    if record.get("prepared_id") != expected:
        raise PreparationError("prepared_id does not match canonical prepared job bytes")
    verify_id(record.get("spec_id"), "prepared spec_id")
    prepared_memory_limit_mib(record)
    if (record["kind"] == "task") == (record.get("task") is None):
        raise PreparationError("prepared job task/command union is inconsistent")
    if (record["kind"] == "command") == (record.get("command") is None):
        raise PreparationError("prepared job task/command union is inconsistent")
    payload = record.get("payload")
    if not isinstance(payload, Mapping) or set(payload) != {"mode", "text", "items"}:
        raise PreparationError("prepared payload has unknown or missing fields")
    if payload["mode"] not in {"none", "text", "files"} or not isinstance(payload["items"], list):
        raise PreparationError("prepared payload mode or items are invalid")
    if ((payload["mode"] == "text") != isinstance(payload["text"], str)
            or (payload["mode"] != "text" and payload["text"] is not None)
            or (payload["mode"] == "files") != bool(payload["items"])):
        raise PreparationError("prepared payload union is inconsistent")
    for index, item in enumerate(payload["items"]):
        if not isinstance(item, Mapping) or set(item) != {"index", "source_path", "identity"}:
            raise PreparationError("prepared payload item has unknown or missing fields")
        if item["index"] != index or not Path(item["source_path"]).is_absolute():
            raise PreparationError("prepared payload item index or source path is invalid")
        identity = item["identity"]
        if not isinstance(identity, Mapping) or identity.get("mode") not in {"metadata", "sha256"}:
            raise PreparationError("prepared file identity mode is invalid")
        identity_fields = ({"mode", "bytes", "mtime_ns"} if identity["mode"] == "metadata"
                           else {"mode", "bytes", "sha256"})
        if set(identity) != identity_fields or type(identity["bytes"]) is not int \
                or identity["bytes"] < 0:
            raise PreparationError("prepared file identity fields are invalid")
        if identity["mode"] == "metadata":
            if type(identity["mtime_ns"]) is not int or identity["mtime_ns"] < 0:
                raise PreparationError("prepared metadata identity is invalid")
        else:
            verify_id(identity["sha256"], "prepared source sha256")
    routing = record.get("routing")
    if not isinstance(routing, Mapping) or set(routing) != {
            "requirements", "force_device", "allow_fallback", "engine"}:
        raise PreparationError("prepared routing has unknown or missing fields")
    if type(routing["allow_fallback"]) is not bool:
        raise PreparationError("prepared routing fields are invalid")
    try:
        requirements = list(normalize_capabilities(routing["requirements"],
                                                   "prepared requirements"))
    except ValueError as exc:
        raise PreparationError(str(exc)) from exc
    if requirements != routing["requirements"]:
        raise PreparationError("prepared requirements are not canonical")
    for field in ("force_device", "engine"):
        if routing[field] is not None and (not isinstance(routing[field], str)
                                           or not routing[field]):
            raise PreparationError(f"prepared routing {field} is invalid")
    output = record.get("output")
    if not isinstance(output, Mapping) or set(output) != {"root_override", "reservations"} \
            or not isinstance(output["reservations"], list):
        raise PreparationError("prepared output has unknown or missing fields")
    if output["root_override"] is not None and not isinstance(output["root_override"], str):
        raise PreparationError("prepared output root override is invalid")
    seen_item_indexes: set[int] = set()
    for reservation in output["reservations"]:
        if not isinstance(reservation, Mapping) or set(reservation) != {"item_index", "stem"}:
            raise PreparationError("prepared output reservation is invalid")
        item_index = reservation["item_index"]
        if type(item_index) is not int or item_index < 0 or item_index in seen_item_indexes \
                or item_index >= len(payload["items"]):
            raise PreparationError("prepared output reservation index is invalid")
        stem = reservation["stem"]
        if not isinstance(stem, str) or not stem or Path(stem).name != stem:
            raise PreparationError("prepared output reservation stem is invalid")
        seen_item_indexes.add(item_index)
    cost = record.get("cost")
    if not isinstance(cost, Mapping) or set(cost) != {
            "status", "unit", "value", "relative_uncertainty", "provenance", "bucket_id"}:
        raise PreparationError("prepared cost has unknown or missing fields")
    if cost["status"] not in {"exact", "approximate", "unestimated"}:
        raise PreparationError("prepared cost status is invalid")
    if cost["status"] == "unestimated":
        if any(cost[field] is not None for field in
               ("unit", "value", "relative_uncertainty", "bucket_id")) \
                or cost["provenance"] != "none":
            raise PreparationError("unestimated prepared cost has invented values")
    else:
        if not isinstance(cost["unit"], str) or not cost["unit"]:
            raise PreparationError("prepared cost unit is invalid")
        if type(cost["value"]) not in (int, float) or not math.isfinite(float(cost["value"])) \
                or cost["value"] < 0:
            raise PreparationError("prepared cost value is invalid")
        uncertainty = cost["relative_uncertainty"]
        if type(uncertainty) not in (int, float) or not math.isfinite(float(uncertainty)) \
                or not 0 <= uncertainty <= 1:
            raise PreparationError("prepared cost uncertainty is invalid")
        verify_id(cost["bucket_id"], "prepared cost bucket_id")
    verify_id(record.get("work_id"), "prepared work_id")
    if record["kind"] == "command":
        command = record["command"]
        if not isinstance(command, Mapping) or set(command) != {"argv"} \
                or not isinstance(command["argv"], list) or not command["argv"] \
                or any(not isinstance(value, str) or "\x00" in value for value in command["argv"]):
            raise PreparationError("prepared raw command argv is invalid")
        if record["spec_id"] != RAW_COMMAND_SPEC_ID or record["task"] is not None:
            raise PreparationError("prepared raw command spec identity is invalid")
        semantic = {"spec_id": RAW_COMMAND_SPEC_ID, "payload": payload,
                    "argv": command["argv"], "device": routing["force_device"],
                    "allow_fallback": routing["allow_fallback"]}
    else:
        task = record["task"]
        if not isinstance(task, Mapping) or set(task) != {"name", "options"} \
                or not isinstance(task["name"], str) or not isinstance(task["options"], dict):
            raise PreparationError("prepared configured task fields are invalid")
        semantic = {"spec_id": record["spec_id"], "payload": payload,
                    "options": task["options"], "requirements": routing["requirements"],
                    "output_root": output["root_override"]}
    if record["work_id"] != sha256_id(semantic):
        raise PreparationError("work_id does not match prepared semantic work")
    canonical_json(record)


def validate_prepared_against_spec(record: Mapping[str, Any], spec: Mapping[str, Any]) -> None:
    """Prove a self-consistent prepared record obeys its frozen resolved contract."""
    from .task_contract import TaskContractError, validate_resolved_task_spec

    validate_prepared_job(record)
    try:
        validate_resolved_task_spec(dict(spec))
    except TaskContractError as exc:
        raise PreparationError(f"resolved spec is invalid: {exc}") from exc
    if record["kind"] != "task" or record["spec_id"] != spec["spec_id"] \
            or record["task"]["name"] != spec["task_name"]:
        raise PreparationError("prepared task and resolved spec disagree")
    definition = spec["definition"]
    task = record["task"]
    if _typed_options(definition, task["options"]) != task["options"]:
        raise PreparationError("prepared options disagree with frozen defaults or declarations")
    configured = set(definition["routing"]["requirements"])
    for option_name, value_map in definition["routing"]["requirements_by_option"].items():
        configured.update(value_map[task["options"][option_name]])
    if not configured.issubset(record["routing"]["requirements"]):
        raise PreparationError("prepared task omits configuration-owned routing requirements")
    engine = record["routing"]["engine"]
    if engine is not None and engine not in {
            adapter["engine"] for adapter in spec["adapters"].values()}:
        raise PreparationError("prepared engine is not provided by the frozen adapters")
    forced = record["routing"]["force_device"]
    if forced is not None:
        forced_adapter = spec["adapters"].get(forced)
        if forced_adapter is None:
            raise PreparationError("prepared force_device is not provided by the frozen adapters")
        if engine is not None and forced_adapter["engine"] != engine:
            raise PreparationError(
                "prepared force_device does not provide the requested engine"
            )
    root = record["output"]["root_override"]
    if root is not None and (not root or not definition["output"]["allow_root_override"]):
        raise PreparationError("prepared output-root override violates the frozen contract")
    policy = definition["output"]["reservation"]
    reservations = record["output"]["reservations"]
    if (policy == "none") != (not reservations):
        raise PreparationError("prepared reservations disagree with the frozen output policy")
    if reservations:
        items = record["payload"]["items"]
        if len(reservations) != len(items):
            raise PreparationError("prepared reservations do not cover every input item")
        suffix = record["work_id"].removeprefix("sha256:")
        for reservation, item in zip(reservations, items):
            expected_prefix = _source_stem(item["source_path"], item["index"]) + "-"
            if reservation["item_index"] != item["index"] \
                    or reservation["stem"] != expected_prefix + suffix:
                raise PreparationError("prepared reservation is not the frozen collision-safe name")
    contract = definition["cost"]
    cost = record["cost"]
    measure = contract["measure"]
    if measure == "none":
        if cost["status"] != "unestimated":
            raise PreparationError("prepared cost invents an estimate")
    else:
        if cost["unit"] != contract["unit"]:
            raise PreparationError("prepared cost unit disagrees with the frozen contract")
        bucket = {name: task["options"][name] for name in contract["bucket_options"]
                  if name in task["options"]}
        if cost["bucket_id"] != sha256_id(bucket):
            raise PreparationError("prepared cost bucket disagrees with frozen options")
        if cost["provenance"] != measure:
            raise PreparationError("prepared cost provenance disagrees with frozen authority")
        expected = _cost(definition, record["payload"], task["options"])
        if cost != expected:
            raise PreparationError("prepared core-measured cost disagrees with frozen payload")


def as_fleet_task(record: Mapping[str, Any], spec: Mapping[str, Any]) -> Any:
    """Project frozen work into the existing pure placement/execution carrier.

    This bridge contains no workflow vocabulary. It is temporary only in name:
    ``FleetTask`` remains a useful generic value object after the legacy parser
    and semantic columns disappear.
    """
    from .models import FleetTask

    validate_prepared_job(record)
    if record["spec_id"] != spec.get("spec_id"):
        raise PreparationError("prepared job and resolved spec disagree")
    if record["kind"] == "task" and record["task"]["name"] != spec.get("task_name"):
        raise PreparationError("prepared task name and resolved spec disagree")
    if record["kind"] == "task":
        validate_prepared_against_spec(record, spec)
    payload = record["payload"]
    inputs = [item["source_path"] for item in payload["items"]]
    if record["kind"] == "command":
        task_name = "__command__"
        options = {"argv": list(record["command"]["argv"])}
    else:
        task_name = record["task"]["name"]
        options = dict(record["task"]["options"])
        options.update({
            "_prepared_id": record["prepared_id"],
            "_work_id": record["work_id"],
            "_reservations": record["output"]["reservations"],
        })
    return FleetTask(
        task_name=task_name,
        text=payload["text"],
        inputs=inputs,
        options=options,
        force_device=record["routing"]["force_device"],
        engine=record["routing"]["engine"],
        output_root=record["output"]["root_override"],
        requires=tuple(record["routing"]["requirements"]),
        prepared=dict(record),
        resolved_spec=dict(spec),
    )


def prepared_features(record: Mapping[str, Any]) -> Any:
    from .models import JobFeatures

    payload = record["payload"]
    cost = record["cost"]
    return JobFeatures(
        input_bytes=sum(item["identity"]["bytes"] for item in payload["items"]),
        file_count=len(payload["items"]),
        text_chars=len(payload["text"] or ""),
        prepared_units=cost["value"],
        units_status=cost["status"],
        relative_uncertainty=cost["relative_uncertainty"],
    )
