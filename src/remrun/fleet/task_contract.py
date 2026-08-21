"""Strict declarative fleet-task contracts and content-addressed resolution.

Configured workflow names live only in ``devices.toml``.  Core owns this small,
closed vocabulary of protocol primitives and rejects absence, unknown fields and
incoherent combinations before a job can be prepared.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
from pathlib import Path
from typing import Any, Iterable, Mapping


class TaskContractError(ValueError):
    """A configured task cannot be interpreted without guessing."""


_NAME_RE = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")
_OPTION_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]*$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_RESERVED_OPTIONS = {"argv", "spec_id", "prepared_id", "work_id"}
_PLACEHOLDERS = {"{stage}", "{manifest}", "{output_root}"}
_MAX_MEASURE_TIMEOUT_S = 300.0


def canonical_json(value: Any) -> str:
    """Canonical JSON used by every durable identity in the fleet protocol."""
    _reject_unrepresentable(value, "document")
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False)


def sha256_id(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def verify_id(value: str, field: str = "digest") -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise TaskContractError(f"{field} must be sha256:<64 lowercase hex>")
    return value


def _reject_unrepresentable(value: Any, field: str) -> None:
    if isinstance(value, str):
        if "\x00" in value:
            raise TaskContractError(f"{field} contains NUL")
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise TaskContractError(f"{field} contains invalid Unicode") from exc
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise TaskContractError(f"{field} must be finite")
    elif isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TaskContractError(f"{field} keys must be strings")
            _reject_unrepresentable(key, f"{field} key")
            _reject_unrepresentable(item, f"{field}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_unrepresentable(item, f"{field}[{index}]")
    elif value is not None and not isinstance(value, (bool, int)):
        raise TaskContractError(f"{field} has unsupported value type {type(value).__name__}")


def _table(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TaskContractError(f"{field} must be a table")
    return dict(value)


def _closed(table: Mapping[str, Any], allowed: Iterable[str], field: str) -> None:
    unknown = sorted(set(table) - set(allowed))
    if unknown:
        raise TaskContractError(f"{field} contains unknown field(s): {', '.join(unknown)}")


def _required(table: Mapping[str, Any], names: Iterable[str], field: str) -> None:
    missing = sorted(set(names) - set(table))
    if missing:
        raise TaskContractError(f"{field} missing required field(s): {', '.join(missing)}")


def _enum(value: Any, choices: set[str], field: str) -> str:
    if not isinstance(value, str) or value not in choices:
        raise TaskContractError(f"{field} must be one of {sorted(choices)}")
    return value


def _bool(value: Any, field: str) -> bool:
    if type(value) is not bool:
        raise TaskContractError(f"{field} must be boolean")
    return value


def _number(value: Any, field: str, *, minimum: float | None = None,
            maximum: float | None = None) -> float:
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        raise TaskContractError(f"{field} must be a finite number")
    result = float(value)
    if minimum is not None and result < minimum:
        raise TaskContractError(f"{field} must be >= {minimum}")
    if maximum is not None and result > maximum:
        raise TaskContractError(f"{field} must be <= {maximum}")
    return result


def _token(value: Any, field: str) -> str:
    if not isinstance(value, str) or _TOKEN_RE.fullmatch(value) is None:
        raise TaskContractError(f"{field} must be a non-empty protocol token")
    return value


def _tokens(value: Any, field: str, *, nonempty: bool = False) -> list[str]:
    if not isinstance(value, list):
        raise TaskContractError(f"{field} must be a list")
    out = sorted({_token(item, f"{field} item") for item in value})
    if nonempty and not out:
        raise TaskContractError(f"{field} must not be empty")
    return out


def _argv(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise TaskContractError(f"{field} must be a non-empty argv array")
    if any(not isinstance(token, str) or "\x00" in token for token in value):
        raise TaskContractError(f"{field} must contain only strings without NUL")
    if not value[0]:
        raise TaskContractError(f"{field}[0] must name an executable")
    return list(value)


def _validate_input(raw: Any) -> dict[str, Any]:
    table = _table(raw, "input")
    _closed(table, {"mode", "extensions", "split", "file_identity"}, "input")
    _required(table, {"mode", "split"}, "input")
    mode = _enum(table["mode"], {"none", "text", "files", "text-or-files"}, "input.mode")
    split = _enum(table["split"], {"never", "per-item"}, "input.split")
    file_capable = mode in {"files", "text-or-files"}
    if split == "per-item" and not file_capable:
        raise TaskContractError("input.split=per-item requires file-capable input")
    out: dict[str, Any] = {"mode": mode, "split": split}
    if file_capable:
        _required(table, {"extensions", "file_identity"}, "input")
        exts = table["extensions"]
        if not isinstance(exts, list) or not exts:
            raise TaskContractError("input.extensions must be a non-empty list")
        normalized: set[str] = set()
        for ext in exts:
            if not isinstance(ext, str) or (ext != "*" and
                    (not ext.startswith(".") or ext != ext.lower() or len(ext) < 2)):
                raise TaskContractError(
                    "input.extensions entries must be '*' or lowercase suffixes beginning with '.'")
            normalized.add(ext)
        out["extensions"] = sorted(normalized)
        out["file_identity"] = _enum(
            table["file_identity"], {"metadata", "sha256"}, "input.file_identity")
    elif "extensions" in table or "file_identity" in table:
        raise TaskContractError("input extensions/file_identity are forbidden for non-file input")
    return out


def _validate_prepare(raw: Any) -> dict[str, Any]:
    table = _table(raw, "prepare")
    _closed(table, {"mode"}, "prepare")
    _required(table, {"mode"}, "prepare")
    return {"mode": _enum(table["mode"], {"none"}, "prepare.mode")}


def _validate_routing(raw: Any, options: Mapping[str, Any]) -> dict[str, Any]:
    table = _table(raw, "routing")
    allowed = {"requirements", "requirements_by_option"}
    _closed(table, allowed, "routing")
    _required(table, allowed, "routing")
    fixed = _tokens(table["requirements"], "routing.requirements")
    mappings = _table(table["requirements_by_option"], "routing.requirements_by_option")
    out: dict[str, dict[str, list[str]]] = {}
    for option_name, raw_map in mappings.items():
        option = options.get(option_name)
        if option is None:
            raise TaskContractError(
                f"routing requirements name undeclared option {option_name!r}")
        if option["type"] != "string" or "values" not in option:
            raise TaskContractError(
                f"routing option {option_name!r} must be a string with closed values")
        if not option["required"] and "default" not in option:
            raise TaskContractError(
                f"routing option {option_name!r} must be required or have a default")
        value_map = _table(raw_map, f"routing.requirements_by_option.{option_name}")
        permitted = set(option["values"])
        if set(value_map) != permitted:
            missing = sorted(permitted - set(value_map))
            extra = sorted(set(value_map) - permitted)
            detail = []
            if missing:
                detail.append("missing " + ", ".join(missing))
            if extra:
                detail.append("unknown " + ", ".join(extra))
            raise TaskContractError(
                f"routing option {option_name!r} mapping is not exhaustive: {'; '.join(detail)}")
        out[option_name] = {
            value: _tokens(value_map[value],
                           f"routing.requirements_by_option.{option_name}.{value}")
            for value in sorted(value_map)
        }
    return {"requirements": fixed, "requirements_by_option": out}


def _validate_options(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    table = _table(raw, "options")
    out: dict[str, Any] = {}
    for name, value in table.items():
        if _OPTION_RE.fullmatch(name) is None or name.startswith("_") or name in _RESERVED_OPTIONS:
            raise TaskContractError(f"invalid or reserved option name {name!r}")
        spec = _table(value, f"options.{name}")
        _closed(spec, {"type", "required", "default", "values"}, f"options.{name}")
        _required(spec, {"type", "required"}, f"options.{name}")
        kind = _enum(spec["type"], {"string", "integer", "number", "boolean"},
                     f"options.{name}.type")
        required = _bool(spec["required"], f"options.{name}.required")

        def typed(item: Any, field: str) -> Any:
            valid = ((kind == "string" and isinstance(item, str)) or
                     (kind == "integer" and type(item) is int) or
                     (kind == "number" and type(item) in (int, float) and
                      math.isfinite(float(item))) or
                     (kind == "boolean" and type(item) is bool))
            if not valid:
                raise TaskContractError(f"{field} must match declared type {kind}")
            return item

        row: dict[str, Any] = {"type": kind, "required": required}
        if "default" in spec:
            if required:
                raise TaskContractError(f"options.{name}.default forbidden when required=true")
            row["default"] = typed(spec["default"], f"options.{name}.default")
        if "values" in spec:
            if not isinstance(spec["values"], list) or not spec["values"]:
                raise TaskContractError(f"options.{name}.values must be a non-empty list")
            row["values"] = [typed(item, f"options.{name}.values") for item in spec["values"]]
            if "default" in row and row["default"] not in row["values"]:
                raise TaskContractError(f"options.{name}.default is outside its values")
        out[name] = row
    return out


def _validate_cost(raw: Any, input_spec: Mapping[str, Any],
                   options: Mapping[str, Any]) -> dict[str, Any]:
    table = _table(raw, "cost")
    allowed = {
        "measure", "unit", "divisor", "bucket_options", "verify_relative_tolerance",
        "command",
    }
    _closed(table, allowed, "cost")
    _required(table, {"measure", "bucket_options"}, "cost")
    measure = _enum(table["measure"],
                    {"none", "input-bytes", "text-codepoints", "item-count",
                     "external-scalar-v1"},
                    "cost.measure")
    bucket = table["bucket_options"]
    if not isinstance(bucket, list) or any(not isinstance(name, str) for name in bucket):
        raise TaskContractError("cost.bucket_options must be an option-name list")
    bucket = sorted(set(bucket))
    unknown = sorted(set(bucket) - set(options))
    if unknown:
        raise TaskContractError(f"cost.bucket_options names undeclared option(s): {', '.join(unknown)}")
    mode = input_spec["mode"]
    if mode == "text-or-files" and measure != "none":
        raise TaskContractError(
            "text-or-files requires cost.measure=none; declare separate tasks for exact "
            "text and file cost units"
        )
    if measure == "input-bytes" and mode not in {"files", "text-or-files"}:
        raise TaskContractError("input-bytes requires file-capable input")
    if measure == "text-codepoints" and mode not in {"text", "text-or-files"}:
        raise TaskContractError("text-codepoints requires text-capable input")
    if measure == "item-count" and mode not in {"files", "text-or-files"}:
        raise TaskContractError("item-count requires file-capable input")
    if measure == "external-scalar-v1" and mode != "files":
        raise TaskContractError("external-scalar-v1 requires input.mode=files")
    out: dict[str, Any] = {"measure": measure, "bucket_options": bucket}
    if measure == "none":
        if set(table) & {"unit", "divisor", "verify_relative_tolerance", "command"}:
            raise TaskContractError("cost unit/divisor/verification/command forbidden when measure=none")
    elif measure == "external-scalar-v1":
        _required(table, {"unit", "command"}, "cost")
        if "divisor" in table:
            raise TaskContractError("cost.divisor is forbidden for external-scalar-v1")
        out["unit"] = _token(table["unit"], "cost.unit")
        out["verify_relative_tolerance"] = _number(
            table.get("verify_relative_tolerance", 0.0),
            "cost.verify_relative_tolerance", minimum=0.0, maximum=0.01,
        )
        command = _table(table["command"], "cost.command")
        _closed(command, {"argv", "timeout_s", "identity_paths"}, "cost.command")
        _required(command, {"argv", "timeout_s", "identity_paths"}, "cost.command")
        argv = _argv(command["argv"], "cost.command.argv")
        executable = Path(argv[0]).expanduser()
        if not executable.is_absolute():
            raise TaskContractError("cost.command.argv[0] must be an absolute executable")
        executable = executable.resolve()
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise TaskContractError("cost.command.argv[0] must resolve to an executable file")
        if argv.count("{request}") != 1:
            raise TaskContractError(
                "cost.command.argv requires exactly one whole-token {request} placeholder"
            )
        shell_chars = set("|;&<>`\n\r")
        for token in argv:
            if (token != "{request}" and ("{" in token or "}" in token)):
                raise TaskContractError(
                    "cost.command.argv placeholders must occupy one whole token"
                )
            if any(char in token for char in shell_chars):
                raise TaskContractError("cost.command.argv must not contain shell syntax")
        raw_paths = command["identity_paths"]
        if not isinstance(raw_paths, list) or not raw_paths:
            raise TaskContractError("cost.command.identity_paths must be a non-empty path list")
        identity_paths: list[str] = []
        for index, raw_path in enumerate(raw_paths):
            if not isinstance(raw_path, str) or not raw_path or "\x00" in raw_path:
                raise TaskContractError(
                    f"cost.command.identity_paths[{index}] must be a path string"
                )
            path = Path(raw_path).expanduser()
            if not path.is_absolute() or not path.is_file():
                raise TaskContractError(
                    f"cost.command.identity_paths[{index}] must resolve to an existing file"
                )
            identity_paths.append(str(path.resolve()))
        if len(set(identity_paths)) != len(identity_paths):
            raise TaskContractError("cost.command.identity_paths contains duplicates")
        if str(executable) not in identity_paths:
            raise TaskContractError(
                "cost.command.identity_paths must include the resolved executable"
            )
        out["command"] = {
            "argv": [str(executable), *argv[1:]],
            "timeout_s": _number(
                command["timeout_s"], "cost.command.timeout_s",
                minimum=0.001, maximum=_MAX_MEASURE_TIMEOUT_S,
            ),
            "identity_paths": identity_paths,
        }
    else:
        _required(table, {"unit", "divisor"}, "cost")
        if "command" in table:
            raise TaskContractError("cost.command requires measure=external-scalar-v1")
        out["unit"] = _token(table["unit"], "cost.unit")
        out["divisor"] = _number(table["divisor"], "cost.divisor", minimum=1e-300)
        out["verify_relative_tolerance"] = _number(
            table.get("verify_relative_tolerance", 0.0),
            "cost.verify_relative_tolerance", minimum=0.0, maximum=0.01,
        )
    return out


def _validate_output(raw: Any, input_spec: Mapping[str, Any]) -> dict[str, Any]:
    table = _table(raw, "output")
    allowed = {"reservation", "allow_root_override", "verification", "missing_mapping",
               "no_change"}
    _closed(table, allowed, "output")
    _required(table, {"reservation", "allow_root_override", "verification"}, "output")
    reservation = _enum(table["reservation"],
                        {"none", "source-stem-v1", "content-work-stem-v1"},
                        "output.reservation")
    verification = _enum(table["verification"], {"none", "mapped-tree-change-v1"},
                         "output.verification")
    out = {"reservation": reservation,
           "allow_root_override": _bool(table["allow_root_override"],
                                        "output.allow_root_override"),
           "verification": verification}
    if reservation != "none" and input_spec["mode"] not in {"files", "text-or-files"}:
        raise TaskContractError("output reservation requires file-capable input")
    if reservation == "content-work-stem-v1" and input_spec.get("file_identity") != "sha256":
        raise TaskContractError("content-work-stem-v1 requires sha256 file identity")
    if verification == "mapped-tree-change-v1":
        _required(table, {"missing_mapping", "no_change"}, "output")
        out["missing_mapping"] = _enum(table["missing_mapping"], {"skip", "final"},
                                               "output.missing_mapping")
        out["no_change"] = _enum(table["no_change"], {"allow", "final"},
                                         "output.no_change")
    elif "missing_mapping" in table or "no_change" in table:
        raise TaskContractError("output mapping policies require mapped verification")
    return out


def _validate_completion(raw: Any, batching: str, output: Mapping[str, Any]) -> dict[str, Any]:
    table = _table(raw, "completion")
    allowed = {"protocol", "evidence", "companion", "allowed_publication",
               "unstructured_memory"}
    _closed(table, allowed, "completion")
    _required(table, allowed, "completion")
    protocol = _enum(table["protocol"], {"exit-code-v1", "item-result-v2"},
                     "completion.protocol")
    evidence = _enum(table["evidence"], {"never", "multi-item", "always"},
                     "completion.evidence")
    companion = _enum(table["companion"], {"forbidden", "optional", "required"},
                      "completion.companion")
    publication = _tokens(table["allowed_publication"],
                          "completion.allowed_publication", nonempty=True)
    if not set(publication) <= {"none", "produced", "reused"}:
        raise TaskContractError("completion.allowed_publication has unknown value")
    memory = _enum(table["unstructured_memory"], {"ignore", "patterns-v1"},
                   "completion.unstructured_memory")
    reservation = output["reservation"]
    if protocol == "exit-code-v1" and not (
        batching == "never" and evidence == "never" and companion == "forbidden" and
        reservation == "none" and publication == ["none"]
    ):
        raise TaskContractError("exit-code-v1 requires the closed raw-result-safe combination")
    if batching == "compatible" and (protocol != "item-result-v2" or
                                      evidence not in {"multi-item", "always"}):
        raise TaskContractError("compatible batching requires item-result-v2 with evidence")
    if reservation == "none" and set(publication) - {"none"}:
        raise TaskContractError("output publication requires a reservation")
    if reservation != "none" and protocol != "item-result-v2":
        raise TaskContractError("output reservation requires item-result-v2")
    if companion == "required" and (reservation == "none" or
                                     not set(publication) & {"produced", "reused"}):
        raise TaskContractError("required companion needs output-bearing publication")
    return {"protocol": protocol, "evidence": evidence, "companion": companion,
            "allowed_publication": publication, "unstructured_memory": memory}


def _validate_adapters(raw: Any, devices: set[str], definition: Mapping[str, Any]) -> dict[str, Any]:
    table = _table(raw, "adapters")
    if not table:
        raise TaskContractError("at least one adapter is required")
    out: dict[str, Any] = {}
    for device, value in table.items():
        if device not in devices:
            raise TaskContractError(f"adapter device {device!r} is not in the device registry")
        spec = _table(value, f"adapters.{device}")
        allowed = {"engine", "argv", "output_root", "pool", "memory_kind",
                   "capability_paths", "provides"}
        _closed(spec, allowed, f"adapters.{device}")
        _required(spec, allowed - {"output_root"}, f"adapters.{device}")
        argv = _argv(spec["argv"], f"adapters.{device}.argv")
        output_root = spec.get("output_root")
        if output_root is not None and (not isinstance(output_root, str) or not output_root or
                                        "\x00" in output_root):
            raise TaskContractError(f"adapters.{device}.output_root must be a target path")
        needs_root = (definition["output"]["reservation"] != "none" or
                      definition["output"]["verification"] != "none" or
                      "{output_root}" in argv)
        if needs_root and output_root is None:
            raise TaskContractError(f"adapters.{device}.output_root is required")
        pool = spec["pool"]
        if pool is not False:
            pool = _token(pool, f"adapters.{device}.pool")
        paths = spec["capability_paths"]
        if not isinstance(paths, list) or any(not isinstance(path, str) or not path or "\x00" in path
                                              for path in paths):
            raise TaskContractError(f"adapters.{device}.capability_paths must be a path list")
        for token in argv:
            if "{" not in token and "}" not in token:
                continue
            valid = token in _PLACEHOLDERS or (
                token.startswith("{opt:") and token.endswith("}") and
                token[5:-1] in definition["options"]
            )
            if not valid:
                raise TaskContractError(
                    f"adapters.{device}.argv placeholder must occupy one whole token and be available")
        out[device] = {
            "engine": _token(spec["engine"], f"adapters.{device}.engine"),
            "argv": argv,
            "output_root": output_root,
            "pool": pool,
            "memory_kind": _enum(spec["memory_kind"], {"none", "cpu", "gpu"},
                                 f"adapters.{device}.memory_kind"),
            "capability_paths": list(paths),
            "provides": _tokens(spec["provides"], f"adapters.{device}.provides"),
        }
    return out


def validate_task_definition(name: str, raw: Any, devices: Iterable[str]) -> dict[str, Any]:
    """Validate and canonicalize one exact ``TaskDefinitionV1`` table."""
    if not isinstance(name, str) or _NAME_RE.fullmatch(name) is None:
        raise TaskContractError(f"invalid task name {name!r}")
    table = _table(raw, f"fleet.tasks.{name}")
    allowed = {"input", "prepare", "routing", "execution", "cost", "output",
               "completion", "options", "adapters"}
    _closed(table, allowed, f"fleet.tasks.{name}")
    _required(table, allowed - {"options"}, f"fleet.tasks.{name}")
    input_spec = _validate_input(table["input"])
    prepare = _validate_prepare(table["prepare"])
    execution = _table(table["execution"], "execution")
    _closed(execution, {"batching", "replay"}, "execution")
    _required(execution, {"batching", "replay"}, "execution")
    batching = _enum(execution["batching"], {"never", "compatible"},
                     "execution.batching")
    replay = _enum(execution["replay"], {"at-most-once-v1", "idempotent-v1"},
                   "execution.replay")
    options = _validate_options(table.get("options"))
    routing = _validate_routing(table["routing"], options)
    cost = _validate_cost(table["cost"], input_spec, options)
    output = _validate_output(table["output"], input_spec)
    completion = _validate_completion(table["completion"], batching, output)
    definition: dict[str, Any] = {
        "schema": 1,
        "input": input_spec,
        "prepare": prepare,
        "routing": routing,
        "execution": {"batching": batching, "replay": replay},
        "cost": cost,
        "output": output,
        "completion": completion,
        "options": options,
    }
    definition["adapters"] = _validate_adapters(table["adapters"], set(devices), definition)
    _reject_unrepresentable(definition, f"fleet.tasks.{name}")
    return definition


def resolve_task_spec(name: str, raw: Any, *, devices: Iterable[str], repo_root: Path) -> dict[str, Any]:
    """Return one immutable, configuration-owned ``ResolvedTaskSpecV1`` blob."""
    definition = validate_task_definition(name, raw, devices)
    adapters: dict[str, Any] = {}
    for device, adapter in definition["adapters"].items():
        adapters[device] = {"adapter_id": sha256_id(adapter), **adapter}
    blob: dict[str, Any] = {"schema": 1, "task_name": name, "definition": definition,
                            "adapters": adapters}
    blob["spec_id"] = sha256_id(blob)
    validate_resolved_task_spec(blob)
    return blob


def validate_resolved_task_spec(spec: Any) -> None:
    """Validate the closed resolved shape, not only its self-consistent hash."""
    if not isinstance(spec, dict) or set(spec) != {
            "schema", "task_name", "definition", "adapters", "spec_id"}:
        raise TaskContractError("resolved task spec has unknown or missing fields")
    if spec["schema"] != 1 or not isinstance(spec["task_name"], str) \
            or _NAME_RE.fullmatch(spec["task_name"]) is None:
        raise TaskContractError("resolved task spec schema or name is invalid")
    verify_id(spec["spec_id"], "resolved spec_id")
    if sha256_id({key: value for key, value in spec.items() if key != "spec_id"}) != spec["spec_id"]:
        raise TaskContractError("resolved spec_id does not match canonical spec bytes")
    definition = spec["definition"]
    if not isinstance(definition, dict) or set(definition) != {
            "schema", "input", "prepare", "routing", "execution", "cost", "output",
            "completion", "options", "adapters"} or definition.get("schema") != 1:
        raise TaskContractError("resolved task definition shape is invalid")
    if definition["prepare"] != {"mode": "none"}:
        raise TaskContractError("resolved task definition must use prepare.mode=none")
    adapters = spec["adapters"]
    if not isinstance(adapters, dict) or set(adapters) != set(definition["adapters"]):
        raise TaskContractError("resolved adapter roster disagrees with the definition")
    for device, adapter in adapters.items():
        if not isinstance(adapter, dict) or "adapter_id" not in adapter:
            raise TaskContractError(f"resolved adapter {device!r} is malformed")
        verify_id(adapter["adapter_id"], f"resolved adapter {device} adapter_id")
        descriptor = {key: value for key, value in adapter.items() if key != "adapter_id"}
        if sha256_id(descriptor) != adapter["adapter_id"] \
                or descriptor != definition["adapters"][device]:
            raise TaskContractError(f"resolved adapter {device!r} identity disagrees")
    _reject_unrepresentable(spec, "resolved task spec")


def resolve_tasks(config: Any) -> dict[str, dict[str, Any]]:
    raw_tasks = getattr(config, "fleet_tasks", None) or {}
    if not isinstance(raw_tasks, dict):
        raise TaskContractError("devices.toml [fleet.tasks] must be a table")
    return {
        name: resolve_task_spec(name, raw, devices=config.devices, repo_root=config.repo_root)
        for name, raw in raw_tasks.items()
    }
