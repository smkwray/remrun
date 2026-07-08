"""Task adapters: the (task_type, device) -> engine/command/output-root table,
feature extraction, and option bucketing.

The adapter table is DATA, loaded at fleet startup from ``devices.toml``
``[fleet.adapters.<task>.<device>]`` (template: ``config/fleet_adapters.example.toml``).
The published core ships NO deployment-specific engines, worker paths, or output roots —
the entire workflow is user config. Nothing here loads a model or runs anything (that's
the executor's job). Controller-side, stdlib-only.
"""
from __future__ import annotations

import os
import re
import subprocess
import tempfile
from dataclasses import replace
from pathlib import Path

from .models import FleetTask, JobFeatures

# Generic OCR-eligible input extensions used for input expansion/features.
OCR_EXTS = {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".webp", ".bmp"}

# (task_type, device) -> adapter spec, POPULATED FROM CONFIG at fleet startup (configure()). The
# published core ships NO entries — every engine, worker command, and output root is user config in
# devices.toml [fleet.adapters.<task>.<device>] (see config/fleet_adapters.example.toml). Spec keys:
# ``engine`` (cost-profile + capability label), ``output_root`` (recorded for our manifest),
# ``output_in_cmd`` (whether the worker takes it as an arg), ``capability`` (paths that gate the
# engine), ``cmd`` (token list; {stage}/{output_root}/{opt:<name>} placeholders), ``pool``,
# ``memory_kind`` ('gpu'|'cpu').
ADAPTERS: dict[tuple[str, str], dict] = {}


def load_adapters(config) -> dict[tuple[str, str], dict]:  # noqa: ANN001
    """Flatten ``config.fleet_adapters`` ([fleet.adapters.<task>.<device>] in devices.toml) into
    {(task, device): spec}. The published core ships none — the workflow is entirely user config."""
    out: dict[tuple[str, str], dict] = {}
    for task, devmap in (getattr(config, "fleet_adapters", None) or {}).items():
        if isinstance(devmap, dict):
            for device, spec in devmap.items():
                if isinstance(spec, dict):
                    out[(task, device)] = dict(spec)
    return out


def set_adapters(adapters: dict[tuple[str, str], dict]) -> None:
    """Replace the active adapter table IN PLACE (called at fleet startup and by tests). Mutating
    rather than rebinding is deliberate: other modules do ``from .adapters import ADAPTERS``, which
    binds the name to this dict object — rebinding would leave them pointing at the stale original."""
    ADAPTERS.clear()
    ADAPTERS.update(adapters)


def configure(config) -> None:  # noqa: ANN001
    """Load the adapter table from config at a fleet entry point.

    This intentionally clears the table when config has no adapters. The dict is
    still mutated in-place (not rebound) so modules that imported ``ADAPTERS`` see
    the new content rather than a stale private deployment table. Tests that need
    adapters should call ``set_adapters`` or provide them through config.
    """
    set_adapters(load_adapters(config))


def adapter_for(task_type: str, device: str) -> dict | None:
    return ADAPTERS.get((task_type, device))


def supported_devices(task_type: str) -> list[str]:
    return [dev for (t, dev) in ADAPTERS if t == task_type]


def pool_for(task: FleetTask, device: str) -> str | None:
    """Configured exclusive resource pool for ``task`` on ``device``.

    None means no resource mutex is needed. Pool names are user config, not core
    workflow identifiers.
    """
    a = adapter_for(task.task_type, device)
    if a is None:
        return None
    pool = a.get("pool")
    return str(pool) if pool else None


def resolve_output_root(task: FleetTask, device: str) -> str | None:
    if task.output_root:
        return task.output_root
    a = adapter_for(task.task_type, device)
    return a["output_root"] if a else None


def render_command(task: FleetTask, device: str, stage_dir: str, output_root: str) -> list[str]:
    """Render the device command token list for ``task`` on ``device``.

    For ``cmd`` tasks the command comes from ``task.options['argv']`` verbatim.
    """
    if task.task_type == "cmd":
        argv = list(task.options.get("argv", []))
        if not argv:
            raise ValueError("cmd task requires options['argv']")
        return [_sub(t, stage_dir, output_root, task.options) for t in argv]
    a = adapter_for(task.task_type, device)
    if not a:
        raise ValueError(f"no adapter for task={task.task_type!r} device={device!r}")
    return [_sub(t, stage_dir, output_root, task.options) for t in a["cmd"]]


def _sub(token: str, stage: str, output_root: str, opts: dict) -> str:
    token = token.replace("{stage}", stage).replace("{output_root}", output_root)
    if "{opt:" in token:
        for k, v in opts.items():
            token = token.replace("{opt:" + k + "}", str(v))
    return token


# --- feature extraction (controller-side, stdlib-only) --------------------

_PDF_PAGE_RE = re.compile(rb"/Type\s*/Page(?![s])")


def _approx_pdf_pages(path: Path) -> tuple[int, bool]:
    """Rough page count without a PDF library: count /Type /Page markers; fall
    back to a size heuristic when the PDF uses object streams (markers compressed
    away). Always approximate — the learned profile corrects it over time."""
    try:
        data = path.read_bytes()
    except OSError:
        return 1, True
    n = len(_PDF_PAGE_RE.findall(data))
    if n > 0:
        return n, True
    return max(1, len(data) // 40000), True   # ~40 KB/page fallback


def extract_features(task: FleetTask) -> JobFeatures:
    if task.task_type == "tts":
        if task.text is not None:
            return JobFeatures(input_bytes=len(task.text.encode("utf-8")),
                               file_count=1, text_chars=len(task.text), pages=0,
                               pages_approx=False)
        chars = 0
        nbytes = 0
        for p in task.inputs:
            fp = Path(p)
            try:
                nbytes += fp.stat().st_size
                chars += len(fp.read_text(encoding="utf-8", errors="replace"))
            except OSError:
                pass
        return JobFeatures(input_bytes=nbytes, file_count=len(task.inputs),
                           text_chars=chars, pages=0, pages_approx=False)

    if task.task_type == "ocr":
        pages = 0
        nbytes = 0
        files = 0
        approx = False
        for p in task.inputs:
            fp = Path(p)
            if fp.suffix.lower() not in OCR_EXTS:
                continue
            files += 1
            try:
                nbytes += fp.stat().st_size
            except OSError:
                pass
            if fp.suffix.lower() == ".pdf":
                pg, ap = _approx_pdf_pages(fp)
                pages += pg
                approx = approx or ap
            else:
                pages += 1                      # one image == one page
        return JobFeatures(input_bytes=nbytes, file_count=files, text_chars=0,
                           pages=max(pages, files), pages_approx=approx)

    # cmd: size only
    nbytes = 0
    for p in task.inputs:
        try:
            nbytes += Path(p).stat().st_size
        except OSError:
            pass
    return JobFeatures(input_bytes=nbytes, file_count=len(task.inputs))


# Option keys that affect cost per task type (feed the profile's option_bucket).
_BUCKET_KEYS = {
    "tts": ("voice", "speed"),
    "ocr": ("profile", "engine"),
    "cmd": (),
}


def option_bucket(task: FleetTask) -> str:
    keys = _BUCKET_KEYS.get(task.task_type, ())
    parts = [f"{k}={task.options[k]}" for k in keys if k in task.options]
    v = task.options.get("_variant")          # configured cost-regime variant
    if v:
        parts.insert(0, f"v={v}")
    return ",".join(parts) if parts else "default"


# --- classification (optional cost-regime variant; pre-placement) --------
# A deployment may have cost regimes that the load-balancer must know before
# placement (for example, different engines or preprocessing modes). Core does
# not infer workflow-specific regimes. It only runs a configured external
# classifier hook or applies an optional configured default variant.


def _run_classifier(cmd_template: str, task: FleetTask) -> str | None:
    """Run a configured external classifier (`{input}` -> first input path, or a
    temp file holding the task text); its last stdout line is the variant. Cheap,
    controller-side, never raises."""
    inp = task.inputs[0] if task.inputs else None
    tmp = None
    if inp is None and task.text is not None:
        tmp = tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8")
        tmp.write(task.text)
        tmp.close()
        inp = tmp.name
    if inp is None:
        return None
    try:
        out = subprocess.run(cmd_template.replace("{input}", inp), shell=True,
                             capture_output=True, text=True, timeout=30,
                             creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        lines = [ln.strip() for ln in (out.stdout or "").splitlines() if ln.strip()]
        return lines[-1] if lines else None
    except (OSError, subprocess.SubprocessError):
        return None
    finally:
        if tmp:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass


def _configured_default_variant(task_type: str, fleet_cfg: dict | None) -> str | None:
    defaults = (fleet_cfg or {}).get("default_variants", {})
    if not isinstance(defaults, dict):
        return None
    variant = defaults.get(task_type)
    return str(variant) if variant else None


def classify_variant(task: FleetTask, fleet_cfg: dict | None) -> str | None:
    """Cost-regime variant for ``task`` from config, or None.

    Supported config:
      * ``[fleet.classify] <task_type> = "command {input}"``: command stdout
        last non-empty line is the variant.
      * ``[fleet.default_variants] <task_type> = "name"``: static fallback.

    No core heuristic knows a private worker/model split.
    """
    hooks = (fleet_cfg or {}).get("classify", {})
    cmd = hooks.get(task.task_type) if isinstance(hooks, dict) else None
    if cmd:
        v = _run_classifier(str(cmd), task)
        if v:
            return v
    return _configured_default_variant(task.task_type, fleet_cfg)


def with_variant(task: FleetTask, fleet_cfg: dict | None) -> FleetTask:
    """Return ``task`` tagged with its configured variant (in options['_variant'])
    so option_bucket/profile lookup pick the right cost regime. Call once, before
    placement."""
    v = classify_variant(task, fleet_cfg)
    if not v:
        return task
    opts = dict(task.options)
    opts["_variant"] = v
    return replace(task, options=opts)


def engine_for(task: FleetTask, device: str) -> str:
    if task.engine:
        return task.engine
    a = adapter_for(task.task_type, device)
    return a["engine"] if a else "default"


def memory_kind_for(task: FleetTask, device: str) -> str:
    """'gpu' if this task/device runs a model on the GPU (footprint = VRAM on a
    discrete-GPU box, unified RAM on a Mac); 'cpu' otherwise (footprint = system
    RAM). Drives which resource the fit check gates on."""
    a = adapter_for(task.task_type, device)
    return a.get("memory_kind", "cpu") if a is not None else "cpu"
