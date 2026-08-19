from __future__ import annotations

import json
import math
import subprocess
import textwrap
from dataclasses import dataclass
from typing import Literal

from .models import Device
from .resource_envelope import Metric
from .transport import BaseTransport, TransportError

_PROBE_PROGRAM = textwrap.dedent(
    r"""
    import csv
    import ctypes
    import io
    import json
    import os
    import platform
    import re
    import subprocess
    import time

    def command(argv):
        try:
            return subprocess.run(
                argv, text=True, capture_output=True, timeout=3, check=False
            ).stdout
        except Exception:
            return ""

    def linux_cpu_sample():
        def read():
            fields = open("/proc/stat", encoding="ascii").readline().split()[1:]
            values = [int(value) for value in fields]
            idle = values[3] + (values[4] if len(values) > 4 else 0)
            return sum(values), idle
        first_total, first_idle = read()
        time.sleep(0.5)
        second_total, second_idle = read()
        delta = second_total - first_total
        return None if delta <= 0 else 100.0 * (1.0 - (second_idle - first_idle) / delta)

    def darwin_cpu_sample():
        out = command(["iostat", "-c", "2", "-w", "1"])
        lines = out.splitlines()
        for index, line in enumerate(lines):
            header = line.lower().split()
            if not {"us", "sy", "id"}.issubset(header):
                continue
            idle_index = header.index("id")
            for data in reversed(lines[index + 1:]):
                values = data.split()
                if len(values) <= idle_index:
                    continue
                try:
                    return 100.0 - float(values[idle_index])
                except ValueError:
                    continue
        return None

    def windows_cpu_sample():
        class FILETIME(ctypes.Structure):
            _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]
        def value(item):
            return (item.high << 32) | item.low
        def read():
            idle, kernel, user = FILETIME(), FILETIME(), FILETIME()
            if not ctypes.windll.kernel32.GetSystemTimes(
                ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)
            ):
                return None
            return value(idle), value(kernel), value(user)
        first = read()
        time.sleep(0.5)
        second = read()
        if first is None or second is None:
            return None
        idle = second[0] - first[0]
        total = (second[1] - first[1]) + (second[2] - first[2])
        return None if total <= 0 else 100.0 * (1.0 - idle / total)

    def windows_affinity_count():
        try:
            process_mask = ctypes.c_size_t()
            system_mask = ctypes.c_size_t()
            process = ctypes.windll.kernel32.GetCurrentProcess()
            ok = ctypes.windll.kernel32.GetProcessAffinityMask(
                process, ctypes.byref(process_mask), ctypes.byref(system_mask)
            )
            count = int(process_mask.value).bit_count() if ok else 0
            return count or None
        except Exception:
            return None

    def effective_cores(system):
        count = getattr(os, "process_cpu_count", lambda: None)() or os.cpu_count()
        source = "os-cpu-count"
        confidence = "total-capacity"
        if system == "windows":
            affinity = windows_affinity_count()
            if affinity is not None:
                return affinity, "process-affinity", "exact"
            return count, source, confidence
        if hasattr(os, "sched_getaffinity"):
            try:
                count = len(os.sched_getaffinity(0))
                source = "process-affinity"
                confidence = "exact"
            except Exception:
                pass
        if system == "linux":
            try:
                quota, period = open("/sys/fs/cgroup/cpu.max", encoding="ascii").read().split()[:2]
                if quota != "max":
                    limited = float(quota) / float(period)
                    count = min(float(count), limited) if count else limited
                    source = "process-affinity-cgroup"
                    confidence = "exact"
            except Exception:
                try:
                    quota = float(open(
                        "/sys/fs/cgroup/cpu/cpu.cfs_quota_us", encoding="ascii"
                    ).read())
                    period = float(open(
                        "/sys/fs/cgroup/cpu/cpu.cfs_period_us", encoding="ascii"
                    ).read())
                    if quota > 0 and period > 0:
                        limited = quota / period
                        count = min(float(count), limited) if count else limited
                        source = "process-affinity-cgroup"
                        confidence = "exact"
                except Exception:
                    pass
        return count, source, confidence

    def linux_memory():
        values = {}
        for line in open("/proc/meminfo", encoding="ascii"):
            key, _, rest = line.partition(":")
            match = re.search(r"\d+", rest)
            if match:
                values[key] = int(match.group()) * 1024
        return values.get("MemTotal"), values.get("MemAvailable")

    def darwin_memory():
        total_text = command(["sysctl", "-n", "hw.memsize"]).strip()
        total = int(total_text) if total_text.isdigit() else None
        out = command(["vm_stat"])
        page_match = re.search(r"page size of\s+(\d+)\s+bytes", out)
        page = int(page_match.group(1)) if page_match else 4096
        pages = {}
        for line in out.splitlines():
            match = re.match(r"Pages\s+([^:]+):\s+(\d+)", line)
            if match:
                pages[match.group(1).strip().lower()] = int(match.group(2))
        free = pages.get("free")
        inactive = pages.get("inactive")
        available = None if free is None or inactive is None else (free + inactive) * page
        return total, available

    def windows_memory():
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("length", ctypes.c_uint32),
                ("memory_load", ctypes.c_uint32),
                ("total_phys", ctypes.c_uint64),
                ("avail_phys", ctypes.c_uint64),
                ("total_page", ctypes.c_uint64),
                ("avail_page", ctypes.c_uint64),
                ("total_virtual", ctypes.c_uint64),
                ("avail_virtual", ctypes.c_uint64),
                ("avail_extended_virtual", ctypes.c_uint64),
            ]
        status = MEMORYSTATUSEX()
        status.length = ctypes.sizeof(status)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return None, None
        return int(status.total_phys), int(status.avail_phys)

    def gpu_snapshot(system, machine):
        query = [
            "nvidia-smi",
            "--query-gpu=uuid,name,utilization.gpu,memory.free,memory.total",
            "--format=csv,noheader,nounits",
        ]
        nvidia_status = "unavailable"
        nvidia_detail = "nvidia-smi unavailable"
        try:
            result = subprocess.run(
                query, text=True, capture_output=True, timeout=3, check=False
            )
            out = result.stdout
            if result.returncode == 0:
                nvidia_status = "measured"
                nvidia_detail = ""
            elif result.stderr.strip():
                nvidia_detail = "nvidia-smi failed"
        except subprocess.TimeoutExpired:
            out = ""
            nvidia_status = "timeout"
            nvidia_detail = "nvidia-smi timed out"
        except Exception:
            out = ""
        devices = []
        malformed_rows = 0
        for row in csv.reader(io.StringIO(out)):
            if len(row) < 5:
                malformed_rows += 1
                continue
            try:
                devices.append({
                    "id": row[0].strip(),
                    "name": row[1].strip(),
                    "util_pct": float(row[2]),
                    "vram_free_bytes": int(float(row[3]) * 1024 * 1024),
                    "vram_total_bytes": int(float(row[4]) * 1024 * 1024),
                })
            except Exception:
                malformed_rows += 1
                continue
        if devices:
            status = "partial" if malformed_rows else "measured"
            detail = f"{malformed_rows} malformed nvidia-smi row(s)" if malformed_rows else ""
            return "discrete", devices, status, detail
        if nvidia_status == "measured" and out.strip():
            return "unknown", [], "malformed", "nvidia-smi returned no valid device rows"
        if system == "darwin" and machine in ("arm64", "aarch64"):
            ioreg = command(["ioreg", "-r", "-d", "1", "-w", "0", "-c", "AGXAccelerator"])
            match = re.search(r'"Device Utilization %"\s*=\s*(\d+)', ioreg)
            util = float(match.group(1)) if match else None
            return "unified", [{
                "id": "unified",
                "name": "Apple GPU",
                "util_pct": util,
                "vram_free_bytes": None,
                "vram_total_bytes": None,
            }], ("measured" if util is not None else "partial"), (
                "" if util is not None else "Apple GPU utilization unavailable"
            )
        return "unknown", [], nvidia_status, nvidia_detail

    system = platform.system().lower()
    machine = platform.machine().lower()
    sample_interval_ms = 1000 if system == "darwin" else 500
    logical = os.cpu_count()
    effective, effective_source, effective_confidence = effective_cores(system)
    if system == "linux":
        busy = linux_cpu_sample()
        total, available = linux_memory()
    elif system == "darwin":
        busy = darwin_cpu_sample()
        total, available = darwin_memory()
    elif system == "windows":
        busy = windows_cpu_sample()
        total, available = windows_memory()
    else:
        busy, total, available = None, None, None
    gpu_kind, gpus, gpu_status, gpu_detail = gpu_snapshot(system, machine)
    print(json.dumps({
        "platform": system,
        "machine": machine,
        "cpu": {
            "logical_cores": logical,
            "effective_cores": effective,
            "effective_source": effective_source,
            "effective_confidence": effective_confidence,
            "busy_pct": busy,
            "sample_interval_ms": sample_interval_ms,
        },
        "ram": {"total_bytes": total, "available_bytes": available},
        "gpu": {
            "kind": gpu_kind,
            "devices": gpus,
            "status": gpu_status,
            "detail": gpu_detail,
        },
    }, separators=(",", ":")))
    """
).strip()


@dataclass(frozen=True)
class GPUResourceSnapshot:
    id: str
    name: str
    util_pct: Metric
    vram_free_bytes: Metric
    vram_total_bytes: Metric


@dataclass(frozen=True)
class ResourceSnapshot:
    status: str
    platform: str
    machine: str
    logical_cores: Metric
    effective_cores: Metric
    cpu_busy_pct: Metric
    cpu_sample_interval_ms: int | None
    ram_total_bytes: Metric
    ram_available_bytes: Metric
    gpu_kind: Literal["discrete", "unified", "none", "unknown"]
    gpus: tuple[GPUResourceSnapshot, ...] = ()
    detail: str = ""


def _unknown_metric(status: str, source: str = "resource-probe") -> Metric:
    return Metric(None, status, source, "unknown")  # type: ignore[arg-type]


def unavailable_snapshot(status: str, detail: str) -> ResourceSnapshot:
    metric = _unknown_metric(status)
    return ResourceSnapshot(
        status=status,
        platform="",
        machine="",
        logical_cores=metric,
        effective_cores=metric,
        cpu_busy_pct=metric,
        cpu_sample_interval_ms=None,
        ram_total_bytes=metric,
        ram_available_bytes=metric,
        gpu_kind="unknown",
        detail=detail,
    )


def _metric(
    value: object,
    *,
    source: str,
    confidence: str,
    maximum: float | None = None,
) -> Metric:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return _unknown_metric("unavailable", source)
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0 or (
        maximum is not None and numeric > maximum
    ):
        return _unknown_metric("malformed", source)
    normalized: int | float = int(numeric) if numeric.is_integer() else numeric
    return Metric(normalized, "measured", source, confidence)


def parse_resource_probe(text: str) -> ResourceSnapshot:
    try:
        payload = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return unavailable_snapshot("malformed", "probe output is not valid JSON")
    if not isinstance(payload, dict):
        return unavailable_snapshot("malformed", "probe output must be an object")
    cpu = payload.get("cpu")
    ram = payload.get("ram")
    gpu = payload.get("gpu")
    if not isinstance(cpu, dict) or not isinstance(ram, dict) or not isinstance(gpu, dict):
        return unavailable_snapshot("malformed", "probe output is missing resource objects")

    logical = _metric(
        cpu.get("logical_cores"),
        source="os-cpu-count",
        confidence="exact",
    )
    effective_source = cpu.get("effective_source")
    if not isinstance(effective_source, str) or not effective_source:
        effective_source = "os-cpu-count"
    effective_confidence = cpu.get("effective_confidence")
    if not isinstance(effective_confidence, str) or not effective_confidence:
        effective_confidence = "total-capacity"
    effective = _metric(
        cpu.get("effective_cores"),
        source=effective_source,
        confidence=effective_confidence,
    )
    busy = _metric(
        cpu.get("busy_pct"),
        source="kernel-interval",
        confidence="interval",
        maximum=100,
    )
    total = _metric(
        ram.get("total_bytes"),
        source="kernel-total",
        confidence="exact",
    )
    available = _metric(
        ram.get("available_bytes"),
        source="kernel-available",
        confidence="kernel-estimate",
    )
    if (
        total.value is not None
        and available.value is not None
        and float(available.value) > float(total.value)
    ):
        available = _unknown_metric("malformed", "kernel-available")

    raw_kind = gpu.get("kind")
    kind: Literal["discrete", "unified", "none", "unknown"] = (
        raw_kind if raw_kind in {"discrete", "unified", "none"} else "unknown"
    )
    gpu_issues: list[str] = []
    raw_gpu_status = gpu.get("status")
    if raw_gpu_status in {"partial", "malformed", "timeout", "unavailable"}:
        detail = gpu.get("detail")
        gpu_issues.append(
            str(detail).strip() if isinstance(detail, str) and detail.strip()
            else f"gpu probe {raw_gpu_status}"
        )
    elif kind == "unknown":
        gpu_issues.append("gpu kind is unknown")
    devices: list[GPUResourceSnapshot] = []
    raw_devices = gpu.get("devices")
    if isinstance(raw_devices, list):
        for raw in raw_devices:
            if not isinstance(raw, dict):
                gpu_issues.append("gpu probe returned a non-object device row")
                continue
            identifier = raw.get("id")
            name = raw.get("name")
            if not isinstance(identifier, str) or not isinstance(name, str):
                gpu_issues.append("gpu probe returned a device row without identity")
                continue
            util = _metric(
                raw.get("util_pct"),
                source="device-counter",
                confidence="device-counter",
                maximum=100,
            )
            if kind == "unified":
                free = _unknown_metric("not_applicable", "unified-memory")
                gpu_total = _unknown_metric("not_applicable", "unified-memory")
            else:
                free = _metric(
                    raw.get("vram_free_bytes"),
                    source="nvidia-smi",
                    confidence="device-counter",
                )
                gpu_total = _metric(
                    raw.get("vram_total_bytes"),
                    source="nvidia-smi",
                    confidence="device-counter",
                )
                if (
                    free.value is not None
                    and gpu_total.value is not None
                    and float(free.value) > float(gpu_total.value)
                ):
                    free = _unknown_metric("malformed", "nvidia-smi")
                if any(metric.status != "measured" for metric in (util, free, gpu_total)):
                    gpu_issues.append(f"gpu {identifier} has incomplete or malformed counters")
            devices.append(
                GPUResourceSnapshot(
                    id=identifier,
                    name=name,
                    util_pct=util,
                    vram_free_bytes=free,
                    vram_total_bytes=gpu_total,
                )
            )
    else:
        gpu_issues.append("gpu devices must be a list")
    if kind == "discrete" and not devices:
        gpu_issues.append("discrete gpu probe returned no valid device rows")

    essentials = (effective, busy, total, available)
    status = (
        "ok"
        if all(metric.status == "measured" for metric in essentials) and not gpu_issues
        else "partial"
    )
    interval = cpu.get("sample_interval_ms")
    sample_interval = (
        int(interval)
        if isinstance(interval, int) and not isinstance(interval, bool) and interval > 0
        else None
    )
    return ResourceSnapshot(
        status=status,
        platform=str(payload.get("platform") or ""),
        machine=str(payload.get("machine") or ""),
        logical_cores=logical,
        effective_cores=effective,
        cpu_busy_pct=busy,
        cpu_sample_interval_ms=sample_interval,
        ram_total_bytes=total,
        ram_available_bytes=available,
        gpu_kind=kind,
        gpus=tuple(devices),
        detail="; ".join(dict.fromkeys(gpu_issues)),
    )


def probe_target_resources(
    transport: BaseTransport,
    device: Device,
    *,
    timeout_sec: float,
) -> ResourceSnapshot:
    """Capture one best-effort selected-target snapshot; never change placement."""
    try:
        result = transport.exec(
            [device.remote_python or "python3", "-c", _PROBE_PROGRAM],
            cwd="C:\\" if device.is_windows else "/",
            timeout=timeout_sec,
            telemetry=False,
        )
    except subprocess.TimeoutExpired as exc:
        return unavailable_snapshot("timeout", f"resource probe timed out: {exc}")
    except TransportError as exc:
        detail = str(exc)
        status = "timeout" if "timed out" in detail.lower() or "timeout" in detail.lower() \
            else "unavailable"
        return unavailable_snapshot(status, detail)
    except Exception as exc:  # best-effort probe must not abort an optional workload
        return unavailable_snapshot(
            "unavailable",
            f"resource probe failed: {type(exc).__name__}: {exc}",
        )
    if result.exit_code != 0:
        return unavailable_snapshot(
            "unavailable",
            (result.stderr or result.stdout or f"probe exit {result.exit_code}").strip(),
        )
    return parse_resource_probe(result.stdout)
