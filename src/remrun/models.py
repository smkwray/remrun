from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .gpu_topology import parse_gpu_memory_topology

if TYPE_CHECKING:
    from .bootstrap import BootstrapPlan


@dataclass(frozen=True)
class Device:
    name: str
    enabled: bool
    role: str
    kind: str
    os: str
    address_candidates: list[str]
    project_root: str
    state_root: str
    cache_root: str
    # Disabled devices remain excluded from automatic and fleet placement. This
    # opt-in only permits an explicitly named ordinary run/plan target.
    allow_explicit_run: bool = False
    # Enabled devices may still be reserved for explicit jobs while they are
    # being qualified or when they are unsuitable for general auto-placement.
    automatic_placement: bool = True
    tags: list[str] = field(default_factory=list)
    max_jobs: int = 1
    notes: str = ""
    user: str = ""
    remote_python: str = "python3"
    ssh_opts: list[str] = field(default_factory=list)
    tailscale_ip: str = ""
    login_shell: bool = True
    shell: str = "bash"
    env: dict[str, str] = field(default_factory=dict)
    path: list[str] = field(default_factory=list)
    venv_root: str = ""
    # Best-effort cancellation actions for this runner. Schema is transport-specific but
    # intentionally data-only (for example: {process_patterns=[], wsl_process_patterns=[],
    # lock_paths=[]}); empty means cancel only clears remrun queue state.
    cancel: dict[str, Any] = field(default_factory=dict)
    # Optional host-RAM reclaim action for the fleet dispatcher. Data-only, e.g.
    # {command=["~\\...\\EmptyStandbyList.exe", "workingsets"]}. The dispatcher runs it ONLY when
    # this device is an idle fleet candidate that a queued job would otherwise not fit in host RAM
    # (see fleet.dispatcher._reclaim_marginal_devices). Empty = never reclaim (no behavior change).
    reclaim: dict[str, Any] = field(default_factory=dict)
    # Named target-side actions. Each action is an allowlisted argv plus a persistent
    # inbox; `remrun action` stages explicit files there and records a target-side
    # receipt before invoking it. Keeping this data on the device avoids turning the
    # CLI into another unrestricted remote-shell surface.
    actions: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Hardware classification (informs --auto load balancing). Defaults 0 = unknown.
    perf_cores: int = 0
    eff_cores: int = 0
    ram_gb: float = 0.0
    vram_gb: float = 0.0
    # Optional device-level declaration for whether GPU memory is separately
    # allocatable or shared with host RAM. Unset means resolve from telemetry.
    gpu_memory_topology: str = "auto"
    # Raw versioned resource policy. Validation belongs to the opt-in resource
    # envelope path; preserving the original value ensures missing and malformed
    # policy never become plausible defaults during ordinary configuration load.
    resource_policy: object | None = None
    # Optional versioned hard memory guard. Unlike resource_policy this is an
    # execution boundary: when present, every command path must initialize it
    # before user code and may not be disabled by a normal CLI flag.
    memory_guard: object | None = None

    def __post_init__(self) -> None:
        if type(self.allow_explicit_run) is not bool:
            raise ValueError("allow_explicit_run must be a boolean")
        if type(self.automatic_placement) is not bool:
            raise ValueError("automatic_placement must be a boolean")
        topology = parse_gpu_memory_topology(self.gpu_memory_topology)
        object.__setattr__(self, "gpu_memory_topology", topology)
        if topology == "unified" and self.vram_gb > 0:
            raise ValueError(
                "gpu_memory_topology='unified' cannot declare separate vram_gb"
            )

    @classmethod
    def from_mapping(cls, name: str, data: dict[str, Any]) -> "Device":
        return cls(
            name=name,
            enabled=bool(data.get("enabled", True)),
            role=str(data.get("role", "runner")),
            kind=str(data.get("kind", "ssh-posix")),
            os=str(data.get("os", "unknown")),
            address_candidates=list(data.get("address_candidates", [])),
            project_root=str(data.get("project_root", "")),
            state_root=str(data.get("state_root", "")),
            cache_root=str(data.get("cache_root", "")),
            allow_explicit_run=data.get("allow_explicit_run", False),
            automatic_placement=data.get("automatic_placement", True),
            tags=list(data.get("tags", [])),
            max_jobs=int(data.get("max_jobs", 1)),
            notes=str(data.get("notes", "")),
            user=str(data.get("user", "")),
            remote_python=str(data.get("remote_python", "python3")),
            ssh_opts=list(data.get("ssh_opts", [])),
            tailscale_ip=str(data.get("tailscale_ip", "")),
            login_shell=bool(data.get("login_shell", True)),
            shell=str(data.get("shell", "bash")),
            env={str(k): str(v) for k, v in dict(data.get("env", {})).items()},
            path=[str(p) for p in data.get("path", [])],
            venv_root=str(data.get("venv_root", "")),
            cancel=dict(data.get("cancel", {}) or {}),
            reclaim=dict(data.get("reclaim", {}) or {}),
            actions={str(k): dict(v) for k, v in dict(data.get("actions", {}) or {}).items()
                     if isinstance(v, dict)},
            perf_cores=int(data.get("perf_cores", 0) or 0),
            eff_cores=int(data.get("eff_cores", 0) or 0),
            ram_gb=float(data.get("ram_gb", 0) or 0),
            vram_gb=float(data.get("vram_gb", 0) or 0),
            gpu_memory_topology=parse_gpu_memory_topology(data.get("gpu_memory_topology")),
            resource_policy=data["resource_policy"] if "resource_policy" in data else None,
            memory_guard=data["memory_guard"] if "memory_guard" in data else None,
        )

    @property
    def is_windows(self) -> bool:
        return self.os.lower().startswith("win")

    def all_addresses(self) -> list[str]:
        """Address candidates with an optional Tailscale IP tried first."""
        addrs: list[str] = []
        if self.tailscale_ip:
            addrs.append(self.tailscale_ip)
        for a in self.address_candidates:
            if a not in addrs:
                addrs.append(a)
        return addrs

    def cpu_capacity(self, eff_weight: float = 0.5) -> float:
        """Perf-core-equivalent compute capacity (efficiency cores discounted).

        Used to compare devices by *spare* compute under load. Returns 0.0 when
        cores are unspecified (caller falls back to raw CPU usage in that case).
        """
        return float(self.perf_cores) + float(self.eff_cores) * eff_weight


@dataclass(frozen=True)
class ProjectContext:
    local_project_root: Path
    project_id: str
    relative_cwd: str
    local_cwd: Path


@dataclass(frozen=True)
class WorkloadSpec:
    """One explicitly selected versioned project resource adapter."""

    name: str
    adapter_id: str
    adapter_version: int
    work_unit: str
    require_envelope: bool = False
    require_receipt: bool = False
    protocol: int = 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "adapter_id": self.adapter_id,
            "adapter_version": self.adapter_version,
            "work_unit": self.work_unit,
            "require_envelope": self.require_envelope,
            "require_receipt": self.require_receipt,
            "protocol": self.protocol,
        }


@dataclass(frozen=True)
class RunPlan:
    target: Device
    project: ProjectContext
    command: list[str]
    transfer_mode: str
    project_config_path: Path | None
    excludes: list[str] = field(default_factory=list)
    hash_below_bytes: int = 0
    project_config: dict[str, Any] = field(default_factory=dict)
    json: bool = False
    write_scope: str | None = None
    write_scope_paths: list[str] = field(default_factory=list)
    # Preference-ordered candidates for --auto (target is candidates[0] until the
    # CLI resolves reachability/load). Single element for an explicit target.
    candidates: list[Device] = field(default_factory=list)
    workload: WorkloadSpec | None = None
    bootstrap: BootstrapPlan | None = None

    def as_dict(self) -> dict[str, Any]:
        result = {
            "target": {
                "name": self.target.name,
                "kind": self.target.kind,
                "os": self.target.os,
                "address_candidates": self.target.address_candidates,
                "project_root": self.target.project_root,
                "state_root": self.target.state_root,
                "tags": self.target.tags,
                "max_jobs": self.target.max_jobs,
            },
            "project": {
                "local_project_root": str(self.project.local_project_root),
                "project_id": self.project.project_id,
                "relative_cwd": self.project.relative_cwd,
                "local_cwd": str(self.project.local_cwd),
            },
            "command": self.command,
            "transfer_mode": self.transfer_mode,
            "project_config_path": str(self.project_config_path) if self.project_config_path else None,
            "excludes": self.excludes,
            "hash_below_bytes": self.hash_below_bytes,
            "write_scope": self.write_scope,
            "write_scope_paths": self.write_scope_paths,
        }
        if self.workload is not None:
            result["workload"] = self.workload.as_dict()
        if self.bootstrap is not None:
            result["bootstrap"] = self.bootstrap.as_dict()
        return result
