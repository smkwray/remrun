from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


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
    # Hardware classification (informs --auto load balancing). Defaults 0 = unknown.
    perf_cores: int = 0
    eff_cores: int = 0
    ram_gb: float = 0.0
    vram_gb: float = 0.0

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
            perf_cores=int(data.get("perf_cores", 0) or 0),
            eff_cores=int(data.get("eff_cores", 0) or 0),
            ram_gb=float(data.get("ram_gb", 0) or 0),
            vram_gb=float(data.get("vram_gb", 0) or 0),
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

    def as_dict(self) -> dict[str, Any]:
        return {
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
