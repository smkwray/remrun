from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

MIB = 1024 * 1024
GIB = 1024 * MIB
_SCHEMA = 3
_KEYS = frozenset({"schema", "command_limit_fraction", "host_reserve_fraction"})
_REQUIRED_KEYS = frozenset({"schema"})
_ADMISSION_SCHEMA = 2
PREDICTION_HEADROOM_FACTOR = 1.25
RESERVATION_TTL_SECONDS = 30 * 60
_ALLOWANCE_BASES = frozenset(
    {
        "explicit_command_limit",
        "learned_profile_plus_25_percent",
        "learned_profile_live_headroom",
        "unprofiled_available_backed",
    }
)


class MemoryGuardConfigError(ValueError):
    """A device memory guard is malformed or unsupported."""


@dataclass(frozen=True)
class MemoryGuard:
    """Validated relative memory policy for one guarded POSIX target."""

    command_limit_fraction: float | None
    host_reserve_fraction: float | None
    max_jobs: int
    schema: int = _SCHEMA

    def as_dict(self) -> dict[str, int | float]:
        result: dict[str, int | float] = {"schema": self.schema}
        if self.command_limit_fraction is not None:
            result["command_limit_fraction"] = self.command_limit_fraction
        if self.host_reserve_fraction is not None:
            result["host_reserve_fraction"] = self.host_reserve_fraction
        return result


@dataclass(frozen=True)
class MemoryReservation:
    """One target-local guarded-capacity lease returned by admission."""

    lease_id: str
    lease_token: str
    state_root: str
    allowance_bytes: int
    control_overhead_bytes: int
    capacity_bytes: int
    max_command_bytes: int
    min_available_bytes: int
    host_total_bytes: int
    safe_concurrency: int
    expires_at: float
    # Admission allowance and command enforcement are intentionally separate.
    # Learned and unprofiled allowances reserve capacity but do not impose an
    # RSS kill ceiling; only an explicit owner limit populates this field.
    command_limit_bytes: int | None = None
    allowance_basis: str | None = None
    allocation_rule: str | None = None
    remaining_backed_capacity_bytes: int | None = None
    strict_margin_bytes: int | None = None
    learned_allowance_bytes: int | None = None
    live_backed_allowance_bytes: int | None = None
    learned_allowance_live_backed: bool | None = None
    predicted_rss_bytes: int | float | None = None

    @property
    def effective_command_limit_bytes(self) -> int | None:
        """Return the execution ceiling, if this is an explicit-limit lease.

        The basis fallback keeps older in-process transport doubles and older
        target receipts readable. New admission responses carry
        ``command_limit_bytes`` explicitly, while non-explicit bases always
        remain unlimited at the process-tree layer.
        """
        if self.allowance_basis != "explicit_command_limit":
            return None
        if self.command_limit_bytes is not None:
            return self.command_limit_bytes
        return self.allowance_bytes

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> "MemoryReservation":
        lease = payload.get("lease")
        if not isinstance(lease, Mapping):
            raise ValueError("memory admission omitted lease")

        def positive_int(name: str) -> int:
            value = lease.get(name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"memory admission lease.{name} is invalid")
            return value

        lease_id = lease.get("lease_id")
        lease_token = lease.get("lease_token")
        state_root = lease.get("state_root")
        expires_at = lease.get("expires_at")
        if not isinstance(lease_id, str) or not lease_id:
            raise ValueError("memory admission lease_id is invalid")
        if not isinstance(lease_token, str) or not lease_token:
            raise ValueError("memory admission lease_token is invalid")
        if not isinstance(state_root, str) or not state_root:
            raise ValueError("memory admission state_root is invalid")
        if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
            raise ValueError("memory admission expires_at is invalid")
        if not math.isfinite(float(expires_at)) or float(expires_at) <= 0:
            raise ValueError("memory admission expires_at is invalid")
        allowance_basis = lease.get("allowance_basis")
        if allowance_basis is not None and (
            not isinstance(allowance_basis, str)
            or allowance_basis not in _ALLOWANCE_BASES
        ):
            raise ValueError("memory admission allowance_basis is invalid")

        def optional_nonnegative_int(name: str) -> int | None:
            value = lease.get(name)
            if value is None:
                return None
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"memory admission lease.{name} is invalid")
            return value

        learned_allowance_bytes = optional_nonnegative_int("learned_allowance_bytes")
        live_backed_allowance_bytes = optional_nonnegative_int(
            "live_backed_allowance_bytes"
        )
        learned_allowance_live_backed = lease.get("learned_allowance_live_backed")
        if learned_allowance_live_backed is not None and not isinstance(
            learned_allowance_live_backed, bool
        ):
            raise ValueError(
                "memory admission lease.learned_allowance_live_backed is invalid"
            )
        predicted_rss_bytes = lease.get("predicted_rss_bytes")
        if predicted_rss_bytes is not None:
            if (
                isinstance(predicted_rss_bytes, bool)
                or not isinstance(predicted_rss_bytes, (int, float))
                or not math.isfinite(float(predicted_rss_bytes))
                or predicted_rss_bytes <= 0
            ):
                raise ValueError("memory admission lease.predicted_rss_bytes is invalid")

        allocation_rule = lease.get("allocation_rule")
        if allocation_rule is not None and allocation_rule != (
            "unprofiled_live_headroom_v1"
        ):
            raise ValueError("memory admission allocation_rule is invalid")
        allowance_bytes = positive_int("allowance_bytes")
        control_overhead_bytes = positive_int("control_overhead_bytes")
        capacity_bytes = positive_int("capacity_bytes")
        max_command_bytes = positive_int("max_command_bytes")
        command_limit_bytes = lease.get("command_limit_bytes")
        if command_limit_bytes is not None:
            if (
                isinstance(command_limit_bytes, bool)
                or not isinstance(command_limit_bytes, int)
                or command_limit_bytes <= 0
                or command_limit_bytes % MIB != 0
            ):
                raise ValueError("memory admission lease.command_limit_bytes is invalid")
            if command_limit_bytes > max_command_bytes:
                raise ValueError(
                    "memory admission command limit exceeds the policy ceiling"
                )
        if allowance_basis == "explicit_command_limit" and command_limit_bytes is None:
            # Accept the pre-repair explicit wire shape while normalizing it to
            # the owner-limit meaning.
            command_limit_bytes = allowance_bytes
        elif (
            allowance_basis != "explicit_command_limit"
            and command_limit_bytes is not None
        ):
            raise ValueError(
                "memory admission command limit requires explicit_command_limit basis"
            )
        min_available_bytes = positive_int("min_available_bytes")
        host_total_bytes = positive_int("host_total_bytes")
        if capacity_bytes != allowance_bytes + control_overhead_bytes:
            raise ValueError("memory admission lease capacity arithmetic is invalid")
        if allowance_bytes > max_command_bytes:
            raise ValueError("memory admission allowance exceeds the command ceiling")
        if capacity_bytes + min_available_bytes > host_total_bytes:
            raise ValueError("memory admission lease exceeds physical memory")
        return cls(
            lease_id=lease_id,
            lease_token=lease_token,
            state_root=state_root,
            allowance_bytes=allowance_bytes,
            control_overhead_bytes=control_overhead_bytes,
            capacity_bytes=capacity_bytes,
            max_command_bytes=max_command_bytes,
            min_available_bytes=min_available_bytes,
            host_total_bytes=host_total_bytes,
            safe_concurrency=positive_int("safe_concurrency"),
            expires_at=float(expires_at),
            command_limit_bytes=command_limit_bytes,
            allowance_basis=allowance_basis,
            allocation_rule=allocation_rule,
            remaining_backed_capacity_bytes=optional_nonnegative_int(
                "remaining_backed_capacity_bytes"
            ),
            strict_margin_bytes=optional_nonnegative_int("strict_margin_bytes"),
            learned_allowance_bytes=learned_allowance_bytes,
            live_backed_allowance_bytes=live_backed_allowance_bytes,
            learned_allowance_live_backed=learned_allowance_live_backed,
            predicted_rss_bytes=predicted_rss_bytes,
        )

    def as_dict(self, *, include_token: bool = False) -> dict[str, object]:
        data: dict[str, object] = {
            "lease_id": self.lease_id,
            "state_root": self.state_root,
            "allowance_bytes": self.allowance_bytes,
            "command_limit_enforced": self.effective_command_limit_bytes is not None,
            "enforced_command_limit_bytes": self.effective_command_limit_bytes,
            "control_overhead_bytes": self.control_overhead_bytes,
            "capacity_bytes": self.capacity_bytes,
            "max_command_bytes": self.max_command_bytes,
            "policy_command_ceiling_bytes": self.max_command_bytes,
            "min_available_bytes": self.min_available_bytes,
            "host_total_bytes": self.host_total_bytes,
            "safe_concurrency": self.safe_concurrency,
            "expires_at": self.expires_at,
        }
        if include_token:
            data["lease_token"] = self.lease_token
        if self.allowance_basis is not None:
            data["allowance_basis"] = self.allowance_basis
        if self.effective_command_limit_bytes is not None:
            data["command_limit_bytes"] = self.effective_command_limit_bytes
        for name in (
            "allocation_rule",
            "remaining_backed_capacity_bytes",
            "strict_margin_bytes",
            "learned_allowance_bytes",
            "live_backed_allowance_bytes",
            "learned_allowance_live_backed",
            "predicted_rss_bytes",
        ):
            value = getattr(self, name)
            if value is not None:
                data[name] = value
        return data


@dataclass(frozen=True)
class MemoryAdmissionResult:
    """Validated result of a target-local reserve/renew/release operation."""

    status: str
    reason: str
    detail: str
    payload: dict[str, object]
    reservation: MemoryReservation | None = None

    @property
    def admitted(self) -> bool:
        return self.status == "admitted" and self.reservation is not None

    @classmethod
    def from_payload(cls, payload: object) -> "MemoryAdmissionResult":
        if not isinstance(payload, dict) or payload.get("schema") != _ADMISSION_SCHEMA:
            raise ValueError("memory admission returned an invalid schema")
        status = payload.get("status")
        if status not in {"admitted", "refused", "released"}:
            raise ValueError("memory admission returned an invalid status")
        reason = payload.get("reason")
        detail = payload.get("detail")
        if not isinstance(reason, str) or not isinstance(detail, str):
            raise ValueError("memory admission omitted reason/detail")
        reservation = (
            MemoryReservation.from_payload(payload) if status == "admitted" else None
        )
        return cls(
            status=status,
            reason=reason,
            detail=detail,
            payload=dict(payload),
            reservation=reservation,
        )

    @classmethod
    def refused(cls, reason: str, detail: str) -> "MemoryAdmissionResult":
        payload: dict[str, object] = {
            "schema": _ADMISSION_SCHEMA,
            "status": "refused",
            "reason": reason,
            "detail": detail,
        }
        return cls("refused", reason, detail, payload, None)


def _fraction(value: object, field: str, device_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MemoryGuardConfigError(
            f"device {device_name!r} memory_guard.{field} must be a finite fraction"
        )
    result = float(value)
    if not math.isfinite(result) or not 0.0 < result < 1.0:
        raise MemoryGuardConfigError(
            f"device {device_name!r} memory_guard.{field} must be greater than 0 and below 1"
        )
    return result


def parse_memory_guard(
    raw: object | None,
    *,
    ram_gb: object = None,
    device_name: str,
    max_jobs: object = 1,
    device_kind: str = "",
    device_os: str = "",
) -> MemoryGuard | None:
    """Validate the strict relative guard table.

    ``ram_gb`` remains an accepted keyword for compatibility but is intentionally
    not an authority for protection. Physical RAM is measured on the target under
    the admission-ledger lock. A present guard is supported only on POSIX transports.
    """

    del ram_gb
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise MemoryGuardConfigError(
            f"device {device_name!r} memory_guard must be a table"
        )

    keys = set(raw)
    missing = _REQUIRED_KEYS - keys
    unknown = keys - _KEYS
    if missing:
        raise MemoryGuardConfigError(
            f"device {device_name!r} memory_guard missing key(s): "
            + ", ".join(sorted(missing))
        )
    if unknown:
        raise MemoryGuardConfigError(
            f"device {device_name!r} memory_guard unknown key(s): "
            + ", ".join(sorted(str(key) for key in unknown))
        )

    schema = raw["schema"]
    if isinstance(schema, bool) or not isinstance(schema, int) or schema != _SCHEMA:
        raise MemoryGuardConfigError(
            f"device {device_name!r} memory_guard.schema must be {_SCHEMA}"
        )
    if isinstance(max_jobs, bool) or not isinstance(max_jobs, int) or max_jobs <= 0:
        raise MemoryGuardConfigError(
            f"device {device_name!r} max_jobs must be a positive integer when memory_guard is enabled"
        )
    kind = str(device_kind).lower()
    os_name = str(device_os).lower()
    if kind == "ssh-powershell" or os_name.startswith("win"):
        raise MemoryGuardConfigError(
            f"device {device_name!r} memory_guard schema 3 is not proved on Windows"
        )
    if kind and kind not in {"local-sim", "ssh-posix"}:
        raise MemoryGuardConfigError(
            f"device {device_name!r} memory_guard schema 3 requires a POSIX transport"
        )

    command_fraction = (
        _fraction(raw["command_limit_fraction"], "command_limit_fraction", device_name)
        if "command_limit_fraction" in raw
        else None
    )
    reserve_fraction = (
        _fraction(raw["host_reserve_fraction"], "host_reserve_fraction", device_name)
        if "host_reserve_fraction" in raw
        else None
    )
    if (
        reserve_fraction is not None
        and command_fraction is not None
        and command_fraction + reserve_fraction > 1.0
    ):
        raise MemoryGuardConfigError(
            f"device {device_name!r} memory guard command limit plus host reserve exceeds RAM"
        )

    return MemoryGuard(
        command_limit_fraction=command_fraction,
        host_reserve_fraction=reserve_fraction,
        max_jobs=max_jobs,
    )
