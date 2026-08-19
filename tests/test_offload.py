from __future__ import annotations

from pathlib import Path

from remrun.config import RemrunConfig, offload_policy, offload_threshold


def cfg(offload: dict) -> RemrunConfig:
    return RemrunConfig(
        repo_root=Path("/x"), defaults={}, devices={}, project_roots={}, offload=offload
    )


def test_table_policy_and_threshold():
    c = cfg({
        "default": {"policy": "ask", "ram_gb": 5.0, "wall_sec": 300},
        "LAPTOP": {"policy": "auto", "ram_gb": 1.0, "wall_sec": 120, "note": "weak box"},
    })
    assert offload_policy(c, "LAPTOP") == "auto"
    t = offload_threshold(c, "LAPTOP")
    assert t["ram_gb"] == 1.0
    assert t["wall_sec"] == 120
    assert t["note"] == "weak box"


def test_host_match_is_case_insensitive():
    c = cfg({"default": {"policy": "ask"}, "laptop": {"policy": "auto", "ram_gb": 1.0}})
    assert offload_policy(c, "LAPTOP") == "auto"
    assert offload_threshold(c, "LAPTOP")["ram_gb"] == 1.0


def test_default_fallback_for_unknown_host():
    c = cfg({"default": {"policy": "ask", "ram_gb": 5.0, "wall_sec": 300}})
    assert offload_policy(c, "UNKNOWN") == "ask"
    assert offload_threshold(c, "UNKNOWN")["ram_gb"] == 5.0


def test_bare_string_policy_backcompat_uses_default_threshold():
    c = cfg({"default": "ask", "LAPTOP": "auto"})
    assert offload_policy(c, "LAPTOP") == "auto"
    # A bare-string entry carries no threshold → conservative workstation default.
    assert offload_threshold(c, "LAPTOP")["ram_gb"] == 5.0


def test_project_override_wins_over_host_policy():
    c = cfg({"LAPTOP": {"policy": "auto", "ram_gb": 1.0}})
    assert offload_policy(c, "LAPTOP", {"run": {"offload": "never"}}) == "never"


def test_empty_offload_table_defaults_to_ask():
    c = cfg({})
    assert offload_policy(c, "LAPTOP") == "ask"
    assert offload_threshold(c, "LAPTOP")["ram_gb"] == 5.0
