from pathlib import Path

from remrun.job_observation import active_job_observation_enabled
from remrun.protocol import build_capabilities_document


ROOT = Path(__file__).resolve().parents[1]


def test_rwo5_capabilities_remain_unavailable() -> None:
    document = build_capabilities_document()
    assert document["features"]["target_fenced_admission"] == "unavailable"
    assert document["features"]["durable_fleet_launch"] == "unavailable"
    assert document["features"]["service_sessions"] == "unavailable"
    assert document["documents"]["requests"] == []
    assert document["documents"]["receipts"] == []


def test_rwo5_does_not_connect_current_fleet_or_durable_runner() -> None:
    for relative in (
        "src/remrun/fleet/dispatcher.py",
        "src/remrun/fleet/executor.py",
        "src/remrun/fleet/queue.py",
        "src/remrun/_durable_runner.py",
    ):
        source = (ROOT / relative).read_text(encoding="utf-8")
        assert "target_resources" not in source
        assert "TargetResourceClient" not in source
        assert "resource-owner-run" not in source


def test_target_owner_has_no_source_bound_keeper_or_readiness_marker() -> None:
    source = (ROOT / "src/remrun/remote/runner.py").read_text(encoding="utf-8")
    assert "resource-job-keeper" not in source
    assert "target-resource-owner.active" not in source


def test_rwo5_leaves_observer_off_and_coordination_legacy() -> None:
    assert active_job_observation_enabled({}) is False
    example = (ROOT / "config/devices.example.toml").read_text(encoding="utf-8")
    assert 'mode = "legacy"' in example


def test_rwo5_has_no_consumer_specific_vocabulary() -> None:
    paths = [
        ROOT / "src/remrun/target_resources.py",
        ROOT / "src/remrun/schemas/target-resource-policy.v1.schema.json",
        ROOT / "src/remrun/schemas/target-resource-receipt.v1.schema.json",
    ]
    forbidden = ("consumer-specific", "model", "workflow", "warm", "unload", "preset")
    combined = "\n".join(path.read_text(encoding="utf-8").lower() for path in paths)
    for term in forbidden:
        assert term not in combined
