from pathlib import Path

from remrun.job_observation import active_job_observation_enabled
from remrun.protocol import build_capabilities_document


ROOT = Path(__file__).resolve().parents[1]


def test_tranche_b_promotes_only_the_integrated_target_features() -> None:
    document = build_capabilities_document()
    assert document["features"]["target_fenced_admission"] == "stable"
    assert document["features"]["durable_fleet_launch"] == "stable"
    assert document["features"]["service_sessions"] == "unavailable"
    assert document["documents"]["requests"] == [
        {"schema": "remrun.target-resource-policy", "version": 1}
    ]
    assert document["documents"]["receipts"] == [
        {"schema": "remrun.target-resource-receipt", "version": 1}
    ]


def test_tranche_b_connects_one_operation_identity_across_existing_substrates() -> None:
    executor = (ROOT / "src/remrun/fleet/executor.py").read_text(encoding="utf-8")
    queue = (ROOT / "src/remrun/fleet/queue.py").read_text(encoding="utf-8")
    durable = (ROOT / "src/remrun/_durable_runner.py").read_text(encoding="utf-8")
    observer = (ROOT / "src/remrun/_job_observer.py").read_text(encoding="utf-8")
    assert "TargetResourceClient" in executor
    assert "record_target_acceptance" in queue
    assert "resource-operation" in durable
    assert "--start-gate-file" in observer


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
