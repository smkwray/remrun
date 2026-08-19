from __future__ import annotations

from copy import deepcopy

from remrun.protocol import build_capabilities_document


EXPECTED = {
    "schema": "remrun.capabilities",
    "version": 1,
    "protocol": {"major": 1, "minor": 0},
    "package_version": "0.1.0",
    "documents": {
        "requests": [],
        "receipts": [],
        "errors": [{"schema": "remrun.error", "version": 1}],
    },
    "features": {
        "capabilities": "stable",
        "task_preparation": "unavailable",
        "target_fenced_admission": "unavailable",
        "durable_fleet_launch": "unavailable",
        "service_sessions": "unavailable",
    },
    "coordination": {
        "scope": "controller_local_queue",
        "accepted_work_recovery": "origin_controller_only",
        "unaccepted_queue_recovery": "origin_controller_only",
        "ambiguous_acceptance_retry_scope": "none",
        "global_ordering": False,
        "global_idempotency": False,
        "cross_target_exactly_once": False,
    },
}


def _compatible(
    document: dict,
    *,
    client_major: int = 1,
    required_features: tuple[str, ...] = ("capabilities",),
    required_documents: tuple[tuple[str, int], ...] = (),
    permit_experimental: bool = False,
    mutation=lambda: None,
) -> bool:
    if document.get("schema") != "remrun.capabilities" or document.get("version") != 1:
        return False
    if document.get("protocol", {}).get("major") != client_major:
        return False
    features = document.get("features", {})
    allowed = {"stable", "experimental"} if permit_experimental else {"stable"}
    if any(features.get(name, "unavailable") not in allowed for name in required_features):
        return False
    declared = {
        (entry.get("schema"), entry.get("version"))
        for values in document.get("documents", {}).values()
        for entry in values
    }
    if any(pair not in declared for pair in required_documents):
        return False
    mutation()
    return True


def test_initial_capabilities_document_is_golden_pinned() -> None:
    assert build_capabilities_document() == EXPECTED


def test_package_version_is_diagnostic_only_for_compatibility() -> None:
    changed = deepcopy(EXPECTED)
    changed["package_version"] = "999.0-local"

    assert _compatible(EXPECTED)
    assert _compatible(changed)


def test_different_major_rejects_before_mutation() -> None:
    mutated = False

    def mutate() -> None:
        nonlocal mutated
        mutated = True

    changed = deepcopy(EXPECTED)
    changed["protocol"]["major"] = 2

    assert not _compatible(changed, mutation=mutate)
    assert not mutated


def test_newer_same_major_minor_accepts_unknown_additions() -> None:
    changed = deepcopy(EXPECTED)
    changed["protocol"]["minor"] = 99
    changed["new_top_level"] = {"future": True}
    changed["features"]["future_feature"] = "stable"

    assert _compatible(changed)


def test_missing_or_experimental_required_feature_rejects_by_default() -> None:
    missing = deepcopy(EXPECTED)
    del missing["features"]["capabilities"]
    experimental = deepcopy(EXPECTED)
    experimental["features"]["capabilities"] = "experimental"

    assert not _compatible(missing)
    assert not _compatible(experimental)
    assert _compatible(experimental, permit_experimental=True)


def test_required_document_schema_must_be_declared() -> None:
    assert _compatible(EXPECTED, required_documents=(("remrun.error", 1),))
    assert not _compatible(EXPECTED, required_documents=(("remrun.future", 1),))


def test_inert_source_presence_cannot_promote_a_feature() -> None:
    document = build_capabilities_document()

    assert document["features"]["target_fenced_admission"] == "unavailable"
    assert document["features"]["durable_fleet_launch"] == "unavailable"
