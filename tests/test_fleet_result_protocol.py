from __future__ import annotations

import copy

import pytest

from remrun.fleet.result_protocol import ResultProtocolError, validate_result_envelope


SPEC = "sha256:" + "1" * 64
ADAPTER = "sha256:" + "2" * 64
PREPARED = "sha256:" + "3" * 64
MEASURE = "sha256:" + "4" * 64


def _completion() -> dict:
    return {"allowed_publication": ["none", "produced", "reused"],
            "companion": "optional"}


def _item() -> dict:
    return {
        "job_id": "job1", "prepared_id": PREPARED, "index": 0,
        "outcome": "succeeded", "disposition": "none", "retry_after_s": None,
        "publication": "produced", "work_performed": True,
        "outputs": ["result/out.bin"], "companion": None, "message": None,
        "failure_code": None, "resource": "none",
        "work_units": {"unit": "pages", "value": 4, "measure_id": MEASURE},
        "elapsed_s": 2.5,
        "details": {},
    }


def _envelope(item: dict | None = None) -> dict:
    return {"schema": 2, "batch_id": "batch", "spec_id": SPEC,
            "adapter_id": ADAPTER, "items": [item or _item()]}


def _validate(envelope: dict) -> list[dict]:
    return validate_result_envelope(
        envelope, batch_id="batch", spec_id=SPEC, adapter_id=ADAPTER,
        expected_items=[{"job_id": "job1", "prepared_id": PREPARED,
                         "index": 0, "cost_unit": "pages",
                         "cost_status": "exact", "measure_id": MEASURE,
                         "prepared_value": 4.0, "verify_relative_tolerance": 0.0,
                         "reservations": [{"item_index": 0, "stem": "out"}]}],
        completion=_completion(),
    )


def test_valid_exact_result_is_accepted() -> None:
    assert _validate(_envelope()) == [_item()]


def test_frozen_legacy_work_units_v1_remains_readable() -> None:
    item = _item()
    item["work_units"] = {"unit": "pages", "value": 4}
    rows = validate_result_envelope(
        _envelope(item), batch_id="batch", spec_id=SPEC, adapter_id=ADAPTER,
        expected_items=[{
            "job_id": "job1", "prepared_id": PREPARED, "index": 0,
            "cost_unit": "pages", "cost_status": "exact", "measure_id": None,
            "prepared_value": None, "verify_relative_tolerance": 0.0,
            "reservations": [{"item_index": 0, "stem": "out"}],
        }],
        completion=_completion(),
    )
    assert rows[0]["work_units"] == {"unit": "pages", "value": 4}


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda env: env.__setitem__("batch_id", "other"), "batch_id"),
        (lambda env: env.__setitem__("spec_id", "sha256:" + "9" * 64), "does not match"),
        (lambda env: env["items"][0].__setitem__("prepared_id", "sha256:" + "8" * 64),
         "prepared_id"),
        (lambda env: env["items"][0].__setitem__("index", 1), "index"),
        (lambda env: env["items"][0].__setitem__("disposition", "retry"),
         "requires disposition none"),
        (lambda env: env["items"][0].__setitem__("outputs", ["../escape"]),
         "safe relative path"),
        (lambda env: env["items"][0].__setitem__("surprise", True), "unknown or missing"),
    ],
)
def test_result_attribution_and_coherence_fail_closed(change, message: str) -> None:  # noqa: ANN001
    envelope = _envelope(copy.deepcopy(_item()))
    change(envelope)
    with pytest.raises(ResultProtocolError, match=message):
        _validate(envelope)


def test_retry_later_requires_delay_and_failure_message() -> None:
    item = _item()
    item.update({"outcome": "failed", "disposition": "retry-later",
                 "retry_after_s": 30, "publication": "none", "outputs": [],
                 "message": "scratch full", "resource": "scratch"})
    assert _validate(_envelope(item))[0]["resource"] == "scratch"


def test_failed_item_cannot_claim_published_output() -> None:
    item = _item()
    item.update({"outcome": "failed", "disposition": "retry", "message": "failed"})
    normalized = _validate(_envelope(item))[0]
    assert normalized["outcome"] == "review"
    assert normalized["disposition"] == "none"
    assert normalized["outputs"] == ["result/out.bin"]


def test_details_are_optional_but_unknown_fields_are_not() -> None:
    item = _item()
    item.pop("details")
    assert _validate(_envelope(item))[0]["details"] == {}
    item["unknown"] = True
    with pytest.raises(ResultProtocolError, match="unknown or missing"):
        _validate(_envelope(item))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("message", 42, "message"),
        ("failure_code", "not a token!", "token"),
        ("work_units", {"unit": None, "value": 1}, "wrong fields"),
        ("outcome", [], "outcome or disposition"),
        ("disposition", {}, "outcome or disposition"),
        ("publication", [], "publication"),
        ("resource", {}, "resource"),
    ],
)
def test_result_scalar_types_fail_closed(field: str, value, message: str) -> None:  # noqa: ANN001
    item = _item()
    item[field] = value
    with pytest.raises(ResultProtocolError, match=message):
        _validate(_envelope(item))


def test_missing_or_duplicate_items_fail_closed() -> None:
    with pytest.raises(ResultProtocolError, match="count"):
        _validate({**_envelope(), "items": []})


def test_output_must_match_frozen_reservation_stem() -> None:
    item = _item()
    item["outputs"] = ["result/someone-elses-output.bin"]
    with pytest.raises(ResultProtocolError, match="reservation stem"):
        _validate(_envelope(item))


def test_companion_must_match_its_reservation() -> None:
    item = _item()
    item["companion"] = "result/foreign.json"
    with pytest.raises(ResultProtocolError, match="companion.*reservation"):
        _validate(_envelope(item))


def test_paths_are_unique_across_rows_and_companions() -> None:
    first = _item()
    second = {**_item(), "job_id": "job2", "prepared_id": "sha256:" + "4" * 64,
              "index": 1, "outputs": ["result/out.bin"]}
    envelope = {**_envelope(), "items": [first, second]}
    with pytest.raises(ResultProtocolError, match="globally unique"):
        validate_result_envelope(
            envelope, batch_id="batch", spec_id=SPEC, adapter_id=ADAPTER,
            expected_items=[
                {"job_id": "job1", "prepared_id": PREPARED, "index": 0,
                 "cost_unit": "pages", "cost_status": "exact", "measure_id": MEASURE,
                 "prepared_value": 4.0, "verify_relative_tolerance": 0.0,
                 "reservations": [{"item_index": 0, "stem": "out"}]},
                {"job_id": "job2", "prepared_id": second["prepared_id"], "index": 1,
                 "cost_unit": "pages", "cost_status": "exact", "measure_id": MEASURE,
                 "prepared_value": 4.0, "verify_relative_tolerance": 0.0,
                 "reservations": [{"item_index": 0, "stem": "out"}]},
            ],
            completion=_completion(),
        )


def test_work_measure_identity_or_value_mismatch_becomes_review() -> None:
    for work_units in (
        {"unit": "seconds", "value": 4, "measure_id": MEASURE},
        {"unit": "pages", "value": 4, "measure_id": "sha256:" + "9" * 64},
        {"unit": "pages", "value": 5, "measure_id": MEASURE},
    ):
        item = _item()
        item["work_units"] = work_units
        normalized = _validate(_envelope(item))[0]
        assert normalized["outcome"] == "review"
        assert normalized["failure_code"] == "work_measure_mismatch"


def test_output_and_companion_cannot_alias_in_one_item() -> None:
    item = _item()
    item["companion"] = item["outputs"][0]
    with pytest.raises(ResultProtocolError, match="globally unique"):
        _validate(_envelope(item))


def test_unhashable_job_id_is_a_protocol_error() -> None:
    item = _item()
    item["job_id"] = []
    with pytest.raises(ResultProtocolError, match="unknown job"):
        _validate(_envelope(item))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("spec_id", []),
        ("adapter_id", {}),
    ],
)
def test_malformed_envelope_digest_scalars_are_protocol_errors(field, value) -> None:  # noqa: ANN001
    env = _envelope()
    env[field] = value
    with pytest.raises(ResultProtocolError, match=field):
        _validate(env)


def test_malformed_prepared_id_scalar_is_a_protocol_error() -> None:
    env = _envelope()
    env["items"][0]["prepared_id"] = []
    with pytest.raises(ResultProtocolError, match="prepared_id"):
        _validate(env)


def test_result_schema_requires_an_integer_scalar() -> None:
    env = _envelope()
    env["schema"] = 2.0
    with pytest.raises(ResultProtocolError, match="schema"):
        _validate(env)


@pytest.mark.parametrize(
    "path",
    ["result//out.bin", "result/./out.bin", "./result/out.bin", "result/out.bin/"],
)
def test_result_paths_require_canonical_spelling(path: str) -> None:
    env = _envelope()
    env["items"][0]["outputs"] = [path]
    with pytest.raises(ResultProtocolError, match="canonical safe relative path"):
        _validate(env)


def test_lexical_output_companion_alias_is_rejected_before_uniqueness() -> None:
    env = _envelope()
    env["items"][0]["companion"] = "result/./out.bin"
    with pytest.raises(ResultProtocolError, match="canonical safe relative path"):
        _validate(env)
