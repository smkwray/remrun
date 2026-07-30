from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from remrun.state import (
    RetentionPolicy,
    UnknownCompletionHazardError,
    clear_unknown_completion_hazard,
    prune_state,
    read_unknown_completion_hazard,
    unknown_completion_hazard_path,
    write_unknown_completion_hazard,
)


PROJECT_ID = "projects/tdcsim"
TARGET = "RUNNER"
RUN_ID = "20260728T231455Z-RUNNER-tdcsim"
CREATED_AT = "2026-07-28T23:15:07Z"


def test_unknown_completion_hazard_is_atomic_versioned_controller_state(
    tmp_path: Path,
) -> None:
    record = write_unknown_completion_hazard(
        PROJECT_ID,
        TARGET,
        RUN_ID,
        tmp_path,
        created_at=CREATED_AT,
    )

    project_hash = hashlib.sha256(PROJECT_ID.encode("utf-8")).hexdigest()
    path = tmp_path / "hazards" / "project" / project_hash / "unknown.json"
    assert unknown_completion_hazard_path(PROJECT_ID, tmp_path) == path
    assert json.loads(path.read_text(encoding="utf-8")) == record == {
        "version": 1,
        "project_id": PROJECT_ID,
        "target": TARGET,
        "run_id": RUN_ID,
        "created_at": CREATED_AT,
        "completion_state": "unknown",
    }
    assert not path.with_suffix(".json.tmp").exists()


def test_unknown_completion_hazard_survives_fresh_read_and_is_idempotent(
    tmp_path: Path,
) -> None:
    first = write_unknown_completion_hazard(
        PROJECT_ID,
        TARGET,
        RUN_ID,
        tmp_path,
        created_at=CREATED_AT,
    )

    # The reader has no in-memory registry: a fresh call reconstructs the hazard solely
    # from controller state, which is the restart-persistence contract.
    assert read_unknown_completion_hazard(PROJECT_ID, Path(str(tmp_path))) == first
    assert write_unknown_completion_hazard(
        PROJECT_ID,
        TARGET,
        RUN_ID,
        tmp_path,
        created_at="a later retry must not rewrite created_at",
    ) == first


def test_different_unknown_run_cannot_replace_existing_hazard(tmp_path: Path) -> None:
    path = unknown_completion_hazard_path(PROJECT_ID, tmp_path)
    write_unknown_completion_hazard(
        PROJECT_ID,
        TARGET,
        RUN_ID,
        tmp_path,
        created_at=CREATED_AT,
    )
    before = path.read_bytes()

    with pytest.raises(UnknownCompletionHazardError, match=RUN_ID):
        write_unknown_completion_hazard(
            PROJECT_ID,
            "OTHER",
            "20260728T231500Z-OTHER-tdcsim",
            tmp_path,
        )

    assert path.read_bytes() == before


def test_clear_requires_matching_run_id(tmp_path: Path) -> None:
    path = unknown_completion_hazard_path(PROJECT_ID, tmp_path)
    write_unknown_completion_hazard(PROJECT_ID, TARGET, RUN_ID, tmp_path)
    before = path.read_bytes()

    assert not clear_unknown_completion_hazard(PROJECT_ID, "another-run", tmp_path)
    assert path.read_bytes() == before
    assert clear_unknown_completion_hazard(PROJECT_ID, RUN_ID, tmp_path)
    assert not path.exists()
    assert read_unknown_completion_hazard(PROJECT_ID, tmp_path) is None
    assert not clear_unknown_completion_hazard(PROJECT_ID, RUN_ID, tmp_path)


@pytest.mark.parametrize(
    "malformed",
    [
        b"{not-json",
        b"\xff\xfe",
        json.dumps(
            {
                "version": 2,
                "project_id": PROJECT_ID,
                "target": TARGET,
                "run_id": RUN_ID,
                "created_at": CREATED_AT,
                "completion_state": "unknown",
            }
        ).encode("utf-8"),
        json.dumps(
            {
                "version": 1,
                "project_id": "another/project",
                "target": TARGET,
                "run_id": RUN_ID,
                "created_at": CREATED_AT,
                "completion_state": "unknown",
            }
        ).encode("utf-8"),
    ],
)
def test_malformed_unknown_hazard_is_preserved_and_fails_closed(
    tmp_path: Path,
    malformed: bytes,
) -> None:
    path = unknown_completion_hazard_path(PROJECT_ID, tmp_path)
    path.parent.mkdir(parents=True)
    path.write_bytes(malformed)

    with pytest.raises(UnknownCompletionHazardError):
        read_unknown_completion_hazard(PROJECT_ID, tmp_path)
    assert path.read_bytes() == malformed

    with pytest.raises(UnknownCompletionHazardError):
        clear_unknown_completion_hazard(PROJECT_ID, RUN_ID, tmp_path)
    assert path.read_bytes() == malformed

    with pytest.raises(UnknownCompletionHazardError):
        write_unknown_completion_hazard(PROJECT_ID, TARGET, RUN_ID, tmp_path)
    assert path.read_bytes() == malformed


def test_prune_state_does_not_remove_unknown_completion_hazard(tmp_path: Path) -> None:
    path = unknown_completion_hazard_path(PROJECT_ID, tmp_path)
    write_unknown_completion_hazard(PROJECT_ID, TARGET, RUN_ID, tmp_path)
    before = path.read_bytes()

    old = datetime(2026, 7, 1, tzinfo=timezone.utc)
    run_id = (old - timedelta(days=30)).strftime("%Y%m%dT%H%M%SZ") + "-RUNNER-proj"
    run_dir = tmp_path / "runs" / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "summary.json").write_text('{"exit_code": 0}', encoding="utf-8")
    prune_state(
        RetentionPolicy(),
        state_root=tmp_path,
        now=old,
        keep=0,
    )

    assert not run_dir.exists()
    assert path.read_bytes() == before
