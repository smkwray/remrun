from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from scripts import syncthing_race_harness as harness


def test_litter_paths_detects_syncthing_temp_and_conflict(tmp_path: Path):
    (tmp_path / "~syncthing~foo.tmp").write_text("tmp")
    (tmp_path / "doc.sync-conflict-20260701.txt").write_text("conflict")
    (tmp_path / "ok.txt").write_text("ok")
    names = sorted(p.name for p in harness.litter_paths(tmp_path))
    assert names == ["doc.sync-conflict-20260701.txt", "~syncthing~foo.tmp"]


def test_write_payload_is_deterministic_size_and_hash(tmp_path: Path):
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    ha = harness.write_payload(a, 1)
    hb = harness.write_payload(b, 1)
    assert a.stat().st_size == 1024 * 1024
    assert ha == hb == harness.sha256(a)


def test_build_start_args_adapts_to_v2_help(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    def fake_help(_: str, *args: str) -> str:
        if args == ():
            return "Commands:\n  serve                  Run Syncthing (default)\n"
        return ""

    monkeypatch.setattr(harness, "_help_text", fake_help)
    args = harness.build_start_args("syncthing", tmp_path / "home", 8384)
    assert args[0] == "syncthing"
    assert "serve" not in args
    assert "--no-default-folder" not in args
    assert "--gui-address=127.0.0.1:8384" in args


def test_build_start_args_uses_legacy_no_default_folder(monkeypatch: pytest.MonkeyPatch,
                                                       tmp_path: Path):
    def fake_help(_: str, *args: str) -> str:
        assert args == ()
        return "--home=PATH\n--no-browser\n--no-restart\n--no-default-folder\n--no-upgrade\n"

    monkeypatch.setattr(harness, "_help_text", fake_help)
    args = harness.build_start_args("syncthing", tmp_path / "home", 8384)
    assert args[0] == "syncthing"
    assert "serve" not in args
    assert "--no-default-folder" in args


@pytest.mark.skipif(os.environ.get("REMRUN_RUN_SYNCTHING_TEST") != "1",
                    reason="live Syncthing harness is opt-in")
def test_live_two_instance_syncthing_race_harness(tmp_path: Path):
    binary = os.environ.get("SYNCTHING_BIN", "syncthing")
    if not (shutil.which(binary) or Path(binary).exists()):
        pytest.skip("syncthing binary not found")
    result = harness.run_harness(syncthing=binary, workdir=tmp_path / "race",
                                 timeout_s=90, size_mib=4)
    assert result["ok"] is True
