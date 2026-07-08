"""Tests for `remrun sync` — content-hash + baseline (clock-safe) folder sync."""
import os
from pathlib import Path

import pytest

from remrun.config import RemrunConfig
from remrun.manifest import FileEntry
from remrun.models import Device
from remrun.sync import (
    EXIT_CONFLICT,
    EXIT_INFRA,
    EXIT_INTERNAL,
    EXIT_OK,
    SyncError,
    classify_sync,
    device_os_key,
    remote_spec_to_tree,
    resolve_sync_paths,
    run_sync,
    run_sync_result,
)
from remrun.transfer_plan import ABORT_CONFLICT, NONE, PULL, PUSH


def _local_sim_device() -> Device:
    return Device.from_mapping("LOCAL_SIM", {"kind": "local-sim", "os": "posix",
                                             "project_root": "/tmp/x", "cache_root": "/tmp/x/c"})


def _config(sync_roots: dict | None = None, devices: dict | None = None) -> RemrunConfig:
    return RemrunConfig(
        repo_root=Path("."), defaults={}, devices=devices or {"LOCAL_SIM": _local_sim_device()},
        project_roots={}, offload={}, sync_roots=sync_roots or {},
    )


def _set_mtime(path: Path, seconds: float) -> None:
    ns = int(seconds * 1_000_000_000)
    os.utime(path, ns=(ns, ns))


def _fe(path: str, size: int, sha: str | None, mtime_ns: int = 0) -> FileEntry:
    return FileEntry(path=path, kind="file", size=size, mtime_ns=mtime_ns, sha256=sha)


# --- path mapping ----------------------------------------------------------

def test_device_os_key():
    assert device_os_key(Device.from_mapping("M", {"os": "macos"})) == "macos"
    assert device_os_key(Device.from_mapping("W", {"os": "windows"})) == "windows"
    assert device_os_key(Device.from_mapping("D", {"os": "darwin"})) == "macos"
    assert device_os_key(Device.from_mapping("P", {"os": "posix"})) == "default"


def test_resolve_named_tree_picks_device_os_base():
    cfg = _config(sync_roots={"outputs": {"windows": "C:\\w\\outputs", "macos": "/m/outputs",
                                       "default": "/d/outputs"}})
    m = resolve_sync_paths(cfg, "outputs/ocr", Device.from_mapping("MACBOX", {"os": "macos"}))
    assert m.remote_base == "/m/outputs" and m.remote_sub == "ocr" and m.tree == "outputs"
    w = resolve_sync_paths(cfg, "outputs/ocr", Device.from_mapping("WINBOX", {"os": "windows"}))
    assert w.remote_base == "C:\\w\\outputs" and w.remote_sub == "ocr"


def test_resolve_authority_default_remote_and_override():
    cfg = _config(sync_roots={
        "outputs": {"default": "/d/outputs"},
        "scratch": {"default": "/d/scratch", "authority": "conflict"},
    })
    dev = Device.from_mapping("MACBOX", {"os": "macos"})
    assert resolve_sync_paths(cfg, "outputs", dev).authority == "remote"
    assert resolve_sync_paths(cfg, "scratch", dev).authority == "conflict"


def test_resolve_rejects_traversal_subpath():
    cfg = _config(sync_roots={"outputs": {"default": "/d/outputs"}})
    with pytest.raises(SyncError):
        resolve_sync_paths(cfg, "outputs/../tts", Device.from_mapping("MACBOX", {"os": "macos"}))


def test_resolve_explicit_remote_override(tmp_path: Path):
    cfg = _config()
    local = tmp_path / "here"
    m = resolve_sync_paths(cfg, str(local), _local_sim_device(), remote_override="/remote/out")
    assert m.remote_base == "/remote/out" and m.remote_sub == "" and m.tree == "(explicit)"
    assert m.local_root == local.resolve()


def test_resolve_unknown_tree_raises():
    cfg = _config(sync_roots={"outputs": {"default": "/d/outputs"}})
    with pytest.raises(SyncError):
        resolve_sync_paths(cfg, "nope/x", Device.from_mapping("MACBOX", {"os": "macos"}))


# --- remote_spec_to_tree (reverse mapping; pure, no SSH) — Phase 2b -----------

def test_remote_spec_to_tree_maps_adapter_output_roots():
    cfg = _config(sync_roots={
        "outputs": {"windows": "D:\\shared\\outputs", "macos": "~/shared/outputs", "default": "~/shared/outputs"},
        "tts": {"windows": "D:\\shared\\tts", "macos": "~/shared/tts", "default": "~/shared/tts"},
    })
    mac = Device.from_mapping("MACBOX", {"os": "macos"})
    win = Device.from_mapping("WINBOX", {"os": "windows"})
    # the real adapter output roots map back to (tree, sub)
    assert remote_spec_to_tree(cfg, mac, "~/shared/outputs/ocr") == ("outputs", "ocr")
    assert remote_spec_to_tree(cfg, win, "D:\\shared\\outputs\\ocr") == ("outputs", "ocr")
    assert remote_spec_to_tree(cfg, mac, "~/shared/tts") == ("tts", "")       # tree root, empty sub
    assert remote_spec_to_tree(cfg, win, "D:\\shared\\tts") == ("tts", "")
    assert remote_spec_to_tree(cfg, mac, "~/elsewhere/out") is None         # no tree contains it


def test_remote_spec_to_tree_longest_base_wins():
    cfg = _config(sync_roots={
        "outputs": {"default": "/sync/outputs"},
        "outputs_ocr": {"default": "/sync/outputs/ocr"},   # deeper base must win the prefix race
    })
    dev = Device.from_mapping("P", {"os": "posix"})
    assert remote_spec_to_tree(cfg, dev, "/sync/outputs/ocr/doc") == ("outputs_ocr", "doc")


def test_remote_spec_to_tree_windows_is_case_insensitive_but_keeps_sub_case():
    cfg = _config(sync_roots={"outputs": {"windows": "D:\\shared\\outputs"}})
    win = Device.from_mapping("WINBOX", {"os": "windows"})
    # different case + forward slashes still maps; the returned subpath keeps its original case
    assert remote_spec_to_tree(cfg, win, "d:/SHARED/outputs/OCR/Doc") == ("outputs", "OCR/Doc")


def test_remote_spec_to_tree_no_base_for_os_returns_none():
    cfg = _config(sync_roots={"outputs": {"windows": "D:\\shared\\outputs"}})   # no macos/default base
    mac = Device.from_mapping("MACBOX", {"os": "macos"})
    assert remote_spec_to_tree(cfg, mac, "~/shared/outputs/ocr") is None


# --- run_sync_result reports remote_files (Phase 2b output verification) ------

def test_run_sync_result_reports_remote_files(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (remote / "a.md").write_text("x")
    (remote / "b.md").write_text("y")
    res = run_sync_result(_config(), arg=str(local), device_name="LOCAL_SIM",
                          remote_override=str(remote), direction="pull",
                          state_root=tmp_path / "st")
    assert res.exit_code == EXIT_OK
    assert res.remote_files == 2
    assert sorted(res.pulled) == ["a.md", "b.md"]


def test_run_sync_result_zero_remote_files_when_empty(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()   # empty remote tree → the dispatcher reads remote_files==0 as a mismatch
    res = run_sync_result(_config(), arg=str(local), device_name="LOCAL_SIM",
                          remote_override=str(remote), direction="pull",
                          state_root=tmp_path / "st")
    assert res.exit_code == EXIT_OK and res.remote_files == 0 and res.pulled == []


# --- classify_sync (pure; content + authority, never mtime) -----------------

def test_classify_same_content_is_noop_regardless_of_mtime():
    local = {"a": _fe("a", 10, "H", mtime_ns=999)}
    remote = {"a": _fe("a", 10, "H", mtime_ns=1)}
    assert classify_sync(local, remote, "remote")[0].action == NONE


def test_classify_remote_authority_pulls_difference():
    local = {"a": _fe("a", 10, "LOCAL", mtime_ns=10**9)}   # newer mtime, but stale content
    remote = {"a": _fe("a", 12, "REMOTE", mtime_ns=1)}
    assert classify_sync(local, remote, "remote")[0].action == PULL


def test_classify_local_authority_pushes_difference():
    assert classify_sync({"a": _fe("a", 10, "L")}, {"a": _fe("a", 12, "R")}, "local")[0].action == PUSH


def test_classify_conflict_authority_flags_difference():
    assert classify_sync({"a": _fe("a", 10, "L")}, {"a": _fe("a", 12, "R")},
                         "conflict")[0].action == ABORT_CONFLICT


def test_classify_only_one_side_is_additive():
    actions = {c.path: c.action for c in classify_sync(
        {"only_local": _fe("only_local", 3, "X")}, {"only_remote": _fe("only_remote", 4, "Y")}, "remote")}
    assert actions == {"only_local": PUSH, "only_remote": PULL}


def test_classify_missing_hash_treated_as_different_not_same():
    local = {"a": _fe("a", 10, None, mtime_ns=5)}
    remote = {"a": _fe("a", 10, None, mtime_ns=9)}
    assert classify_sync(local, remote, "remote")[0].action == PULL


# --- end-to-end apply (LOCAL_SIM, via --remote so it's OS-independent) ------

def _run(local: Path, remote: Path, direction="both", dry_run=False):
    cfg = _config()
    return run_sync(cfg, arg=str(local), device_name="LOCAL_SIM", remote_override=str(remote),
                    direction=direction, dry_run=dry_run, state_root=local.parent / "syncstate")


def test_stale_local_with_newer_mtime_does_not_overwrite_remote(tmp_path: Path):
    # local has a LATER mtime but stale content; remote (producer) must win → pull.
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "out.md").write_text("STALE local content (but newer mtime)")
    (remote / "out.md").write_text("fresh remote output")
    _set_mtime(local / "out.md", 5000)
    _set_mtime(remote / "out.md", 1000)
    assert _run(local, remote) == EXIT_OK
    assert (local / "out.md").read_text() == "fresh remote output"
    assert (remote / "out.md").read_text() == "fresh remote output"


def test_same_size_different_content_detected_by_hash(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "c.md").write_text("AAAAAAAA")
    (remote / "c.md").write_text("BBBBBBBB")  # same length, different bytes
    _set_mtime(local / "c.md", 1500)
    _set_mtime(remote / "c.md", 1500)         # equal mtime
    assert _run(local, remote) == EXIT_OK
    assert (local / "c.md").read_text() == "BBBBBBBB"


def test_remote_only_is_pulled(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (remote / "new.md").write_text("only on remote")
    assert _run(local, remote) == EXIT_OK
    assert (local / "new.md").read_text() == "only on remote"


def test_local_only_is_pushed_in_both_mode(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "mine.txt").write_text("local work")
    assert _run(local, remote) == EXIT_OK
    assert (remote / "mine.txt").read_text() == "local work"


def test_pull_only_never_writes_remote(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "mine.txt").write_text("local work")
    (remote / "theirs.md").write_text("remote work")
    assert _run(local, remote, direction="pull") == EXIT_OK
    assert (local / "theirs.md").read_text() == "remote work"
    assert not (remote / "mine.txt").exists()


def test_push_only_never_writes_local(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "mine.txt").write_text("local work")
    (remote / "theirs.md").write_text("remote work")
    assert _run(local, remote, direction="push") == EXIT_OK
    assert (remote / "mine.txt").read_text() == "local work"
    assert not (local / "theirs.md").exists()


def test_dry_run_changes_nothing(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (remote / "new.md").write_text("only on remote")
    assert _run(local, remote, dry_run=True) == EXIT_OK
    assert not (local / "new.md").exists()


def test_missing_remote_root_errors(tmp_path: Path):
    local = tmp_path / "L"
    local.mkdir()
    assert _run(local, tmp_path / "does-not-exist") == EXIT_INFRA


def test_invalid_direction_errors(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    assert _run(local, remote, direction="sideways") == EXIT_INTERNAL


# --- baseline change detection (clock-safe direction) & rollback net --------

def test_baseline_detects_which_side_changed(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "x").write_text("v0")
    (remote / "x").write_text("v0")           # identical → first --both establishes baseline
    assert _run(local, remote) == EXIT_OK

    # Only LOCAL changes → must PUSH (change detection, not mtime).
    (local / "x").write_text("local v1")
    _set_mtime(local / "x", 1)                # deliberately OLD mtime; must not matter
    assert _run(local, remote) == EXIT_OK
    assert (remote / "x").read_text() == "local v1"

    # Only REMOTE changes → must PULL.
    (remote / "x").write_text("remote v2")
    assert _run(local, remote) == EXIT_OK
    assert (local / "x").read_text() == "remote v2"


def test_baseline_both_changed_is_conflict(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "x").write_text("v0")
    (remote / "x").write_text("v0")
    assert _run(local, remote) == EXIT_OK     # baseline established
    (local / "x").write_text("local edit")
    (remote / "x").write_text("remote edit")  # both diverge from baseline
    rc = _run(local, remote)
    assert rc == EXIT_CONFLICT
    assert (local / "x").read_text() == "local edit"   # local NOT clobbered


def test_pull_overwrite_snapshots_prior_local(tmp_path: Path):
    # The rollback net: overwriting a local file backs up its prior version.
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "x").write_text("OLD local — recover me")
    (remote / "x").write_text("new remote")
    assert _run(local, remote, direction="pull") == EXIT_OK
    assert (local / "x").read_text() == "new remote"
    backups = list((tmp_path / "syncstate" / "conflicts").rglob("x"))
    assert backups and backups[0].read_text() == "OLD local — recover me"


def test_conflict_authority_saves_aside(tmp_path: Path, monkeypatch):
    state = tmp_path / "state"
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "c.md").write_text("local version")
    (remote / "c.md").write_text("remote version DIFFERENT")
    # Pin the local OS key so the mapping is deterministic on any host (else on Linux
    # current_os_key()=="default" would resolve local to the remote dir).
    monkeypatch.setattr("remrun.sync.current_os_key", lambda: "macos")
    cfg = _config(sync_roots={"scratch": {
        "macos": str(local), "default": str(remote), "authority": "conflict"}})
    rc = run_sync(cfg, arg="scratch", device_name="LOCAL_SIM", direction="both", state_root=state)
    assert rc == EXIT_CONFLICT
    assert (local / "c.md").read_text() == "local version"  # never clobbered
    saved = list((state / "conflicts").rglob("c.md"))
    assert any(s.read_text() == "remote version DIFFERENT" for s in saved)


def test_casefold_collision_detection():
    from remrun.config import case_insensitive, casefold_collisions
    assert case_insensitive("windows") and case_insensitive("macos")
    assert not case_insensitive("default")
    c = casefold_collisions(["A.txt", "a.txt", "b.txt", "sub/X", "sub/x"])
    assert set(c) == {"a.txt", "sub/x"}
    assert casefold_collisions(["a.txt", "b.txt", "c/d"]) == {}


def test_casefold_collision_aborts_on_insensitive_target(tmp_path: Path, monkeypatch):
    # Force an insensitive remote target and feed two folded-equal local files being pushed.
    # (Built via manifests so it's portable — a case-insensitive host FS can't hold both.)
    import remrun.sync as sync_mod
    monkeypatch.setattr(sync_mod, "current_os_key", lambda: "default")        # local sensitive
    monkeypatch.setattr(sync_mod, "device_os_key", lambda d: "windows")       # remote insensitive
    monkeypatch.setattr(sync_mod, "build_manifest",
                        lambda *a, **k: {"A.txt": _fe("A.txt", 1, "x"), "a.txt": _fe("a.txt", 1, "y")})
    monkeypatch.setattr(sync_mod, "make_transport", lambda d: _StubInsensitiveTransport())
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    rc = run_sync(_config(), arg=str(local), device_name="LOCAL_SIM",
                  remote_override=str(remote), direction="push", state_root=tmp_path / "st")
    assert rc == EXIT_CONFLICT


class _StubInsensitiveTransport:
    def probe(self):
        from remrun.transport import ProbeResult
        return ProbeResult(reachable=True, address="x", detail="ok", remote_os="windows")
    def expand_remote(self, p): return p
    def remote_join(self, root, sub): return root + ("/" + sub if sub else "")
    def remote_path_exists(self, p): return True
    def manifest(self, *a, **k): return {}


class _BatchingLocalTransport:
    def __init__(self, remote: Path):
        self.remote = remote
        self.pulled_batches: list[list[str]] = []
        self.pushed_batches: list[list[str]] = []

    def probe(self):
        from remrun.transport import ProbeResult
        return ProbeResult(reachable=True, address="x", detail="ok", remote_os="posix")

    def expand_remote(self, p): return p
    def remote_join(self, root, sub): return str(Path(root) / Path(sub)) if sub else str(root)
    def remote_path_exists(self, p): return Path(p).exists()

    def ensure_remote_dir(self, p):
        Path(p).mkdir(parents=True, exist_ok=True)

    def manifest(self, root, excludes, hash_below_bytes=0):
        from remrun.manifest import build_manifest
        return build_manifest(Path(root), excludes, hash_below_bytes=hash_below_bytes)

    def pull_file(self, remote_path, local_path):
        import shutil
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(remote_path, local_path)

    def push_file(self, local_path, remote_path):
        import shutil
        Path(remote_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, remote_path)

    def pull_files(self, remote_root, local_root, rel_paths):
        self.pulled_batches.append(list(rel_paths))
        for rel in rel_paths:
            self.pull_file(Path(remote_root) / Path(rel), Path(local_root) / Path(rel))

    def push_files(self, local_root, remote_root, rel_paths):
        self.pushed_batches.append(list(rel_paths))
        for rel in rel_paths:
            self.push_file(Path(local_root) / Path(rel), Path(remote_root) / Path(rel))

    def delete_remote(self, remote_path):
        Path(remote_path).unlink(missing_ok=True)


def test_sync_batches_multi_file_pull(tmp_path: Path, monkeypatch):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (remote / "a.txt").write_text("A")
    (remote / "sub").mkdir()
    (remote / "sub" / "b.txt").write_text("B")
    transport = _BatchingLocalTransport(remote)
    monkeypatch.setattr("remrun.sync.make_transport", lambda d: transport)
    rc = run_sync(_config(), arg=str(local), device_name="LOCAL_SIM",
                  remote_override=str(remote), direction="pull", state_root=tmp_path / "st")
    assert rc == EXIT_OK
    assert sorted(transport.pulled_batches[0]) == ["a.txt", "sub/b.txt"]
    assert (local / "a.txt").read_text() == "A"
    assert (local / "sub" / "b.txt").read_text() == "B"


def test_sync_batches_multi_file_push(tmp_path: Path, monkeypatch):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "a.txt").write_text("A")
    (local / "sub").mkdir()
    (local / "sub" / "b.txt").write_text("B")
    transport = _BatchingLocalTransport(remote)
    monkeypatch.setattr("remrun.sync.make_transport", lambda d: transport)
    rc = run_sync(_config(), arg=str(local), device_name="LOCAL_SIM",
                  remote_override=str(remote), direction="push", state_root=tmp_path / "st")
    assert rc == EXIT_OK
    assert sorted(transport.pushed_batches[0]) == ["a.txt", "sub/b.txt"]
    assert (remote / "a.txt").read_text() == "A"
    assert (remote / "sub" / "b.txt").read_text() == "B"


def test_vanished_local_root_refuses_to_delete_remote(tmp_path: Path):
    # If a baseline exists and the local root then vanishes, sync must NOT mirror that
    # as a wholesale remote deletion (the audit's blocker — reproduced & guarded).
    import shutil
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "keep.md").write_text("important remote output")
    (remote / "keep.md").write_text("important remote output")
    assert _run(local, remote) == EXIT_OK          # establish baseline
    shutil.rmtree(local)                            # local root disappears
    assert _run(local, remote) == EXIT_INFRA        # refused
    assert (remote / "keep.md").exists()            # remote tree intact


def test_remote_deleted_local_modified_is_conflict_not_crash(tmp_path: Path):
    local, remote = tmp_path / "L", tmp_path / "R"
    local.mkdir()
    remote.mkdir()
    (local / "x.md").write_text("v0")
    (remote / "x.md").write_text("v0")
    assert _run(local, remote) == EXIT_OK           # baseline
    (local / "x.md").write_text("local edit")       # local modified
    (remote / "x.md").unlink()                       # remote deleted
    assert _run(local, remote) == EXIT_CONFLICT      # conflict, no crash
    assert (local / "x.md").read_text() == "local edit"  # local untouched
