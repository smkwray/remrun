"""fleet CLI input resolution: clipboard -> text/files/folder, and folder-expansion filters."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from remrun.fleet import cli
from remrun.fleet.models import FleetTask
from remrun.fleet.queue import FleetQueue
from remrun.output import Reporter


def test_resolve_clipboard_folder(tmp_path):
    d = tmp_path / "docs"
    d.mkdir()
    text, inputs = cli._resolve_clipboard("ocr", str(d))
    assert text is None and inputs == [str(d)]


def test_resolve_clipboard_files_strips_quotes_keeps_order(tmp_path):
    a, b = tmp_path / "a.pdf", tmp_path / "b.pdf"
    a.write_text("x")
    b.write_text("y")
    text, inputs = cli._resolve_clipboard("ocr", f'"{a}"\n"{b}"')   # Windows 'Copy as path' quotes
    assert text is None and inputs == [str(a), str(b)]


def test_resolve_clipboard_text_is_tts_only():
    assert cli._resolve_clipboard("tts", "Hello there, read me.") == ("Hello there, read me.", [])
    assert cli._resolve_clipboard("ocr", "Hello there.") == (None, [])   # can't OCR raw text


def test_resolve_clipboard_space_separated_paths_on_one_line(tmp_path):
    # The real-world failure this guards: two file paths joined by a SPACE on one line (a shell-
    # style selection). Without the whitespace fallback these resolve to literal text and TTS reads
    # the path aloud into one mis-named file, instead of narrating each file into <stem>.m4a.
    a, b = tmp_path / "T07.md", tmp_path / "T01.md"
    a.write_text("x")
    b.write_text("y")
    text, inputs = cli._resolve_clipboard("tts", f"{a} {b}")
    assert text is None and inputs == [str(a), str(b)]


def test_resolve_clipboard_prose_never_mistaken_for_paths():
    # The whitespace fallback must only fire when EVERY token is a real file; ordinary prose with
    # many spaces must stay literal TTS text, or normal read-aloud would break.
    prose = "The quick brown fox jumps over the lazy dog and reads this sentence aloud."
    assert cli._resolve_clipboard("tts", prose) == (prose, [])


def test_split_tasks_multi_file_becomes_one_job_each(tmp_path):
    # A folder / multi-file tts submit must fan out to one job per file so the dispatcher can
    # spread files across devices; a single multi-input job is pinned to one device and never
    # splits. Each split job carries exactly one input and a DISTINCT idempotency key.
    from remrun.fleet.models import FleetTask
    files = [str(tmp_path / f"f{i}.md") for i in range(3)]
    base = FleetTask(task_type="tts", text=None, inputs=files, options={}, idempotency_key="k")
    split = cli._split_tasks(base)
    assert [t.inputs for t in split] == [[f] for f in files]
    assert len({t.idempotency_key for t in split}) == 3


def test_split_tasks_passes_through_single_and_text_and_cmd(tmp_path):
    from remrun.fleet.models import FleetTask
    one = FleetTask(task_type="tts", text=None, inputs=["a.md"], options={}, idempotency_key="1")
    txt = FleetTask(task_type="tts", text="read me", inputs=[], options={}, idempotency_key="2")
    cmd = FleetTask(task_type="cmd", text=None, inputs=["a", "b"], options={}, idempotency_key="3")
    assert cli._split_tasks(one) == [one]
    assert cli._split_tasks(txt) == [txt]
    assert cli._split_tasks(cmd) == [cmd]   # cmd is never fanned out


def test_direct_folder_run_uses_one_manifest_item_per_file(tmp_path, monkeypatch):
    folder = tmp_path / "docs"
    folder.mkdir()
    for name in ("a.pdf", "b.png", "c.txt"):
        (folder / name).write_text("x")
    args = SimpleNamespace(
        task_type="ocr", text=None, input=[str(folder)], clipboard=False,
        device="MACBOX", engine=None, opt=[], output_root="~/sync/out",
        argv=None, allow_fallback=False, no_lease=True, json=False,
    )
    seen = []
    monkeypatch.setattr(cli, "load_config", lambda: object())
    monkeypatch.setattr(
        cli.executor,
        "run_group",
        lambda tasks, *_a, **_k: seen.extend(tasks) or {"ok": True},
    )

    assert cli.cmd_run(args, Reporter(json_events=False)) == cli.EXIT_OK
    assert [Path(task.inputs[0]).name for task in seen] == ["a.pdf", "b.png"]
    assert all(len(task.inputs) == 1 for task in seen)


def test_direct_run_json_preserves_memory_guard_payload(monkeypatch, capsys):
    guard = {
        "status": "terminated",
        "reason": "command_memory_limit",
        "detail": "limit exceeded",
        "command_started": True,
    }
    args = SimpleNamespace(
        task_type="cmd", text=None, input=[], clipboard=False,
        device="MACBOX", engine=None, opt=[], output_root=None,
        argv=["python", "-c", "pass"], allow_fallback=False, no_lease=True, json=True,
    )
    monkeypatch.setattr(cli, "load_config", lambda: object())
    monkeypatch.setattr(cli.executor, "run_group", lambda *_a, **_k: {
        "ok": False,
        "phase": "memory_guard",
        "memory_guard": guard,
        "error": "memory guard terminated after command start",
        "no_retry": True,
    })

    assert cli.cmd_run(args, Reporter(json_events=False)) == cli.EXIT_ERROR
    payload = json.loads(capsys.readouterr().out)
    assert payload["memory_guard"] == guard
    assert payload["phase"] == "memory_guard"
    assert payload["no_retry"] is True


def test_status_text_surfaces_last_error(tmp_path, monkeypatch, capsys):
    state = tmp_path / "state"
    q = FleetQueue(state / "fleet" / "fleet.db")
    try:
        jid = q.enqueue(FleetTask(task_type="cmd"))
        q.set_state(
            jid,
            "failed_final",
            error="memory guard terminated after command start: command_memory_limit",
        )
    finally:
        q.close()
    monkeypatch.setattr(cli, "default_state_root", lambda: state)

    assert cli.cmd_status(SimpleNamespace(json=False, limit=20), Reporter()) == cli.EXIT_OK
    stderr = capsys.readouterr().err
    assert "state=failed_final" in stderr
    assert 'error="memory guard terminated after command start: command_memory_limit"' in stderr


def test_resolve_clipboard_empty():
    assert cli._resolve_clipboard("tts", "   ") == (None, [])


def test_expand_inputs_tts_folder_keeps_only_text(tmp_path):
    d = tmp_path / "f"
    d.mkdir()
    (d / "a.txt").write_text("x")
    (d / "b.md").write_text("y")
    (d / "c.png").write_text("z")
    out = cli._expand_inputs("tts", [str(d)])
    assert sorted(Path(p).name for p in out) == ["a.txt", "b.md"]


def test_expand_inputs_ocr_folder_keeps_only_ocr_exts(tmp_path):
    d = tmp_path / "f"
    d.mkdir()
    (d / "a.pdf").write_text("x")
    (d / "b.txt").write_text("y")
    out = cli._expand_inputs("ocr", [str(d)])
    assert [Path(p).name for p in out] == ["a.pdf"]


def test_expand_inputs_explicit_file_not_ext_filtered(tmp_path):
    f = tmp_path / "note.rst"
    f.write_text("x")
    assert cli._expand_inputs("tts", [str(f)]) == [str(f)]


def test_route_line_variants():
    assert cli._route_line("ocr", {"device": "MACBOX"}, True, 1) == "OCR -> MACBOX - runs now"
    assert cli._route_line("tts", {"device": "MACBOX", "device_busy": True}, False, 2) \
        == "TTS -> MACBOX - queued (#2), resource busy"
    assert cli._route_line("ocr", {"device": "WINBOX"}, False, 3) == "OCR -> WINBOX - queued (#3)"
    # no device -> surface the per-device skip reason
    assert cli._route_line("tts", {"device": None,
                                   "skipped": {"WINBOX": "insufficient host RAM"}}, False, 1) \
        == "TTS: no device (WINBOX: insufficient host RAM)"
    assert cli._route_line("ocr", {"device": None, "note": "no eligible device"}, False, 1) \
        == "OCR: no device (no eligible device)"


def test_build_task_allow_fallback_marks_forced_submit():
    args = SimpleNamespace(task_type="tts", text="hello", input=[], clipboard=False,
                           device="WINBOX", engine=None, opt=[], output_root=None,
                           argv=None, allow_fallback=True)
    task = cli._build_task(args)
    assert task.force_device == "WINBOX"
    assert task.options["_allow_fallback"] is True
    assert task.options["_preferred_device"] == "WINBOX"
