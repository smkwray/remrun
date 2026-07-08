"""Adapters: feature extraction, option bucketing, command rendering, output roots."""
from __future__ import annotations

from remrun.fleet import adapters
from remrun.fleet.models import FleetTask


def test_tts_text_features():
    t = FleetTask(task_type="tts", text="hello world")
    f = adapters.extract_features(t)
    assert f.text_chars == 11 and not f.pages_approx and f.units("tts") == 11 / 1000.0


def test_ocr_image_counts_one_page_each(tmp_path):
    (tmp_path / "a.png").write_bytes(b"x")
    (tmp_path / "b.jpg").write_bytes(b"y")
    (tmp_path / "note.txt").write_text("ignored")     # not an OCR extension
    t = FleetTask(task_type="ocr", inputs=[str(tmp_path / "a.png"), str(tmp_path / "b.jpg"),
                                           str(tmp_path / "note.txt")])
    f = adapters.extract_features(t)
    assert f.file_count == 2 and f.pages == 2


def test_ocr_pdf_page_estimate(tmp_path):
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4\n/Type /Page\n/Type /Page\n/Type /Pages\n")  # 2 pages, not /Pages
    t = FleetTask(task_type="ocr", inputs=[str(pdf)])
    f = adapters.extract_features(t)
    assert f.pages == 2 and f.pages_approx


def test_option_bucket_and_engine():
    t = FleetTask(task_type="ocr", options={"profile": "fast", "engine": "x", "irrelevant": "z"})
    assert adapters.option_bucket(t) == "engine=x,profile=fast" or \
           adapters.option_bucket(t) == "profile=fast,engine=x"  # order by key list
    assert adapters.engine_for(FleetTask(task_type="ocr"), "WINBOX") == "ocr-remote"


def test_render_command_substitutes_stage_and_output():
    t = FleetTask(task_type="ocr")
    cmd = adapters.render_command(t, "WINBOX", r"C:\tmp\stage\in", r"C:\outputs\ocr")
    assert r"C:\tmp\stage\in" in cmd and r"C:\outputs\ocr" in cmd
    assert cmd[0] == "powershell.exe"


def test_tts_win_command_has_no_outputroot_arg():
    # This worker decides its output location and rejects -OutputRoot.
    cmd = adapters.render_command(FleetTask(task_type="tts"), "WINBOX", r"C:\tmp\in", r"C:\outputs\tts")
    assert "-OutputRoot" not in cmd and "-InputDir" in cmd
    assert "{output_root}" not in " ".join(cmd)          # no unsubstituted placeholder


def test_render_cmd_task_uses_argv():
    t = FleetTask(task_type="cmd", options={"argv": ["echo", "{output_root}"]})
    cmd = adapters.render_command(t, "MACBOX", "/tmp/in", "/out")
    assert cmd == ["echo", "/out"]


def test_classify_defaults_are_config_driven():
    # Core no longer guesses task-specific regimes; users can set static defaults.
    assert adapters.classify_variant(FleetTask(task_type="tts", text="a heron"), {}) is None
    cfg = {"default_variants": {"tts": "plain", "ocr": "vision"}}
    assert adapters.classify_variant(FleetTask(task_type="tts", text="a heron"), cfg) == "plain"
    assert adapters.classify_variant(FleetTask(task_type="ocr", inputs=["x.pdf"]), cfg) == "vision"


def test_classify_external_hook_overrides(tmp_path):
    # An external hook's last stdout line is the variant (here a trivial echo).
    t = FleetTask(task_type="ocr", inputs=[str(tmp_path / "doc.pdf")])
    v = adapters.classify_variant(t, {"classify": {"ocr": "echo fast"}})
    assert v == "fast"


def test_with_variant_folds_into_bucket():
    t = adapters.with_variant(FleetTask(task_type="ocr", inputs=["x.pdf"]),
                              {"default_variants": {"ocr": "vision"}})
    assert t.options.get("_variant") == "vision"
    assert adapters.option_bucket(t) == "v=vision"
    # cmd tasks have no regime -> unchanged
    assert adapters.with_variant(FleetTask(task_type="cmd"), {}).options.get("_variant") is None


def test_ocr_output_root_from_adapter():
    assert adapters.resolve_output_root(FleetTask(task_type="ocr"), "WINBOX") == r"C:\outputs\ocr"
    assert adapters.resolve_output_root(FleetTask(task_type="ocr"), "MACBOX") == "~/outputs/ocr"
