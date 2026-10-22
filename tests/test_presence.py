"""The owner states presence once, in the routing file; remrun must read it exactly."""
from __future__ import annotations

from datetime import datetime

from remrun.presence import parse_presence

DEVICES = ["macbox", "winbox", "tablet", "linuxbox"]
ROUTING = """# Routing

## Global

- winbox: Mon 00:00-24:00 (not presence: wrong section)

## Owner presence

`remrun --auto` reads this section; prose lines like this one are ignored.

- macbox, WINBOX: Wed 04:30-09:30; Sat 00:00-20:00 (owner note)
- tablet: Sun 00:00-24:00

## some-project

- linuxbox: Tue 00:00-01:00 (project rule, not presence)
"""
WED = datetime(2026, 10, 21, 6, 0)


def parsed(text: str = ROUTING):
    return parse_presence(text, devices=DEVICES, source="ROUTING.md")


def test_only_the_presence_section_counts_and_names_match_configured_devices():
    policy = parsed()
    assert WED.weekday() == 2
    assert policy.status == "valid"
    # Upper-case `WINBOX` is the configured winbox; rules under other headings are not presence.
    assert sorted(policy.windows) == ["macbox", "tablet", "winbox"]
    assert policy.window_end("winbox", WED) == datetime(2026, 10, 21, 9, 30)
    assert policy.window_end("linuxbox", datetime(2026, 10, 20, 0, 30)) is None
    assert policy.window_end("winbox", datetime(2026, 10, 19, 12, 0)) is None


def test_window_end_is_exclusive_and_24_00_runs_to_midnight():
    policy = parsed()
    assert policy.window_end("macbox", datetime(2026, 10, 21, 9, 30)) is None
    assert policy.window_end("macbox", datetime(2026, 10, 21, 4, 30)) is not None
    assert policy.window_end("tablet", datetime(2026, 10, 25, 23, 59)) == datetime(2026, 10, 26, 0, 0)


def test_a_bad_bullet_disables_the_whole_policy_instead_of_half_reading_it():
    for bullet, needle in [
        ("- winbox: Wednesday 04:30-09:30", "not 'Day HH:MM-HH:MM'"),
        ("- winbox: Wed 9:30-10:00", "not 'Day HH:MM-HH:MM'"),
        ("- winbox: Wed 10:00-09:30", "ends before it starts"),
        ("- winbox: Wed 04:30-25:00", "invalid time"),
        ("- winbox: Xyz 04:30-09:30", "unknown day"),
        ("- winbax: Wed 04:30-09:30", "unknown device"),
        ("- winbox Wed 04:30-09:30", "unknown device"),
        ("- winbox on Wednesday mornings", "expected"),
        ("1. winbox: Wed 04:30-09:30", "expected"),
        ("* winbox: Wednesday 04:30-09:30", "not 'Day HH:MM-HH:MM'"),
    ]:
        policy = parsed(ROUTING.replace("- tablet: Sun 00:00-24:00", bullet))
        assert policy.status == "malformed", bullet
        assert needle in policy.detail and policy.detail.startswith("line "), policy.detail
        # Nothing parsed before the bad line survives as a plausible partial schedule.
        assert policy.window_end("macbox", WED) is None


def test_missing_section_is_absent_not_an_empty_valid_schedule():
    policy = parsed("# Routing\n\n## Global\n\n- nothing here\n")
    assert policy.status == "absent"


def test_next_window_start_looks_ahead_across_the_week():
    policy = parsed()
    thursday = datetime(2026, 10, 22, 5, 0)
    assert policy.next_window_start("macbox", thursday) == datetime(2026, 10, 24, 0, 0)
    # Inside Wednesday's window the next start is Saturday's, not the current one.
    assert policy.next_window_start("winbox", WED) == datetime(2026, 10, 24, 0, 0)
    assert policy.next_window_start("linuxbox", thursday) is None


def _fleet_presence(tmp_path, monkeypatch, capsys, routing: str, *argv: str):
    import json

    from remrun import presence as presence_mod
    from remrun.cli import main

    root = tmp_path / "remrun"
    (root / "config").mkdir(parents=True)
    (root / "ROUTING.md").write_text(routing, encoding="utf-8")
    path = (root / "ROUTING.md").as_posix()
    (root / "config" / "devices.toml").write_text(
        "[scheduler.routing_file]\n"
        f'default = "{path}"\nmacos = "{path}"\nwindows = "{path}"\n'
        + "".join(f'\n[devices.{name}]\nkind = "local-sim"\nos = "posix"\n'
                  for name in DEVICES),
        encoding="utf-8",
    )
    monkeypatch.setenv("REMRUN_ROOT", str(root))
    monkeypatch.setattr(presence_mod, "local_now", lambda: WED)
    code = main(["fleet", "presence", "--json", *argv])
    return code, json.loads(capsys.readouterr().out)


def test_fleet_presence_answers_scripts_from_the_routing_file(tmp_path, monkeypatch, capsys):
    """A loop that waits out owner windows asks remrun instead of keeping its own copy."""
    code, payload = _fleet_presence(tmp_path, monkeypatch, capsys, ROUTING,
                                    "--device", "winbox", "--in", "240")
    assert code == 0
    assert payload["status"] == "valid"
    assert payload["evaluated_at"] == "2026-10-21T10:00"
    assert payload["devices"] == [{"name": "winbox", "window_until": None,
                                   "next_window_start": "2026-10-24T00:00"}]


def test_fleet_presence_fails_loud_on_a_broken_schedule(tmp_path, monkeypatch, capsys):
    broken = ROUTING.replace("- tablet: Sun 00:00-24:00", "- tablet: weekends")
    code, payload = _fleet_presence(tmp_path, monkeypatch, capsys, broken)
    assert code != 0
    assert payload["status"] == "malformed"


def test_every_markdown_bullet_marker_is_a_presence_item():
    """`*` and `+` are ordinary Markdown bullets; skipping them would leave a valid empty schedule."""
    for marker in ("*", "+"):
        policy = parsed(ROUTING.replace("- tablet: Sun 00:00-24:00", f"{marker} tablet: Sun 00:00-24:00"))
        assert policy.status == "valid", marker
        assert policy.window_end("tablet", datetime(2026, 10, 25, 12, 0)) is not None, marker


def test_fleet_presence_rejects_a_numbered_item(tmp_path, monkeypatch, capsys):
    numbered = ROUTING.replace("- tablet: Sun 00:00-24:00", "1. tablet: Sun 00:00-24:00")
    code, payload = _fleet_presence(tmp_path, monkeypatch, capsys, numbered)
    assert code != 0
    assert payload["status"] == "malformed"
    assert all(row["window_until"] is None for row in payload["devices"])
