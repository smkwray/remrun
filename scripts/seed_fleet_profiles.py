#!/usr/bin/env python3
"""(OPTIONAL) Materialize the shared fleet cost table into a controller's LOCAL store.

**You usually do NOT need this.** The measured device costs now live in the synced,
version-controlled `config/fleet_costs.toml`, which `remrun fleet plan/run` loads
automatically as the base for placement (see `fleet.config.load_costs` /
`fleet.profiles.merge_costs`). So a fresh controller is never cost-blind — it does not
"forget" how to estimate OCR/TTS jobs — without running anything.

This script just copies those shared numbers into the LOCAL EWMA store
(`fleet_profiles.json`, per-controller, never synced) as observations, e.g. if you want
to pre-seed the EWMA before any real runs. The single source of truth is
`config/fleet_costs.toml`; this reads from it (it does not hardcode the numbers).

Usage:
  python scripts/seed_fleet_profiles.py [--state-root PATH] [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from remrun.config import load_config  # noqa: E402
from remrun.fleet import profiles as fp  # noqa: E402
from remrun.fleet.config import load_fleet_costs  # noqa: E402
from remrun.state import default_state_root, utc_now_iso  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--state-root", help="override the controller state root")
    ap.add_argument("--dry-run", action="store_true", help="print, write nothing")
    args = ap.parse_args(argv)

    state_root = Path(args.state_root).expanduser() if args.state_root else default_state_root()
    shared = load_fleet_costs(load_config())   # key "task|engine|device|bucket" -> fields
    print(f"state root: {state_root}")
    print(f"fleet profile file: {fp._path(state_root)}")
    print(f"shared cost rows (config/fleet_costs.toml): {len(shared)}")
    now = utc_now_iso()
    for key, row in sorted(shared.items()):
        task, engine, device, bucket = key.split("|", 3)
        line = (f"  {task:3} {device:4} {bucket:9} fixed={row.get('fixed_load_s')!s:>6}s "
                f"var={row.get('var_per_unit_s')!s:>6} rss={row.get('peak_rss_mb')!s:>7}MB "
                f"vram={row.get('peak_vram_mb')!s:>7}MB  [{engine}]")
        if args.dry_run:
            print("DRY " + line)
            continue
        fp.update_profile(state_root, task, engine, device, bucket,
                          fixed_load_s=row.get("fixed_load_s"),
                          var_per_unit_s=row.get("var_per_unit_s"),
                          peak_rss_mb=row.get("peak_rss_mb"),
                          peak_vram_mb=row.get("peak_vram_mb"), now=now)
        print("SEED" + line)
    if not args.dry_run:
        n = len(fp.load_profiles(state_root))
        print(f"done: {len(shared)} rows materialized; local store now has {n} entries.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
