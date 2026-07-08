# Scheduling, congestion, telemetry, and learned placement

> **Status (2026-06-28): largely implemented.** `--auto` honors `[scheduler]`
> order + project `[placement]` hints, fails over to a reachable device, and
> load-balances by CPU utilization weighted by per-device perf-core capacity.
> Telemetry (peak RAM + CPU, whole process tree) records on both backends (POSIX
> `getrusage`, Windows Job Object) and feeds EWMA per-(project, command) job
> profiles that drive RAM-headroom placement and a skip-probe shortcut for trivial
> jobs. Still aspirational below: GPU-memory awareness, project data-policy hints,
> and `bench`. The design notes that follow describe the full intended model.

## Goal

`remrun --auto` should eventually choose the best device for a command using static capabilities, dynamic congestion, project hints, and run history.

Known devices:

```text
macbox = POSIX/macOS runner, primary
winbox = Windows runner, fallback
```

## Initial scheduler

Phase 1 behavior:

```text
if explicit target: use it
if --auto: choose the configured primary if enabled/reachable, else fallback
```

This is enough to start.

## Static capabilities

Device config can define:

```toml
[devices.macbox]
tags = ["macstudio", "macos", "arm64", "primary", "high-ram", "r", "python"]
max_jobs = 2

[devices.winbox]
tags = ["windows", "x64", "fallback", "stata", "matlab"]
max_jobs = 2
```

Future resource fields:

```toml
[devices.macbox.resources]
cores = 20
memory_gb = 128
speed_class = 1.0

[devices.winbox.resources]
cores = 16
memory_gb = 64
gpu = true
speed_class = 0.8
```

## Dynamic congestion probes

Before scheduling, remrun should optionally probe:

```text
reachability
current remrun queue depth
CPU load
available RAM
disk free
network roundtrip/throughput estimate
recent failures
```

Implementation can use a small remote runner/shim that returns JSON. Python + psutil is a good optional path, but the protocol should allow native PowerShell/POSIX fallbacks.

Example probe response:

```json
{
  "device": "macbox",
  "reachable": true,
  "load_avg_1m": 2.3,
  "cpu_percent": 41.0,
  "memory_available_mb": 74231,
  "disk_free_mb": 812000,
  "remrun_active_jobs": 1,
  "remrun_max_jobs": 2
}
```

## Learned resource usage

Each run should record compact telemetry:

```json
{
  "project_id": "paper1",
  "command_signature": "Rscript do/tmp/bootstrap_test.R --reps <N>",
  "device": "macbox",
  "duration_sec": 1842.2,
  "exit_code": 0,
  "peak_rss_mb_observed": 42117,
  "avg_cpu_pct": 760,
  "files_pushed": 7,
  "files_pulled": 19,
  "bytes_pushed": 1823912,
  "bytes_pulled": 874002331
}
```

This does not need to be perfect. Sampling process-tree memory every 1-5 seconds is good enough for scheduling hints.

## Synced history without sync conflicts

Do not use one synced SQLite DB written by all devices.

Prefer append-only per-device JSONL:

```text
~/.local/state/remrun/history/macbox/2026-06.jsonl
~/.local/state/remrun/history/winbox/2026-06.jsonl
~/.local/state/remrun/history/<submitter-device>/2026-06.jsonl
```

Each device writes only its own file. A later compactor can create aggregate summaries:

```text
~/.local/state/remrun/history/aggregates/project-command-stats.json
```

Active full logs remain local and trimmed.

## Command signatures

Commands often include variable numeric arguments or temp filenames. For learning, compute a normalized signature:

```text
Rscript do/tmp/bootstrap_test.R --reps <NUM>
python do/tmp/profile_model.py --n <NUM>
make estimates
stata-mp -b do <PATH>
```

Use exact command plus normalized signature. Never let the normalized signature replace the exact command in logs.

## Scheduling score

Future `--auto` score:

```text
score =
  static capability match
  + project preference
  + learned speed advantage
  + available memory fit
  - current CPU/load penalty
  - queue penalty
  - transfer-size penalty
  - recent failure penalty
```

## Parallel jobs

Default policy:

```text
one unknown in-place writer per project
```

This is conservative but necessary. Arbitrary commands may write overlapping files.

Opt-in parallelism:

```bash
remrun run --scope spec_a --auto -- Rscript do/spec_a.R
remrun run --scope spec_b --auto -- Rscript do/spec_b.R
```

Project config:

```toml
[parallel.scopes.spec_a]
paths = ["results/spec_a/**", "logs/spec_a/**"]

[parallel.scopes.spec_b]
paths = ["results/spec_b/**", "logs/spec_b/**"]
```

Only allow concurrent writers when a future scope-aware baseline model or external
sandbox makes attribution safe. remrun enforces today's model conservatively:
`--scope <name>` must refer to a configured scope, all project writers serialize,
and pullback fails as a conflict if the command wrote outside the declared scope paths.

## Benchmark mode

`remrun bench` should not mutate canonical project outputs by default.

Safe benchmark semantics:

```text
run the same command on multiple devices in sandbox/copy mode
measure duration/resources
optionally compare outputs
then either report winner or rerun winner in-place
```

Future examples:

```bash
remrun bench macbox,winbox -- python do/tmp/profile_model.py
remrun bench macbox,winbox --winner-rerun -- Rscript do/tmp/model.R
remrun bench macbox,winbox --compare-outputs -- make estimates-test
```
