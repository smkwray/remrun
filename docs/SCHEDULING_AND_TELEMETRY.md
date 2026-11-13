# Scheduling, congestion, telemetry, and learned placement

> **Status (2026-08-03): largely implemented.** `--auto` honors `[scheduler]`
> order + project `[placement]` hints, fails over when a device is unreachable or
> has a candidate-local preflight conflict, and load-balances by CPU utilization weighted by
> per-device perf-core capacity. A fallback candidate may push controller bytes
> outward but is rejected if its preflight would pull or delete locally. Telemetry
> records whole-tree CPU and concurrent process-tree peak RAM on both backends.
> Timing/CPU profiles use an EWMA; memory admission uses the observed RSS high-water
> mark plus its guard margin. Profiles drive RAM-headroom placement and a skip-probe
> shortcut for trivial jobs.
> `bench` is implemented. GPU-memory-aware scheduling and project data-policy
> hints remain incomplete; the design notes below include future extensions.

## Goal

`remrun --auto` should eventually choose the best device for a command using static capabilities, dynamic congestion, project hints, and run history.

Known devices:

```text
macbox = POSIX/macOS runner, primary
winbox = Windows runner, fallback
```

## Initial scheduler

Current behavior:

```text
if an ordinary plan/run names a target: use it when enabled or allow_explicit_run=true
if --auto: probe and rank enabled candidates using placement, reachability, and load
if bench: use enabled targets only, whether named or selected from scheduler order
if a candidate has a candidate-local conflict before running: try the next candidate
for later candidates: refuse any plan that would pull or delete on the controller
```

`allow_explicit_run` is deliberately narrow. It lets an operator address a paused
device for an ordinary `plan` or `run` while keeping that device out of automatic
placement, fleet dispatch and reclamation, and benchmark execution.

An unreachable candidate performs no preflight and therefore does not consume the
first-attempt reconciliation semantics. Global controller-tree conflicts such as
`local-vanished` abort the whole run.

## Owner presence

The owner states once, in a Markdown routing file, when they may be at a device.
devices.toml names the file per controller OS:

```toml
[scheduler.routing_file]
macos = "~/notes/ROUTING.md"
windows = '~\notes\ROUTING.md'
```

Only list items (`-`, `*` or `+`) under its `## Owner presence` heading count, one per device
group, in the controller's local time; prose lines there are ignored:

```markdown
## Owner presence

- macbox, winbox: Wed 04:30-09:30; Sat 00:00-20:00 (owner note)
```

For a heavy job — predicted to run at least `[scheduler] heavy_job_seconds`, or never
measured — `--auto` takes a device inside one of its windows out of the ranking whenever
another reachable candidate remains. When that changes the pick it reports
`target_reason = "presence-avoided"`; otherwise the ordinary reason stands. If
every reachable candidate is inside a window the job still runs, on the best of them, as
`presence-fallback`: presence never refuses work. A named target is used as given, with an
`owner_presence` warning. A list item that does not parse (a numbered one included), or names
an unknown device, marks the whole policy `malformed`; an unreadable or unconfigured file is
`unavailable`. Neither applies any rule, and both are reported in the `owner_presence` event,
`plan --probe --json`, and `fleet presence` (which then exits nonzero). Every run records its
`target_reason` and `label` under `plan` in `summary.json`.

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

The profile namespace collapses recognized nested agent-worktree paths back to the
logical repository while run IDs, locks, transfers, and completion fences retain the
exact checkout identity. Common direct, `uv run`, and simple shell-wrapped pytest
launches share one signature, but pytest-xdist worker settings remain distinct: `-n 1`
must not authorize the memory allowance for `-n 10`. Shell programs containing pipes,
redirections, or other control operators remain opaque rather than being guessed.

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

## Shared admission (`remrun admit`)

`remrun admit` keeps one queue per device so that agents and projects sharing a fleet take
turns on contended hardware. It decides who may start; `remrun run` waits for that decision
when a run declares the resources it will hold (see "Declared runs" below). The ledger is
one SQLite file in the state root of the controller that owns it
(`admission/ledger-v1.sqlite3`), which must be on a local filesystem.

**One authority.** With `[admission] coordinator` empty (the default) every controller keeps
its own ledger. To share one ledger, name the controller that owns it in `devices.toml`:

```toml
[admission]
coordinator = "macbox"
```

Every other controller then runs each `remrun admit` call, and each admission step of a
declared run, as `remrun admit ... --json` on that device over its configured SSH
transport, with the device's `path` entries prepended so `remrun` is found. The
coordinator's hostname must match its device name. If the coordinator cannot be reached
or cannot run `remrun`, the call fails; nothing falls back to a local ledger.

```bash
remrun admit acquire --device macbox --project paper1 --thread t1 --cpu 8 --ram-gib 16 \
  --wait 600 --json
remrun admit acquire --device macbox --project vision --thread t2 --key pool/gpu --device-bound
remrun admit status <ID> --json
remrun admit release <ID>     # end a grant once its work has finished
remrun admit cancel <ID>      # withdraw a request that is still waiting
remrun admit list [--device macbox] [--json]
```

A request names one device, its root project, its owning thread (`--logical-thread` names
the work stream when it differs), and what it will hold: `--cpu` cores, `--ram-gib`, and
any `--key`. Keys are capacity-one, per device, and use the target resource key vocabulary
(for example `pool/gpu`). `--request-id` makes `acquire` idempotent: repeating the same
request returns its current state, and reusing an id for a different request is an error.
Without it remrun picks an id and prints it.

**Order.** Waiting requests on a device are ordered by class, then by when the root project
last received a grant on that device (never first), then by age. Classes, highest first:
protected, `--device-bound` (the work needs something only this device has), `--critical`,
and everything else. A request passed by a newer one only because of class becomes
protected, so class can pass it at most once.

**Grants.** Promotion grants the head of the queue while it fits and stops at the first
request that does not. Nothing behind a blocked head is granted, however small. A request
fits when all of these hold:

- the device's grants plus this request stay within its budget, for CPU and RAM separately.
  The budget is what the device's `resource_policy` offers at zero load: the same reserve
  and cap a workload launch envelope applies;
- no grant on the device holds one of its keys;
- a fresh probe's offer covers it after subtracting grants younger than
  `[admission] settle_seconds`, whose load the probe may not show yet.

A resource the request does not ask for never blocks it. A device with no
`resource_policy`, or whose probe fails or lacks a CPU or RAM value, grants nothing. A
request larger than the device's whole budget is cancelled (`exceeds_device_budget`).

**Lifetimes.** A waiting request stays queued only while it is renewed: repeat the same
`acquire`, or let `--wait` do it. A request not renewed within `[admission]
wait_ttl_seconds` is cancelled (`heartbeat_expired`). A grant never expires; only `release`
ends it.

`acquire` exits 0 when granted, 75 while still waiting, and 1 on an error or when its id
has already ended. JSON output gives the state, `position`, and the ids `ahead` in the
queue. After `acquire` it also gives the facts the decision used and why the head of the
queue is blocked: `facts_unknown`, `key_held`, `ledger_cpu`, `ledger_ram`, `live_cpu`, or
`live_ram`. Interrupting `acquire --wait` withdraws its request. Releasing a grant does
not probe the device; the next waiter's poll (at most every 10 seconds) promotes the queue.

### Declared runs

```bash
remrun run macbox --cpu 8 --ram-gib 16 -- python do/fit.py
remrun run --auto --cpu 2 --key pool/gpu --device-bound --admission-wait 900 -- python train.py
```

A run that declares any of `--cpu N`, `--ram-gib G`, `--key K` (repeatable),
`--device-bound` or `--critical` takes a turn from admission before it reconciles,
reserves memory, or starts anything on the target. A run that declares none of them is
unchanged and never reads `[admission]`. The request's root project is the run's project
id, its owning thread is `--thread` (default: `$T3CODE_THREAD_ID`, else this controller and
process), and `--logical-thread` names the work stream when it differs. `--thread`,
`--logical-thread` and `--admission-wait` without a declaration are refused.

- **Explicit target**: the run queues on that device.
- **`--auto`**: in the usual candidate order, the run takes the first device that grants at
  once and cancels the requests it made on the others. If none grants, it waits on the
  first candidate whose capacity it could measure (else the first candidate) and cancels the
  rest. Once granted, the run is bound to that device and does not fail over.

While it waits the run renews its request and prints one `admission_waiting` event each
time its place changes, with its `position`, the ids `ahead` of it, and why the head is
blocked. After `--admission-wait` seconds (default `[admission] run_wait_seconds`) without
a grant it cancels its request, emits `admission_timeout`, and exits 75 without having
started anything. An unreachable coordinator exits 4 (`admission_unavailable`).

The grant is recorded under `plan.admission` in the run's `summary.json` (and a durable
run's record), and is released only when remrun knows the command is not running:

- a run that ends, fails, or is interrupted before its command started releases at exit;
- a run whose completion is unknown (a lost connection, an interrupt during the command)
  keeps its grant until `remrun resolve-unknown RUN --confirmed-ended` records the end;
- a `--durable` run that detaches (exit 4) keeps its grant until `remrun resume RUN` or
  `remrun cancel RUN` finalizes it.

Nothing releases a grant on a timer. If a release itself fails, the run reports
`admission_release_failed`; release the grant with `remrun admit release ID` once the run
is known to have ended.

## Benchmark mode

`remrun bench` is implemented. It runs the command locally unless `--no-local` is
set, then runs it through the full remrun round trip on each selected target. Those
are real executions and can change project outputs, so benchmark only idempotent or
regenerable commands whose local and remote execution is acceptable.

It records measured profiles and prints an offload recommendation; it does not
change routing policy by itself.

Examples:

```bash
remrun bench macbox,winbox -- python do/tmp/profile_model.py
remrun bench macbox,winbox --no-local -- Rscript do/tmp/model.R
```
