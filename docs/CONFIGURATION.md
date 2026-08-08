# Configuration design

## Global synced config

Global config lives in:

```text
~/remrun/config/
C:\tools\remrun\config\
```

Seed files:

```text
config/defaults.toml
config/devices.example.toml
config/devices.toml       # private, ignored
```

This config is safe to sync as long as it does not contain secrets. Tailscale IPs are not generally secrets, but they are personal infrastructure details and should be removed before public release.

## Example devices

```toml
[devices.macbox]
kind = "ssh-posix"
os = "macos"
address_candidates = ["macbox.local", "macbox"]
project_root = "~/projects"
state_root = "~/.local/state/remrun"
tags = ["macos", "arm64", "primary"]
max_jobs = 2

[devices.winbox]
kind = "ssh-powershell"
os = "windows"
# Windows PowerShell 5.1 is rejected because it cannot preserve arbitrary
# native argv. Use PowerShell 7.3 or newer. Direct PowerShell-language commands
# and top-level .cmd/.bat commands are also rejected: use a native application,
# a .ps1 wrapper that accepts positional data, or an explicit pwsh -Command.
shell = "pwsh"
address_candidates = ["winbox.local", "winbox"]
project_root = "C:\\Users\\you\\projects"
state_root = "~\\AppData\\Local\\remrun\\state"
tags = ["windows", "x64", "fallback"]
max_jobs = 2
```

Agents may later add:

```toml
tailscale_ip = "100.x.y.z"
address_candidates = ["100.x.y.z", "macbox.local"]
```

## Per-device hard memory guard

A POSIX device can opt into the strict relative memory boundary:

```toml
[devices.macbox]
ram_gb = 64 # scheduler/display metadata; not the protection authority
max_jobs = 2

[devices.macbox.memory_guard]
schema = 3
command_limit_fraction = 0.3125
```

The command fraction must be finite, greater than zero, and below one. Unknown
keys are rejected. By default, the host reserve is 25% of physical RAM, bounded
to 4--16 GiB and rounded upward to MiB. An explicit
`host_reserve_fraction` remains available; when present, both fractions must sum
to no more than one. Schema 3 is an intentional compatibility break: prior
schemas are rejected rather than silently reinterpreted. Windows targets are
explicitly rejected while the same schema-3 semantics are
unproved there. Physical RAM is sampled on the target under the
admission-ledger lock. The per-command ceiling is rounded downward to MiB.
`max_jobs` independently caps
guarded leases. Policy maxima do not reserve capacity for jobs that did not ask
for it. Concurrency has two independent gates:

```text
active guarded leases < max_jobs
the exact live ledger transaction fits each requested allowance
```

For a command without a learned RSS profile, remrun derives an
**unprofiled live-capacity allowance** from current available memory after the host
reserve, existing guarded commitments, measured control overhead, and a one-MiB
strict-comparison margin. It caps that capacity at the per-command ceiling and
requires at least 1 MiB. A missing
profile therefore does not commit the ceiling or cause refusal while ordinary live
headroom exists. The granted allowance is the first run's hard process-tree limit;
a job that reaches it is terminated and its measured peak informs later profiles.
With a positive learned profile, remrun instead reserves the observed process-tree
RSS high-water mark plus 25% headroom, rounded upward to MiB, and refuses rather
than clipping when that evidence-based allowance exceeds the command ceiling.
Admission receipts label the allowance basis and byte counts.

A tool-owned command may instead request an explicit positive whole-MiB hard limit through
the generic transport seam. That allowance is recorded as
`explicit_command_limit`, receives no prediction headroom, and remains subject to the same
target-derived per-command ceiling, atomic lease transaction, `max_jobs`, and host reserve.
It is a configured safety boundary, not an observed or predicted RSS value. Git-sync uses
this seam: its fixed repository-root probe is capped at no more than 128 MiB, while guarded
status/pull/push/bootstrap require `[git_sync].remote_memory_limit_mib` or the CLI override
because bundle and worktree operations can scale with repository size.

Recognized nested agent worktrees share the logical repository's profile namespace,
but resource-shaping command details remain distinct. In particular, pytest-xdist
profiles include the explicit worker value (`-n 4`, `-n 10`, and the configured
default are separate). This lets measurements follow a project across worktrees
without letting a low-parallelism run authorize a higher-parallelism one.

Before project reconciliation or user code, `run --auto` asks each ranked
POSIX candidate to atomically create a bounded lease under
`<state_root>/memory-guard/v2`. The ledger is protected with `fcntl.flock`, so
controllers sharing the target state root cannot both admit capacity from the
same snapshot. An unsafe automatic candidate is skipped before its project tree
is touched. An explicit target is never redirected. When no candidate is safe,
`run` returns exit 5 with `phase = "memory_admission"`,
`error = "no safe target capacity"`, and the structured target result; it does
not create a second queue. Expired unclaimed leases are reclaimed. Claimed
leases remain conservative while their recorded helper, root identity, or
process group is alive.

After helper staging and all other prelaunch mutation, the controller renews the
lease while holding the same target-local lock. The helper then starts only a
closed-gate control process and claims the same lease under that lock. Reserve,
renew, and claim each perform the same conservative capacity transaction before
committing the ledger. The gate cannot release unless the claim owns the same
lease and that transaction still passes. On renewal or claim refusal, the
matching lease is removed atomically; a later controller can then acquire the
reclaimed capacity.

The transaction brackets guarded attribution with three host-availability
samples while the ledger lock is held:

```text
H0 -> P0 -> H1 -> P1 -> H2

available_floor = min(H0.available, H1.available, H2.available)
private_credit_i = sum, over stable PID identities in both P0 and P1,
                   min(private_resident_at_P0, private_resident_at_P1)
required_available = host_reserve
                   + sum(max(0, capacity_i - private_credit_i))
safe iff available_floor > required_available
```

`capacity_i` is the user-command allowance plus a separately reserved control
process budget. The user command is still enforced against its own allowance;
control-process overhead cannot consume or enlarge that limit. The bracket
rejects both growth and shrink races between readings: only private resident
memory continuously attributable to the same process identity across both
guarded snapshots receives credit, and the lowest surrounding host sample is
authoritative.

The credited metric is additive physical attribution, not summed RSS. Linux
uses `Private_Clean + Private_Dirty` from one `/proc/<pid>/smaps_rollup` read per
process. macOS sums `pri_private_pages_resident` from
`PROC_PIDREGIONINFO` region records. Shared mappings receive no credit, and COW
pages receive credit only after the kernel reports them as private. Thus memory
already absent from live availability is credited once without treating shared
or COW RSS as additive. Full uncredited capacity remains reserved for anything
that cannot be measured coherently. The guard does not throttle CPU; jobs whose
transactions pass remain concurrent.

During execution, each helper reactively terminates its own process tree if the
command ceiling or host reserve is reached. At a host-floor breach every guarded
helper that observes the breach kills its own tree. Without a resident
coordinator, selecting one deterministic victim cannot prove that enough memory
will be recovered in time. This fail-safe is intentionally destructive and does
not claim control over direct SSH commands, unrelated local processes, work that
escaped before observation, or execution surviving abrupt helper death.

The launch handshake distinguishes process trees from threads and preserves
exact argv and command exits. `command_started = false` is emitted only before
the gate opens or after an explicit `exec` failure proves argv did not start. If
the gate opened but exec confirmation is interrupted, the private result is
omitted and the existing completion-unknown fail-safe blocks a retry rather than
reporting a false non-start. `--no-telemetry` disables optional metrics only; it
never disables admission or enforcement. A device without `memory_guard` keeps
its prior behavior.

## Fleet views and task adapters

Fleet resource tables use compact percentages by default:

```toml
[fleet.resources]
usage_display = "percent"  # or "amounts" for used/total RAM, VRAM, and disk
```

The JSON form always retains exact amounts, percentages, measurement semantics,
and status regardless of the table setting. Resource probing reads bounded operating-
system capacity metadata; it does not enumerate files. macOS primary-disk usage treats
space available for important usage as available, while other platforms report allocated
usage from their primary-volume metadata.

OCR, TTS, and arbitrary-command workers are device-specific configuration rather than
built-in model dependencies. Start from `config/fleet_adapters.example.toml`. A folder
submission becomes one logical job per eligible file so the dispatcher can place work
across devices and still batch compatible items into one worker invocation. Multi-item
OCR/TTS workers must return per-file results matching the staged manifest; unattributed
success is finalized without automatic retry.

`remrun fleet jobs` reads target-local observation registries across the fleet. Querying
is always read-only. Launch-side registration remains off unless the controller explicitly
sets `REMRUN_FLEET_JOBS_OBSERVE=1`; enable it only after the target's native lifecycle gate
passes. This switch changes observation only, not queue placement or memory-guard policy.

## Machine-local overrides

Future support should allow non-synced or ignored local overrides:

```text
config/local.macbox.toml
config/local.winbox.toml
config/local.<current-hostname>.toml
```

Use for secrets, usernames, nonportable paths, and experimental settings.

## Project config

Optional project config lives in:

```text
<project>/do/remrun/remrun.toml
```

Example:

```toml
[placement]
primary = "macbox"
fallback = ["winbox"]

[[placement.rules]]
match_command = "stata|\\.do$"
prefer = "winbox"

[transfer]
exclude = [
  "data/raw/**",
  "data/vendor/**",
  "scratch/**",
  "results/cache/**"
]

[resources]
schema = 1
# default_workload = "example.analysis"

[resources.workloads."example.analysis"]
protocol = 1
adapter_id = "example.resource-policy"
adapter_version = 1
work_unit = "case"
require_envelope = false
require_receipt = true
```

Project config must be optional. It is for optimizations and hints only.

Resource adaptation is additionally opt-in: select a schema-1 declaration with
`remrun run --workload example.analysis ...`, or deliberately set
`default_workload`. For a selected run, the resource feature adds only
`REMRUN_RUN_CONTEXT`; remrun does not rewrite argv or worker flags. The versioned
context and project-written receipt live under the selected target's configured
state root, never in the project tree, and each is limited to 64 KiB. Device
headroom policy is explicit per device and is never inferred from remote access
or `role`. Unified-memory devices report shared system RAM, not invented VRAM.

## Config precedence

Recommended merge order:

```text
built-in defaults
config/defaults.toml
config/devices.toml
config/local.<device>.toml
project do/remrun/remrun.toml
CLI flags
```

CLI flags win.

## Paths and project IDs

Project ID is the path relative to the configured project root.

Examples:

```text
~/projects/paper1                -> paper1
~/projects/client/foo            -> client/foo
C:\projects\paper1               -> paper1
```

Equivalent remote path is:

```text
<device.project_root>/<project_id>/<relative_cwd>
```

Use POSIX-style project IDs internally. Convert separators only at the device boundary.
