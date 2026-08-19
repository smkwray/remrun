# remrun

`remrun` runs an arbitrary command from a local project on a remote machine and
makes the end state look as if it had run locally: it reconciles the project's
files to the remote, runs the command in the equivalent remote directory, and
pulls changed outputs back to the same local paths. Syncthing is a convenience,
not a correctness dependency.

> **New here?** Start with [How to run it](#how-to-run-it), then copy and edit
> `config/devices.example.toml` for your machines.

## Status

`remrun` is a public alpha. Its documented safety boundaries are stable enough for
regular use, while configuration and command details may still evolve before 1.0.

The core runner is implemented and tested on POSIX/macOS and Windows SSH targets:
safe project reconciliation and pullback, conflict preservation, target scheduling,
resource telemetry, external-tree `sync`, commit-only `git-sync`, allowlisted target
actions, and optional fleet dispatch with live resource, job, and SSH-mesh views.
POSIX targets can opt into a RAM-relative hard memory guard that admits work before
project mutation and terminates only the protected command tree if its granted limit
or host reserve is breached. An unknown command receives a fair share of capacity
that is live and fully backed across the remaining `max_jobs` slots, bounded by the
configured per-command maximum; it does not monopolize every open slot merely because
no profile exists. Learned commands use their
observed process-tree RSS high-water mark plus guard headroom. The guard cannot govern
GPU or unified-memory allocations that the operating
system does not attribute to process-tree RSS; do not use it as containment for those
workloads. The Windows `ssh-powershell` command surface requires `pwsh` 7.3+ and supports
native applications plus `.ps1` entry points with every following token preserved as
positional data. Direct PowerShell cmdlets, functions, filters, configurations, and
aliases are rejected before user code: already-resolvable commands fail during
preflight and the target process rechecks after reconciliation. Remrun's exact
`list[str]` command API has no typed distinction between a named parameter, a switch,
and positional data. Put named-parameter syntax inside an explicit `.ps1` wrapper, or
intentionally invoke `pwsh -NoProfile -Command <source>` as a native application.
Top-level `.cmd`/`.bat` commands, including bare names resolved through
`PATH`/`PATHEXT`, remain rejected
because the PowerShell-to-`cmd.exe` path is proved to corrupt some argv. Windows is
therefore a narrower command surface than POSIX.

The default coordination mode remains `legacy`: one controller may write a project at
a time. Versioned-runner, lease/fencing, and snapshot components are experimental and
disabled; they are not a supported multi-controller execution path. A network
disconnect during an ordinary run can leave completion unknown; remrun reports that
state and requires a read-only process or artifact check before retrying. For a long,
noninteractive command that must survive controller sleep or connectivity loss, an
SSH target selected automatically or named explicitly can use a target-supervised
durable run and later resume from the originating controller.

## How to run it

remrun has **no install step required** — use the launcher in `bin/`. Always run
it **from inside a project** under the configured project root. The command goes after `--`.

```bash
# macOS/Linux: add bin to PATH or call it directly
~/remrun/bin/remrun run macbox -- Rscript do/analysis.R

# Windows:
C:\tools\remrun\bin\remrun.cmd run macbox -- python do\compute.py
```

Optional: `python -m pip install -e .` from the repo root puts a `remrun` command
on PATH (entry point `remrun.cli:main`). The launchers set `REMRUN_ROOT` and
`PYTHONPATH` for you; an editable install does not need them.

### Commands

```bash
remrun --version                     # show the installed/source version
remrun devices                      # list configured devices
remrun doctor                       # show config root, devices, project roots, state root
remrun plan macbox -- <cmd>         # show what a run would do; mutates nothing
remrun plan --auto --probe -- <cmd> # live-probe and show the same ranked target run would try
remrun run macbox -- <cmd>          # reconcile -> run remotely -> pull outputs back
remrun run --auto -- <cmd>          # probe/rank targets; conflict-safe failover
remrun run --durable --auto -- <cmd>  # auto-select once, then survive source disconnect
remrun run --durable macbox -- <cmd>  # explicit target; same durable behavior
remrun resume <run_id>              # recover logs, exit code, and pullback
remrun resume <run_id> --no-wait    # report authenticated target state and return
remrun run --scope spec_a --auto -- <cmd>  # opt-in declared write scope
remrun run macbox --memory-limit-mib 8192 -- <cmd>  # optional hard limit for a heavy first run
remrun status [DEVICE] [--limit N]  # recent runs, optionally filtered by target
remrun logs [last|<run_id>] [--json]
remrun clean [--older-than 30d] [--keep N] [--dry-run]   # prune state folder
remrun bench [targets] -- <cmd>     # time local vs. full remrun round-trip; recommend offload
remrun bench [targets] --no-local -- <cmd>   # skip local leg and assume offload
remrun sync <tree>/<sub> macbox     # project-less folder sync (pull-biased; see below)
remrun sync outputs/reports macbox --pull --dry-run   # only pull remote-newer; show plan, change nothing
remrun git-sync macbox              # sync Git commits with a peer without syncing .git/
remrun git-sync winbox --pull --branch main    # transfer main + tags; fast-forward main only
remrun git-sync winbox --pull       # on a repo-less project: bootstrap from the peer's history
remrun git-sync winbox --bootstrap  # same, explicit (working tree left untouched)
remrun git-sync macbox --status     # non-mutating branch/dirty/hook diagnostics
remrun git-sync --install-hook      # post-commit best-effort push to [git_sync].peers
remrun git-sync --uninstall-hook    # remove remrun's hook and restore any prior hook
remrun runner install macbox        # install + probe the inert versioned helper
remrun runner probe macbox          # verify the exact pinned helper and SQLite store
remrun resolve-unknown <run_id> --confirmed-ended  # clear a verified completion fence
```

### Durable runs

`--durable` is a narrow opt-in for unattended ordinary commands. After remrun records
a positive acknowledgement, a detached target-side supervisor owns the process and
bounded stdout/stderr spools. The command can finish while the source laptop sleeps or
its network disappears. `resume` from the same controller and project verifies the
saved identity, waits if needed, restores the exact exit code and logs, performs the
normal conflict-safe pullback once, and cleans the target record. Repeating `resume`
does not run the command or pull outputs a second time.

The first invocation still waits by default; interrupt or close it only after the
`durable_acknowledged` event is visible. If connectivity is lost before that positive
acknowledgement, remrun fails closed and the command must not be retried until its
completion state is resolved.

With `--auto`, remrun uses the ordinary reachability, learned RAM/duration, live-load,
memory-admission, and conflict-safe preflight selection path. It may shop another
candidate only before durable launch. Immediately before launch it records
`durable_target_bound`; from then on the selected target is permanent and any ambiguous
launch result fails closed instead of risking duplicate execution elsewhere.

This v1 surface deliberately supports one selected `ssh-posix` or `ssh-powershell`
target, noninteractive input, and the controller that created the run. It does not
support workload envelopes, target reboot restart, adoption from a different
controller, or a global fence against another controller starting an unrelated
ordinary run. Ordinary `remrun run` behavior is unchanged.

### Fleet utilities

Fleet mode is project-less: it stages explicit inputs, runs strictly declarative tasks
or an intrinsic exact command, and keeps its queue and telemetry outside project trees.
Configured workers exist only for a job or compatible burst; remrun requires no resident
worker service.

```bash
remrun fleet resources                         # CPU, load/queue, RAM, GPU/VRAM, disk
remrun fleet jobs [--device NAME]              # active jobs across all controllers
remrun fleet mesh [--no-hops]                  # measured SSH reachability matrix
remrun fleet plan zotomatic --input items/      # arbitrary configured name; preview only
remrun fleet run zotomatic --input items/       # synchronous placement + execution
remrun fleet submit zotomatic --input items/    # durable queue submission
remrun fleet submit zotomatic --input items/ --json  # queue IDs only; no live device probes
remrun fleet submit zotomatic --input items/ --json --preview-route  # add non-binding live route
remrun fleet submit zotomatic --device macbox --memory-limit-mib 8192 --input item.zot
remrun fleet command run --device macbox --memory-limit-mib 8192 -- tool --flag
remrun fleet dispatch --drain                   # batch compatible jobs, then exit
remrun fleet status                             # queue state and recent jobs
remrun fleet clear                              # release queue leases/cooldowns
remrun fleet cancel                             # clear queue and stop configured workers
```

The controller-side fleet queue requires WAL mode on SQLite 3.51.3 or later, or
the 3.50.7 / 3.44.6 backports. `remrun doctor` reports the controller's SQLite
version and queue journal mode. An older runtime may still act as a worker; only
commands that open its local fleet queue are refused.

`fleet resources`, `fleet jobs`, and `fleet mesh` are read-only. Resource probes read
bounded operating-system metadata rather than scanning files. Interactive resource
tables add rows as devices answer; JSON and redirected output remain deterministic.
RAM, VRAM, and primary-disk usage default to compact percentages; set
`[fleet.resources] usage_display = "amounts"` for used/total values. On macOS, disk
usage uses the system's important-usage capacity so automatically reclaimable space is
treated as available. On Windows, `LOAD` is ready waiters per core (`q`); on POSIX it is
one-minute runnable demand per core (`x`), so the two are related congestion signals but
not numerically identical.

Per-item submissions may share one compatible device invocation while retaining one
manifest and result row per prepared job. A worker that omits or mismatches required
evidence fails terminally and is not retried automatically, avoiding duplicate outputs.

`--memory-limit-mib N` is an operator-owned hard sampled process-tree RSS ceiling, not a
cost or demand estimate. It is available on configured `fleet plan`, `submit`, and `run`,
and on all three intrinsic `fleet command` actions. The selected target must have the
memory guard enabled and must admit the exact limit against its live ledger, per-command
ceiling, `max_jobs`, and host reserve. No-limit submissions retain the existing
`PreparedJobV1` identity; an explicit limit produces `PreparedJobV2` and binds the limit
and `submit-explicit` provenance into `prepared_id`. It does not change `work_id`, cost,
`memory_kind`, or learned profiles.

`fleet jobs` launch-side registration is a separate, real
activation seam and is **off by default**: ordinary `run` and fleet dispatcher commands
continue through the established `exec()` path unless the controller process explicitly
sets `REMRUN_FLEET_JOBS_OBSERVE=1`. Enable it only after the target's bounded native
Windows/macOS gates pass. Direct callers of the explicit `exec_observed()` transport API
are opting into that launch boundary themselves.

With that setting, `fleet jobs` also shows each in-progress `git-sync` as one
operation row: its project, source controller, peer, and direction. CPU, thread, and RAM
cells intentionally show `-`, because a Git sync spans local Git, transfer, and peer Git;
partial per-process numbers would be misleading. Simultaneous operations from different
projects appear as separate rows.

### Common operational pitfalls

The most common failure modes and their fixes:

1. **Windows controller: invoke from PowerShell or cmd, not Git Bash.** Git Bash
   mangles quoted argv after `--` (a `-c "a; b"` payload gets split on spaces).
   `C:\...\remrun\bin\remrun.cmd run macbox -- <cmd>` from PowerShell arrives clean.
2. **Python projects: bare `python` is usually NOT on the remote login-shell PATH.**
   Either set `[run] use_venv = true` in `<project>/do/remrun/remrun.toml`
   (activates the project-local `.venv` on the target device), or wrap the command:
   `remrun run macbox -- uv run python script.py` (uv resolves the project venv itself).
3. **Big gitignored data trees are NOT excluded by default** — remrun deliberately
   does not read `.gitignore` (outputs are often gitignored but load-bearing). A
   project with a multi-GB `data/` tree pays a multi-minute manifest/hash tax on
   *every* run until you add project excludes. Baseline overhead on a lean surface
   is ~5 s/round-trip. Fix once per project in `do/remrun/remrun.toml`:
   `[transfer] exclude = ["data/**", "tmp/**", ...]` (added to the global excludes;
   excluded paths are neither pushed nor pulled back — keep result/output dirs
   in-surface, or rely on Syncthing to deliver them).
4. **One writer per project, and a killed run can leave a stale lock.** Runs of the
   same project serialize across ALL devices (`--auto` failover included) — don't
   launch a second remrun job for a project while one is in flight. If a run is
   killed/crashes mid-flight, its lock persists and every later run fails with
   "already locked": check the printed lock path under
   `%LOCALAPPDATA%\remrun\locks\project\<hash>\whole.lock` (or
   `~/.local/state/remrun/locks/...`) and confirm the recorded holder PID is dead.
   If the command may have reached `command_started`, a dead controller PID is not
   enough: first inspect the named remote process and expected artifacts to prove
   the remote work ended. Only after both checks may the stale lock be moved aside
   or removed and the command retried.
5. **An automation timeout does not prove remrun exited.** A supervising tool may stop
   waiting while the local remrun process and remote command continue. Confirm the local
   process reached a terminal exit before starting another run for the project. A lock
   whose recorded PID is still alive is correct and must not be deleted.
6. **An SSH reset after `command_started` means completion is unknown.** The remote
   command may still be running even though the controller exits `4`. Do not immediately
   retry a mutating command. Read the persisted `completion_state=unknown` guidance and
   probe the runner's process/artifact state first.
7. **A Syncthing-delivered project usually has NO `.git` — that is normal, not
   broken.** `.git` is excluded from Syncthing, so a project arriving on a new device
   is a full working tree with no history. Do not `git clone` over it (that would fight
   the tree). Run `remrun git-sync <peer-with-history> --pull` once: it bootstraps the
   repo in place (`git init` + full-history fetch + HEAD set to the peer's tip) and
   leaves the working tree byte-for-byte untouched, so your uncommitted local work
   survives and shows up as modified/untracked vs the fetched HEAD.
8. **Default excludes drop `*.lock`, `dist/`, and `target/` — outputs written there are
   neither pushed nor pulled back.** These remain excluded as conventional generated/cache
   surfaces, so a lockfile a command regenerates (`uv.lock`, `renv.lock`) or a retained
   product written under `dist/`/`target/` will not return. `build/` is intentionally
   included because real build commands commonly put their requested product there.
   Project `[transfer] exclude` only adds patterns; sanity-check unusual output paths with
   `remrun plan`.
9. **Quote compound remote shell commands.** In
   `remrun run macbox -- cmd1 && cmd2`, the controller shell consumes `&&` and runs `cmd2`
   locally. Either issue two remrun calls or pass one remote shell argument, for example
   `remrun run macbox -- sh -lc '<cmd1> && <cmd2>'`.

`sync` converges a folder that lives **outside** the project tree (a configured fleet
output trees) with a device. It is **pull-biased**: the remote is usually the producer
and the local copy is behind it, so remote-newer wins (local may be older than remote),
local-newer pushes, and genuinely-ambiguous conflicts are saved aside under the state
root without ever clobbering a newer remote. Stateless (no baseline) → additive, never
deletes. Trees are named in `[sync_roots]` (`config/devices.toml`); `--remote <path>` is
an escape hatch for any folder. Flags: `--pull`/`--push`/`--both` (default), `--exclude`,
`--dry-run`, `--json`. A push reports `push_verified` only after a fresh remote manifest
confirms every pushed path is visible with the expected size and SHA-256; missing or
mismatched paths fail with exit `3`.

`action` places explicit files in a persistent target inbox and runs one named,
allowlisted target-side command from `[devices.<NAME>.actions.<action>]`. It is the
small reverse-control seam for operations such as delivering a prepared download to a
travel Mac and invoking an existing local trigger; it is not a second arbitrary-shell
interface. Inputs are never silently overwritten. A content-derived idempotency receipt
prevents a completed action from running twice and refuses ambiguous retries after a
disconnect. An action with no inputs requires an explicit `--key`.

```bash
remrun action macbox ingest --input ~/Downloads/input.zip
remrun action macbox ingest --input ~/Downloads/input.zip --dry-run
```

If the action consumes a prepared folder, sync that folder first:

```bash
remrun sync ~/Downloads/ready-batch macbox \
  --remote '~/Downloads/ready-batch' --push
```

The configured action determines what happens after staging; keep its command narrow
and allowlisted in the private device configuration.

`bench` targets default to the configured scheduler order. It records measured
profiles and prints a recommendation; it changes no run behavior. The local leg runs
the job on **this** machine, so only bench where running locally is acceptable — for a
job too heavy to run here, use `--no-local`.

`git-sync` exchanges Git history with one peer device using Git bundles over remrun's
existing transport. It deliberately does **not** sync `.git/` as ordinary files, because
Git internals are many small, churny files and make background sync tools work too hard.
Default direction is `--both` (pull then push); `--pull` and `--push` are available. Peer
branches are fetched into device-namespaced refs like `refs/remotes/winbox/main`, and local
or peer branches advance only by clean fast-forward. Divergence exits `2` and leaves both
branches untouched. A dirty checked-out branch is fetched but not advanced; clean it up
and merge/fast-forward manually. `git-sync` moves committed history, not uncommitted
worktree edits. Remote validation requires the mapped path itself to be the Git worktree
root, not merely a child of some parent repository; a failed probe reports the exact cwd,
argv, exit code, and raw stdout/stderr. `--status` is non-mutating: it uses a temporary bare
repository plus a peer bundle to report `up_to_date`, `ahead`, `would_fast_forward`, or
`diverged`, along with tracked-dirty flags, content/mode-only/untracked counts, and hook
diagnostics. On a guarded POSIX target, the fixed repository-root probe (and `--dry-run`)
runs under a built-in hard process-tree cap of at most 128 MiB. Full `--status`, pull,
push, and bootstrap paths are not classified as bounded metadata: bundle packing/fetching
and worktree scans can scale with repository size. They therefore require an explicit
positive integer `[git_sync].remote_memory_limit_mib` or the one-off
`--remote-memory-limit-mib <MiB>`. The value is a per-remote-Git-command hard cap, not a
learned RSS measurement; admission still reserves that allowance atomically and preserves
the target's configured host reserve. A guard refusal or termination is reported once and
is never reinterpreted as a missing ref, divergence, or permission to retry. Unguarded
targets retain the existing behavior. From a
repo-less tree it reports each peer branch as `bootstrap_available` and does not create
local Git metadata. A history-hub
fast-forward that preserves a dirty tree prints the same counts and explicitly warns that the
peer is not a clean-checkout build surface.

**Bootstrapping a repo-less project.** A Syncthing-synced tree (with `.git` excluded)
arrives on a new device as a full working tree with no Git metadata, while a peer holds
the authoritative history. `remrun git-sync <peer> --pull` (or the explicit `--bootstrap`)
on such a project seeds it: `git init` (with `core.autocrlf false`, plus `core.longpaths
true` on Windows so deep artifact paths >260 chars do not read as phantom modifications),
a full-history fetch of the peer's branches over the same bundle transport, and then it
points the local branch at the peer's HEAD with `update-ref` + `symbolic-ref` + `git reset
--mixed`. The working tree is left **byte-for-byte untouched** — never `reset --hard`,
checkout, or clean — because the arriving tree is typically *ahead* of history (uncommitted
work) and must survive. If the project has a `.githooks/` dir, `core.hooksPath` is set to
it. The report states: repo created, N commits fetched, HEAD set to `<sha>`, working tree
untouched, and M modified / K untracked vs HEAD. Degenerate cases are handled cleanly: an
unreachable peer, a missing peer repo, or an unborn/empty peer repo all report and leave
no half-initialized `.git` behind. Bootstrap verifies the transferred bundle, the peer HEAD
object/ref, the installed local HEAD/branch, and a nonzero commit count before reporting
success. A later `--pull` also recovers an existing empty/unborn `.git` (for example, one left
by an interrupted older bootstrap) without touching worktree bytes. `--push`-only on a
repo-less or unborn project refuses (nothing to push); `--bootstrap` on a nonempty existing
repo refuses.

`git-sync --install-hook` installs a marked `.git/hooks/post-commit` wrapper. The hook
starts best-effort background `git-sync <peer> --push --quiet` jobs for `[git_sync].peers`
(or for the device passed to `--install-hook`), then exits immediately; offline peers do
not block a commit. If a prior `post-commit` hook exists, remrun backs it up beside the
hook and restores it on `--uninstall-hook`. Hook output is appended to a small bounded log
under the controller state root (`logs/gitsync-hook/<project>.log`) so silent background
skips are inspectable.

The safe default refuses to advance a dirty checked-out branch. For a dedicated Git-history
hub—or an arriving controller—whose worktree bytes travel independently through Syncthing,
`advance_dirty_worktree = true` may be set in the global `[git_sync]` block (or the project's
`[git_sync]` hints). A proved fast-forward in either direction then uses `git reset --mixed`:
HEAD and the index advance, but no worktree file is created, changed, or deleted. Missing or
differing Syncthing bytes remain visibly dirty until they converge.

For repositories that sit beside the normal project tree, configure
`[git_sync.project_roots]` with a broader common parent. The broader mapping applies only
to Git history exchange; `run`, `plan`, and `bench` keep the narrower `[project_roots]`
transfer boundary. Git-sync first preserves the project leaf detected under that ordinary
boundary, then rebases its project ID onto the broader root; this also keeps marker-less
repo-bootstrap/status paths at the same leaf. A repository at `work/remrun` can then exchange
with a peer alongside ordinary `work/proj/foo` repositories without a one-off config override.

For cross-platform synced working trees, prefer a project `.gitattributes` such as
`* text eol=lf`. Otherwise Windows `core.autocrlf` can rewrite checked-out file line
endings, and a background file sync can make the Mac checkout look like it has tracked
local edits. `git-sync` will correctly refuse to advance a tracked-dirty checkout in
that case.

`runner install` installs and verifies the optional versioned helper used to evaluate
crash-safe multi-controller coordination. It initializes a target-local participant store
and checks protocol, filesystem, and SQLite prerequisites. It does **not** change ordinary
`run` behavior; `[coordination] mode = "legacy"` remains the supported default. Most users
do not need to install the helper.

Target forms for `run`/`plan`: an explicit device (`macbox`, `winbox`, `LOCAL_SIM`),
`--auto`, the literal `auto`, or omit it (defaults to auto). Run flags:
`--dry-run`, `--no-pullback`, `--no-telemetry`, `--json`.

Exit codes: `0` ok · `1` config/internal · `2` conflict (pre-run, **or** an
unresolved post-run divergence — local edited during the run and differs from the
command's output; the command may have exited 0, see `command_exit_code` in the
summary) · `3` transfer failure · `4` remote-exec/unreachable · otherwise the remote
command's own code.

### LOCAL_SIM (no remote needed)

`LOCAL_SIM` simulates a remote on the local filesystem — useful for trying the
reconcile/pullback flow without SSH. It maps the project into
`/tmp/remrun-sim/projects/<project>`.

## Configuration

### Devices — `config/devices.toml` (synced, no secrets)

Per device: `kind` (`ssh-posix` / `ssh-powershell` / `local-sim`),
`address_candidates`, `project_root`, plus optional:

- `user`, `remote_python`, `ssh_opts`, `tailscale_ip`
- `login_shell` / `shell` (POSIX: default `bash -lc` so the remote PATH matches
  your normal environment — needed to find e.g. Homebrew's `Rscript`; Windows
  `ssh-powershell` targets require `shell = "pwsh"` with PowerShell 7.3 or newer,
  and reject top-level `.cmd`/`.bat` commands)
- `venv_root` — base dir for external per-project virtualenvs, used only when a
  project sets `[run] venv_layout = "external"` (the default is project-local `.venv`)
- `path` (list, prepended to PATH) and `[devices.<NAME>.env]` (env vars) — declare
  per-device tool locations here, especially on Windows where there's no login shell

`config/devices.toml` may also declare a Git-only common parent:

```toml
[git_sync]
peers = ["macbox"]
# Optional history-hub behavior; advances HEAD+index while preserving worktree bytes.
advance_dirty_worktree = true

[git_sync.project_roots]
macos = "~/work"
windows = 'C:\work'
default = "~/work"
```

The experimental versioned runner is explicitly disabled by default:

```toml
[coordination]
mode = "legacy"       # ordinary single-writer coordination
device = "macbox"     # optional experimental coordination target
protocol = 1
```

### Project hints — `<project>/do/remrun/remrun.toml` (optional)

Never required; only optimizes behavior. See `examples/project/do/remrun/remrun.toml`.
For a linked Git worktree allowed with `REMRUN_ALLOW_WORKTREE=1`, a config inside that
worktree wins. If absent, remrun reads the main worktree's private config as identified by
`git worktree list --porcelain`; it does not copy the file, track it, or change the worktree's
project root.

```toml
[run]
use_venv = true            # project-local .venv on each device (bin/ or Scripts\ on PATH, VIRTUAL_ENV set)
# venv_layout = "external" # instead use <device.venv_root>/<project leaf>
# [run.venv] macbox = "~/venvs/foo"   # or pin explicit paths per device

[env]
OMP_NUM_THREADS = "4"      # env vars for every command in this project

[placement]
primary = "macbox"
fallback = ["winbox"]
[[placement.rules]]
match_command = "stata|\\.do$"
prefer = "winbox"

[git_sync]
peers = ["macbox", "winbox"]  # used by `remrun git-sync --install-hook`
# Guarded targets require an explicit hard cap for bundle/status/push/pull work.
# remote_memory_limit_mib = 2048  # illustrative only; select for the target/repository

[transfer]
exclude = ["data/raw/**", "scratch/**"]   # narrow the active surface (ADDED to global excludes)

[parallel.scopes.spec_a]
paths = ["results/spec_a/**", "logs/spec_a/**"]
```

Write scopes are optional and conservative. A scoped run must name a configured
scope and returns a conflict if the remote command changes a path outside that
scope, preserving the escaped remote file under the state root rather than pulling
it into the project. Scoped and unscoped runs currently serialize per project; the
scope is a safety/validation boundary, not a parallel-writer guarantee.

By default the venv is **project-local** — `<project>/.venv` on each device. It is
device-local and not synced (it holds platform binaries; `.venv` is excluded from
transfer). Create it yourself on each device; `use_venv` just activates it. Set
`venv_layout = "external"` to instead use `~/venvs/<project>` (macOS) /
`C:\venvs\<project>` (Windows) — useful only for a project synced by a raw
cloud-storage mount that would churn on an in-tree `.venv`.

## State, retention, telemetry

remrun keeps its own bookkeeping **outside** the project tree, in a user path:
`%LOCALAPPDATA%\remrun` (Windows) / `~/.local/state/remrun` (macOS), override with
`REMRUN_STATE_ROOT`. It holds per-run journals (`summary.json`, logs, manifest
snapshots), per-(device,project) baselines (for safe deletes), conflict backups,
and locks. All regenerable and safe to delete.

It is **self-limiting** and kept lean (defaults ~3–7 days): logs are capped
(`max_full_log_mb`), and after each `run` **and** `sync` the state is pruned by the
tiered `[logging]` policy in `config/defaults.toml` — strip heavy artifacts after
`full_log_retention_days` (3), delete the run dir after `summary_retention_days` (7).
The **rollback/backup area** (`conflicts/<id>/backup` — the prior version of any file a
run/sync overwrites or deletes, so a mistake is recoverable) is bounded three ways so it
can't balloon on large media: files over `backup_below_mb` (50) aren't snapshotted,
snapshots are deleted after `backup_retention_days` (3), and a hard `max_backup_mb`
(1024) budget prunes the oldest beyond it. Prune on demand with `remrun clean`
(`--older-than` / `--keep` / `--dry-run`).

Each run records `peak_rss_mb` and `avg_cpu_pct` in the summary and `status`
(stdlib-only). ssh-powershell reads whole-tree peak memory from its Win32 Job Object;
ssh-posix samples the concurrent RSS sum of the known process tree and drains that tree
before accepting the measurement. CPU% is whole-tree on both. Disable optional
telemetry with `--no-telemetry` or `[telemetry] enabled = false`. On a device with a
configured `memory_guard`, those controls suppress optional metrics only; the hard
preflight, command ceiling, host reserve, and fail-safe cleanup remain active.

## Additional portability notes

- **SSH aliases:** put the authenticated alias or IP first in `address_candidates`.
  The backend tries candidates in order and lands on a working one.
- **Login shell matters:** a bare ssh shell may have only `/usr/bin:/bin:...`
  (missing tools from Homebrew or user shell setup). `login_shell` (default on) fixes the PATH.
- **Tilde paths:** the backend resolves the remote `$HOME` at probe time and
  expands `~` itself (shell `~` is blocked by quoting).
- **`python` vs `python3` differ per device:** set `remote_python` per device if one
  name is a launcher stub or missing from the SSH session PATH.
  (`python` is absent). A command using a bare `python`/`python3` token will not run on
  both. `bench` sends the *same* command to every leg, so for a portable benchmark use a
  no-arg builtin (e.g. `whoami`), a project venv (`[run] use_venv`, which puts the right
  interpreter first on PATH), or bench one device at a time.

## Developing / testing

Clone the repository, then run tests from a virtualenv **outside** the synced tree
(so it isn't replicated to other devices):

```bash
git clone https://github.com/smkwray/remrun.git
cd remrun
python -m venv /tmp/remrun-venv          # NOT inside this repo
/tmp/remrun-venv/bin/pip install pytest ruff
PYTHONPATH=src:. /tmp/remrun-venv/bin/pytest
/tmp/remrun-venv/bin/ruff check src tests
```

Do not leave `.venv` / `__pycache__` / `.pytest_cache` in the synced tree.

## Docs map

- `docs/ARCHITECTURE.md`, `TRANSFER_MODEL.md`, `REMOTE_PROTOCOL.md`,
  `CONFIGURATION.md`, `PROJECT_CONFIG.md`, `AGENT_OUTPUT_SPEC.md` — design contracts.
- `docs/PUBLIC_RELEASE.md` — publication/update checklist.
