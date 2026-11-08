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
POSIX targets can opt into a RAM-relative memory guard that admits work before
project mutation and terminates only the protected command tree if an explicit owner
limit or the host reserve is breached. An unknown command receives all capacity that
is live and fully backed above the host reserve, bounded by an explicitly configured
per-command maximum when one is present; it is never assigned a smaller allowance
merely because no profile exists. Learned commands use their observed process-tree RSS
high-water mark plus guard headroom for admission/concurrency only. Learned and
unprofiled allowances are not RSS kill ceilings; receipts state whether a command
limit is enforced. The guard cannot govern
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

On an observed Windows run, Ctrl-C attempts to terminate only remrun's exact named
Job Object and reports `owned_job_cancelled`. Observation remains optional and permits
programs to break away for compatibility with shared build jobs. Escaped processes and
interruptions during startup remain unproved, so cancellation always retains the
completion-unknown fence until `resolve-unknown --confirmed-ended`.

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
remrun cancel <run_id>              # stop that run's own processes, then finalize like resume
remrun run --scope spec_a --auto -- <cmd>  # opt-in declared write scope
remrun run macbox --memory-limit-mib 8192 -- <cmd>  # optional hard limit for a heavy first run
remrun run macbox --wait-lock 600 -- <cmd>  # wait up to 600 s for a running holder of the project lock
remrun run macbox --transfer none -- <cmd>  # run in the target's checkout as it is; move no files
remrun run macbox --transfer none --verify src/model.py -- <cmd>  # first prove that file matches
remrun run macbox --transfer commit --output results/ -- <cmd>  # run HEAD privately; return results/
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

While waiting, a failed status read (an SSH drop, a network that comes back after
sleep) is retried with a growing delay for up to five minutes; status reads never
touch the job. If the target stays unreachable after acknowledgement, the wait stops
with `durable_detached`, exit code `4`, and the summary's `guidance` names the
`remrun resume <run_id>` command that reattaches. Only `resume --no-wait` and an
interrupt after acknowledgement stop waiting with exit code `0`. A durable summary's
`duration_sec` is the command's runtime on the target's own clock, and a successful,
attributable durable run updates the learned job costs like an ordinary run.

`remrun cancel <run_id>` stops a durable run, from the same controller and project as
`resume`. It records the request with the run's saved state, then asks the target's job
observer to stop exactly the processes it registered for that run: on POSIX the run's
own process group, signalled only after the run's root process, matched by PID and start
time, is seen inside it: SIGTERM first, so the command can flush or checkpoint, then
SIGKILL for anything still running ten seconds later, sent only while a process seen in
the group at that check is still in it; on Windows the run's own Job
Object, terminated at once. No process
group is ever taken from a process listing. Processes the command deliberately moved to
another group, session or Job are not stopped. Once the target confirms the processes
are gone, `cancel` finalizes like `resume`: it reports the stopped command's exit code
(usually 143 on POSIX, 137 if it had to be killed) and pulls outputs back as after any other nonzero exit. A run that had
already finished is finalized exactly as `resume` would. If another remrun process is
still waiting on the run (the original `run --durable`, or a `resume`), `cancel` only
stops the command and exits `0`; the waiting process finalizes it, but only once the
cancel has confirmed the processes are gone (it waits up to two minutes for that and
reports `cancel_pending`). If the target's durable state there has failed closed,
`cancel` instead exits `4` with completion unknown. A cancel that signalled the command but could not confirm
its processes are gone leaves completion unknown, for any later `resume` too: prove
the processes ended, then use `resolve-unknown`. `cancel` stops
nothing while the run is still starting or when the target holds no record with the
run's exact identity; it then exits `4`, completion stays unknown, and the guidance
names `remrun resume <run_id>`. Events: `cancel_requested`, `cancel_refused`,
`cancel_handed_off`, `cancel_pending`, `completion_unknown`.

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

### Untransferred runs (`--transfer none`)

`--transfer none` runs the command in the target's existing checkout of the project,
as that checkout is. remrun does not reconcile the tree before the command, does not
pull outputs back after it, and records no baseline. Use it when the target already
holds the code and data (for example because Syncthing keeps it current) and you want
several long jobs of one project to run at once, or the project is too large to scan
on every run. It works with `--durable`, `--auto` and `--label`.

- The checkout, including the directory you run from, must already exist on the
  target; remrun never creates or changes files in it. With `--auto`, a target without
  the checkout is skipped.
- remrun assumes the target's tree is current. `--verify PATH` (repeatable; a file
  path relative to the project root, no globs) proves it for the files that matter:
  before starting, remrun compares the SHA-256 of the local file with the target's
  copy. If they differ, or the target cannot hash its copy, the run stops with exit `2`
  before the command starts; with `--auto` the next target is tried. The verified
  digests are recorded under `verify` in the run's `summary.json`.
- Outputs stay where the command wrote them on the target. Syncthing, or a later
  ordinary run, brings them back.
- No project lock is taken, so untransferred runs of one project can run side by
  side. Each records its own completion fence, which `resume` and `resolve-unknown`
  clear for that run only. An untransferred run still refuses to start while an
  ordinary run of the project has unknown completion. An ordinary (reconciling) run
  refuses a target while an untransferred run of the project may still be working
  there (`target-busy-untransferred`; `--auto` tries another target), so it never
  changes files under that run or pulls back its half-written outputs. Likewise, an
  untransferred run refuses a target where an ordinary run of the project may still
  hold the project lock (`target-busy-ordinary`; `--auto` tries another target).
- A project that declares `[run.bootstrap]` must already be set up on the target. An
  untransferred run checks readiness but never runs setup; run one ordinary run there
  first.
- It cannot be combined with `--scope` or a resource workload. A successful
  untransferred run updates the learned job costs like an ordinary one.

`[transfer] mode = "none"` in `config/defaults.toml` makes this the default; then
`--transfer safe` gives one run the ordinary behavior. `plan` accepts both flags.

### Commit runs (`--transfer commit`)

```
remrun run DEVICE --transfer commit --output PATH [--output PATH]... [--input PATH]... -- <cmd>
```

`--transfer commit` runs the command on the project's committed `HEAD` in a private
directory under the target's `state_root` (`<state_root>/work/<id>/tree`), then pulls
back only the `--output` paths. It never reads or writes the target's own checkout of
the project. Use it for long jobs that must run on exactly one commit, several at a
time per project, without the target's checkout changing under them. It works with
`--durable`, `--auto`, `--label`, `resume` and `cancel`. The target needs `git` on its
`PATH`.

- **Paths.** `--output` (required, repeatable) and `--input` (repeatable) take paths
  relative to the project root, without globs. An output ending in `/` is a directory
  and everything under it, including names a `[transfer] global_exclude` pattern
  matches (`results/x.tmp`, `results/__pycache__/`). An output that is itself under a
  global exclude pattern (`.git/`, `node_modules/`) is refused.
- **Source.** The project root must be its git repository's top level. A tracked file
  that differs from `HEAD` refuses the run (exit `2`) unless it is under an `--output`
  or named by `--input`. `--input FILE` copies that working-tree file (tracked or not)
  over the commit before the command starts. It must be a regular file with no symlink
  at or above it, in the working tree or in `HEAD`, so the copy can neither read from
  nor write outside the project's own tree. The commit and each input's size and
  SHA-256 are recorded under `source` in `summary.json`. Nothing else uncommitted
  reaches the command. Submodules are refused, and Git LFS content is not fetched.
- **Seeding.** remrun keeps one bare git cache per project under
  `<state_root>/work/git/` on the target and sends it a git bundle: the whole history the
  first time, then only commits it lacks. Each run clones that cache into its own tree
  (with `core.longpaths` on) and checks out the commit.
- **Disk floor.** Before writing anything to the target, remrun measures the free
  space of the disk that holds its `state_root`. If the tree, the inputs and the bundle
  would leave less than `[transfer] work_min_free_fraction` (default `0.15`) of the disk
  free, or the free space cannot be measured, or `git` is missing, that target is
  refused (exit `2`; with `--auto` the next one is tried).
- **Outputs.** After the command, its changes under the `--output` paths come back
  through the ordinary pullback: unchanged local files are backed up and replaced, with
  the result hash-checked; an output you edited locally during the run is kept, the
  run's copy is saved under the run's conflict directory, and the run exits `2`. The
  outputs come back after a failed or cancelled command too.
- **Other writes.** A file written or deleted outside the outputs that the project's
  transfer excludes cover is scratch: counted (`scratch_writes`) and discarded. Any
  other is an unexpected write: listed in the run's `work` record, never copied into
  the project, and the run exits `2`.
- **Cleanup.** The private tree is removed only after exit `0`, a conflict-free
  pullback and no unexpected writes. Otherwise it stays on the target with the reason
  in `summary.json` (`work.state = "retained"`), and the next commit run from this
  controller to that target removes retained trees older than
  `[logging] failed_log_retention_days`. A removal that fails is reported and retried by
  `resume` and by the next commit run there.
- **Concurrency.** No project lock is taken. Each run holds a lease on its outputs
  until its pullback ends: a commit run whose outputs overlap a live one's (one path
  is a prefix of the other) is refused before any target is touched. Commit runs with
  disjoint outputs, and ordinary runs, may run at the same time. A commit run still
  refuses while an ordinary run of the project has unknown completion.
- **Environment.** The command runs in `tree/<your relative directory>`. The project's
  virtual environment is the target's usual one (an absolute path), so a declared
  `[run.bootstrap]` must already be ready there; a commit run never runs setup. Code
  the environment imports from an editable install may come from the target's
  checkout rather than the commit; put `src` first on the import path to be sure.
- It cannot be combined with `--scope`, `--no-pullback`, `--verify` or a resource
  workload. A clean commit run updates the learned job costs.

### Fleet utilities

Fleet mode is project-less: it stages explicit inputs, runs strictly declarative tasks
or an intrinsic exact command, and keeps its queue and telemetry outside project trees.
Configured workers exist only for a job or compatible burst; remrun requires no resident
worker service.

```bash
remrun fleet resources                         # CPU, load/queue, RAM, GPU/VRAM, disk
remrun fleet jobs [--device NAME]              # active jobs across all controllers
remrun observe --label "regen" -- CMD...       # run CMD on this machine, visible to fleet jobs
remrun fleet mesh [--no-hops]                  # measured SSH reachability matrix
remrun fleet presence [--device NAME] [--in M]  # owner presence windows (no probe)
remrun fleet plan zotomatic --input items/      # arbitrary configured name; preview only
remrun fleet plan zotomatic --input items/ --save --json  # save exact prepared placement
remrun fleet run zotomatic --input items/       # synchronous placement + execution
remrun fleet submit zotomatic --input items/    # durable queue submission
remrun fleet submit zotomatic --input items/ --request-id ui-action-17
remrun fleet submit --plan PLAN_ID --json        # consume/replay one exact saved plan
remrun fleet submit zotomatic --input items/ --json  # queue IDs only; no live device probes
remrun fleet submit zotomatic --input items/ --json --preview-route  # add non-binding live route
remrun fleet submit zotomatic --device macbox --memory-limit-mib 8192 --input item.zot
remrun fleet run zotomatic --input item.zot --return-root ./returned
remrun fleet storage enroll /path/to/shared/root
remrun fleet storage bind --device winbox 'Z:\shared-root'
remrun fleet command run --device macbox --memory-limit-mib 8192 -- tool --flag
remrun fleet dispatch --drain                   # batch compatible jobs, then exit
remrun fleet dispatch --submission ID           # drain only this immutable submission
remrun fleet status                             # queue state and recent jobs
remrun fleet status --request-id ui-action-17 --json  # exact accepted submission
remrun fleet clear                              # release queue leases/cooldowns
remrun fleet cancel                             # clear queue and stop configured workers
```

When `--priority` is present on `fleet plan --save`, it is frozen into the saved
plan and is not supplied again to `fleet submit --plan`.

`--preview-route` is valid only with `--json`. Fixed placement snapshots use a
guardless probe transport because they run only remrun-owned resource/capability scripts; they
do not stage the arbitrary-command memory-guard helper. A device is measured locally only when
its synced declaration explicitly uses `role = "controller"`, its OS/backend matches this process,
and both its device key and every configured address alias corroborate the local host. Ambiguous,
unresolved, or contradictory identity falls back to the normal transport path; `local-sim` is never
replaced by controller evidence.

Ordinary `fleet plan` remains a non-binding preview. `fleet plan --save` succeeds only
when every prepared item is placed exactly once; it freezes those selected devices into
the normal prepared-job identities and returns a controller-local `plan_id`. Submitting
that token enqueues those exact prepared records atomically. Repeating the token after a
lost response returns the original submission and job IDs. A caller may supply
`--request-id` without a saved plan for exact correlation and lookup. Reusing a request
identity for different prepared work fails closed. Multi-device saved plans use each
adapter's configured output root; a single controller-supplied `--output-root` is rejected
for such a split. `fleet dispatch --submission` and `--request-id` are self-terminating
scoped drains and never claim unrelated queued work.

The controller-side fleet queue requires WAL mode on SQLite 3.51.3 or later, or
the 3.50.7 / 3.44.6 backports. `remrun doctor` reports the controller's SQLite
version and queue journal mode. An older runtime may still act as a worker; only
commands that open its local fleet queue are refused.

An otherwise qualified task with no duration profile is placed on exactly one
deterministically selected device and reports `Uncalibrated placement; no duration estimate.`
Unknown time is JSON `null`, never zero. `fleet dispatch --drain --json` always emits one final
`DrainResultV1` document; unplaceable queued work exits 2 rather than silently succeeding.
Each dispatch lifecycle event on stderr carries
`"schema":"remrun.fleet.lifecycle","version":1`.

`fleet resources`, `fleet jobs`, and `fleet mesh` are read-only. Resource probes read
bounded operating-system metadata rather than scanning files. Interactive resource
tables add rows as devices answer; JSON and redirected output remain deterministic.
RAM, VRAM, and primary-disk usage default to compact percentages; set
`[fleet.resources] usage_display = "amounts"` for used/total values. On macOS, disk
usage uses the system's important-usage capacity so automatically reclaimable space is
treated as available. On Windows, `LOAD` is ready waiters per core (`q`); on POSIX it is
one-minute runnable demand per core (`x`), so the two are related congestion signals but
not numerically identical. Windows resource collection is delivered through a fixed-size
PowerShell control-source launcher (script bytes on stdin, not on the SSH command line);
`reachable` remains an auth/network fact and `probe_status` reports delivery, execution,
timeout, and protocol outcomes so a failed probe cannot look blank-but-healthy.

Per-item submissions may share one compatible device invocation while retaining one
manifest and result row per prepared job. A post-launch loss of completion evidence becomes
`completion_unknown` and holds the idempotency key for explicit review; it is never replayed
automatically under `at-most-once-v1`. A verified work-measure mismatch also enters review and
is excluded from duration learning.

`--memory-limit-mib N` is an operator-owned hard sampled process-tree RSS ceiling, not a
cost or demand estimate. It is available on configured `fleet plan`, `submit`, and `run`,
and on all three intrinsic `fleet command` actions. The selected target must have the
memory guard enabled and must admit the exact limit against its live ledger, per-command
ceiling, `max_jobs`, and host reserve. No-limit submissions retain the existing
`PreparedJobV1` identity; an explicit limit produces `PreparedJobV2` and binds the limit
and `submit-explicit` provenance into `prepared_id`. It does not change `work_id`, cost,
`memory_kind`, or learned profiles.

`fleet jobs` launch-side registration is **off by default** and is activated per target:
ordinary `run` and fleet dispatcher commands continue through the established `exec()` path
unless the target device declares `observe_jobs = true`. Declare it only after
`native-gates/fleet_jobs_native_gate.py` passes for that target. Direct callers of the
explicit `exec_observed()` transport API are opting into that launch boundary themselves.

A job registered before the target's current boot is never shown: a restart ends every
process, so `fleet jobs` counts it as stale (`registry.stale_hidden`) instead of listing it as
`UNKNOWN`, and reports the boot time it used as `registry.boot_time_unix_ns`. Only jobs that
started more than 10 minutes before the boot are hidden, which absorbs small clock corrections
made after boot. If the target's boot time cannot be read, the field is `null` and nothing is
hidden on that basis. On Windows, a shutdown with Fast Startup enabled does not count as a
boot; use Restart, or disable Fast Startup, for jobs from before it to clear.

`remrun observe [--label TEXT] -- CMD...` runs a command on this machine under the same
observer, so a job started natively (without `run`) appears in every controller's `fleet jobs`
with its project directory, label, threads, and RAM. The command inherits remrun's standard
streams; on POSIX with a controlling terminal it becomes the foreground job, so prompts, input,
and Ctrl-C work as they would without remrun. It returns the command's exit status, which
observation or terminal cleanup never replaces. This machine must be a configured device
declaring `observe_jobs`; otherwise the command still runs, unobserved, with a warning.

When this controller declares `observe_jobs`, `fleet jobs` also shows each in-progress
`git-sync` as one
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
   (activates the configured venv), optionally declare `[run.bootstrap]` so the first
   run creates and populates it from pinned project inputs, or wrap the command:
   `remrun run macbox -- uv run python script.py` (uv resolves the project venv itself).
3. **Big gitignored data trees are NOT excluded by default** — remrun deliberately
   does not read `.gitignore` (outputs are often gitignored but load-bearing). A
   project with a multi-GB `data/` tree pays a multi-minute manifest/hash tax on
   *every* run until you add project excludes. Baseline overhead on a lean surface
   is ~5 s/round-trip. Fix once per project in `do/remrun/remrun.toml`:
   `[transfer] exclude = ["data/**", "tmp/**", ...]` (added to the global excludes;
   excluded paths are neither pushed nor pulled back — keep result/output dirs
   in-surface, or rely on Syncthing to deliver them).
4. **One writer per project; a killed run's lock is reclaimed only when that is safe.**
   Runs of the same project serialize across ALL devices (`--auto` failover included).
   A second run fails at once with "already locked" while the holder's process is
   running; pass `--wait-lock SECONDS` to wait for it instead (one `lock_wait` event,
   then polling with backoff). It also waits out the running holder's own
   unknown-completion fence, but refuses if that run ends and leaves the fence behind,
   and never waits on a fence with no running holder. If a run is killed or the machine
   sleeps, its lock under
   `%LOCALAPPDATA%\remrun\locks\project\<hash>\` (or `~/.local/state/remrun/locks/...`)
   stays behind. The next run removes it and reports `stale_lock_reclaimed` (old
   run_id, pid, target, scope, acquired_at) when the recorded PID has provably exited on
   this machine and no unknown-completion hazard is recorded for the project. Otherwise
   it refuses and says why after `not reclaimed:`: the PID is running, the lock
   metadata is missing or unreadable, its PID cannot be checked, or the killed run may
   have left a remote command running (item 6). Never delete a lock by hand while that
   hazard stands; resolve it first.
5. **An automation timeout does not prove remrun exited.** A supervising tool may stop
   waiting while the local remrun process and remote command continue. Confirm the local
   process reached a terminal exit before starting another run for the project, or use
   `--wait-lock`. A lock whose recorded PID is still alive is correct and must not be
   deleted.
6. **An SSH reset after `command_started` means completion is unknown.** The remote
   command may still be running even though the controller exits `4`. Do not immediately
   retry a mutating command. Read the persisted `completion_state=unknown` guidance and
   probe the runner's process/artifact state first. To stop such a command by hand,
   signal its own process IDs. On POSIX, remrun's remote helper leads a process group of
   its own when job observation, telemetry or the memory guard wraps the run (telemetry
   and the memory guard keep the terminal's group if the SSH session has a terminal). A
   run with none of these starts the command directly in the SSH session's process
   group, which some SSH servers share with other sessions, so never signal that group.
7. **A Syncthing-delivered project usually has NO `.git` — that is normal, not
   broken.** `.git` is excluded from Syncthing, so a project arriving on a new device
   is a full working tree with no history. Do not `git clone` over it (that would fight
   the tree). Run `remrun git-sync <peer-with-history> --pull` once: it bootstraps the
   repo in place (`git init` + full-history fetch + HEAD set to the peer's tip) and
   leaves the working tree byte-for-byte untouched, so your uncommitted local work
   survives and shows up as modified/untracked vs the fetched HEAD.
8. **Default excludes drop `dist/` and `target/` — outputs written there are
   neither pushed nor pulled back.** These remain excluded as conventional generated/cache
   surfaces. Dependency lockfiles such as `Cargo.lock`, `uv.lock`, and `renv.lock` are
   included; projects can exclude their own transient locks. `build/` is intentionally
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

With `--branch NAME`, status, pull, push, and bootstrap bundle/fetch exactly that named
branch plus tags; no other branch refs are transferred. Without `--branch`, the established
all-branches-plus-tags behavior remains. An explicit empty or invalid branch name fails before
transport. For an existing repository, a missing named source branch remains a clean
`missing_peer_ref` / `skipped_missing_peer_ref` result while tags still transfer; it is not
reinterpreted as a bundle failure.

**Bootstrapping a repo-less project.** A Syncthing-synced tree (with `.git` excluded)
arrives on a new device as a full working tree with no Git metadata, while a peer holds
the authoritative history. `remrun git-sync <peer> --pull` (or the explicit `--bootstrap`)
on such a project seeds it: `git init` (with `core.autocrlf false`, plus `core.longpaths
true` on Windows so deep artifact paths >260 chars do not read as phantom modifications),
an all-branch-plus-tags fetch over the same bundle transport by default. With `--branch
NAME`, bootstrap instead fetches only that named branch plus tags and points the local branch
at that ref; otherwise it points the local branch at the peer's HEAD. Both paths use
`update-ref` + `symbolic-ref` + `git reset --mixed`. The working tree is left
**byte-for-byte untouched** — never `reset --hard`,
checkout, or clean — because the arriving tree is typically *ahead* of history (uncommitted
work) and must survive. If the project has a `.githooks/` dir, `core.hooksPath` is set to
it. The report states: repo created, N commits fetched, HEAD set to `<sha>`, working tree
untouched, and M modified / K untracked vs HEAD. Degenerate cases are handled cleanly: an
unreachable peer, a missing peer repo, or an empty peer repo all report and leave no
half-initialized `.git` behind. Without `--branch`, an unborn/invalid peer HEAD also refuses;
with `--branch`, that named ref remains the authority. Bootstrap verifies the transferred bundle,
the selected branch object/ref (or peer HEAD when no branch is explicit), the installed local
HEAD/branch, and a nonzero commit count before reporting success. A later `--pull` also recovers
an existing empty/unborn `.git` (for example, one left
by an interrupted older bootstrap) without touching worktree bytes. `--push`-only on a
repo-less or unborn project refuses (nothing to push); `--bootstrap` on a nonempty existing
repo refuses.

`git-sync --install-hook` installs a marked `.git/hooks/post-commit` wrapper. The hook
starts best-effort background `git-sync <peer> --push --quiet` jobs for `[git_sync].peers`
(or for the device passed to `--install-hook`), then exits immediately; offline peers do
not block a commit. If a prior `post-commit` hook exists, remrun backs it up beside the
hook and restores it on `--uninstall-hook`. Hook output is appended to a small bounded log
under the controller state root (`logs/gitsync-hook/<project>.log`) so silent background
skips are inspectable. The hook names the Python interpreter by the path Python reports
(a package manager's stable link), not its versioned install folder, so a patch upgrade
does not break it. `git-sync --status` reports `hook_interpreter_ok` and emits
`git_sync_hook_broken` when the interpreter a hook names no longer exists; reinstall the
hook to repair it.

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
`--dry-run`, `--no-pullback`, `--no-telemetry`, `--json`, `--allow-large-push` (skip the
push guard below for one run), `--transfer safe|none|commit`, `--verify PATH`,
`--output PATH` and `--input PATH` (see untransferred and commit runs above), and
`--label TEXT` (a short
name `fleet jobs` shows instead of the executable; spaces are stored as underscores). An owner
who deliberately accepts
the target's memory risk may use `--bypass-memory-guard` with an explicitly named target. The
override skips admission, reservation, and the command guard; it is rejected for automatic
placement and cannot be combined with `--memory-limit-mib`.

Exit codes: `0` ok · `1` config/internal · `2` conflict (pre-run, **or** an
unresolved post-run divergence — local edited during the run and differs from the
command's output; the command may have exited 0, see `command_exit_code` in the
summary) · `3` transfer failure · `4` remote-exec/unreachable · `5` a safety guard
refused (memory policy, or the push guard: `phase = "preflight"` with a `push_guard`
record) · otherwise the remote command's own code.

**Push guard.** Before transferring anything, preflight refuses a push of more than
`[transfer] max_push_files` files or `max_push_mb` MB (defaults in `config/defaults.toml`:
10,000 files, 4096 MB) and lists the directories holding the most files and bytes. Usually
the fix is a project exclude for a scratch or cache tree. For a legitimate first sync of a
large project, rerun with `--allow-large-push`, or raise either key in the project's
`do/remrun/remrun.toml` `[transfer]` table. `remrun plan --probe` reports the push the
target would get (`push` in `--json`) and whether a run would be refused.

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
- `venv_root` — absolute (or `~`-relative) base dir for external per-project
  virtualenvs. It is required by `[run.bootstrap]`; ordinary `use_venv` runs may
  still use the project-local `.venv` default.
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
use_venv = true            # required by [run.bootstrap]
# venv_layout = "external" # required by [run.bootstrap] unless [run.venv] overrides
# [run.venv] macbox = "~/venvs/foo"   # must be beneath device.venv_root for bootstrap

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

By default an ordinary `use_venv` run uses a **project-local** `<project>/.venv` on
each device. It is device-local and not synced (it holds platform binaries; `.venv` is
excluded from transfer). A project that declares `[run.bootstrap]` must instead opt
into `use_venv = true` and use an external environment strictly beneath the device's
configured `venv_root` (or a confined per-device override). The first run creates or
populates that environment; later runs reuse it only while its expected interpreter
and fingerprint-bound environment marker remain present. Bootstrap accepts arbitrary
argv arrays and is not a filesystem write sandbox; steps therefore remain responsible
for idempotence and must not mutate declared lock inputs. See
`docs/PROJECT_CONFIG.md` for the complete generic bootstrap example and fail-closed
behavior.

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
