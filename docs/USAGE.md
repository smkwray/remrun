# Using remrun

This is the command reference. Run project commands (`run`, `plan`, `bench`, `git-sync`) from inside a project under a configured project root, and put the command to run after `--`. Most commands accept `--json`; [AGENT_OUTPUT_SPEC.md](AGENT_OUTPUT_SPEC.md) describes the output.

## Commands

```bash
remrun --version
remrun devices                          # list configured devices
remrun doctor                           # config root, devices, project roots, state root, SQLite
remrun capabilities                     # the protocol contract this version supports

remrun plan macbox -- <cmd>             # show what a run would do; changes nothing
remrun plan --auto --probe -- <cmd>     # probe devices and show the ranked choice
remrun run macbox -- <cmd>              # reconcile, run remotely, pull outputs back
remrun run --auto -- <cmd>              # pick a device automatically
remrun run --durable --auto -- <cmd>    # survive the controller sleeping or going offline
remrun resume <run_id> [--no-wait]      # reattach to a durable run and finalize it
remrun cancel <run_id>                  # stop a durable run, then finalize it
remrun resolve-unknown <run_id> --confirmed-ended   # clear a completion fence after checking

remrun status [DEVICE] [--limit N]      # recent runs
remrun logs [last|<run_id>]             # a run's output
remrun clean [--older-than 30d] [--keep N] [--dry-run]   # prune the state folder

remrun bench [macbox,winbox] [--no-local] -- <cmd>  # time local vs. remote
remrun sync <tree>/<sub> macbox [--pull|--push|--both] [--dry-run]
remrun action macbox <action> --input FILE [--dry-run]
remrun git-sync macbox [--pull|--push] [--branch NAME] [--status]
remrun observe [--label TEXT] -- <cmd>  # run here, visible to `fleet jobs`
remrun admit acquire|release|status|cancel|withdraw|list ...   # shared admission
remrun fleet ...                        # see "Fleet commands" below
```

`remrun <device> -- <cmd>` is shorthand for `remrun run <device> -- <cmd>`.

## Running a command

### Choosing a target

The target can be a device name, `--auto`, the word `auto`, or nothing (which means auto). Automatic placement uses the project's `[placement]` hints, the scheduler order, learned run times and memory use, and live load; [SCHEDULING_AND_TELEMETRY.md](SCHEDULING_AND_TELEMETRY.md) explains the ranking. If a device is unreachable, or its copy of the project conflicts with yours, `--auto` moves on to the next one. A named target never fails over. `plan --probe` contacts each candidate and reports reachability, load and how much the run would push; `plan --check-git` also compares each candidate's Git checkout with yours.

### Run options

| Option | Effect |
|---|---|
| `--dry-run` | Print the plan and run nothing |
| `--no-pullback` | Do not pull outputs back after the command |
| `--no-telemetry` | Skip CPU and memory metrics (a memory guard stays active) |
| `--label TEXT` | Name shown by `fleet jobs` instead of the program name (1 to 64 letters, digits, spaces or `. _ : @ + -`) |
| `--wait-lock SECONDS` | Wait for a running holder of the project lock instead of failing |
| `--allow-large-push` | Skip the push-size guard for this run |
| `--scope NAME` | Check that the command writes only inside a declared write scope ([PROJECT_CONFIG.md](PROJECT_CONFIG.md)) |
| `--workload NAME` | Select a declared resource workload ([PROJECT_CONFIG.md](PROJECT_CONFIG.md)) |
| `--transfer safe\|none\|commit` | Choose how files move; see [Transfer modes](#transfer-modes) |
| `--verify PATH` | With `--transfer none`, check that the target's copy of this file matches yours |
| `--output PATH`, `--input PATH` | With `--transfer commit`, the paths to return and the working-tree files to copy over the commit |
| `--durable` | Hand the command to a supervisor on the target; see [Durable runs](#durable-runs) |
| `--memory-limit-mib N` | Hard memory limit for the command's process tree on a guarded target |
| `--bypass-memory-guard` | Skip memory admission and the guard; needs a named target and cannot be combined with `--memory-limit-mib` |
| `--cpu N`, `--ram-gib G`, `--key K`, `--device-bound`, `--critical`, `--admission-wait SECONDS` | Wait for a turn from shared admission before starting ([SCHEDULING_AND_TELEMETRY.md](SCHEDULING_AND_TELEMETRY.md)) |

`plan` takes `--auto`, `--scope`, `--workload`, the transfer options, `--probe`, `--check-git` and `--json`.

### The project lock

An ordinary run holds its project's lock on the controller for the whole run, so runs of one project take turns across all devices, `--auto` failover included. A second run fails at once with "already locked" while the holder is running. With `--wait-lock SECONDS` it waits instead (one `lock_wait` event, then polling); it also waits out the holder's own completion fence, but refuses if that run ends and leaves the fence behind.

If a run is killed or the machine sleeps, its lock stays under `locks/project/<hash>/` in the state folder. The next run removes it and reports `stale_lock_reclaimed` when the recorded process has provably exited on this machine and no unknown completion is recorded for the project. Otherwise it refuses and explains why after `not reclaimed:`. Never delete a lock by hand while the project has an unknown completion.

The steps that write into the local project tree (an ordinary run's preflight and pullback, and a commit run's pullback) also take a local-write lock for that tree. Another process waits for it for up to 30 minutes and then fails that step with exit `3` before writing anything. `remrun sync` does not take this lock.

### When completion is unknown

If the SSH connection resets after `command_started`, the remote command may still be running. remrun exits `4`, records `completion_state=unknown`, and refuses further runs of the project until you clear it. Check the target's processes or outputs first, then run `remrun resolve-unknown <run_id> --confirmed-ended`. For a durable run, run `remrun resume <run_id>` first; use `resolve-unknown` only if `resume` reports that the target's record is missing or failed. The fence is local to this controller's state folder.

To stop such a command by hand, signal its own process IDs. On POSIX targets, remrun's remote helper gives the command a process group of its own when job observation wraps the run, and also for telemetry or the memory guard when the SSH session has no terminal. Otherwise the command stays in the SSH session's process group, which some SSH servers share with other sessions, so never signal that group.

An automation timeout does not mean remrun exited: the local remrun process and the remote command may still be running. Confirm the local process has ended before starting another run of the project, or use `--wait-lock`.

### Windows targets

`ssh-powershell` targets need `pwsh` 7.3 or newer (`shell = "pwsh"`). They run native programs and `.ps1` scripts, and pass every following token as a positional argument. remrun rejects PowerShell cmdlets, functions, filters, configurations and aliases before running anything, because it cannot tell a named parameter or switch from plain text. It also rejects `.cmd` and `.bat` files, including bare names found through `PATH`, because the PowerShell-to-`cmd.exe` path corrupts some arguments. Put named parameters inside a `.ps1` wrapper, or call `pwsh -NoProfile -Command <source>` as the program.

On a Windows target with job observation, Ctrl-C stops remrun's own Job Object for the run and reports `owned_job_cancelled`. Programs may break away from that Job Object, so completion stays unknown until you confirm the command ended and run `resolve-unknown`.

## Durable runs

`--durable` is for long, noninteractive commands that should keep running while the controller sleeps or loses its network. It works with one `ssh-posix` or `ssh-powershell` target, chosen by name or by `--auto`, and cannot be combined with a resource workload.

After the target acknowledges the run (the `durable_acknowledged` event), a supervisor on the target owns the command and keeps bounded copies of its output. The first invocation still waits by default; interrupt or close it only after `durable_acknowledged` appears. If the connection is lost before then, remrun cannot know whether the command started, and completion is unknown.

While waiting, a failed status check is retried with a growing delay for up to five minutes. If the target stays unreachable, remrun stops waiting with `durable_detached` and exit `4`, and its guidance names the `remrun resume <run_id>` command to use later. `resume --no-wait`, or an interrupt after acknowledgement, stops waiting with exit `0`.

`remrun resume <run_id>`, from the same controller and project, checks the saved identity, waits if needed, restores the exit code and logs, pulls outputs back once, and removes the target's record. Repeating it never runs the command or pulls outputs a second time. `--no-wait` reports the current state and returns.

With `--auto`, remrun may try other candidates only until it records `durable_target_bound`, just before launch. From then on the target is fixed, and an unclear launch result stops the run rather than risk starting it twice.

`remrun cancel <run_id>`, from the same controller and project, stops a durable run's own processes and then finalizes it like `resume`:

- On POSIX targets it sends SIGTERM to the run's process group, so the command can flush or checkpoint, and SIGKILL to anything still there ten seconds later. The stopped command usually reports exit code 143, or 137 if it had to be killed. On Windows it terminates the run's Job Object at once.
- Processes the command moved to another process group, session or Job Object are not stopped.
- If another remrun process is waiting on the run (the original `run --durable`, or a `resume`), `cancel` only stops the command and exits `0`, and the waiting process finalizes it.
- If the run is still starting, or the target has no record of it, `cancel` stops nothing and exits `4`. If it cannot confirm that the processes are gone, completion stays unknown: check that they ended, then run `resolve-unknown`.

Durable runs do not restart after a target reboot, cannot be adopted by another controller, and do not stop another controller from starting an unrelated run of the project.

## Transfer modes

`--transfer safe` (the default) reconciles the project before the command and pulls outputs back after it. `--transfer none` runs in the target's existing copy of the project and moves no files, for projects the target already holds. `--transfer commit` runs the committed `HEAD` in a private folder on the target and returns only the `--output` paths. `[transfer] mode` in `config/defaults.toml` sets the default. [TRANSFER_MODEL.md](TRANSFER_MODEL.md) describes all three, the push-size guard, and how conflicts are handled.

## Status, logs and the state folder

`remrun status` lists recent runs with their exit code, files pushed and pulled, conflicts and peak memory; give a device name to filter, and `--limit N` for more (default 10). `remrun logs` prints the last run's output, or a named run's.

remrun keeps its own records outside the project: `~/.local/state/remrun` on macOS and Linux, `%LOCALAPPDATA%\remrun` on Windows, or `REMRUN_STATE_ROOT`. It holds each run's `summary.json`, logs and manifest snapshots, the baselines that make deletions safe, conflict copies and backups, and locks. After each `run` and `sync`, remrun prunes it using `[logging]` in `config/defaults.toml`:

| Setting | Default | Effect |
|---|---|---|
| `full_log_retention_days` | 3 | Remove logs and manifests of successful runs, keeping `summary.json` |
| `failed_log_retention_days` | 7 | Keep the logs of failed runs this long |
| `summary_retention_days` | 7 | Remove the run folder |
| `max_full_log_mb` | 100 | Cap each captured log, keeping its head and tail |
| `backup_below_mb` | 50 | Do not back up files larger than this before overwriting them |
| `backup_retention_days` | 3 | Remove backups and conflict copies after this |
| `max_backup_mb` | 1024 | Remove the oldest backups beyond this total |

`remrun clean --older-than DAYS`, `--keep N` and `--dry-run` prune on demand.

Each run records `peak_rss_mb` and `avg_cpu_pct` for the command's whole process tree. Turn this off with `--no-telemetry` or `[telemetry] enabled = false`; on a device with a memory guard, that turns off only the metrics, not the guard.

## Benchmarking (`bench`)

`remrun bench [targets] -- <cmd>` runs the command on this machine and then through a full remrun round trip on each target (default: the scheduler order), records the timings, and prints a recommendation. It changes no routing by itself. Both legs really run, so use it only for commands that are safe to repeat; `--no-local` skips the local leg for jobs too heavy to run here. Every leg gets the same command, so a bare `python` or `python3` may not exist on all of them: use a project virtual environment (`[run] use_venv`) or bench one device at a time.

## Folders outside a project (`sync`)

`remrun sync` converges a folder outside any project tree with one device. Name trees in `[sync_roots]` in `config/devices.toml` and pass `<tree>/<subpath>`, or give any folder with `--remote <path>`. It assumes the remote side is usually the producer: remote-newer files win, local-newer files are pushed, and unclear conflicts are saved aside in the state folder without overwriting either side. It keeps no baseline, so it adds and updates files but never deletes.

Options: `--pull`, `--push` or `--both` (default), `--exclude PATTERN`, `--dry-run`, `--json`. A push reports `push_verified` only after a fresh remote listing shows every pushed file with the expected size and SHA-256; otherwise it exits `3`. Named trees use only the global excludes from `config/defaults.toml`.

## Target actions (`action`)

`remrun action DEVICE NAME --input FILE` copies the input files into a persistent inbox on the target and runs one allowlisted command configured under `[devices.<device>.actions.<name>]`. It never overwrites an existing input, and a receipt keyed to the inputs' content stops a completed action from running twice and refuses an unclear retry after a disconnect. An action with no inputs needs `--key`. If the action reads a prepared folder, `remrun sync` that folder first.

```bash
remrun action macbox ingest --input ~/Downloads/input.zip --dry-run
remrun sync ~/Downloads/ready-batch macbox --remote '~/Downloads/ready-batch' --push
```

## Git history (`git-sync`)

`remrun git-sync DEVICE` exchanges Git commits with one peer through Git bundles over remrun's SSH transport, instead of syncing `.git/` as files. The default direction is `--both` (pull, then push). Peer branches arrive as `refs/remotes/<device>/<branch>`, and branches move only by fast-forward. A diverged branch exits `2` and is left alone. A checked-out branch with uncommitted changes is fetched but not advanced. `git-sync` moves commits only, never uncommitted edits.

- `--branch NAME` transfers that branch and tags only; without it, all branches and tags move.
- `--status` changes nothing and reports each branch as `up_to_date`, `ahead`, `would_fast_forward` or `diverged`, with dirty-file counts and hook checks. `--dry-run` checks both repositories without fetching.
- The device's mapped path must itself be the repository root. A failed check reports the directory, command, exit code and output.

### Projects with no Git history

A project that arrived through a file-sync tool that skips `.git/` is a full working tree with no repository. `remrun git-sync PEER --pull` (or `--bootstrap`) creates one in place: it runs `git init` (with `core.autocrlf false`, and `core.longpaths true` on Windows), fetches the peer's history (or one branch with `--branch`), points the local branch at the peer's tip, and sets the index with `git reset --mixed`. Working-tree files are never touched, so uncommitted work survives and shows as modified or untracked. If the project has a `.githooks/` folder, `core.hooksPath` is set to it. An unreachable, missing or empty peer repository leaves no partial `.git` behind. `--push` alone refuses on a project with no history, and `--bootstrap` refuses on a repository that already has commits.

### Commit hook

`git-sync --install-hook` adds a `post-commit` hook that starts background pushes to the devices in `[git_sync].peers` (or to the device you name) and returns at once, so an offline peer never blocks a commit. An existing hook is backed up and restored by `--uninstall-hook`. Hook output goes to `logs/gitsync-hook/<project>.log` in the state folder. `--status` reports `hook_interpreter_ok`, and `git_sync_hook_broken` when the Python the hook names no longer exists; reinstall the hook to fix it.

### History hubs

By default a checked-out branch with uncommitted changes is not advanced. For a machine whose working files arrive separately through a sync tool, set `advance_dirty_worktree = true` in `[git_sync]` (in `config/devices.toml` or the project config). A proven fast-forward then moves `HEAD` and the index with `git reset --mixed` and leaves every working-tree file as it is.

### Repositories outside the project root

`[git_sync.project_roots]` in `config/devices.toml` sets a broader root used only by `git-sync`, so a repository beside your projects can sync without widening what `run` transfers.

### Line endings

For working trees shared between Windows and other systems, add a `.gitattributes` such as `* text eol=lf`. Otherwise `core.autocrlf` can rewrite line endings, the checkout looks modified, and `git-sync` correctly refuses to advance it.

### Memory limits on guarded targets

On a target with a memory guard, the repository-root check (and `--dry-run`) runs under a built-in 128 MiB cap. Status, pull, push and bootstrap can grow with repository size, so they need `[git_sync] remote_memory_limit_mib` or `--remote-memory-limit-mib N`, a hard cap applied to each remote Git command. A guard refusal is reported as such, never as a missing branch or divergence.

## Shared admission (`admit`)

`remrun admit` queues requests for CPU cores, RAM or named resources on a device, so heavy work from several callers takes turns there. `remrun run` joins the same queue when you pass `--cpu`, `--ram-gib`, `--key` or another admission option. `admit acquire` exits `0` when granted and `75` while still waiting. See "Shared admission" in [SCHEDULING_AND_TELEMETRY.md](SCHEDULING_AND_TELEMETRY.md).

## Fleet commands

`remrun fleet` works without a project. It shows what every configured device is doing, and runs configured tasks or exact commands on staged inputs, with its queue and records in the controller's state folder. Workers start for a job or a batch and exit afterwards; there is no resident service.

```bash
remrun fleet resources                        # CPU, load, RAM, GPU and disk per device
remrun fleet jobs [--device NAME]             # running remrun jobs across all controllers
remrun fleet mesh [--no-hops]                 # which devices can reach which over SSH
remrun fleet presence [--device NAME]         # presence windows from the routing file
remrun fleet plan TASK --input items/         # preview placement; runs nothing
remrun fleet plan TASK --input items/ --save --json   # save an exact placement
remrun fleet submit --plan PLAN_ID --json     # queue a saved placement
remrun fleet submit TASK --input items/       # queue a job
remrun fleet run TASK --input item.zot        # place, stage and run one job now
remrun fleet run TASK --input item.zot --return-root ./returned
remrun fleet command run --device macbox -- tool --flag   # one exact command, never batched
remrun fleet dispatch --drain                 # run queued jobs in batches, then exit
remrun fleet status                           # queue and recent jobs
remrun fleet clear                            # drop queued and running jobs, release leases
remrun fleet cancel                           # clear the queue and stop workers on every device
remrun fleet storage enroll ROOT              # register a shared folder for faster staging
remrun fleet storage bind --device winbox 'Z:\shared-root'
```

Tasks are defined under `[fleet.tasks.<name>]` in `config/devices.toml`; start from `config/fleet_tasks.example.toml`, and see "Fleet views and declarative tasks" in [CONFIGURATION.md](CONFIGURATION.md).

- `resources`, `jobs`, `mesh` and `presence` only read. Resource probes read operating-system counters and never scan files. RAM, VRAM and disk show as percentages; set `[fleet.resources] usage_display = "amounts"` for used and total. On Windows, `LOAD` is ready threads per core (`q`); on POSIX it is the one-minute run queue per core (`x`), so the two are related but not equal.
- `fleet jobs` lists runs only from targets that set `observe_jobs = true`. Set it after `native-gates/fleet_jobs_native_gate.py` passes for that target. Jobs from before the target's last boot are counted in `registry.stale_hidden` rather than shown (with a 10-minute margin for clock corrections). On Windows, a shutdown with Fast Startup enabled does not count as a boot.
- `remrun observe [--label TEXT] -- <cmd>` runs a command on this machine and registers it, so it appears in every controller's `fleet jobs` with its project, label, threads and memory. It keeps your terminal (prompts, input and Ctrl-C work normally) and returns the command's exit status. This machine must be a configured device with `observe_jobs = true`; otherwise the command still runs, unregistered, with a warning. When this controller observes jobs, each `git-sync` in progress also shows as one row.
- A saved plan (`plan --save`) succeeds only when every item is placed exactly once. Submitting the same `PLAN_ID` again returns the original submission instead of queueing twice. `--request-id` tags a submission so `fleet status --request-id` can find it; reusing one for different work is refused. `--priority` given to `plan --save` is stored in the plan. `--preview-route` adds a live placement preview to `submit --json`.
- `--memory-limit-mib N` on `plan`, `submit`, `run` and `fleet command` sets a hard memory limit for the job; the target must have a memory guard and room for it.
- A task with no timing history runs on one device and reports `Uncalibrated placement; no duration estimate.`; unknown times are `null`, never zero. `fleet dispatch --drain --json` ends with one summary document, and exits `2` if queued work could not be placed. Work whose result was lost after launch becomes `completion_unknown` and waits for review rather than running again.
- The fleet queue needs SQLite 3.51.3 or newer (or 3.50.7 or 3.44.6) on the controller; `remrun doctor` shows the version. An older SQLite can still act as a worker.
