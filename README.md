# remrun

`remrun` runs an arbitrary command from a local project on a remote machine and
makes the end state look as if it had run locally: it reconciles the project's
files to the remote, runs the command in the equivalent remote directory, and
pulls changed outputs back to the same local paths. Syncthing is a convenience,
not a correctness dependency.

> **New here? Start with this file.** This is a working tool, not a seed.

## Status

The core runner is implemented and tested on POSIX/macOS and Windows SSH targets:
safe project reconciliation and pullback, conflict preservation, target scheduling,
resource telemetry, external-tree `sync`, commit-only `git-sync`, allowlisted target
actions, and optional fleet dispatch.

The default coordination mode remains `legacy`: one controller may write a project at
a time. Versioned-runner, lease/fencing, and snapshot components are experimental,
disabled groundwork and are not a supported multi-controller execution path. A network
disconnect after a remote command starts can leave completion unknown; remrun reports
that state and requires a read-only process or artifact check before retrying.

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
remrun devices                      # list configured devices
remrun doctor                       # show config root, devices, project roots, state root
remrun plan macbox -- <cmd>         # show what a run would do; mutates nothing
remrun run macbox -- <cmd>          # reconcile -> run remotely -> pull outputs back
remrun run --auto -- <cmd>          # probe/rank targets; conflict-safe failover
remrun run --scope spec_a --auto -- <cmd>  # opt-in declared write scope
remrun status [DEVICE] [--limit N]  # recent runs, optionally filtered by target
remrun logs [last|<run_id>] [--json]
remrun clean [--older-than 30d] [--keep N] [--dry-run]   # prune state folder
remrun bench [targets] -- <cmd>     # time local vs. full remrun round-trip; recommend offload
remrun bench [targets] --no-local -- <cmd>   # skip local leg and assume offload
remrun sync <tree>/<sub> macbox     # project-less folder sync (pull-biased; see below)
remrun sync outputs/reports macbox --pull --dry-run   # only pull remote-newer; show plan, change nothing
remrun git-sync macbox              # sync Git commits with a peer without syncing .git/
remrun git-sync winbox --pull --branch main    # arrival pull; fast-forward only
remrun git-sync winbox --pull       # on a repo-less project: bootstrap from the peer's history
remrun git-sync winbox --bootstrap  # same, explicit (working tree left untouched)
remrun git-sync macbox --status     # non-mutating branch/dirty/hook diagnostics
remrun git-sync --install-hook      # post-commit best-effort push to [git_sync].peers
remrun git-sync --uninstall-hook    # remove remrun's hook and restore any prior hook
remrun runner install macbox        # install + probe the inert versioned helper
remrun runner probe macbox          # verify the exact pinned helper and SQLite store
```

### Agent gotchas (learned in production — read before your first `run`)

Nine things bite first-time callers; all have one-line fixes:

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
5. **A yielded command-runner session is still a live remrun process.** A command
   runner may return a session ID after its output wait expires (often 30 s);
   that is not the command's exit. Keep polling that same session until it reports a
   terminal exit before starting another run for the project. A lock whose recorded PID
   is still alive is correct and must not be deleted.
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

`sync` converges a folder that lives **outside** the project tree (the fleet OCR/TTS
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
worktree edits. `--status` is non-mutating: it uses a temporary bare repository plus a peer
bundle to report `up_to_date`, `ahead`, `would_fast_forward`, or `diverged`, along with
tracked-dirty flags, content/mode-only/untracked counts, and hook diagnostics. From a
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
transfer boundary. A repository at `work/remrun` can then exchange with a peer alongside
ordinary `work/proj/foo` repositories without a one-off config override.

For cross-platform synced working trees, prefer a project `.gitattributes` such as
`* text eol=lf`. Otherwise Windows `core.autocrlf` can rewrite checked-out file line
endings, and a background file sync can make the Mac checkout look like it has tracked
local edits. `git-sync` will correctly refuse to advance a tracked-dirty checkout in
that case.

`runner install` is rollout groundwork for crash-safe multi-controller coordination. It
installs a content-addressed self-contained helper through an RRFRAME2 payload, verifies
the installed source hash, initializes the runner's local SQLite participant store, and
checks protocol/filesystem/`sqlite3` prerequisites. It does **not** change `run` behavior;
`[coordination] mode = "legacy"` remains the default until the later authority, execution,
and transactional-reconcile production gates pass.

The shadow authority store is disposable only as a whole coordination identity. If it is
reset, initialize a **new `cluster_id`** and re-enroll targets. Recreating epoch 1 under an
existing cluster is intentionally rejected by participants when its key differs; do not
delete participant enrollment state to make an old cluster identity reusable.

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
  your normal environment — needed to find e.g. Homebrew's `Rscript`)
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

The versioned-runner rollout is explicitly gated:

```toml
[coordination]
mode = "legacy"       # runner-v1 is not enabled by Step 3
device = "macbox"     # future pinned coordination runner
protocol = 1
```

### Project hints — `<project>/do/remrun/remrun.toml` (optional)

Never required; only optimizes behavior. See `examples/project/do/remrun/remrun.toml`.

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
(stdlib-only). Caveat on RSS scope: ssh-powershell uses a Win32 Job Object
(`PeakJobMemoryUsed` = true whole-process-tree peak), but ssh-posix uses
`getrusage(RUSAGE_CHILDREN).ru_maxrss`, which is the peak of the **largest single
child**, not the sum of a multi-process tree — so a job that fans out into many
concurrent processes is under-reported on POSIX. CPU% is whole-tree on both. Disable
with `--no-telemetry` or `[telemetry] enabled = false`.

## Operational gotchas (learned the hard way)

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
