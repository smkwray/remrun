# remrun architecture plan

## Goal

`remrun` lets agents run arbitrary project commands on remote devices while preserving the normal project layout and minimizing dependence on Syncthing timing.

The target end-state is:

```text
Agent in local project cwd calls remrun.
remrun reconciles the active project surface with a remote device.
The command runs in the equivalent remote cwd.
Outputs land in the normal project tree on the remote.
remrun immediately returns changed outputs to the same local paths.
Syncthing remains free to converge later.
```

## Core concepts

### Active run surface

The active run surface is the subset of project files that `remrun` actively reconciles before and after a run.

Default:

```text
all files under the project root
minus global cache/library/temp excludes
minus optional project-specific excludes
```

This differs from Git tracking. Gitignored outputs may be important and should often be included.

### Real project tree execution

Default runs happen in the real remote project tree:

```text
macbox: ~/projects/<project>/<relative-cwd>
winbox: C:\projects\<project>\<relative-cwd>
```

This is intentional. If a command writes `tables/main.tex`, that output belongs in `tables/main.tex`.

### Out-of-tree remrun state

`remrun` state lives outside project trees:

```text
~/.local/state/remrun/
%LOCALAPPDATA%\remrun\
D:\remrun\state\
```

State includes:

```text
run manifests
locks
stdout/stderr captures
telemetry
queues
conflict backups
remote runner packages
```

## Components

### 1. CLI

Responsibilities:

- Parse `remrun run`, `remrun plan`, `remrun devices`, `remrun doctor`, `remrun status`, `remrun logs`, `remrun bench`.
- Preserve the remote command's exit code.
- Emit agent-friendly status and optional JSON events.

### 2. Config loader

Sources, in order:

1. Synced defaults in `<remrun>/config/defaults.toml`.
2. Synced device registry in `<remrun>/config/devices.toml`.
3. Optional machine-local overrides, e.g. `<remrun>/config/local.<device>.toml`.
4. Optional project config: `<project>/do/remrun/remrun.toml`.
5. CLI flags.

The first implementation can start with 1, 2, 4, and CLI flags.

### 3. Project detector

Given the local cwd, determine:

```text
local project root
project id relative to the configured root
relative cwd inside the project
remote project root for target device
remote cwd
```

Example:

```text
local cwd:        ~/projects/paper1/analysis/specs
local root:       ~/projects/paper1
project id:       paper1
relative cwd:     analysis/specs
macbox project dir: ~/projects/paper1
macbox cwd:         ~/projects/paper1/analysis/specs
```

### 4. Device registry

The registry is synced in `config/devices.toml` and contains:

```text
device name
OS/backend kind
address candidates
project root
state/cache roots
tags/capabilities
max_jobs
role primary/fallback
```

Known devices:

```text
macbox = POSIX/macOS runner, primary
winbox = Windows runner, fallback
```

### 5. Scheduler

For explicit targets, scheduling is trivial.

For `--auto`, score devices using:

```text
reachability
static role and tags
project placement hints
command placement rules
current remrun queue
CPU/RAM pressure
disk free
historical speed/memory for similar commands
recent failures
estimated transfer cost
```

Initial implementation can choose the configured primary if reachable, otherwise fallback.

### 6. Transfer planner

Build local and remote manifests for the active run surface. Classify every included path:

```text
same
local-newer
remote-newer
local-only
remote-only
both-changed
local-deleted-known
remote-deleted-known
unknown deletion
excluded
```

The planner produces an explicit reconciliation plan:

```text
pull remote-newer non-conflicting files
push local-newer files
push local-only files
pull remote-only included files
delete only when known-safe
abort on both-changed conflicts
```

### 7. Remote executor

Executes the arbitrary command in the equivalent remote cwd.

Requirements:

- Use a project-level writer lock; declared write scopes currently narrow validation, not concurrency.
- Capture stdout/stderr to local remrun state and optionally stream.
- Return the command's true exit code.
- Record start/end times and resource telemetry.

### 8. Post-run collector

After command completion:

1. Build or receive a post-run remote manifest.
2. Diff against the pre-run remote manifest.
3. Determine created/modified/deleted files caused by the run.
4. Pull changed files back to the same local paths.
5. Before overwriting, verify local paths were not changed during the run.
6. Save conflicts outside the project tree.

### 9. Run journal

Each run records a compact JSON summary:

```json
{
  "run_id": "20260628T120000Z-macbox-paper1-a1b2c3",
  "project_id": "paper1",
  "target": "macbox",
  "command": ["Rscript", "do/tmp/test.R"],
  "exit_code": 0,
  "duration_sec": 123.4,
  "files_pushed": 3,
  "files_pulled_pre": 1,
  "files_pulled_post": 8,
  "conflicts": 0,
  "peak_rss_mb_observed": 42117,
  "avg_cpu_pct": 650
}
```

Full logs are retained temporarily; summaries can be kept longer.

## Default run flow

```text
1. Parse command.
2. Detect project root and relative cwd.
3. Load config.
4. Resolve device or schedule target.
5. Probe target.
6. Acquire project/host lock.
7. Compare active run surface.
8. Pull non-conflicting remote-newer files.
9. Push local-newer and local-only files.
10. Record pre-run manifest.
11. Execute command remotely in-place.
12. Record post-run manifest.
13. Pull changed outputs back to local project paths.
14. Save conflicts outside project tree if needed.
15. Write run summary.
16. Exit with remote command exit code.
```

## Why this is not CI

CI systems usually run in clean workspaces and publish artifacts. `remrun` should default to in-place execution because the user wants identical paths and arbitrary commands, including temporary scripts created by agents.

Sandbox/CI-like behavior can be added later as an explicit mode:

```bash
remrun run --sandbox macbox -- make risky-test
```

## Why this is not Syncthing control software

`remrun` may optionally inspect Syncthing status later, but correctness must come from direct reconciliation and conflict checks.
