# Remote protocol and runner contract

The local CLI should not assume that the synced remrun checkout is current on the remote. It should be able to upload a small versioned remote runner/shim into the remote state directory.

## Transport abstraction

Each backend must provide:

```python
exec(argv_or_script, cwd=None, env=None, timeout=None) -> ExecResult
push_file(local_path, remote_path) -> TransferResult
pull_file(remote_path, local_path) -> TransferResult
push_tree(plan) -> TransferResult
pull_tree(plan) -> TransferResult
manifest(remote_project_root, include_rules, exclude_rules) -> Manifest
probe() -> ProbeResult
```

Initial backends:

```text
local-sim       local filesystem simulation for tests
ssh-posix       macbox / macOS or Linux / POSIX shell
ssh-powershell  winbox / Windows OpenSSH + PowerShell
```

## Remote runner spec

The local CLI should write a run spec JSON:

```json
{
  "protocol_version": 1,
  "run_id": "20260628T120000Z-macbox-paper1-a1b2c3",
  "project_id": "paper1",
  "project_root": "~/projects/paper1",
  "relative_cwd": "analysis",
  "command": ["Rscript", "do/tmp/test.R"],
  "env": {},
  "capture": true,
  "telemetry": {"enabled": true, "sample_interval_sec": 2}
}
```

Remote runner returns summary JSON:

```json
{
  "run_id": "...",
  "exit_code": 0,
  "started_at": "2026-06-28T12:00:00Z",
  "ended_at": "2026-06-28T12:02:03Z",
  "stdout_path": "...",
  "stderr_path": "...",
  "peak_rss_mb_observed": 1234,
  "avg_cpu_pct": 220
}
```

## POSIX backend notes

Use SSH for command execution. Use rsync when available for efficient incremental transfers. Fall back to tar/scp only when necessary.

Avoid interactive shells. Execute scripts with explicit quoting.

## Windows backend notes

Use OpenSSH + PowerShell where possible. Convert project paths at the boundary. Avoid assuming POSIX tools exist on Windows. The `ssh-powershell` execution seam requires `pwsh` 7.3+ and supports native executables, cmdlets, aliases, and `.ps1` scripts. It rejects top-level `.cmd`/`.bat` files, including bare names resolved through `PATH`/`PATHEXT`, because the current PowerShell-to-`cmd.exe` seam is proved to corrupt some arguments. This is a narrower surface than POSIX arbitrary-command execution.

PowerShell runner should accept JSON run specs and return JSON summaries.

## Versioning

Remote runner should expose:

```bash
remrun-remote --version-json
```

Local CLI checks the remote runner version and uploads a compatible shim if missing or stale.

## Locks

Use lock files/directories in remote state root, not project root:

```text
<state_root>/locks/project/<hash(project_id)>.lock
```

Default lock scope:

```text
one in-place writer per project
```

Declared project scopes may narrow pullback validation: `remrun run --scope <name>`
uses `[parallel.scopes.<name>]` and rejects any changed/deleted remote path outside
the declared scope. Scoped and unscoped runs still serialize per project because
baseline attribution is project-wide.
