# Implementation plan

## Phase 0: seed scaffold

Already included:

```text
CLI skeleton
config loader
project detector
manifest builder
transfer planner skeleton
local simulation backend
architecture docs
example configs
test scaffolding
```

## Phase 1: planning-only CLI

Finish and harden:

```bash
remrun devices
remrun doctor
remrun plan macbox -- <command>
remrun plan --auto -- <command>
```

Requirements:

- Correctly detect project root under configured POSIX or Windows project roots.
- Resolve project ID and relative cwd.
- Resolve configured device registry.
- Print JSON plan when requested.
- Never mutate files in planning mode.

## Phase 2: local simulation backend

Use `LOCAL_SIM` to simulate a remote project tree on the same machine.

This lets agents test:

```text
manifest comparison
remote-newer pull
local-newer push
remote-only pull
delete policy
post-run output pullback
conflict handling
```

No SSH needed.

## Phase 3: manifest and conflict engine

Implement:

```text
scan active run surface
apply excludes
generate file metadata
optional small-file hashes
compare local/remote manifests
classify differences
produce reconciliation plan
```

Unit-test the conflict matrix heavily.

## Phase 4: safe transfer reconciliation

Implement Mode 1:

```text
pull remote-newer non-conflicting included files
pull remote-only included files
push local-newer files
push local-only files
apply known-safe deletes only
abort on both-changed conflicts
```

Start with local simulation; then plug into SSH backends.

## Phase 5: SSH/POSIX backend

Implement for a POSIX/macOS SSH runner:

```text
address resolution from candidates: hostname, `.local` name, optional Tailscale IP
remote state dir creation
remote manifest via uploaded Python runner or POSIX script
rsync transfers when available
remote command execution in equivalent cwd
stdout/stderr capture
exit code propagation
```

## Phase 6: post-run changed-file pullback

After remote command:

```text
compare post-run remote manifest to pre-run remote manifest
identify changed/created/deleted project files
before pull, compare local file to pre-run local manifest
if local also changed, save conflict outside project tree
otherwise pull to same relative path
```

## Phase 7: Windows backend

Implement PowerShell/OpenSSH backend:

```text
path conversion
remote state creation
manifest generation via PowerShell or Python
copy files safely
execute command in target cwd
collect telemetry
```

Avoid assuming rsync exists on Windows.

## Phase 8: run journal and log retention

Implement local state:

```text
runs/<run_id>/summary.json
runs/<run_id>/stdout.log
runs/<run_id>/stderr.log
runs/<run_id>/pre_local_manifest.json
runs/<run_id>/pre_remote_manifest.json
runs/<run_id>/post_remote_manifest.json
conflicts/<run_id>/...
```

Implement:

```bash
remrun status
remrun logs last
remrun clean
```

## Phase 9: scheduler and telemetry

Implement:

```text
probe devices
record resource summaries
choose --auto target
respect max_jobs
fallback from primary to another configured device when primary busy/unreachable
learn per-project command performance
```

## Phase 10: project hints

Support optional:

```text
<project>/do/remrun/remrun.toml
```

Hints include:

```text
transfer excludes
preferred device
fallback device
command placement rules
large data policies
resource hints
parallel output scopes
```

## Phase 11: benchmark mode

Implement `remrun bench` safely:

```text
sandbox by default
run same command on two configured devices
measure duration/resources
optionally compare outputs
optionally rerun winner in-place
```

## Hard parts to test early

- Remote-newer auto-pull without clobbering local edits.
- Local-only temp script push.
- Remote-only generated output pull.
- Deletes with and without previous manifest evidence.
- Large excluded cache directories.
- Windows path conversion.
- Remote command nonzero exit with still-needed output pull.
- Interrupted run and retry.
