# Optional project config

Projects do not need remrun config to work.

When useful, place config at:

```text
<project>/do/remrun/remrun.toml
```

## Minimal example

```toml
[transfer]
exclude = [
  "scratch/**",
  "results/cache/**",
  "data/raw/**"
]
```

## Opt-in target environment bootstrap

Projects may declare an idempotent, target-local environment setup as argv arrays. The
declaration is never inferred from a command or lockfile name. Every step is executed
without a shell, in project-root order, under a target-local lock; `plan` reports
`bootstrap-needed`, `ready`, or `unsupported` and never installs anything.
Bootstrap requires `[run] use_venv = true` and an external target environment. Set
`venv_layout = "external"` and configure each device's absolute (or `~`-relative)
`venv_root`; a per-device `[run.venv]` override is accepted only when it is strictly
confined beneath that root. Local/default layouts, unsafe names, relative paths, and
escaping overrides are rejected before target mutation.

```toml
[run]
use_venv = true
venv_layout = "external"

[run.bootstrap]
schema = 1
lock_inputs = ["pyproject.toml", "uv.lock"]
steps = [
  ["{python}", "-m", "venv", "{venv}"],
  ["{venv_python}", "-m", "pip", "install", "uv==0.11.17"],
  ["{venv_python}", "-m", "uv", "sync", "--active", "--frozen", "--all-extras"],
]
```

`argv = [...]` is the one-step shorthand for `steps = [[...]]`. Supported placeholders
are `{project_root}`, `{python}`, `{venv}`, `{venv_python}`, and `{state_root}`. The
project must opt into `[run] use_venv = true` before using the virtualenv placeholders.
`timeout_seconds` is optional and defaults to 1800 seconds (maximum 86400). Unknown
keys or a schema other than `1` are unsupported.
The controller fingerprints the canonical declaration, its rendered target paths, and
the bytes of every `lock_inputs` path. Those exact paths are included in the
preflight/postrun manifests even when a global exclude (such as `*.lock`) would
otherwise omit them. A matching receipt is retained under the target's configured
state root. Readiness additionally requires the external environment directory, its
expected interpreter, and an environment-local marker authenticated to the same
fingerprint and paths; deleting any of these makes the next run bootstrap again. A
failed receipt may be retried, while an uncertain transport outcome is kept fenced by
the target receipt and lock. Bootstrap still accepts arbitrary argv arrays: Remrun
does not sandbox their filesystem writes, so a project should make its steps
idempotent and avoid mutating declared lock inputs.

## Placement hints

```toml
[placement]
primary = "macbox"
fallback = ["winbox"]

[[placement.rules]]
match_command = "stata|\\.do$"
prefer = "winbox"

[[placement.rules]]
match_command = "bootstrap|simulation|monte"
prefer = "macbox"
min_memory_gb = 64
```

## Opt-in resource workloads

```toml
[resources]
schema = 1
# default_workload = "example.analysis"  # optional explicit project-wide opt-in

[resources.workloads."example.analysis"]
protocol = 1
adapter_id = "example.resource-policy"
adapter_version = 1
work_unit = "case"
require_envelope = false
require_receipt = true
```

Select the declaration explicitly with:

```bash
remrun run --workload example.analysis --auto -- python analysis.py
```

## Git-sync memory boundary on guarded targets

```toml
[git_sync]
# Example syntax only: choose the limit for the target and repository.
remote_memory_limit_mib = 2048
```

The project value overrides the global `[git_sync]` value; the CLI
`--remote-memory-limit-mib` overrides both. It is a hard process-tree cap applied to each
remote Git command in status, pull, push, and bootstrap, not a measured RSS profile.
Without a value, guarded targets still permit the fixed repository probe and `--dry-run`
under a built-in cap, but refuse before repository-scaling work or local bootstrap
metadata is created. Unguarded targets do not require the setting.

With neither `--workload` nor `default_workload`, resource adaptation is inert:
there is no extra probe, environment variable, context file, or receipt. When a
workload is selected, remrun probes only the chosen target, writes
`run-context.v1.json` beneath that target's configured
`<state_root>/runs/<run_id>/`, and exports one variable,
`REMRUN_RUN_CONTEXT`, containing its target-native path. Remrun never rewrites
the command or its worker flags. The project reads the context, chooses its own
settings, and atomically writes the requested receipt beside the context. Both
versioned JSON documents are limited to 64 KiB.

The context reports unavailable measurements as `null`, not zero. Unified-memory
GPUs never receive fabricated VRAM totals, free-memory figures, or VRAM budgets;
system available RAM remains the shared memory constraint.

## Transfer/data policies

```toml
[transfer]
exclude = [
  "scratch/**",
  "results/cache/**",
  "logs/huge/**"
]

[data.raw]
path = "data/raw"
policy = "must_exist_on_remote"

[data.cache]
path = "data/cache"
policy = "never_copy"
```

Policy meanings:

```text
normal                included in active run surface
never_copy            exclude from remrun transfer
must_exist_on_remote  do not copy, but preflight should verify path exists on target
sync_later            exclude from immediate pullback; rely on background sync or explicit command
```

## Write scopes

Write scopes are implemented for commands whose writes are known to stay inside
declared paths. They are never required.

```toml
[parallel.scopes.spec_a]
paths = ["results/spec_a/**", "logs/spec_a/**"]

[parallel.scopes.spec_b]
paths = ["results/spec_b/**", "logs/spec_b/**"]
```

Use them with:

```bash
remrun run --scope spec_a --auto -- Rscript do/spec_a.R
```

Scoped and unscoped runs currently serialize per project. The scope narrows
post-run validation, not the project baseline: after the remote command finishes,
remrun compares the pre/post remote manifests; if any changed or deleted path is
outside the declared `paths`, the run returns a conflict and saves changed remote
files under the state root instead of pulling them into the project.

Only use scopes when commands are known not to write outside the declared paths.

## Agent notes

Projects may optionally include:

```text
do/remrun/AGENTS.md
```

This is for project-specific examples, not launcher scripts.
