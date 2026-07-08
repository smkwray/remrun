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

## Resource hints

```toml
[resources.default]
cores = 4
memory_gb = 16
expected_duration = "30m"

[resources.heavy]
match_command = "bootstrap|simulate"
cores = 12
memory_gb = 64
```

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
