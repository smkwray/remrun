# Transfer and reconciliation model

## Default: Mode 1 / safe

Mode 1 is the default.

It reconciles the active run surface conservatively and incrementally:

```text
remote-newer, no conflict -> pull to local, continue
remote-only included      -> pull to local, continue
local-newer               -> push to remote
local-only                -> push to remote
both changed same path    -> abort before running
unknown remote deletion   -> flag; do not propagate destructively by default
known local deletion      -> delete remote only if manifest proves safe
excluded                  -> ignore
```

This makes `remrun` independent of Syncthing while still friendly to it.

## Why remote-newer is pulled by default

If the remote has something the local device lacks, likely explanations are:

1. A previous remrun created it and post-run pull failed or was interrupted.
2. The user/agent worked on the remote and Syncthing has not caught up.
3. Syncthing is paused, failed, or behind.
4. The file is remote-only cache/scratch that should have been excluded.

For included files, the safe default is to pull non-conflicting remote-newer files before running. That catches the local device up before the next remote job.

## Deletions

Deletions are dangerous because a missing local file could mean either:

```text
it was intentionally deleted locally
```

or:

```text
it was created remotely and never arrived locally
```

Default policy:

```text
Do not delete unknown remote-only files during pre-run reconciliation.
Propagate deletions only when remrun has previous manifest evidence.
Back up before applying command-caused deletions locally.
```

Potential strict flags:

```bash
remrun run --strict-delete macbox -- <command>
remrun run --mirror macbox -- <command>
```

These are future modes and should not be default.

## Active run surface

Default active surface:

```text
all project files
minus global excludes
minus project-specific excludes
```

Default global excludes are in `config/defaults.toml`.

Do not use `.gitignore` as the default exclude list. Many outputs are gitignored but still important.

## Project-specific excludes

A project can narrow the active surface:

```toml
# <project>/do/remrun/remrun.toml

[transfer]
exclude = [
  "data/raw/**",
  "data/vendor/**",
  "scratch/**",
  "results/cache/**",
  "logs/huge/**"
]
```

These paths are ignored by remrun's active reconciliation. Syncthing or project-specific tooling may still handle them.

## Large files

Large files should be handled carefully:

```text
small source/scripts: push/pull freely
medium outputs: push/pull normally
large raw data: prefer must-exist-on-remote policies
large caches: exclude
large generated outputs: pull if included, warn if above threshold
```

Future project config can define data policies:

```toml
[data.raw]
path = "data/raw"
policy = "must_exist_on_remote"

[data.cache]
path = "data/cache"
policy = "never_copy"
```

## Manifest comparison

Use a staged comparison:

```text
path + type + size + mtime for fast scan
hash smaller or suspicious files
hash conflicted same-path files before declaring conflict
```

Store manifests outside the project tree. Previous manifests allow safe delete detection and smarter remote-newer classification.

## Conflict handling

Conflicts should be explicit and agent-friendly.

Before running:

```text
if both local and remote changed the same included file differently:
  abort
  save diagnostic metadata outside project tree
  do not mutate either side
```

After running:

```text
if remote output path changed but local path also changed during run:
  do not overwrite local
  save remote version outside project tree
  report conflict
```

Do not create synthetic conflict files inside the project tree unless the user later requests that mode.
