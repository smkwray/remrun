# Transfer and reconciliation model

## Default: Mode 1 / safe

Mode 1 (`safe`) is the default. `[transfer] mode` in `config/defaults.toml` sets the
default mode (`safe`, `none` or `commit`); `run --transfer` and `plan --transfer`
override it for one invocation. Any other value is a configuration error.

It reconciles the active run surface conservatively and incrementally:

```text
remote-newer, no conflict -> pull to local, continue
remote-only included      -> pull to local, continue
local-newer               -> push to remote
local-only                -> push to remote
both changed same path    -> abort this candidate before running
unknown remote deletion   -> flag; do not propagate destructively by default
known local deletion      -> delete remote only if manifest proves safe
excluded                  -> ignore
```

This makes `remrun` independent of Syncthing while still friendly to it.

### Automatic candidate shopping

Candidate-local conflict state belongs to one device's project tree and baseline.
With `--auto`, such a conflict rejects that candidate and remrun may try the next
reachable device. Global controller-tree conditions, notably `local-vanished`,
abort the whole run. Explicit targets never fail over.

Candidate shopping cannot use a later device to rewrite the controller's tree:
a fallback plan that would `PULL` or `DELETE_LOCAL` is ineligible. `PUSH` remains
allowed because it sends the controller's bytes outward. The first device that
actually reaches preflight uses the normal reconciliation rules above; an
unreachable device has read or mutated nothing and does not consume that position.

## Mode `none`: run in place

`--transfer none` runs the command in the target's existing checkout and moves no files:

```text
no preflight reconcile -> the command sees the target's bytes as they are
no pullback            -> outputs stay in the target's tree
no baseline            -> the next ordinary run reconciles from the last ordinary baseline
no project lock        -> untransferred runs of one project may run concurrently
```

It assumes the target's tree is current, for example because Syncthing keeps it so.
`--verify PATH` proves that for named files: the target's copy must have the same
SHA-256 as the local copy. Before the command starts, a run refuses with exit 2 and a
conflict record when:

```text
target-checkout-missing -> the checkout or the run's directory is absent on the target
verify-mismatch         -> a --verify file differs on the target
verify-unavailable      -> the target could not hash a --verify file (fail closed)
bootstrap-not-ready     -> a declared [run.bootstrap] is not ready; setup never runs here
target-busy-ordinary    -> an ordinary run of the project may still hold its lock there
```

All five are candidate-local, so `--auto` tries the next candidate. Each untransferred
run keeps its own completion fence; it still refuses while an ordinary run of the
project has unknown completion. An ordinary run refuses a target named by any live
untransferred fence of the project (`target-busy-untransferred`, candidate-local).

## Mode `commit`: a clean HEAD in a private tree

`--transfer commit --output PATH [--input PATH]` runs the committed `HEAD` in
`<state_root>/work/<id>/tree` on the target and returns only the declared outputs:

```text
source        -> HEAD, plus each --input file (SHA-256 recorded); dirty tracked files
                 outside --output/--input refuse the run
seeding       -> a bundle (incremental once the target's cache holds an ancestor) into a
                 bare cache under <state_root>/work/git/, then a local clone and checkout
the command   -> runs in tree/<relative cwd>; the target's checkout is never read or written
outputs       -> ordinary pullback, limited to the --output paths; a symlink or special
                 file at or under one (target or local) is held: not pulled, its local
                 path untouched, the tree is kept, exit 2
other writes  -> under the project's excludes: scratch, discarded
                 anywhere else: reported, never pulled, the tree is kept, exit 2
cleanup       -> the tree is removed only after exit 0, a clean verified pullback, no
                 unexpected writes and no held outputs; otherwise it is kept, and
                 pruned later
no baseline   -> commit runs never read or advance the ordinary baseline
no project lock -> each run leases its outputs instead
```

A declared output returns whatever project or global exclude matches it: `out/`
covers the directory and everything under it (`out/x.tmp` included), `out/a.csv` covers
one file. An output that is itself under a global exclude (`.git/`, `node_modules/`) is
refused when the run is planned. Paths are compared as prefixes.

Refusals before the target is chosen (exit 2, nothing written anywhere):

```text
source-dirty   -> a tracked file differs from HEAD outside --output/--input
output-leased  -> a live commit run of the project holds an overlapping --output
```

An `--input` that is not a regular file, or has a symlink at or above it in the working
tree or in `HEAD`, is refused the same way: the overlay is written into `HEAD`'s
checkout, where such a link would carry the write out of the private tree. A symlink or
special file at, above or under an `--output`, in the working tree or in `HEAD`, is
refused too: remrun returns only regular files.

Refusals of one candidate (exit 2, nothing written to it; `--auto` tries the next):

```text
disk-unmeasured           -> the target's free space at its state root cannot be measured
disk-floor                -> tree + inputs + bundle would leave less than
                             [transfer] work_min_free_fraction of that disk free
git-unavailable           -> git is not on the target's PATH
git-memory-limit-missing  -> a memory-guarded target has no [git_sync]
                             remote_memory_limit_mib for the git steps
bootstrap-not-ready       -> a declared [run.bootstrap] is not ready; setup never runs
```

The output lease is the run's completion fence plus its outputs. It is checked again
and taken under the lock table's guard just before the command starts, and it stands
through the pullback. A pullback that fails, or that holds unsupported outputs
(symlinks or special files), leaves it in place, with the outputs in the retained tree,
until `remrun resolve-unknown` releases it; `remrun resume` releases it only by retrying
a durable run's failed pullback. Leased paths are compared ignoring case and Unicode normalization. Ordinary runs ignore commit fences: a commit run never touches the
target's checkout, and a same-path change on the controller ends in a saved conflict on
whichever pullback comes second, never an overwrite.

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
  abort the current candidate
  save diagnostic metadata outside project tree
  do not mutate either side
  with --auto, try the next candidate under the fallback rules above
```

After running:

```text
if remote output path changed but local path also changed during run:
  do not overwrite local
  save remote version outside project tree
  report conflict
```

Do not create synthetic conflict files inside the project tree unless the user later requests that mode.
