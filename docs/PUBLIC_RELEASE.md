# Public release checklist

Use this checklist before publishing or updating a release. Keep reusable code and
documentation separate from private environment details.

## Remove or generalize

Before public release, scrub:

```text
deployment-specific hostnames or device labels
Tailscale IPs
home directory names
project names
run histories
logs
resource telemetry
private comments in agent docs
any data paths that reveal projects or clients
```

Run both checks before every push, with the history range of what is about to be published:

```bash
bash scripts/hygiene_check.sh
python3 scripts/public_release_check.py --history-base origin/main
```

The hygiene gate checks paths, diffs, and commit attribution. The release checker scans both
surfaces of the next commit: every staged index entry (reported as `:path`) and every
working-tree file that is tracked or untracked but not ignored. Then, for the history range,
it scans every path, blob and message of every commit: an intermediate version, a file name
or a message is published with the branch even when the final tree is clean. Path and file
type do not matter and undecodable bytes are read, not skipped. It fails while a path that
`.gitignore` ignores (private config, `.env`, agent session folders, local state and logs,
continuity notes) or a nested repository would be published, and while any device identity
in the local `config/devices.toml` (name, user, addresses, home-path account) is missing from
the private pattern file. Ignored local config is safe only while it stays untracked.

## Rename or scope if needed

Before publishing, check package and command-name collisions. If `remrun` conflicts with existing software/package names, consider:

```text
sync-remrun
remote-runner
remrunner
runhome
```

The internal command can still be `remrun` if that remains convenient.

## Licensing

The package metadata and `LICENSE` file should agree before release. Re-run the
checker after any license or package-name change.

## Keep private config out of public repo

Recommended layout for public use:

```text
config/devices.example.toml
config/defaults.toml
```

Private use:

```text
config/devices.toml
config/local.<device>.toml
```

The local files `config/devices.toml` and `config/fleet_costs.toml` are ignored and should not be
published. The example files are the public templates.

## Private continuity notes

Private continuity notes are useful on an owner's machine but are not public
documentation as-is. Omit or rewrite root agent instructions, handoff notes,
fleet work-orders, sync work-orders, offload work-orders, and device-specific
validation handoffs before publishing a whole repository snapshot.
If publishing the whole repository, either omit those files from the public
branch/archive or rewrite them as generic historical design notes.

## Test with fake transports

Before release, ensure all core behavior can be tested without real private devices:

```text
local-sim transport
mock ssh transport
temporary directory project roots
```
