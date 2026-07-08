# Public release checklist

`remrun` can eventually become public, but separate public code from private environment details.

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

Run the checked public-surface scan:

```bash
python3 scripts/public_release_check.py
```

The check scans package code, tests, examples, schemas, README, generic docs, and example config.
It intentionally excludes private continuity/work-order notes and ignored local config.

## Rename or scope if needed

Before publishing, check package and command-name collisions. If `remrun` conflicts with existing software/package names, consider:

```text
malus-remrun
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
