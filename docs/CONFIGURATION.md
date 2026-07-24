# Configuration design

## Global synced config

Global config lives in:

```text
~/remrun/config/
C:\tools\remrun\config\
```

Seed files:

```text
config/defaults.toml
config/devices.example.toml
config/devices.toml       # private, ignored
```

This config is safe to sync as long as it does not contain secrets. Tailscale IPs are not generally secrets, but they are personal infrastructure details and should be removed before public release.

## Example devices

```toml
[devices.macbox]
kind = "ssh-posix"
os = "macos"
address_candidates = ["macbox.local", "macbox"]
project_root = "~/projects"
state_root = "~/.local/state/remrun"
tags = ["macos", "arm64", "primary"]
max_jobs = 2

[devices.winbox]
kind = "ssh-powershell"
os = "windows"
address_candidates = ["winbox.local", "winbox"]
project_root = "C:\\Users\\you\\projects"
state_root = "~\\AppData\\Local\\remrun\\state"
tags = ["windows", "x64", "fallback"]
max_jobs = 2
```

Agents may later add:

```toml
tailscale_ip = "100.x.y.z"
address_candidates = ["100.x.y.z", "macbox.local"]
```

## Machine-local overrides

Future support should allow non-synced or ignored local overrides:

```text
config/local.macbox.toml
config/local.winbox.toml
config/local.<current-hostname>.toml
```

Use for secrets, usernames, nonportable paths, and experimental settings.

## Project config

Optional project config lives in:

```text
<project>/do/remrun/remrun.toml
```

Example:

```toml
[placement]
primary = "macbox"
fallback = ["winbox"]

[[placement.rules]]
match_command = "stata|\\.do$"
prefer = "winbox"

[transfer]
exclude = [
  "data/raw/**",
  "data/vendor/**",
  "scratch/**",
  "results/cache/**"
]

[resources.default]
memory_gb = 16
cores = 4
```

Project config must be optional. It is for optimizations and hints only.

## Config precedence

Recommended merge order:

```text
built-in defaults
config/defaults.toml
config/devices.toml
config/local.<device>.toml
project do/remrun/remrun.toml
CLI flags
```

CLI flags win.

## Paths and project IDs

Project ID is the path relative to the configured project root.

Examples:

```text
~/projects/paper1                -> paper1
~/projects/client/foo            -> client/foo
C:\projects\paper1               -> paper1
```

Equivalent remote path is:

```text
<device.project_root>/<project_id>/<relative_cwd>
```

Use POSIX-style project IDs internally. Convert separators only at the device boundary.
