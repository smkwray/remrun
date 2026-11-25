# remrun

`remrun` runs a command from a local project on another machine over SSH, then brings the changed files back to the same local paths, so the project looks as if the command had run locally. Before the command starts, it reconciles the two copies of the project: your changes go to the target, and newer files on the target come back. If you edit a file locally while the command runs, remrun keeps your edit and saves the remote copy aside as a conflict. It is for people who keep the same projects on several machines and want to send heavy work to one with room. It works with or without a file-sync tool such as Syncthing.

remrun is a public alpha: configuration and command-line flags may still change before 1.0. Targets can be macOS or Linux machines reached over SSH, or Windows machines running OpenSSH with PowerShell 7.3 or newer. Run each project from one controller at a time, because the project lock lives on the controller.

## Requirements

- Python 3.11 or newer and an `ssh` client on the controller (the machine you run remrun from). remrun has no other dependencies.
- Key-based SSH login from the controller to each target. remrun runs `ssh` in batch mode, so it cannot answer password prompts.
- Python 3 on each target: `python3` by default, or the name set in the device's `remote_python`.
- On Windows targets, `pwsh` 7.3 or newer as the SSH shell. Commands there must be native programs or `.ps1` scripts; remrun rejects PowerShell cmdlets and `.cmd`/`.bat` files.

## Install

```bash
git clone https://github.com/smkwray/remrun.git
remrun/bin/remrun --version
```

No install step is needed: the launchers in `bin/` (`remrun` for macOS and Linux, `remrun.cmd` or `remrun.ps1` for Windows) run remrun from the checkout. Put `bin/` on your `PATH`, or run `python -m pip install -e .` in the checkout to get a `remrun` command.

## Configure

Settings live in the checkout's `config/` folder. Copy `config/devices.example.toml` to `config/devices.toml` and describe your machines:

```toml
[project_roots]            # where projects live on this controller
macos = "~/projects"
windows = 'C:\projects'
default = "~/projects"     # Linux and other systems

[devices.macbox]
kind = "ssh-posix"
os = "macos"
address_candidates = ["macbox.local"]   # SSH hosts or aliases, tried in order
project_root = "~/projects"
state_root = "~/.local/state/remrun"

[devices.winbox]
kind = "ssh-powershell"
os = "windows"
shell = "pwsh"
login_shell = false
remote_python = "python"
address_candidates = ["winbox.local"]
project_root = 'C:\projects'
state_root = 'C:\remrun-state'
```

The project is the nearest folder above the current directory that holds `.git` (or `.hg`, `.svn` or `do/`), or else the first folder under the project root. The command runs at the same relative path under the device's `project_root`. [docs/CONFIGURATION.md](docs/CONFIGURATION.md) lists the device settings.

A project needs no remrun file. It may add hints in `<project>/do/remrun/remrun.toml`, such as extra excludes, a virtual environment or preferred devices; see [docs/PROJECT_CONFIG.md](docs/PROJECT_CONFIG.md) and `examples/project/do/remrun/remrun.toml`.

## Usage

Run remrun from inside a project and put the command after `--`:

```bash
remrun devices                              # list configured devices
remrun doctor                               # check the configuration
remrun plan macbox -- python analysis.py    # show what a run would do; changes nothing
remrun run macbox -- python analysis.py     # reconcile, run on macbox, pull outputs back
remrun run --auto -- Rscript analysis.R     # let remrun pick a reachable, less busy device
remrun status                               # recent runs
remrun logs last                            # output of the last run
```

Leaving out the target means `--auto`, which skips devices that are unreachable or have a conflict; a named device never fails over. Quote compound commands so your local shell does not split them: `remrun run macbox -- sh -lc 'make data && make report'`.

To try remrun without a second machine, add a device with `kind = "local-sim"` and absolute `project_root` and `state_root` paths; it runs the command locally against a separate copy of the project.

Other commands, documented in [docs/USAGE.md](docs/USAGE.md):

| Command | What it does |
|---|---|
| `run --durable` | Leaves the command with a supervisor on the target, so it survives the controller sleeping; `resume` collects it |
| `run --transfer none` | Runs in the target's existing copy of the project and moves no files |
| `run --transfer commit` | Runs the committed `HEAD` in a private folder on the target and returns only named outputs |
| `sync` | Syncs a folder outside any project with a device |
| `git-sync` | Exchanges Git commits with a device without syncing `.git/` |
| `action` | Places files in a target inbox and runs a named, allowlisted command there |
| `bench` | Times a command locally and through remrun, and recommends where to run it |
| `fleet` | Shows resources and running jobs across devices, and queues configured tasks |

## Safety and limits

- Runs of the same project take turns: a second run fails at once unless you pass `--wait-lock SECONDS`. Runs with `--transfer none` or `--transfer commit` take no project lock.
- remrun does not read `.gitignore`, because outputs are often ignored by Git but still needed. It skips caches and environments such as `.git/`, `.venv/`, `node_modules/`, `dist/` and `target/` (see `config/defaults.toml`), and files written there are not pulled back. Exclude large data folders in the project config, or every run spends time scanning them. A run that would push more than 10,000 files or 4,096 MB stops first; `--allow-large-push` lets one run through.
- If the SSH connection drops after the command starts, remrun cannot tell whether the command finished. It exits `4` and blocks further runs of that project until you confirm the remote command ended and run `remrun resolve-unknown <run_id> --confirmed-ended`.
- On macOS and Linux targets, an optional per-device memory guard checks free memory before a run and stops the command's process tree if it crosses a limit you set or the host's memory reserve. It counts resident memory only, so it cannot contain GPU allocations. Windows targets have no memory guard.
- Logs, locks and conflict copies stay outside the project, in `~/.local/state/remrun` (macOS, Linux), `%LOCALAPPDATA%\remrun` (Windows) or `REMRUN_STATE_ROOT`, and are pruned after each run.

Exit codes: `0` success, `1` configuration or internal error, `2` conflict, `3` transfer failure, `4` remote failure or unknown completion, `5` a safety guard refused or stopped the run; any other code is the remote command's own. A conflict after the run exits `2` even if the command succeeded; its own code is in the run summary.

## Development

```bash
python3 -m venv ~/venvs/remrun-dev
. ~/venvs/remrun-dev/bin/activate
pip install pytest ruff
PYTHONPATH=src:. pytest    # the repository root must be on the path for scripts/
ruff check src tests
```

## Documentation

- [docs/USAGE.md](docs/USAGE.md): all commands and options
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md): common problems and fixes
- [docs/CONFIGURATION.md](docs/CONFIGURATION.md): device settings, memory guard, fleet tasks
- [docs/PROJECT_CONFIG.md](docs/PROJECT_CONFIG.md): per-project hints
- [docs/TRANSFER_MODEL.md](docs/TRANSFER_MODEL.md): reconciliation, conflicts and transfer modes
- [docs/SCHEDULING_AND_TELEMETRY.md](docs/SCHEDULING_AND_TELEMETRY.md): automatic placement and shared admission
- [docs/AGENT_OUTPUT_SPEC.md](docs/AGENT_OUTPUT_SPEC.md): output format and exit codes
- [docs/TARGET_RESOURCE_PROTOCOL.md](docs/TARGET_RESOURCE_PROTOCOL.md): target resource reservations and durable launches

## License

MIT. See [LICENSE](LICENSE).
