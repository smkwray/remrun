# Troubleshooting

Common problems and their fixes. [USAGE.md](USAGE.md) has the full command reference.

## Commands

A compound command runs partly on your machine. In `remrun run macbox -- cmd1 && cmd2`, your local shell consumes `&&` and runs `cmd2` locally. Make two remrun calls, or pass one quoted argument to a remote shell: `remrun run macbox -- sh -lc 'cmd1 && cmd2'`.

Arguments are split on a Windows controller. Git Bash rewrites quoted arguments after `--` (a `-c "a; b"` argument is split on spaces). Call `bin\remrun.cmd` from PowerShell or cmd instead.

A Windows target rejects the command. `ssh-powershell` targets accept native programs and `.ps1` scripts only; cmdlets, functions, aliases and `.cmd`/`.bat` files are rejected. See "Windows targets" in [USAGE.md](USAGE.md#windows-targets).

remrun refuses to run from a Git worktree. A linked worktree would map to a different remote folder and take a separate lock from the main checkout, so remrun refuses it. Run from the main checkout, or set `REMRUN_ALLOW_WORKTREE=1` if you accept that. With the override, a `do/remrun/remrun.toml` inside the worktree wins; without one, remrun reads the main checkout's.

## Tools and Python on the target

`python` is not found on the target. A non-interactive SSH session often has a shorter `PATH` than your terminal. Set `[run] use_venv = true` in the project config to use the project's virtual environment, declare `[run.bootstrap]` so the first run creates it ([PROJECT_CONFIG.md](PROJECT_CONFIG.md)), or run through a tool that finds the environment itself, such as `remrun run macbox -- uv run python script.py`.

Other tools are missing from the target's `PATH`. On POSIX targets, `login_shell = true` (the default) runs commands through `bash -lc`, so the `PATH` matches a normal login and includes tools such as Homebrew's `Rscript`. Windows has no login shell: list tool folders in the device's `path` (prepended to `PATH`) and set variables in `[devices.<name>.env]`.

`python` and `python3` differ between devices. Set `remote_python` for each device whose interpreter has another name or is missing from the SSH session's `PATH`. A command that names `python` or `python3` directly may not run on every device; a project virtual environment avoids that.

## Transfers

Every run is slow. remrun does not read `.gitignore`, so a large data folder is scanned and hashed on every run until you exclude it. Add it to the project config:

```toml
# <project>/do/remrun/remrun.toml
[transfer]
exclude = ["data/**", "tmp/**"]
```

Excluded paths are neither pushed nor pulled back, so keep result folders included, or let a sync tool deliver them.

Outputs in `dist/` or `target/` do not come back. Both are excluded by default as build caches, along with `.venv/`, `node_modules/` and the others in `config/defaults.toml`. `build/` is included, and so are lockfiles such as `uv.lock`, `Cargo.lock` and `renv.lock`. Project excludes can only add patterns. Check unusual output paths with `remrun plan`.

The run stops with exit `5` and a `push_guard` record. The push would exceed `[transfer] max_push_files` (10,000) or `max_push_mb` (4,096). The message lists the folders with the most files and bytes; usually the fix is to exclude a scratch or cache folder. For a real first sync of a large project, pass `--allow-large-push` once, or raise either limit in the project's `[transfer]` table. `remrun plan --probe` shows the push in advance.

## Locks and unfinished runs

"already locked". Another run of the project is still running on this controller. Wait for it, or pass `--wait-lock SECONDS`. A lock whose process is still alive is correct; do not delete it. A lock left by a killed run is removed automatically when that is safe (see "The project lock" in [USAGE.md](USAGE.md#the-project-lock)).

Exit `4` after a dropped connection. The remote command may still be running. Do not retry a command that changes files. Check the target's processes or outputs, then run `remrun resolve-unknown <run_id> --confirmed-ended`; for a durable run, try `remrun resume <run_id>` first. See [USAGE.md](USAGE.md#when-completion-is-unknown).

A script timed out waiting for remrun. The local remrun process and the remote command may both still be running. Confirm the local process has ended before starting another run of the project, or use `--wait-lock`.

## SSH and paths

The wrong address is used. remrun tries `address_candidates` in order (after `tailscale_ip`, if set) and uses the first that works. Put the alias or address that authenticates first.

`~` in a device path. remrun reads the remote `$HOME` when it first connects and expands `~` itself, so `~/projects` works even though the remote shell never sees an unquoted `~`.

## Git

A synced project has no `.git`. File-sync tools usually skip `.git/`, so a project arriving on a new device is a working tree with no history. Do not `git clone` over it. Run `remrun git-sync <peer-with-history> --pull` once: it creates the repository in place and leaves every working file untouched, so uncommitted work shows as modified or untracked.

`git-sync` refuses to advance a checkout that looks modified on one system only. On trees shared with Windows, `core.autocrlf` can rewrite line endings. Add a `.gitattributes` such as `* text eol=lf` to the project.
