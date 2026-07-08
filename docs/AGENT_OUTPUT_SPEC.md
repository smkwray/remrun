# Agent-friendly output specification

`remrun` is primarily run by agents, so output must be stable.

## Streams

Recommended default:

```text
stderr: remrun status lines
stdout: remote command stdout passthrough
stderr: remote command stderr passthrough, interleaved after remrun status where necessary
```

This mirrors common CLI behavior and preserves compatibility with scripts.

## Status lines

Human/agent text mode:

```text
remrun: project paper1
remrun: target macbox
remrun: preflight pulled=2 pushed=1 conflicts=0
remrun: running command="Rscript do/tmp/test.R"
remrun: exit_code=0 duration_sec=123.4 post_pull=7
```

Keep these concise and avoid changing field names casually.

## JSON mode

With `--json`, emit newline-delimited JSON events to stderr:

```json
{"event":"project_detected","project_id":"paper1","relative_cwd":"analysis"}
{"event":"target_selected","device":"macbox","reason":"explicit"}
{"event":"preflight_summary","pulled":2,"pushed":1,"conflicts":0}
{"event":"command_started","run_id":"..."}
{"event":"command_finished","exit_code":0,"duration_sec":123.4}
{"event":"summary","run_id":"...","exit_code":0,"files_pushed":1,"files_pulled_post":7}
```

Remote command output should remain passthrough unless `--capture-only` is set.

## Final summary

Every run should have a final summary object in the local run journal. If `--summary-json` is requested, print it as the final JSON event.

## Exit codes

The process exit code should be:

```text
0     remote command succeeded and remrun finished reconciliation
1     remrun internal/config/preflight error
2     conflict: detected before running the command, OR an unresolved post-run
      divergence (local was edited during the run and differs from the command
      output). For the post-run case the command may itself have exited 0 — its
      own code is preserved as command_exit_code in the run summary.
3     transfer failure
4     remote execution infrastructure failure
N     remote command's exit code, when command ran and remrun could collect status
```

If the remote command exits nonzero, `remrun` should still try to pull logs and changed files unless configured not to.

## Avoiding agent confusion

Do not print instructions like "please wait" or interactive prompts by default. Agents need deterministic output.

If a conflict blocks the run, print:

```text
remrun: conflict: both local and remote changed do/clean.R
remrun: action: not running command
remrun: conflict_state: ~/.local/state/remrun/conflicts/<run_id>/
```

and exit 2.
