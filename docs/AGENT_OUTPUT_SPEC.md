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

## Capabilities protocol

`remrun capabilities --json` is a single-document protocol command, not JSON event mode. On
success it writes exactly one compact `remrun.capabilities` version 1 document to stdout, writes
nothing to stderr, and exits 0. It does not load deployment configuration, inspect devices, open a
queue, or use the network.

Clients must validate the document schema and then apply these compatibility rules before any
queue, launch, or service mutation:

1. Reject a different protocol major, whether older or newer.
2. For the same major, require every needed feature to be `stable` and every needed document
   schema/version pair to be declared. A client may explicitly opt into an `experimental` feature.
3. Accept unknown additive fields and declarations from a newer same-major minor version.
4. Treat a missing feature, an unknown feature status, or an undeclared required schema as
   unsupported. Never infer support from command names, package versions, configuration, or source
   presence.

`package_version` is diagnostic only. Additive fields, feature keys, schema declarations, and
feature promotion increment the protocol minor. Removing or changing a field, type, stable status,
or stable enum meaning requires a new protocol major.

The initial protocol 1.0 document honestly reports controller-local queue coordination. It does
not promise fleet-global ordering, global idempotency, cross-target exactly-once execution,
target-owned recovery, prepared tasks, target-fenced admission, durable fleet launch, or service
sessions.

An internal failure after parsing writes one compact `remrun.error` version 1 document to stderr,
writes nothing to stdout, and exits 1. Argparse usage errors retain argparse text and exit 2.

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
{"event":"memory_guard","status":"ok","command_started":true}
{"event":"command_finished","exit_code":0,"command_exit_code":0,"duration_sec":123.4}
{"event":"summary","run_id":"...","exit_code":0,"files_pushed":1,"files_pulled_post":7}
```

Remote command output should remain passthrough unless `--capture-only` is set.

In `remrun fleet plan --json`, each `batches[].jobs` value is a zero-based index into the input
task list for that plan invocation. It is not a durable queue job ID. `makespan_s` is the planner’s
top-level estimate.

When a fleet submission includes `--memory-limit-mib N`, plan and submit JSON include the
frozen `limits` object. Synchronous execution includes a token-free `memory_limit` receipt.
Durable queue status exposes the same evidence in `last_result` as a
`kind="fleet-attempt-receipt"` record. Treat `requested_mib` as an intentional hard
containment boundary; do not report it as predicted or observed demand. The target's
`allowance_basis="explicit_command_limit"`, `enforced_command_limit_bytes`, policy ceiling,
host reserve, final `memory_metric`, peak, command-start state, and cleanup fields are the
authoritative execution evidence. Lease tokens, lease IDs, and target state paths are never
part of this public receipt.

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
5     configured memory policy refused, terminated, or failed safe. A
      pre-mutation capacity refusal has phase="memory_admission" and a
      structured memory_admission record. Runtime enforcement has a structured
      memory_guard record with whether user code started, the command exit (if
      any), observed thresholds, and cleanup status
N     remote command's exit code, when command ran and remrun could collect status
```

If the remote command exits nonzero, `remrun` should still try to pull logs and changed files unless configured not to.

For a guarded device, remrun emits `command_dispatch` before the protected helper
is contacted and emits `command_started` only when the helper confirms that all
required enforcement was initialized and user code was launched. `--no-telemetry`
suppresses optional telemetry only; it never suppresses the memory guard. Exit 5
can collide numerically with a command that itself exits 5. Automation must use
`phase = "memory_admission"` plus `memory_admission` for a pre-mutation refusal,
or `memory_guard` plus `command_exit_code` for runtime enforcement, to
distinguish those cases.

For a successful non-durable guarded run, the final summary also contains a
token-free `memory_admission` object. Unknown-command receipts identify
`allocation_rule="unprofiled_open_slot_fair_share_v1"`, the allowance and control
overhead, remaining backed capacity, open slots and per-slot capacity at sizing, the
policy ceiling, and the strict margin. Never expect or expose the private lease token.

If an unprofiled command in a non-durable run reaches that fair-share ceiling,
remrun emits `memory_limit_guidance` and records the same object in the final
summary. It reports the fair-share limit, the observed peak as a lower bound, the
target policy ceiling, `partial_effects_may_exist=true`,
`profile_recorded=false`, and the intentional `--memory-limit-mib N` rerun seam.
Remrun does not retry the command automatically.

## Avoiding agent confusion

Do not print instructions like "please wait" or interactive prompts by default. Agents need deterministic output.

If a conflict blocks the run, print:

```text
remrun: conflict: both local and remote changed do/clean.R
remrun: action: not running command
remrun: conflict_state: ~/.local/state/remrun/conflicts/<run_id>/
```

and exit 2.
