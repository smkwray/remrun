# Internal target-resource protocol

This is an internal, unadvertised substrate. It is not connected to current fleet execution,
durable launch, or service sessions, and `remrun capabilities` continues to report
`target_fenced_admission` as unavailable.

Each target keeps one authoritative resource policy and ledger in its versioned participant
database under the target's machine-local remrun state root. The store requires a verified local
filesystem, SQLite rollback journaling, `synchronous=EXTRA`, and serialized write transactions. It
must never be placed in a synced project tree.

## Policy

The canonical policy document is:

```json
{
  "schema": "remrun.target-resource-policy",
  "version": 1,
  "generation": 1,
  "resources": [
    {"key": "pool/gpu", "capacity": 1},
    {"key": "tcp/8188", "capacity": 1}
  ]
}
```

Keys are opaque, case-sensitive identifiers. V1 accepts only capacity one. The target stores a
SHA-256 digest of canonical JSON, requires compare-and-swap generation updates, and refuses an
update while any resource is held. Policy is installed explicitly through the internal client; it
is never inferred from controller configuration.

## Allocation lifecycle

All requested keys are acquired in one database transaction or none are acquired. A successful
reservation receives one globally increasing fence and one random token. Only the token digest is
stored; the raw token appears in the original reservation response and an exact `rpc_id` replay.

Reservations expire after 30 seconds according to a strict OS-native, boot-relative monotonic
clock whose epoch is stable across target helper processes. They may be renewed or cancelled only
while still reserved. A process owner converts a reservation to a claim while
the child launch gate remains closed. Claims do not expire on a timer. Normal release is a
target-side mutation after complete process-tree cleanup is proved; uncertainty retains the holds
as a quarantine. A strict target boot-identity change terminalizes active allocations as
`REBOOTED`, removes their holds, and never restarts user code.

Every mutation presents and verifies the original allocation ID, fence, and token. Fences never
reset on policy changes or reboot. Controller RPCs cannot claim, start, release, quarantine,
publish, or adopt work.

## Controller RPCs

- `target_resource_policy_get`
- `target_resource_policy_install`
- `target_resource_reserve`
- `target_resource_renew`
- `target_resource_cancel`
- `target_resource_status`

RRFRAME2 `rpc_id` replay is the retry boundary. Reusing the same ID and request returns the exact
stored response; reusing it with different bytes fails. A new ID is a new request and never mints a
replacement token for an existing allocation.

The content-addressed runner's `resource-owner-run` stream operation is the only launch boundary.
It receives the token and native argv through framed stdin, not argv, environment, logs, or public
receipts. A source-facing stream front-end starts a detached target owner. The owner commits the
claim while the user-code gate remains closed, emits a `target-resource-claim-receipt`, and then
continues independently if the source disappears. A terminal `target-resource-owner-response` is
emitted only when the source remains connected.

POSIX keeps a private session/process-group control child and accepts only an explicit bounded
`EXEC_CONFIRMED` record produced after `subprocess.Popen` settles the operating system's exec
boundary. EOF, malformed records, timeouts, identity mismatch, and control-child death do not
prove start. Windows has the detached owner hold the named Job and suspended process/thread
handles itself; it assigns the child before acknowledging the claim and resumes only afterward.
No readiness marker or source-bound handle keeper participates in this protocol. Production
consumers remain later work.
