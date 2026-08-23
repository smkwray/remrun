# Target resource and durable fleet-launch protocol

Fleet execution composes this target-local resource authority with the durable runner and job
observer. `remrun capabilities` reports `target_fenced_admission` and `durable_fleet_launch` as
stable. Service sessions remain unavailable and are not implied by this protocol.

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
update while any resource is held. Policy is installed explicitly with `remrun fleet
target-policy install --device NAME --resource KEY ...`; it is never inferred from controller
configuration. `target-policy show` returns the target's current authority. An unchanged install
is idempotent; a changed key set advances the generation by exactly one.

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

The content-addressed runner's `resource-owner-run` stream operation remains the direct owner
boundary for protocol tests and non-fleet consumers.
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
No source-bound handle keeper owns a claim.

## Durable fleet acceptance

For each SSH fleet batch, remrun derives one opaque target operation ID and immutable request
digest. The controller records the target-issued credential in its private queue before launch;
normal `fleet status` output never returns that credential. Positive acceptance occurs only after:

1. the durable runner has persisted the operation and bounded output spools;
2. the observer has established a POSIX process group or Windows Job while user code is suspended;
3. the target resource ledger has claimed the complete opaque key set under the same operation ID,
   digest, fence and token; and
4. durable status records acknowledgment while the one-time start gate is still closed.

The target then advances start certainty from `NO` to `MAYBE`, opens the exact gate, and records
`YES` only after observing the user-process identity. Completion records the exit result and either
proves resource release or retains a quarantine. Windows status replacement retries a bounded
transient file-sharing denial without rebuilding status bytes.

If the controller disconnects, the target continues without a daemon or permanent polling loop.
The origin controller can run `remrun fleet status --job ID --refresh-target --json` (or use an
exact submission identity) to authenticate and read durable state, start certainty, terminal
result, and cleanup receipt. Running/fetching queue work whose controller lease expires is held as
`completion_unknown`, not automatically replayed. Remote durable state is bounded to 256 unresolved
operation directories and then fails closed until terminal evidence is resolved or cleaned.

This protocol does not add active cancellation, a leader, an always-online machine, reboot
restart, public listener, cross-controller adoption, global ordering, or cross-target exactly-once
semantics.
