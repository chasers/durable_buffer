# TLA+ findings

What each spec models, what TLC found, and what is not modeled yet. Read this
before you add or change a spec.

## Replication.tla

Models one partition of the replicated-disk backend across three processes:

| spec action | Elixir |
|---|---|
| `Commit` | `Backend.Replica.commit_async/4` |
| `CompletePending` | `Backend.Replica.handle_message/2` |
| `Truncate` | `Backend.Replica.truncate/1` |
| `DeliverBatch` | `Replica.Writer.replicate_group/2` |
| `DeliverAck` | `Replica.Sender.handle_info({:replica_ack, _}, _)` |
| `Attach` | `Replica.Sender.reconcile/2` |
| `ResyncStep` | `Replica.Sender.handle_info(:resync_step, _)` |
| `Detach` | `Replica.Sender.force_reattach/1` |
| `Overflow` | `Replica.Sender.handle_cast/2`, the `:max_sender_bytes` branch |
| `Crash` | the primary loses the WAL tail it never `datasync`ed |

One batch is one byte, so a WAL byte offset is a sequence index and a batch id
also identifies its content. Channels are ordered, matching Erlang's per-pair
message ordering.

### What passes

`Replication_core` and `Replication_quorum` hold all three invariants under
primary crashes and dropped messages with `fsync: true`. `Replication_loss`
holds them under dropped messages alone. `Replication_nocrash` holds the two
durability invariants across a truncate with `fsync: false`.

The two negative controls prove the protections are load-bearing:

- `Replication_notailcheck` — the writer's tail rule is the only thing that
  stops a dropped batch from leaving a gap. Turn it off and the replica
  appends the next batch at the wrong offset. `NoConflict` fails.
- `Replication_noepoch` — the epoch gates `reconcile/2`, not only the
  writer's accept rule. Turn it off and a replica that missed a truncate,
  and is behind in raw offset, gets its tail patched from a new-epoch WAL.
  `NoConflict` fails permanently. This is the one to remember: the epoch's
  job in `reconcile/2` matters as much as its job in the writer.

### F-1 — `fsync: false` loses acked writes when the primary crashes

Config `Replication` (`AckedInPrimary`, VIOLATED).

The primary writes its WAL without `datasync`, counts its own write as an
ack, reaches the ack target, and replies `:ok`. It then crashes and loses the
unsynced tail. Reads are served from the local WAL, so the caller cannot read
back a write it was told succeeded.

This is the documented stance of the backend, not a surprise. The spec pins
the exact trace, and it is the reason `fsync: true` exists.

### F-2 — reconcile truncates the replica holding the only surviving copy

Config `Replication_ahead` (`AckedSomewhere`, VIOLATED).

The sequel to F-1, and this one is not documented. Trace:

1. The primary commits batch 1 with `fsync: false`.
2. The replica appends it and acks. The caller gets `:ok`.
3. The primary crashes and loses the batch. The replica still has it.
4. The sender re-attaches. `reconcile/2` sees a replica that is *ahead* of
   the primary, so it takes the `truncate_remote/1` branch and wipes the
   replica, then resyncs it from offset zero.

Step 4 destroys the last surviving copy of an acked write. The protocol
treats "replica ahead of primary" as corruption to be discarded, when it is
in fact the only good copy.

**Fixed.** `Backend.Replica.open/2` now heals in the other direction. It asks
every replica for its tail, and pulls back what it is missing from the
furthest replica on its own epoch, before the partition serves an append.
Config `Replication_heal` turns the same scenario green: with `HealOnOpen`,
`fsync: false` and a primary crash, all three invariants hold.

The heal is sound because a replica WAL is always a prefix-extension of the
primary's. The writer appends only at its own tail, and only bytes the
primary sent from its own WAL, so the surplus is exactly what the primary
lost. Healing at *open* rather than at attach is the load-bearing part: the
sender runs after the partition is already accepting appends, and a new
commit would claim the offsets the surplus belongs at.

`Replication_ahead` keeps `HealOnOpen = FALSE` and stays VIOLATED, so the
gate still proves the heal is what fixes it.

The heal alone was not enough. `Replication_heal` stayed VIOLATED until two
further bugs were fixed — F-5 and F-6 below. TLC found both while checking
the heal; neither was visible from reading the code.

### F-5 — an ack from a previous attach is applied after a re-attach

Found while fixing F-2. Config `Replication_heal`.

`Replica.Sender` matched `{:replica_ack, watermark}` without any notion of
which attach produced it. An ack already in flight when the sender detaches
is still in its mailbox after it re-attaches, and was applied there. Trace:

1. The replica appends a batch and acks it. The ack is in flight.
2. The sender detaches before it processes the ack.
3. It re-attaches, decides the replica must be wiped, and truncates it.
4. It then processes the stale ack, advances that member's watermark, and
   forwards it to the primary.

The primary now counts a replica as durable through an offset the replica no
longer holds, and a commit can complete on the strength of it. This is a
false ack — the one thing the epoch was supposed to make impossible.

**Fixed.** Every attach mints a reference that stamps the batches it sends.
The writer echoes it on each ack, and the sender drops any ack carrying a
different reference. The spec models this with a `stale` flag on messages and
acks, set by `Attach`.

### F-6 — reconcile wipes a replica that is only ahead of the unacked queue

Found while fixing F-5, which exposed it. Config `Replication_heal`.

`reconcile/2` compared the replica's tail against `next_needed/1` — the first
offset in the sender's *unacked queue*. Any replica past that point fell
through to the truncate branch:

```elixir
remote_tail == expected -> resume
remote_epoch == state.epoch and remote_offset < next_needed(state) -> resync
true -> truncate_remote(state)
```

A replica is routinely past `next_needed` for a perfectly ordinary reason:
its ack is still in flight. So a detach with an unprocessed ack truncated a
replica that held a valid prefix of the primary WAL. Usually harmless — the
resync and the queue re-send refill it — but if the primary crashes inside
that window the replica is empty and acked data is gone.

**Fixed.** The tail is now compared against the primary's own WAL, which is
the only thing that defines what a replica may hold:

- a different epoch — truncate;
- past the primary's tail — truncate, and log it, since `open/2` should have
  healed from it (F-2);
- behind `next_needed` — resync the gap;
- otherwise — adopt the tail as that member's watermark, drop what the
  replica already has from the queue, and resume live.

Adopting the tail is strictly more accurate than waiting for acks: at attach
the sender has just been told the replica's real durable offset.

### F-3 — a failed truncate `:erpc` opens a divergence window

Config `Replication_staletruncate` (`NoConflict`, VIOLATED).

`Backend.Replica.truncate/1` bumps the epoch, wipes the local WAL, resets
every sender, then `:erpc`s a truncate to each replica. That `:erpc` is
wrapped in `try/catch` and its failure is ignored. Trace:

1. The primary commits batch 1. The sender pipelines it to the writer.
2. Before the writer processes it, the primary truncates. The epoch goes to
   1. `Sender.reset/2` clears the sender's queue, but it cannot recall the
   message already sitting in the writer's mailbox.
3. The `:erpc` truncate to the replica fails. The replica keeps epoch 0.
4. The writer processes the stale batch. Its own epoch is 0 and the batch is
   epoch 0 at its tail, so it accepts and appends pre-truncate data.

The replica now holds data the primary deliberately discarded.

The window closes on its own: the next `Attach` sees `rEpoch < pEpoch`, takes
the `truncate_remote/1` branch, and resyncs. The epoch also stops the stale
ack from counting, because an epoch-0 watermark can never satisfy an epoch-1
target. So this cannot cause a false ack or permanent divergence.

It does mean a replica is **not safe to promote** until its sender has
re-attached after a truncate.

Detaching the sender on truncate does not close the window — TLC rejected
that. The stale batch is delivered before the sender ever re-attaches, so
nothing that acts at attach time is early enough. The window cannot be
closed from the replica side either: the writer has no way to learn a
truncate happened, because the message that would tell it is the one that
failed.

**Handled, not closed.** The primary now tracks which epoch each replica has
confirmed, and reports it:

- `truncate/1` records every replica its `:erpc` reached, and logs a warning
  for each one it did not. It no longer swallows the failure.
- `Sender.reset/2` re-attaches immediately instead of staying attached with a
  new epoch. The sender then compares epochs, truncates the replica itself,
  and keeps retrying on its reconnect timer until the replica confirms. An
  idle partition converges too; before, it waited for the next commit to be
  rejected.
- `DurableBuffer.replica_status/3` exposes `adopted_epoch` and
  `promotable?`, and `await_replicas/3` blocks until every replica is on the
  primary's epoch.

Config `Replication_adopt` checks the two properties that make that report
trustworthy: `AdoptedIsHonest` (the primary never claims an epoch a replica
has not persisted) and `PromotableIsClean` (a replica reported promotable
holds no pre-truncate data). Both PASS. Marking every replica adopted
regardless of whether the `:erpc` reached it turns `AdoptedIsHonest` red, so
the check has teeth.

`Replication_staletruncate` stays VIOLATED. The divergence window is real and
still there; what changed is that the primary now knows about it.

### F-4 — reads were not gated on the ack policy

Configs `Replication_dirtyread` (VIOLATED) and `Replication_gatedread`
(PASS), the same `ReadsAreDurable` invariant with `GateReads` off and on.

`Backend.Replica.stream/2` calls `Local.stream/2` with the static backend
config. That config holds `dir` and `needed_acks`; it does not hold
`watermarks` or `pending`, which live in the partition process. So the read
path structurally cannot know how far durability has reached.

`commit_async/4` writes the local WAL first and hands the batch to the senders
second. Between those points a reader sees a batch that no replica has acked,
that may still fail with `:insufficient_acks`, and that a primary crash can
erase. Every readable byte fails the ack target as soon as it is written.

**Fixed.** Each partition publishes its backend's durable byte offset into
an `:atomics` slot after every commit, completion and truncate.
`DurableBuffer.stream/3` reads that slot to bound the read — lock-free, no
message to the partition. `Backend.Replica.durable_offset/1` is the
`needed_acks`-th largest member watermark on the current epoch, which is
exactly what a commit waits for; `Backend.Local`'s is its `offset`, which
advances only after the write and the `datasync` both return.

The limit is re-read at each chunk boundary rather than captured once, so a
consumer that keeps pulling sees data that becomes durable while it runs. A
stream still ends at the current end of durable data, exactly as it already
ended at EOF. `stream/3` takes `dirty: true` for recovery tooling that wants
the whole WAL.

`ReadsAreDurable` deliberately checks the read limit against the **replica
WALs**, not against the primary's watermarks. Checking belief against belief
would be a tautology; this way a stale watermark fails the invariant too.

`Backend.S3` is not gated and needs no gating: an object exists only once
its PUT succeeded, so its reads are already exact. `Backend.gates_reads?/1`
reports which backends can gate.

One limit is worth stating plainly. At `open/2` the backend seeds its
watermarks from its own WAL tail and from the replica tails it collects for
the heal, so everything that survived a restart is immediately readable. The
gate closes the in-flight window; it does not re-derive durability for data
written before the restart. `Replication_gatedread` therefore models the
crash and the heal, and the invariant holds — but the property it proves is
about the in-flight window.

## Not modeled yet

- Replica-side crashes. Only the primary loses an unsynced tail today.
- A heal from two replicas at different tails. The model picks the furthest,
  but every config runs one replica through the crash path.
- More than one truncate, and more than one primary crash, per behavior.
- Liveness. Every config checks a state invariant. `Spec` has no fairness, so
  "every replica *eventually* adopts the epoch" is not checked — only that
  the primary never claims one that has not. Adding `WF_vars` to the healing
  actions is the follow-up.
- The WAL frame layer: CRC torn-tail recovery in `DurableBuffer.WAL`.
- Group-commit batching and the adaptive flush dwell in
  `DurableBuffer.Partition.Committer`.
- Manual promotion of a follower, including an un-fenced old primary.
- Durable named consumers with acks (issue #2), which do not exist yet.
