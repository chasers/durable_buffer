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
in fact the only good copy. Healing the primary from the replica instead
would need a promotion or catch-up path that does not exist. Until one does,
`fsync: true` is the only configuration where an ack means what it says under
a primary crash.

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
re-attached after a truncate. See the promotion runbook in the README.

Detaching the sender on truncate does not fix it — TLC rejected that. The
stale batch is delivered before the sender ever re-attaches. Any real fix has
to retry the `:erpc` truncate until the replica adopts the new epoch.

### F-4 — reads are not gated on the ack policy

Config `Replication_dirtyread` (`ReadsAreAckDurable`, VIOLATED).

`Backend.Replica.stream/2` calls `Local.stream/2` with the static backend
config. That config holds `dir` and `needed_acks`; it does not hold
`watermarks` or `pending`, which live in the partition process. So the read
path structurally cannot know how far durability has reached.

`commit_async/4` writes the local WAL first and hands the batch to the senders
second. Between those points a reader sees a batch that no replica has acked,
that may still fail with `:insufficient_acks`, and that a primary crash can
erase. Every readable byte fails the ack target as soon as it is written.

Planned fix: `.plans/2026-09-03_02_ack-durable-reads.md`.

## Not modeled yet

- Replica-side crashes. Only the primary loses an unsynced tail today.
- More than one truncate, and more than one primary crash, per behavior.
- The WAL frame layer: CRC torn-tail recovery in `DurableBuffer.WAL`.
- Group-commit batching and the adaptive flush dwell in
  `DurableBuffer.Partition.Committer`.
- Manual promotion of a follower, including an un-fenced old primary.
- Durable named consumers with acks (issue #2), which do not exist yet.
