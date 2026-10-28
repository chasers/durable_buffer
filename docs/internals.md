# DurableBuffer internals

This file tells how DurableBuffer works inside. You do not need it to use the library. For usage, see [README.md](../README.md).

## Local backend files

Each partition has these files in `dir`:

| File | Content |
|---|---|
| `p<index>.wal` | The append-only write-ahead log (WAL) |
| `p<index>.meta` | The epoch and the retained bases. See `DurableBuffer.Meta` |
| `p<index>.idx` | The sparse seek index |
| `p<index>.trim` | The target base of a trim. It exists only while a trim runs |

Each WAL entry has this frame: `<<len::32, crc32::32, payload>>`. `crc32` is the CRC32 checksum of the payload. On open, the backend finds a torn tail by its CRC and cuts it off.

### How a trim rewrites the WAL

1. The backend copies the retained suffix of the WAL to a sibling file.
2. It syncs the sibling file with `datasync`.
3. It renames the sibling file over the WAL.

A crash during the trim leaves the original WAL complete. **The cost is proportional to the bytes that the trim keeps.** Thus a trim is cheap when it runs often.

### Crash safety of a trim

A trim has two durable steps: the WAL rewrite and the new base in `p<index>.meta`. A crash can occur between them.

1. Before the rewrite, the backend writes the new base to `p<index>.trim`.
2. After it writes `p<index>.meta`, it removes `p<index>.trim`.

On open, a remaining `p<index>.trim` shows that a trim stopped. The temporary file of the WAL shows which side of the rename the crash was on:

- **The temporary file exists:** the rewrite did not land. The old base stays.
- **The temporary file is gone:** the rewrite landed. The backend uses the new base.

The backend also writes `p<index>.meta` to a sibling file and renames it. Thus `p<index>.meta` is never a zero-byte file with the bases at zero.

## Seek index

`from:` uses a seek, not a scan. The seek index has one 28-byte record for each group commit:

- The first offset of the batch
- The byte position of the batch
- The commit time of the batch

The backend does a binary search for the last batch at or before the requested offset. Commits are ordered, so all three fields increase together. Thus a search by time or by size is the same binary search. Retention uses this.

### The index is a cache for reads

**A bad index costs a scan from the start of the log. It never gives a wrong answer.**

- On open, the partition drops each record that the WAL does not support. This includes records past the WAL tail, below the retained base, or torn.
- A seek result below the base is also ignored. A crash between the index write and the metadata write can leave the base advanced with old records behind it.
- A missing, old, torn or corrupt index causes a full scan.

### S3 has no index

S3 does not need an index. Its segment keys are offsets. A seek takes the floor key from the object list, which the backend already reads. The list also gives `LastModified` and `Size` for retention.

## Retention timestamps

The seek index holds the timestamps, one for each group commit. Each record already holds the offset and the byte position.

- A timestamp costs 8 bytes for each batch, not for each entry.
- It adds no write to the commit path.
- `index_sync_ms` (default 5 s) sets how often the backend syncs the index. Thus a crash leaves only the last few seconds of commits without a date.

### Data without a date is new data

On open, a partition gives the current time to each retained range that the index cannot date. Thus a lost index makes old data look new. It never makes new data look old.

**A lost index can only delay retention. It never causes an early trim.**

A trim gives the timestamp of the batch that it cuts to the new head. Thus the head keeps its real age.

### Retention while the index rebuilds

Both limits use the index. Thus both are less exact while the index rebuilds. A cut can land only on the boundary of a record that still exists.

- `retention_bytes` recovers first. It needs only byte positions, and each new commit adds one.
- `retention_ms` also waits for the new timestamp on the head to age.

For this reason, set a size limit together with a time limit.

## Trim and replication

**A trim does not change the replication protocol.** Each batch has a logical byte offset. A logical offset counts from the first byte ever written, not from the start of the file.

- A trim of the head moves `base_byte_offset`. It does not change the offset of any batch.
- A trim needs no epoch increment, no resync and no index rebuild.
- The primary sends its new base to each replica by `:erpc`. This is advisory and best-effort. A replica that misses it keeps more data than it needs.

One case needs care: a replica whose tail is below the base of the primary. This occurs when the replica was down while the primary appended and then trimmed. No node has the bytes between the two points. The sender then does these steps:

1. It discards the copy on that replica.
2. It moves the replica base to the base of the primary.
3. It streams forward from that point.

## Replication

### The commit path

- Each replica has one long-lived channel, `DurableBuffer.Replica.Sender`.
- On the replica, a `DurableBuffer.Replica.Writer` appends each batch and syncs it.
- Up to `:max_inflight_commits` batches replicate at the same time for each partition. Callers still get replies in order.
- The replica writer combines all queued batches into one write and one `fsync`.
- Writers start on demand. Their key is `{replica_dir, partition}`.

### Batch stamps

Each batch has the stamp `{epoch, offset}`.

- The epoch is per partition. It increments on a truncate. The backend stores it in `p<index>.meta`.
- The offset is the WAL byte offset where the batch starts.

**A replica appends a batch only when the batch starts exactly at its WAL tail.** Thus a replica cannot silently become different from the primary.

### How a sender heals a replica

All failures heal in the same way:

1. The sender attaches to the replica again.
2. It compares the replica tail with the WAL of the primary.
3. If the replica missed a truncate, the sender truncates the replica first.
4. It streams the missing suffix to the replica.
5. It continues with live traffic.

Some special cases:

- A replica can be ahead of the unacknowledged queue of the sender. This is normal while an ack is in transit. The replica keeps its data, and the sender uses the replica tail as its watermark.
- Each attach makes a new reference. The sender stamps its batches with that reference. Thus the sender drops an ack from an earlier attach.

### Acks are watermarks

An ack is not a confirmation for one batch. A replica replies "durable through `{epoch, offset}`".

- The primary keeps the highest watermark from each member.
- A batch is committed when `ack:` members have watermarks at or past its end. The primary counts as a member.

RabbitMQ quorum queues use the same commit rule. DurableBuffer applies it to a configurable ack policy.

### Primary heal

With `fsync: false`, a primary can lose WAL bytes that a replica has and already acknowledged. Thus a primary heals itself when it opens:

1. `open/2` asks each replica for its tail.
2. It pulls the missing bytes from the replica that is furthest ahead.
3. It checks the CRC of each pulled frame.
4. It syncs the pulled bytes with `datasync`, whatever the `fsync:` setting is.

The partition accepts no append until the heal is done. `heal_timeout:` (default 5 s) limits how long an unreachable replica delays startup. The primary does not use a replica that does not answer.

## Transports

### What uses the transport

Only the batches use the transport. These stay on Erlang distribution:

- Control messages: attach, truncate, trim, remote tail and remote read
- Acks

Control messages are small request and reply round trips. Thus they block nothing. They also let the sender find a dead replica with `Process.monitor/1` on the remote writer.

### The transport contract

A transport must obey these rules:

1. **Deliver the batches for one replica in the order that the sender sent them.** A replica appends a batch only at its WAL tail. Thus two batches in the wrong order cause a full resync.
2. **Report a failure as a return value.** `channel/4` and `send_batch/6` return `{:error, reason}`. The sender then heals by a new attach.

The sender also catches a raise. But a transport that raises when a peer is dead writes log lines that look like a bug.

### Backpressure

`max_sender_bytes` limits the bytes in transit on both paths:

- **Live path:** the unacknowledged queue.
- **Resync path:** the unacknowledged window while the sender resyncs a replica that is behind.

The resync limit is most important for a transport that does not block its caller. All transports except distribution are in this group.

### gen_rpc backpressure

- `send/2` to a remote process blocks the sender when the distribution buffer is full.
- `:gen_rpc.ordered_cast/4` does not block the caller. It gives the payload to the gen_rpc client process, which has an unbounded mailbox.
- The TCP send then blocks the client process. Thus the bytes wait in that mailbox, not in the sender.

`max_sender_bytes` limits these bytes. A replica that is far behind can still put up to that many bytes in the mailbox.

### gen_rpc order

`ordered_cast/4` keeps the order for each `{node, tag}`. The tag is `{replica_dir, partition}`. Thus each partition has its own connection and its own order. Partitions never block each other.
