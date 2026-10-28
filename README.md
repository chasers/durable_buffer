# DurableBuffer

[![CI](https://github.com/chasers/durable_buffer/actions/workflows/ci.yml/badge.svg)](https://github.com/chasers/durable_buffer/actions/workflows/ci.yml)

DurableBuffer is a partitioned buffer for Elixir. An append returns only after its data is **durable**.

You choose where the data is durable:

- **Local disk.** The backend calls `fsync` on a write-ahead log (WAL).
- **Replica nodes.** A set number of nodes acknowledge the write.
- **S3.** The S3 PUT request succeeds.

## Why it is fast

The buffer uses **group commit**. Appends that arrive while a commit runs wait for the next commit. Then they commit together.

- The expensive step (`fsync`, replication or PUT) runs once per batch, not once per append.
- A single append waits for exactly one commit.
- Under load, batches grow by themselves. Throughput grows until the disk, network or S3 is the limit.

## Install

The package is not on Hex. Add it as a git dependency in `mix.exs`:

```elixir
{:durable_buffer, github: "chasers/durable_buffer", tag: "v0.6.2"}
```

Each release has a `v<version>` tag. See [Releases](#releases).

## Quick start

1. Add a buffer to your supervision tree:

   ```elixir
   children = [
     {DurableBuffer,
      name: :events,
      backend: {DurableBuffer.Backend.Local, dir: "/var/lib/events"},
      partitions: 8}
   ]
   ```

2. Append, read and trim from any process:

   ```elixir
   {:ok, offset} = DurableBuffer.append(:events, user_id, payload)
   {:ok, first..last} = DurableBuffer.append_batch(:events, user_id, payloads)
   :ok = DurableBuffer.append_async(:events, user_id, payload)

   :ok = DurableBuffer.sync(:events, user_id)
   :ok = DurableBuffer.sync_all(:events)

   DurableBuffer.stream(:events, user_id) |> Enum.to_list()
   DurableBuffer.stream(:events, user_id, from: 42)
   DurableBuffer.stream(:events, user_id, with_offsets: true)

   DurableBuffer.offsets(:events, user_id)

   :ok = DurableBuffer.trim(:events, user_id)
   :ok = DurableBuffer.trim(:events, user_id, upto: 5_000)
   DurableBuffer.retention(:events, user_id)

   :ok = DurableBuffer.truncate(:events, user_id)
   :ok = DurableBuffer.truncate_all(:events)
   ```

### What each call does

| Call | Result |
|---|---|
| `append/4` | Blocks until the payload is durable. Returns its offset. |
| `append_batch/4` | Appends a list in one call. Replies once, after the commit. Returns the offset range. |
| `append_async/3` | Adds the payload to the queue. Does not wait. |
| `sync/3` | Waits for all queued appends on one partition. Returns commit errors from `append_async/3` entries. |
| `sync_all/2` | Does `sync/3` for all partitions. |
| `stream/3` | Reads durable entries, oldest first. |
| `offsets/2` | Returns `%{first:, durable:, next:}`. |
| `trim/3` | Drops old entries by the retention policy, or below `upto:`. |
| `retention/2` | Returns `%{oldest_age_ms:, bytes:}`. |
| `truncate/3` | Drops all entries in one partition. |
| `truncate_all/2` | Does `truncate/3` for all partitions. |

An append raises `ArgumentError` in the caller if a payload is not iodata.

### Partitions

The buffer hashes each `partition_key` (any term) to one partition. The default count is `System.schedulers_online()`.

- Each partition has its own writer, WAL and backend state.
- Partitions commit in parallel. Add partitions to use more disk bandwidth.
- Each caller frames its own WAL entries (copy and CRC32 checksum) before it sends them. One writer does not do this work for all callers.

## Offsets

Each committed entry gets an offset: 0, 1, 2 and so on. **An offset never repeats.**

- The buffer assigns offsets in commit order. This is also the order of the replies to callers.
- The offset from an append is the true position of the entry in the partition log.
- Use an offset as a resume point, or as the `id:` field of a server-sent event (SSE).
- Offsets survive a restart. A partition counts its entries from the WAL when it opens.

`offsets/2` returns three bounds:

| Key | Meaning |
|---|---|
| `:first` | The oldest offset that the buffer keeps |
| `:durable` | The end of the data that a reader can see |
| `:next` | The offset that the next append gets |

`truncate/3` moves `:first` to `:next`. It does not reset the offsets to zero. Thus a consumer that resumes never reads different data at the same offset.

## Consumers

**The buffer does not track consumers.** It stores no consumer positions. Consumer positions do not stop retention.

Each consumer keeps its own cursor. An SSE client does this with `Last-Event-ID`. A Kafka consumer does this when it stores its offset outside the log.

```elixir
from = MyApp.Cursor.load("worker-1") || 0

try do
  for {offset, payload} <-
        DurableBuffer.stream(:events, user_id, from: from, with_offsets: true) do
    handle(payload)
    MyApp.Cursor.save("worker-1", offset + 1)
  end
rescue
  error in DurableBuffer.OutOfRangeError -> resync_from(error.first)
end
```

### A read below `:first` raises

`stream/3` raises `DurableBuffer.OutOfRangeError` when `from:` is below `:first`.

- The error gives the requested offset and the oldest offset that the buffer keeps. The caller then resyncs on purpose.
- A silent start at `:first` gives a stream with a hole that looks complete. Nothing else protects a consumer that falls behind retention.
- `stream/3` reads the bounds before it builds the stream. Thus the error comes at the call, not during the iteration.

## Retention

Set a time limit, a size limit, or both. The buffer then finds the trim point.

```elixir
{DurableBuffer,
 name: :events,
 backend: {DurableBuffer.Backend.Local, dir: "/var/lib/events"},
 retention_ms: :timer.hours(24 * 7),
 retention_bytes: 10 * 1024 * 1024 * 1024}
```

**The first limit to apply wins.** A time limit does not limit disk use during a traffic spike. A size limit does not limit age when traffic is low.

### Retention runs by itself

A retention limit starts a timer on each partition. You do not have to call `trim/3`.

- `retention_interval_ms` sets the interval. The default is 60 seconds.
- `:infinity` stops the timer. Then you call `trim/3` yourself.
- Each partition starts at a random time in the first interval. Thus the partitions do not all trim at the same time.
- A timed trim waits behind the commits in the queue. It does not interrupt them.

Call `trim/3` yourself only to trim before the next timer tick.

### Results of `trim/3`

| Case | Result |
|---|---|
| No retention limit and no `upto:` | `{:error, :no_retention_policy}` |
| Nothing to drop | `:ok` |
| `upto:` is past the durable offset | `{:error, :not_durable}` |
| The policy point is past the durable offset | The buffer trims to the durable offset |

`upto:` is exclusive. The buffer drops each entry below it, and `upto:` becomes the new `:first`.

### Where the trim lands

- **Local, explicit `upto:`:** exactly at the point.
- **Local, policy point:** at the batch boundary at or below the point.
- **S3:** at the segment boundary at or below the point. S3 drops only segments that are fully below the point.

### Monitor retention

`retention/2` reports what the policy has to work with:

```elixir
{:ok, %{oldest_age_ms: 604_012, bytes: 8_431_923_712}} =
  DurableBuffer.retention(:events, user_id)
```

**Alert on `:oldest_age_ms`.**

- On a busy partition, it stays near `retention_ms`.
- If it climbs far past `retention_ms`, time retention has stopped.
- It is `nil` only for an empty partition.

### Retention and the seek index

Retention uses the seek index of the local backend. Thus you cannot set a retention limit together with `index: false`. That combination raises at startup. Without the raise, the buffer grows with no limit and gives no warning.

A lost or damaged index can only delay a trim. It never causes an early trim. See [docs/internals.md](docs/internals.md#retention-timestamps).

## Options

| Option | Default | Meaning |
|---|---|---|
| `:name` | required | The buffer name (an atom) |
| `:backend` | required | The backend spec, `{module, opts}` |
| `:partitions` | schedulers | The number of partition writers |
| `:max_batch_bytes` | 8 MiB | Commit a batch when it reaches this size |
| `:max_batch_entries` | 5000 | Commit a batch when it has this many entries |
| `:flush_delay_ms` | 0 (adaptive) | The wait before an idle partition commits a new batch. See [Flush delay](#flush-delay) |
| `:max_inflight_commits` | 32 | `Backend.Replica` only. The number of batches that commit at the same time per partition. Replies stay in order |
| `:heal_timeout` | 5 s | `Backend.Replica` only. How long `open/2` waits for a replica tail. After that, the partition opens without that replica |
| `:transport` | `Transport.Distribution` | `Backend.Replica` only. The connection for replicated batches. See [Transports](#transports) |
| `:retention_ms` | none | The maximum age of the data in each partition |
| `:retention_bytes` | none | The maximum bytes in each partition |
| `:retention_interval_ms` | 60 s | The interval of the retention timer. Use a positive integer or `:infinity` |

### Flush delay

The default `flush_delay_ms` is adaptive.

- It is 0 when commits are fast.
- It grows to 2 ms when commits are slow (for example, `fsync` or S3 PUT) and callers append at the same time.
- An explicit value sets a fixed wait.

## Small payloads

Throughput is `fsync rate × entries per commit`. Thus small payloads are fast only when each commit holds many of them.

1. **Use `append_batch/4`** when producers can give a list. It is about 100× faster than single appends at 256 B. It reaches the disk bandwidth limit even with very small payloads.
2. **Use fewer partitions.** More partitions help large payloads. For small payloads, they split the batches across more `fsync` calls. In the bench, 2 partitions are about 2.2× faster than 10 at 256 B.
3. **Keep the adaptive `flush_delay_ms`.** Set a fixed value only to force a trade-off.

The adaptive delay helps only when a slow durability step controls each commit. The bench shows this:

| Replicated bench, 256 B × 256 callers | ops/s |
|---|---|
| `fsync: true`, no delay | 7.1k |
| `fsync: true`, adaptive delay | 34.6k |
| `fsync: true`, best fixed delay | 37.8k |
| `fsync: false`, adaptive delay (stays at 0) | about 334k |
| `fsync: false`, fixed 1 ms delay | about 123k |

- With `fsync: true`, the adaptive delay gets about 90% of the best fixed delay. Its single-caller latency is 3× lower.
- With `fsync: false`, a fixed 1 ms delay cuts throughput. It also adds about 2 ms to each idle append.

Commits run as a pipeline. The next batch forms while the current `fsync` or PUT runs. Thus `append_async/3` and `append_batch/4` keep the disk busy without a wait for each commit.

## Backends

### Local disk

```elixir
{DurableBuffer.Backend.Local, dir: "/var/lib/events"}
```

Each partition has one append-only WAL file, `p<index>.wal`. Each group commit does one write and one `:file.datasync/1`.

| Option | Default | Meaning |
|---|---|---|
| `fsync:` | `true` | `false` skips the `datasync`. Then commits survive a crash of the Erlang VM (BEAM), but not an OS crash or a power loss |
| `index_sync_ms:` | 5 s | How often the seek index is synced to disk |
| `index:` | `true` | `false` turns off the seek index. You cannot use it with retention |

**Keep `fsync: true` on this backend.** With one copy of the data, the `fsync` is the only durability.

The WAL detects torn writes after a crash by CRC. It cuts them off when it opens. For the file layout and the crash safety of a trim, see [docs/internals.md](docs/internals.md#local-backend-files).

### Replicated

```elixir
{DurableBuffer.Backend.Replica,
 dir: "/var/lib/events",
 replicas: [:"node2@host2", :"node3@host3"],
 replica_dir: "/var/lib/events_replica",
 ack: :quorum}
```

Each group commit goes to the local WAL. At the same time, it goes to each replica node, which writes and syncs it.

| Option | Default | Meaning |
|---|---|---|
| `dir:` | required | The WAL directory on the primary |
| `replicas:` | required | The replica nodes |
| `replica_dir:` | `dir` | The WAL directory on each replica |
| `ack:` | `:all` | When a commit is durable. See [The ack policy](#the-ack-policy) |
| `fsync:` | `false` | Sync to disk before each ack. See [Durability](#durability) |
| `max_sender_bytes:` | 64 MiB | The maximum unacknowledged bytes for each replica |
| `transport:` | `Transport.Distribution` | See [Transports](#transports) |

- **Setup:** replica nodes need only the running `:durable_buffer` application. Their writers start on demand.
- **Versions:** the primary and the replicas must run the same `:durable_buffer` version.
- **Slow replicas:** a slow or dead replica stops only its own channel. It does not stop the commit path.
- **Recovery:** a replica that was down, or that lost its disk, catches up by itself.

#### The ack policy

`ack:` sets when a commit is durable. The local write counts as one ack.

| Value | A commit is durable when |
|---|---|
| `:all` (default) | All nodes acknowledge it |
| `:quorum` | A majority of `1 + length(replicas)` acknowledge it |
| an integer | That number of nodes acknowledge it |

A commit returns when it meets the ack target. A missing ack gives `{:error, :insufficient_acks}`. This continues until the replica catches up.

#### Durability

**By default, the replicated backend does not `fsync`** (`fsync: false`). The durability is the ack policy: the data is on N machines. RabbitMQ streams take the same approach.

- A crash of one node is safe. CRC checks find torn writes on recovery.
- A power loss on a quorum of nodes at the same time can lose acknowledged writes.
- `fsync: true` syncs on the primary and on each replica before its ack.

With `fsync: true`, each commit costs more when many partitions share one disk. The adaptive flush delay starts in this case. It gives about 35k ops/s at 256 B × 256 callers on the bench machine. That is about 1.7× the old engine, which always synced.

A primary that restarts heals itself before it accepts an append. It pulls back any bytes that a replica has and it does not have. See [docs/internals.md](docs/internals.md#primary-heal).

#### Reads

`stream/3` reads the WAL of the primary. It stops at the durable offset, which is the point that a commit waits for.

- A reader never sees a batch that can still fail with `:insufficient_acks`.
- The stream reads the durable offset again as it moves. A consumer that keeps pulling gets new durable data.
- The stream ends at the current end of the durable data.
- A reader gets the durable offset from an `:atomics` slot. It sends no message to the partition.

Use `dirty: true` to read the full local WAL, for example in recovery tools:

```elixir
DurableBuffer.stream(:events, user_id)
DurableBuffer.stream(:events, user_id, dirty: true)
```

#### Topology

The buffer has **one static primary** and a static list of followers.

- It has no leader election, no membership protocol and no automatic failover.
- The `replicas:` list is configuration on the primary.
- Followers accept writes only from their primary. Nothing reads from a follower.
- The epoch changes on a truncate, not on a leader change. It is not a fencing token for leaders.

If you need automatic failover, build it above this library.

#### Promote a follower

Promotion is a manual procedure:

1. **Stop the old primary and keep it stopped.** Nothing fences it. If it starts again, it sees data that it does not have and truncates the followers.
2. Find the most current follower:
   - With `ack: :all`, each follower is complete.
   - With `:quorum` or an integer, the followers can differ. For each partition, compare the size of `p<index>.wal` under `replica_dir` on each node. Use the largest.
3. Make sure that the follower has the current epoch. After a truncate, a follower can keep old data for a short time. Do not promote it in that window:

   ```elixir
   {:ok, status} = DurableBuffer.replica_status(:events, user_id)
   status[:"node2@host2"].promotable?

   :ok = DurableBuffer.await_replicas(:events, user_id)
   ```

   See F-3 in [`tla/FINDINGS.md`](tla/FINDINGS.md).
4. Start a buffer on that node with these settings:
   - `dir:` is the `replica_dir` of the follower.
   - `partitions:` is the **same** count as before.
   - `replicas:` lists the remaining nodes.
5. Send producer traffic to the new node.

A primary that only restarts does not need this procedure. It heals itself from the followers when it opens.

Some facts about promotion:

- **The partition count must match.** The buffer hashes keys with `:erlang.phash2(key, partitions)` to `p<index>.wal`. A different count sends a key to a different file.
- The follower WAL uses the same `Backend.Local` code as the primary WAL. It needs no conversion.
- `replica_dir` defaults to `dir`.
- A write that the promoted follower did not acknowledge is lost.

#### Truncate with replicas

`truncate/3` returns without a wait for the replicas.

1. It increments the epoch, clears the local WAL and resets the senders.
2. It sends a truncate to each replica by `:erpc`. A failed call goes to the log.
3. A replica that missed the truncate converges when its sender attaches again.

Until it converges, the replica keeps its old data. It cannot give a false ack, because an old-epoch ack never meets a new-epoch target. But it is not safe to promote. Use `replica_status/3` to see which replicas have the current epoch. Use `await_replicas/3` to wait for all of them.

#### Transports

`transport:` sets the connection that carries replicated batches. The default is `DurableBuffer.Transport.Distribution`, which uses Erlang distribution.

**Head-of-line blocking is the reason to change it.** Erlang distribution uses one TCP connection for each node pair. All processes on the two nodes share it.

- An 8-partition buffer under load has 8 senders on that one socket.
- The cluster heartbeat uses the same socket.
- A delayed heartbeat looks like a node failure. Replication exists to survive a node failure.

Only the batches use the transport. Control messages and acks stay on distribution. See [docs/internals.md](docs/internals.md#transports).

##### gen_rpc

`DurableBuffer.Transport.GenRPC` gives each node pair its own TCP socket, outside distribution:

```elixir
{DurableBuffer.Backend.Replica,
 dir: "/var/lib/events",
 replicas: [:"node2@host2"],
 transport: DurableBuffer.Transport.GenRPC}
```

**Add the dependency yourself**, on every primary and replica node:

```elixir
{:gen_rpc, git: "https://github.com/emqx/gen_rpc.git", tag: "3.6.1"}
```

`:durable_buffer` does not declare it. The maintained fork is not on Hex. Hex does not allow a git dependency in a published package. `init_config/1` raises at startup if the transport is set and `:gen_rpc` is not loaded.

gen_rpc has these costs:

- **A port.** gen_rpc uses its own TCP port on each node. Open it between the nodes. Two nodes on one host need `port_discovery: :stateless` or different `tcp_server_port` values. If not, both use port 5369.
- **Its own TLS settings.** gen_rpc uses plain TCP by default. The Transport Layer Security (TLS) settings of distribution do not apply. Set `ssl_server_options` and `ssl_client_options`.
- **A different backpressure point.** The bytes wait in the unbounded mailbox of the gen_rpc client, not in the sender. `max_sender_bytes` limits them.
- **A whitelist.** If `rpc_module_control` is `:whitelist`, add `DurableBuffer.Replica` to the list. If not, gen_rpc drops each batch and logs it only at debug level. The send still reports success.

`bench/transport_bench.exs` puts an unrelated load on the distribution channel. It then measures both transports.

| Under load | distribution | gen_rpc |
|---|---|---|
| Throughput | falls about 2.5× | stays near its idle value |
| p50 latency | rises 7–9× | rises about 1.3× |
| p99 latency | rises 4.5–7× | rises 1.1–1.7× |

When the channel is idle, the two transports give the same results. See [bench/README.md](bench/README.md) for the numbers.

- **Use gen_rpc** when the node pair carries other traffic in addition to replication.
- **Use distribution** when the pair is quiet. Use it also when the extra port, TLS settings and git dependency cost too much.

### S3

```elixir
{DurableBuffer.Backend.S3,
 bucket: "my-bucket",
 prefix: "buffers/events",
 req_options: [aws_sigv4: [access_key_id: ..., secret_access_key: ...]]}
```

**Add the dependencies yourself.** `:req` and `:req_s3` are optional dependencies. An application without S3 does not get them.

```elixir
{:req, "~> 0.5"},
{:req_s3, "~> 0.2"}
```

`init_config/1` raises at startup if one of them is not loaded.

The backend uses [`req_s3`](https://hex.pm/packages/req_s3).

- Each group commit uploads one segment object: `<prefix>/p<partition>/<offset>.wal`. The key is the first entry offset of the segment.
- An object never changes after the upload.
- Durability is the success of the PUT. Thus there are no torn writes to recover.
- Reads need no durability check. An object exists only after its PUT succeeds.

Credentials come from `req_options` or from the standard `AWS_*` environment variables. To use MinIO or another S3-compatible store, set `aws_endpoint_url_s3:`. In tests, pass `req_options: [plug: {Req.Test, YourStub}]`.

Group commit helps most on S3, because a PUT is slow. While one PUT runs, new appends go into the next segment. Throughput is `batch size × partitions / PUT latency`.

## Benchmarks

Results on a MacBook Pro (M1 Max, internal SSD):

| Backend | Workload | Result |
|---|---|---|
| Local, 10 partitions | 64 KB payloads, 256 callers | about 2 GB/s, `fsync`-durable |
| Local, `append_batch/4` | 256 B payloads | 4.9M entries/s, disk bandwidth limit |
| Replica, 2 nodes, `ack: :all` | `fsync: false` | about 1.4 GB/s, 197k appends/s |
| Replica, 2 nodes, `ack: :all` | 1 caller | about 105 µs median latency |

**See [`bench/README.md`](bench/README.md) for all results.** It includes batch and mixed workloads, latency distributions and small-payload settings.

Run the benchmarks:

```sh
mix run bench/local_bench.exs                        # PARTITIONS=N
mix run bench/replica_bench.exs                      # REPLICAS=2 ACK=all|quorum|N PARTITIONS=N FSYNC=true|false
mix run bench/s3_bench.exs                           # fake S3, S3_SIM_LATENCY_MS=30
S3_BENCH_BUCKET=my-bucket mix run bench/s3_bench.exs # real S3 (AWS_* env vars)
mix run bench/writer_framing_bench.exs               # CALLERS=64 APPENDS=2000 PAYLOAD_KB=256
```

Each backend script prints these results:

- **Throughput:** ops/s and MB/s for each payload size and caller count.
- **Mixed append and stream:** readers stream partitions while writers append.
- **Stream only:** the read path with no writers.
- **Seek:** the time to the first entry of a `from:` read, at different depths.
- **Caller latency:** Benchee median and p99 at different caller counts.

Some scripts have special conditions:

- `writer_framing_bench.exs` uses one partition writer and a no-op backend. It measures the message path of the writer, not durability.
- `replica_bench.exs` starts real replica nodes with `:peer` on the local host. It measures protocol overhead, not network round-trip time.
- `s3_bench.exs` uses an in-memory fake S3 with a simulated PUT latency. Set `S3_BENCH_BUCKET` to use real S3.

## Tests

```sh
mix test
```

The suite covers these areas:

- WAL framing and recovery from torn writes
- Group commit and error propagation
- All three backends. S3 uses a `Req.Test` fake with ListObjectsV2 pagination. Replication uses `:erpc` to the local node.
- Recovery after a restart, end to end

### TLA+ models

The replication protocol has a TLA+ model, which the TLC model checker checks. `tla/Replication.tla` models the primary, the sender and the replica writer. It covers crashes, dropped batches, re-attaches, resyncs and truncates.

```sh
./tla/run check                     # every config vs tla/expected.tsv
./tla/run all                       # every config, full TLC output
./tla/run Replication_core          # one config
```

**`check` is the gate.** The PASS or VIOLATED result of each config must match `tla/expected.tsv`.

- A VIOLATED row is intentional. It is a negative control or a known finding on the current code.
- The runner downloads `tla2tools.jar` the first time. It needs only a JDK.
- See [`tla/README.md`](tla/README.md) for the config matrix.
- See [`tla/FINDINGS.md`](tla/FINDINGS.md) for what each spec proved or found.

CI runs these on each push and pull request:

- The formatter check
- A compile with warnings as errors
- The test suite
- The TLA+ gate

## Releases

To make a release, change `version:` in `mix.exs` and push to `main`.

The release workflow then does these steps:

1. It runs the tests again.
2. It creates the `v<version>` tag.
3. It creates a GitHub release with generated notes.

A push that changes `mix.exs` but not the version does nothing. The workflow finds the existing tag and skips it.
