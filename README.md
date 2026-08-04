# DurableBuffer

A partitioned, group-committed buffer for Elixir, durable to **local disk**,
**N replica nodes**, or **S3**.

Every append blocks until its data is durable per the backend's guarantee —
an `fsync`, a set of replica acks, or a successful S3 PUT. The trick that
makes that fast is **group commit**: all appends that arrive at a partition
while a commit is in flight are committed together, so the expensive
durability step is paid once per batch, not once per append. A lone append
pays exactly one commit of latency; under load, batches grow automatically
and throughput scales until the disk (or network, or S3) is the bottleneck.

## Usage

Start a buffer under your supervision tree:

```elixir
children = [
  {DurableBuffer,
   name: :events,
   backend: {DurableBuffer.Backend.Local, dir: "/var/lib/events"},
   partitions: 8}
]
```

Append, read, and trim:

```elixir
:ok = DurableBuffer.append(:events, user_id, payload)      # blocks until durable
:ok = DurableBuffer.append_batch(:events, user_id, payloads) # N payloads, one call,
                                                            # one reply after commit
:ok = DurableBuffer.append_async(:events, user_id, payload) # enqueue, don't wait
:ok = DurableBuffer.sync(:events, user_id)                  # await pending appends;
                                                            # also surfaces commit errors
                                                            # from async entries
:ok = DurableBuffer.sync_all(:events)

DurableBuffer.stream(:events, user_id) |> Enum.to_list()    # oldest first

:ok = DurableBuffer.truncate(:events, user_id)              # drop consumed data
:ok = DurableBuffer.truncate_all(:events)
```

The `partition_key` (any term) is hashed to one of a fixed number of
partitions (default `System.schedulers_online()`). Each partition has its own
writer, WAL, and backend state, so partitions commit fully in parallel —
use more partitions to saturate your disk.

### Options

| Option | Default | Meaning |
|---|---|---|
| `:name` | required | Buffer name (atom) |
| `:backend` | required | `{module, opts}` backend spec |
| `:partitions` | schedulers | Number of parallel partition writers |
| `:max_batch_bytes` | 8 MiB | Force a flush when a pending batch reaches this size |
| `:max_batch_entries` | 5000 | Force a flush at this many pending entries |
| `:flush_delay_ms` | 0 | Dwell time before committing a batch started while idle — lets batches fill at moderate load, trading median latency for throughput (~+50% at 256 B under load with 1 ms on the bench machine) |

### Tuning for small payloads

Throughput is `fsync rate × entries per commit`, so small payloads are fast
exactly when commits carry many of them:

- **Use `append_batch/4`** whenever producers can hand over lists — it pays
  the messaging cost once per list and is ~100× single appends at 256 B
  (disk-bandwidth-bound even at tiny payload sizes).
- **Use fewer partitions.** More partitions help large payloads (parallel
  bandwidth) but split small-payload batches across more fsyncs:
  2 partitions beat 10 by ~2.2× at 256 B in the bench.
- **Consider `flush_delay_ms: 1`** if median latency matters less than
  throughput at moderate concurrency.

Commits are pipelined internally (batch formation overlaps the in-flight
fsync/PUT), so `append_async` and `append_batch` producers keep the disk fed
without waiting for commit boundaries.

## Backends

### Local disk

```elixir
{DurableBuffer.Backend.Local, dir: "/var/lib/events"}
```

Append-only WAL file per partition (`p<index>.wal`), one write + one
`:file.datasync/1` per group commit. Entries are framed as
`<<len::32, crc32::32, payload>>`; torn tails from crashes are detected by
CRC and truncated on open.

### Replicated

```elixir
{DurableBuffer.Backend.Replica,
 dir: "/var/lib/events",
 replicas: [:"node2@host2", :"node3@host3"],
 replica_dir: "/var/lib/events_replica",
 ack: :quorum}
```

Each group commit is written to the local WAL and, in parallel, shipped over
`:erpc` to every replica node, where it is appended and `datasync`ed by a
`DurableBuffer.Replica.Writer`. Replica nodes need no configuration beyond
running the `:durable_buffer` application; writers start on demand, keyed by
`{replica_dir, partition}`.

`ack:` controls when a commit counts as durable (the local write is one ack):
`:all` (default), `:quorum` (majority of `1 + length(replicas)`), or an
integer. Commits return as soon as the ack target is met; reads are served
from the local WAL.

### S3

```elixir
{DurableBuffer.Backend.S3,
 bucket: "my-bucket",
 prefix: "buffers/events",
 req_options: [aws_sigv4: [access_key_id: ..., secret_access_key: ...]]}
```

Uses [`req_s3`](https://hex.pm/packages/req_s3). Each group commit uploads
one immutable segment object (`<prefix>/p<partition>/<seq>.wal`), so
durability is exactly PUT success and there is no torn-write recovery to do.
Credentials come from `req_options` or the standard `AWS_*` environment
variables; point `aws_endpoint_url_s3:` at MinIO or another S3-compatible
store. In tests, pass `req_options: [plug: {Req.Test, YourStub}]`.

S3's high PUT latency is where group commit matters most: while one PUT is
in flight, every arriving append queues into the next segment, so throughput
is `batch size × partitions / PUT latency` rather than one append per
round trip.

## Benchmarks

On a MacBook Pro (M1 Max, internal SSD), the local backend sustains
**~2 GB/s of fsync-durable appends** (64 KB payloads, 256 concurrent
callers, 10 partitions), and `append_batch` keeps even 256 B payloads
disk-bandwidth-bound at **4.9M entries/s**. Full results for all three
backends — including batch and mixed append + stream workloads, caller
latency distributions, and small-payload tuning — are in
[`bench/README.md`](bench/README.md).

Each script prints an aggregate **throughput** grid (ops/s and MB/s over a
payload-size × caller-concurrency matrix, measured with timed concurrent
loops), a **mixed append + stream** grid (readers re-streaming partitions
while writers append), and Benchee **caller latency** distributions
(median / p99) at several concurrency levels.

```sh
mix run bench/local_bench.exs                       # PARTITIONS=N
mix run bench/replica_bench.exs                     # REPLICAS=2 ACK=all|quorum|N PARTITIONS=N
mix run bench/s3_bench.exs                          # fake S3, S3_SIM_LATENCY_MS=30
S3_BENCH_BUCKET=my-bucket mix run bench/s3_bench.exs # real S3 (AWS_* env vars)
```

`replica_bench.exs` boots real replica nodes with `:peer` on the local host
(measuring protocol overhead, not network RTT). `s3_bench.exs` runs against
an in-memory fake with simulated PUT latency unless `S3_BENCH_BUCKET` is set.

## Testing

```sh
mix test
```

The suite covers WAL framing and torn-tail recovery, group-commit batching
and error propagation, all three backends (S3 via a `Req.Test` fake with
ListObjectsV2 pagination, replication via `:erpc` to the local node), and
end-to-end restart recovery.
