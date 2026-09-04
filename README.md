# DurableBuffer

[![CI](https://github.com/chasers/durable_buffer/actions/workflows/ci.yml/badge.svg)](https://github.com/chasers/durable_buffer/actions/workflows/ci.yml)

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
{:ok, offset} = DurableBuffer.append(:events, user_id, payload)   # blocks until durable
{:ok, first..last} =
  DurableBuffer.append_batch(:events, user_id, payloads)          # N payloads, one call,
                                                                  # one reply after commit
:ok = DurableBuffer.append_async(:events, user_id, payload)       # enqueue, don't wait
:ok = DurableBuffer.sync(:events, user_id)                        # await pending appends;
                                                                  # also surfaces commit
                                                                  # errors from async entries
:ok = DurableBuffer.sync_all(:events)

DurableBuffer.stream(:events, user_id) |> Enum.to_list()          # oldest first, durable only
DurableBuffer.stream(:events, user_id, from: 42)                  # resume at an offset
DurableBuffer.stream(:events, user_id, with_offsets: true)        # {offset, payload}

DurableBuffer.offsets(:events, user_id)                           # %{first:, durable:, next:}

:ok = DurableBuffer.truncate(:events, user_id)                    # drop consumed data
:ok = DurableBuffer.truncate_all(:events)
```

### Logical offsets

Every committed entry gets a monotonic `offset` — 0, 1, 2, … — assigned in
commit-submission order, which is also caller-reply order. The offset an
append returns is the entry's true position in the partition log, so it is
usable directly as a resume point or as an SSE `id:` field.

`offsets/2` reports three bounds:

| key | meaning |
|---|---|
| `:first` | oldest offset still retained |
| `:durable` | end of what a reader can see |
| `:next` | where the next append lands |

Offsets never repeat. `truncate/3` advances `:first` past `:next` rather
than resetting to zero, so a resumed consumer can never silently read
different data at the same offset. They survive a restart: a partition
recovers its count from the WAL on open.

Use `:first` to detect that a resume point predates retention and fall back
to a full resync, instead of silently replaying from the trim point.

`from:` seeks rather than scans. The local backend keeps a sparse index
(`p<index>.idx`, one 20-byte record per group commit) and binary-searches it
for the last batch at or before the wanted offset. The index is a pure
cache: it is written on the commit path but never `datasync`ed, and any
record the WAL does not back is dropped when the partition opens. A missing,
stale, torn or corrupt index costs a scan from the start of the log — never
a wrong answer. S3 needs no index at all: its segment keys *are* offsets, so
a seek picks the floor key from the listing it already does.

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
| `:flush_delay_ms` | 0 (adaptive) | Dwell before committing a batch started while idle. Default is adaptive: 0 normally, growing to 2 ms automatically when commit completions are slow (fsync/PUT-bound) and batches are concurrent. An explicit value fixes the dwell |
| `:max_inflight_commits` | 32 | For backends with pipelined commits (currently `Backend.Replica`): batches committing concurrently per partition; replies stay in order |
| `:heal_timeout` | 5 s | `Backend.Replica` only: how long `open/2` waits for a replica to report its tail before it opens without healing from that node |

### Tuning for small payloads

Throughput is `fsync rate × entries per commit`, so small payloads are fast
exactly when commits carry many of them:

- **Use `append_batch/4`** whenever producers can hand over lists — it pays
  the messaging cost once per list and is ~100× single appends at 256 B
  (disk-bandwidth-bound even at tiny payload sizes).
- **Use fewer partitions.** More partitions help large payloads (parallel
  bandwidth) but split small-payload batches across more fsyncs:
  2 partitions beat 10 by ~2.2× at 256 B in the bench.
- **Leave `flush_delay_ms` at its adaptive default.** The dwell only pays
  where an expensive durability step dominates each commit (fsync, S3
  PUT), and the adaptive default detects exactly that: on the fsync-on
  replicated bench it captured ~90% of the best fixed dwell's throughput
  (34.6k vs 37.8k ops/s at 256 B × 256 callers, ~5× the no-dwell 7.1k)
  while keeping single-caller latency 3× lower than the fixed dwell, and
  with `fsync: false` it stays at zero — a fixed 1 ms dwell there *drops*
  throughput ~334k → ~123k ops/s and puts a ~2 ms floor under idle
  appends. Set an explicit value only to force the trade one way.

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
CRC and truncated on open. Two sidecars sit next to it: `p<index>.meta`
(epoch and retention bounds) and `p<index>.idx` (the sparse seek index).

`fsync: false` skips the `datasync` — commits then survive a BEAM crash but
not an OS crash or power loss. Defaults to `true` for this backend: with a
single copy, the fsync *is* the durability.

### Replicated

```elixir
{DurableBuffer.Backend.Replica,
 dir: "/var/lib/events",
 replicas: [:"node2@host2", :"node3@host3"],
 replica_dir: "/var/lib/events_replica",
 ack: :quorum}
```

Each group commit is written to the local WAL and, in parallel, pipelined to
every replica node over a long-lived per-replica channel
(`DurableBuffer.Replica.Sender`), where it is appended and `datasync`ed by a
`DurableBuffer.Replica.Writer`. Commits overlap: up to
`:max_inflight_commits` batches are replicating concurrently per partition
(callers still get replies in order), and the replica writer group-commits
whatever has queued on its channel into a single write + fsync — so a slow
or dead replica stalls only its own channel, never the commit path. Replica
nodes need no configuration beyond running the `:durable_buffer`
application; writers start on demand, keyed by `{replica_dir, partition}`.

`ack:` controls when a commit counts as durable (the local write is one ack):
`:all` (default), `:quorum` (majority of `1 + length(replicas)`), or an
integer. Commits return as soon as the ack target is met; reads are served
from the local WAL.

A primary that comes back from a crash heals itself first. With
`fsync: false` it can lose WAL bytes a replica already has and already
acked, so `open/2` asks every replica for its tail and pulls back anything
it is missing from the furthest one, before the partition serves a single
append. Pulled bytes are CRC-checked frame by frame and `datasync`ed
whatever the `fsync:` setting is. `heal_timeout:` (5 s) bounds how long an
unreachable replica delays startup; a replica that does not answer is not
consulted.

Every batch is stamped with `{epoch, offset}` — a per-partition epoch that
increments on truncate (persisted in the `p<index>.meta` sidecar alongside
the retention bounds, see `DurableBuffer.Meta`) and the WAL byte offset
where the batch starts. A replica appends a batch only when
it lands exactly at its own WAL tail, so it can never diverge silently, and
every failure heals the same way: the sender re-attaches, compares the
replica's tail against the primary's WAL, and streams the replica the
missing suffix before resuming live traffic (truncating the replica first if
it missed a truncate). A replica merely ahead of the sender's unacked
queue — normal whenever an ack is in flight — keeps its data; the sender
adopts its tail as that member's watermark. Each attach mints a reference
that stamps the batches it sends, so an ack from an earlier attach is
dropped rather than counted. A replica that was down for an hour — or
that lost its disk entirely — catches up automatically; until it has, its
missing acks surface as `:insufficient_acks` errors whenever the ack policy
needs it. Primary and replica nodes must run the same `:durable_buffer`
version.

Acks are durability watermarks rather than per-batch confirmations: a
replica replies "durable through `{epoch, offset}`", the primary keeps the
highest watermark seen per member, and a batch is committed once `ack:`
members (the primary included) have watermarks at or past its end — the
same commit rule RabbitMQ's quorum queues use, generalized to the
configurable ack policy.

**Topology:** one static primary and a static list of followers. There is no
leader election, no membership protocol, and no automatic failover. The
`replicas:` list is configuration on the primary. Followers never accept
writes from anyone else and are never read. The epoch increments on
*truncate*, not on a change of leader, so it is not a leadership fencing
token. If you need automatic failover, put it above this library.

**Promoting a follower** is a manual, operator-driven procedure:

1. **Stop the old primary and keep it stopped.** Nothing fences it. If it
   comes back it re-attaches to the followers, sees data it does not have,
   and truncates them.
2. **Pick the most current follower.** With `ack: :all` every follower is
   complete. With `:quorum` or an integer they can differ — compare the
   `p<index>.wal` file sizes under `replica_dir` on each node and take the
   largest, per partition. Note that a primary which merely *restarts* does
   not need this: it heals itself from the followers at open.
3. **Check that the follower adopted the current epoch.** A truncate whose
   `:erpc` to a follower failed leaves that follower holding pre-truncate
   data until its sender re-attaches. Do not promote inside that window:

   ```elixir
   {:ok, status} = DurableBuffer.replica_status(:events, user_id)
   status[:"node2@host2"].promotable?          # false inside the window

   :ok = DurableBuffer.await_replicas(:events, user_id)   # or block on it
   ```

   See F-3 in [`tla/FINDINGS.md`](tla/FINDINGS.md).
4. **Start a buffer on that node** with `dir:` set to the follower's
   `replica_dir`, the **same** `partitions:` count, and the surviving nodes
   as `replicas:`. A follower's WAL is written by the same `Backend.Local`
   code as a primary's, so it needs no conversion.
5. **Point producers at the new node.**

The partition count must match. Keys are hashed with
`:erlang.phash2(key, partitions)` into `p<index>.wal`, so a different count
sends a key to a different file. Note also that `replica_dir` defaults to
`dir`, and that any write the promoted follower had not acked is gone.

**Truncate and replicas:** `truncate/3` bumps the epoch, wipes the local
WAL, resets the senders and returns. It does not wait for the replicas. Each
replica is sent an `:erpc` truncate; one that fails is logged, and that
replica converges shortly afterwards when its sender re-attaches, compares
epochs and truncates it. Until it does, the replica still holds pre-truncate
data — it cannot cause a false ack, because an old-epoch watermark can never
satisfy a new-epoch target, but it is not safe to promote. Use
`replica_status/3` to see which replicas have adopted the current epoch, or
`await_replicas/3` to block until they all have.

**Reads:** `stream/3` reads the primary's local WAL, gated at the durable
offset — the `ack:`-th largest replica watermark, which is exactly what a
commit waits for. A reader never sees a batch that has not met the policy,
so it never sees one that may still fail with `:insufficient_acks`. The
limit is re-read as the stream advances, so a consumer that keeps pulling
picks up data that becomes durable while it runs; like any read of a file
still being written, the stream ends at the current end of durable data.

Each partition publishes its durable offset into an `:atomics` slot, so a
reader takes it lock-free and sends the partition no message.

Pass `dirty: true` to read the whole local WAL instead, for recovery
tooling:

```elixir
DurableBuffer.stream(:events, user_id)               # durable only
DurableBuffer.stream(:events, user_id, dirty: true)  # everything on disk
```

**Durability model:** by default the replicated backend does *not* fsync
(`fsync: false`) — durability is the ack policy itself, data held on N
machines, the same stance RabbitMQ streams take. Per-node crashes are
handled by CRC torn-tail recovery; the trade-off is that correlated power
loss across an ack-quorum of nodes can lose acked writes. Pass
`fsync: true` to `datasync` on the primary and on every replica before its
ack. Fsyncing per commit costs throughput when many partitions share a
disk, so the adaptive flush dwell (see `flush_delay_ms`) kicks in
automatically under exactly that pressure — with it, `fsync: true`
measures ~35k ops/s at 256 B × 256 callers on the bench machine, ~1.7×
the old always-fsync engine, without taxing idle latency. `FSYNC=true`
toggles it in `replica_bench.exs`.

### S3

```elixir
{DurableBuffer.Backend.S3,
 bucket: "my-bucket",
 prefix: "buffers/events",
 req_options: [aws_sigv4: [access_key_id: ..., secret_access_key: ...]]}
```

Uses [`req_s3`](https://hex.pm/packages/req_s3). Each group commit uploads
one immutable segment object (`<prefix>/p<partition>/<offset>.wal`, keyed by
the segment's first logical entry offset), so durability is exactly PUT
success and there is no torn-write recovery to do.
Reads need no durability gate for the same reason: an object exists only
once its PUT succeeded.
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
disk-bandwidth-bound at **4.9M entries/s**. The replicated backend
(2 replica nodes, `ack: :all`, default `fsync: false`) sustains
**~1.4 GB/s / 197k appends/s** of quorum-acked writes with a ~105 µs
single-caller median. Full results for
all three backends — including batch and mixed append + stream workloads,
caller latency distributions, and small-payload tuning — are in
[`bench/README.md`](bench/README.md).

Each script prints an aggregate **throughput** grid (ops/s and MB/s over a
payload-size × caller-concurrency matrix, measured with timed concurrent
loops), a **mixed append + stream** grid (readers re-streaming partitions
while writers append), and Benchee **caller latency** distributions
(median / p99) at several concurrency levels.

```sh
mix run bench/local_bench.exs                       # PARTITIONS=N
mix run bench/replica_bench.exs                     # REPLICAS=2 ACK=all|quorum|N PARTITIONS=N FSYNC=true|false
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

### TLA+ models

The replication protocol is model-checked. `tla/Replication.tla` models the
primary, the sender and the replica writer across crashes, dropped batches,
re-attaches, resyncs and truncates.

```sh
./tla/run check                     # every config vs tla/expected.tsv
./tla/run all                       # every config, full TLC output
./tla/run Replication_core          # one config
```

`check` is the gate: each config's PASS/VIOLATED result must match
`tla/expected.tsv`. A VIOLATED row is intentional — a negative control, or a
documented finding on the code as it stands. The runner downloads
`tla2tools.jar` on first use and needs only a JDK. See
[`tla/README.md`](tla/README.md) for the config matrix and
[`tla/FINDINGS.md`](tla/FINDINGS.md) for what each spec proved or found.

CI runs formatting, a warnings-as-errors compile, the test suite, and the
TLA+ gate on every push and pull request.

## Releases

Bumping `version:` in `mix.exs` and pushing to `main` cuts a release: the
release workflow re-runs the tests, then creates the `v<version>` tag and a
GitHub release with generated notes. Pushes that touch `mix.exs` without
changing the version are no-ops (the existing tag is detected and skipped).
