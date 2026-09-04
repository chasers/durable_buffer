# Benchmark results

Captured 2026-08-04 on a MacBook Pro (Apple M1 Max, 10 cores, 64 GB RAM,
internal SSD), Erlang/OTP 29, Elixir 1.20.2. Includes commit pipelining,
`append_batch`, and `flush_delay_ms` (results for the latter two below).

Every append is a blocking, durable write — the numbers below are fsync-,
replication-, or PUT-durable, not buffered. Measurements per backend:

- **Throughput** — timed loops of concurrent callers issuing
  `DurableBuffer.append/3`; aggregate ops/s and MB/s.
- **append_batch throughput** (local) — callers issuing
  `DurableBuffer.append_batch/4` with N payloads per call.
- **Mixed append + stream** — writers appending while readers repeatedly
  re-stream whole partitions; both sides measured simultaneously.
- **Stream only** (local) — readers re-streaming with no writers at all, so
  the read path is measured on its own.
- **Seek** (local) — time to the first entry of a `from:` read at
  increasing depth, which is where a scan would show up.
- **Caller latency** — Benchee distributions for a single append under
  increasing `parallel:` load.

> **The append grid below dates from 2026-08-04 and needs a clean
> re-capture.** The harness matched `:ok` from `append/3`, which started
> returning `{:ok, offset}` in 0.4.0, so every bench crashed on its first
> append from that change until 2026-09-04. Nothing compiles or runs
> `bench/`, so it went unnoticed. The read sections below are from
> 2026-09-04 on the fixed harness.
>
> Single-caller rows are fsync-bound and **vary ±40% run to run** on this
> machine — repeated medians of the same build ranged 1.4k-3.7k ops/s. Do
> not read a regression out of one run; alternate builds and take medians.

Reproduce with:

```sh
mix run bench/local_bench.exs
REPLICAS=2 ACK=all mix run bench/replica_bench.exs
mix run bench/transport_bench.exs
mix run bench/s3_bench.exs                              # fake S3, 30ms PUT
S3_BENCH_BUCKET=my-bucket mix run bench/s3_bench.exs    # real S3
```

`BENCH_DURATION_MS`, `BENCH_WARMUP_MS`, and `BENCH_TIME` shorten runs;
`PARTITIONS` overrides partition count. Numbers at the disk-bandwidth
ceiling vary ±20-30% between runs, and single-caller fsync-bound rows vary
considerably more.

## Local (10 partitions)

Group commit in action: one caller is fsync-bound (~2-3k commits/s on this
SSD); adding callers grows batches instead of adding fsyncs, scaling to
**2.3 GB/s** of durable writes at 64 KB payloads (1.9-2.3 GB/s across runs).

```
payload   callers         ops/s      MB/s
256B      1                2.3k       0.6
256B      8                4.8k       1.2
256B      64              18.7k       4.6
256B      256             62.3k      15.2
4KB       1                2.6k      10.3
4KB       8                5.1k      20.0
4KB       64              16.6k      64.9
4KB       256             61.7k     241.0
64KB      1                3.0k     187.3
64KB      8                4.0k     249.4
64KB      64              13.2k     827.9
64KB      256             36.7k    2294.9
```

### append_batch (256 B payloads)

Throughput is `fsync rate × entries per commit`; `append_batch` fills
commits without needing hundreds of callers. At batch 1000 the buffer is
disk-bandwidth-bound even at 256 B — ~75× the best single-append
configuration:

```
batch   callers      entries/s      MB/s
10      1                33.1k       8.1
10      8                51.4k      12.5
10      64              177.9k      43.4
100     1               287.9k      70.3
100     8               501.1k     122.3
100     64               1.43M     350.0
1000    1               890.4k     217.4
1000    8                2.51M     613.1
1000    64               4.87M    1189.4
```

### Small-payload tuning (256 B single appends, ops/s at callers 1/8/64/256)

Fewer partitions concentrate small-payload batches into fewer fsyncs;
`flush_delay_ms` trades idle latency for batch fill at moderate load:

```
partitions=2                 3.6k / 8.1k / 40.5k / 145.1k
partitions=4                 3.2k / 5.0k / 31.2k / 120.1k
partitions=10 (default)      2.0k / 5.3k / 18.5k /  65.7k

flush_delay_ms=1 (10 parts)   478 / 3.6k / 28.0k / 102.1k
flush_delay_ms=5 (10 parts)   159 / 1.3k / 10.0k /  40.0k
```

Mixed append + stream (4 KB payload) — streaming costs writers little;
reads come largely from page cache:

```
writers  readers       w ops/s    w MB/s   r entries/s    r MB/s
1        1                2.1k       8.2        629.4k    2458.7
8        8                4.6k      18.0         1.37M    5337.4
64       8               15.0k      58.7        490.8k    1917.3
256      8               60.6k     236.6        560.7k    2190.3
```

Caller latency (4 KB payload):

```
parallel      average     median     99th %
1           336.74 μs  261.46 μs    2.26 ms
16            2.31 ms    2.13 ms    6.73 ms
128           3.49 ms    3.17 ms   12.16 ms
```

## Stream only (1 partition, 4 KB payloads, 20k entries)

Reads with no writers, re-streaming the whole partition end to end. A single
reader sustains **~1.8-2.2 GB/s**; readers scale to roughly the SSD's read
ceiling, which is where the per-reader rate stops improving.

```
readers      entries/s      MB/s  full scans/s
1               460.0k    1796.9          23.0
8                1.74M    6796.9          87.0
64               2.13M    8333.3         106.7
```

`mixed_grid` measures reads against concurrent appends and cannot separate
the two; this is the read path alone.

## Seek (1 partition, 256 B payloads, 200k entries)

Time to the **first** entry of a `from:` read, which is where a scan would
show up. This is what the sparse index is for.

```
from             us/seek    entries/s      us/seek (index: false)
0                  367.4         2.7k                       366.1
50.0k              327.9         3.0k                     14175.5
100.0k             319.1         3.1k                     20245.7
180.0k             366.7         2.7k                     50406.3
198.0k             352.9         2.8k                     44872.1
```

**Flat with the index, linear without it.** A resume 180k entries into the
log costs the same as one at the head — 367 us — because the index binary-
searches to the batch and skips the few entries inside it. Turn the index
off and the same read scans the log: 50 ms, **137x slower**. That gap is the
index's whole reason for existing, and nothing measured it until now.

The flat cost is dominated by opening the WAL and the index and reading the
first chunk, not by the search.

## Replica (2 replica nodes, ack :all, 10 partitions)

Real `:peer` nodes over Erlang distribution on the same host, so this
measures protocol overhead (serialize + ship + remote group commit + ack),
not network RTT. Every batch is acked by all three copies before the
caller returns. Batches are pipelined over a long-lived channel per
replica (`max_inflight_commits`, default 32), and each replica
group-commits whatever has queued on its channel.

With the default `fsync: false` (durability = the ack quorum, RabbitMQ
streams' stance):

```
payload   callers         ops/s      MB/s
256B      1                8.0k       2.0
256B      8               24.3k       5.9
256B      64              70.5k      17.2
256B      256            197.0k      48.1
4KB       1                7.2k      28.0
4KB       8               20.2k      79.0
4KB       64              24.0k      93.9
4KB       256            118.5k     462.9
64KB      1                5.1k     321.6
64KB      8               14.6k     913.5
64KB      64              21.9k    1367.7
64KB      256              5.0k     310.0
```

(The 64 KB × 256 cell is disk-noise-sensitive: it ranged 5–17k ops/s
(0.3–1.1 GB/s) across runs of the same build as the SSD heated up; the
64-caller row's ~1.4 GB/s is representative of the sustained ceiling.)

With `FSYNC=true` (datasync on the primary and both replicas before ack —
one shared SSD serving 30 fsyncing writers, so this is the pessimal
configuration for it):

```
payload   callers         ops/s      MB/s
256B      256             11.5k       2.8
4KB       256             11.9k      46.4
64KB      256              7.6k     474.1
```

Mixed append + stream (4 KB payload, `fsync: false`; reads served from the
primary's WAL):

```
writers  readers       w ops/s    w MB/s   r entries/s    r MB/s
1        1                5.7k      22.4        406.9k    1589.4
8        8               13.2k      51.5         1.46M    5703.9
64       8               48.7k     190.2        961.6k    3756.1
256      8              119.7k     467.7        757.6k    2959.5
```

Caller latency (4 KB payload, `fsync: false`):

```
parallel      average     median     99th %
1           116.44 μs  104.71 μs  231.79 μs
16          423.67 μs  376.42 μs    1.12 ms
128           1.13 ms    1.01 ms    2.97 ms
```

## S3 (in-memory fake, 30 ms simulated PUT latency, 4 partitions)

The fake isolates the pipeline from network variance: a lone caller gets
exactly `1 / PUT latency` ≈ 32 ops/s, and group commit multiplies that by
~130× at 256 callers — each PUT carries a whole batch. Against real S3,
expect the same shape with real PUT latency (~10-100 ms) substituted.

```
payload   callers         ops/s      MB/s
1KB       1                  31       0.0
1KB       32                513       0.5
1KB       256              4.2k       4.1
16KB      1                  32       0.5
16KB      32                519       8.1
16KB      256              4.2k      64.9
```

Mixed append + stream (1 KB payload) — read MB/s is inflated here because
"S3" is an in-memory Agent; treat the write columns as the signal:

```
writers  readers       w ops/s    w MB/s   r entries/s    r MB/s
1        1                  32       0.0         39.0k      38.1
32       4                 524       0.5        336.0k     328.2
256      8                4.4k       4.3         2.97M    2898.9
```

Caller latency (1 KB payload):

```
parallel      average     median     99th %
1            31.70 ms   31.08 ms   36.80 ms
64           63.50 ms   62.58 ms   71.45 ms
```

At 64 parallel callers the median is ~2× PUT latency: a caller lands mid-PUT,
waits for it to finish, then rides the next group commit.

## Replication transport

`mix run bench/transport_bench.exs`

Append throughput and caller latency for each transport, with and without an
unrelated load on the distribution channel between the same node pair. The
hog is four processes shipping 4 MiB `:erpc` payloads to the replica node —
traffic replication has nothing to do with, which is the point. `HOGS`
(default `0,4`) sets the counts. `BENCH_DURATION_MS`, `BENCH_WARMUP_MS` and
`PARTITIONS` apply as elsewhere.

Both nodes run on this host, so this measures contention for one socket and
the scheduler, not a saturated network. Three runs, 64 KiB payloads, 8
partitions:

| transport | hogs | ops/s | p50 us | p99 us |
|---|---|---|---|---|
| distribution | 0 | 26402 / 29750 / 20578 | 388 / 376 / 377 | 1926 / 1939 / 1886 |
| distribution | 4 | 10360 / 10457 / 10238 | 3356 / 3482 / 2712 | 8743 / 13206 / 10738 |
| gen_rpc | 0 | 29062 / 32108 / 29387 | 403 / 379 / 414 | 2017 / 1933 / 1917 |
| gen_rpc | 4 | 26140 / 14324 / 25035 | 511 / 500 / 516 | 2102 / 2123 / 3206 |

**Idle, the two are the same.** Same p50 to within 10%, same p99, and
overlapping throughput. Nothing here argues for gen_rpc on a quiet pair.

**Under contention, all three columns separate, and the direction is the
same in every run.**

* **Throughput.** Distribution drops to ~10.3k ops/s, from ~26k idle — a
  2.5x fall, and the most repeatable number in the table (10360 / 10457 /
  10238). gen_rpc holds 14k-26k.
* **p50.** Distribution rises 7-9x, to ~2700-3500 us. gen_rpc rises about
  1.3x, to ~510 us.
* **p99.** Distribution rises 4.5-7x, to ~8700-13200 us. gen_rpc stays
  within 1.1-1.7x of idle.

So a contended pair costs distribution most of its throughput and roughly an
order of magnitude of median latency. gen_rpc gives up little of either.

**An earlier version of this bench reported no throughput difference.** That
was a harness bug, not a finding. `hog_loop/2` put its recursive call inside
a `try` — a function-level `catch` wraps the whole body — so it was not a
tail call and each hog's stack grew without bound (measured: 71 MB and
climbing, against 2.7 KB for the fixed loop). The load the hogs applied
therefore drifted across the measurement window instead of staying constant,
which buried the effect. The hogs also were not awaited on kill and the
replica writers were never stopped, so residue accumulated in loop order.
All three are fixed.
