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
- **Caller latency** — Benchee distributions for a single append under
  increasing `parallel:` load.

Reproduce with:

```sh
mix run bench/local_bench.exs
REPLICAS=2 ACK=all mix run bench/replica_bench.exs
mix run bench/s3_bench.exs                              # fake S3, 30ms PUT
S3_BENCH_BUCKET=my-bucket mix run bench/s3_bench.exs    # real S3
```

`BENCH_DURATION_MS`, `BENCH_WARMUP_MS`, and `BENCH_TIME` shorten runs;
`PARTITIONS` overrides partition count. Numbers at the disk-bandwidth
ceiling vary ±20-30% between runs.

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

## Replica (2 replica nodes, ack :all, 10 partitions)

Real `:peer` nodes over Erlang distribution on the same host, so this
measures protocol overhead (serialize + ship + remote fsync + ack), not
network RTT. Every batch is durable on all three copies before the caller
returns. Commit pipelining overlaps the replication round trip with
building the next batch — 64 KB × 256 went from ~514 to ~832 MB/s.

```
payload   callers         ops/s      MB/s
256B      1                1.2k       0.3
256B      8                2.1k       0.5
256B      64               6.2k       1.5
256B      256             24.6k       6.0
4KB       1                1.1k       4.4
4KB       8                2.0k       7.9
4KB       64               6.1k      24.0
4KB       256             23.7k      92.7
64KB      1                1.1k      71.2
64KB      8                1.9k     116.4
64KB      64               5.1k     316.6
64KB      256             13.3k     832.1
```

Mixed append + stream (4 KB payload; reads served from the primary's WAL):

```
writers  readers       w ops/s    w MB/s   r entries/s    r MB/s
1        1                1.2k       4.6        551.9k    2155.9
8        8                1.9k       7.4        914.6k    3572.8
64       8                4.5k      17.4        336.5k    1314.4
256      8               21.6k      84.2        424.3k    1657.4
```

Caller latency (4 KB payload):

```
parallel      average     median     99th %
1           666.04 μs  602.10 μs    2.21 ms
16            5.51 ms    5.47 ms   12.03 ms
128          11.85 ms   10.72 ms   34.61 ms
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
