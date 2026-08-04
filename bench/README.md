# Benchmark results

Captured 2026-08-04 on a MacBook Pro (Apple M1 Max, 10 cores, 64 GB RAM,
internal SSD), Erlang/OTP 29, Elixir 1.20.2.

Every append is a blocking, durable write — the numbers below are fsync-,
replication-, or PUT-durable, not buffered. Three measurements per backend:

- **Throughput** — timed loops of concurrent callers issuing
  `DurableBuffer.append/3`; aggregate ops/s and MB/s.
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
`PARTITIONS` overrides partition count.

## Local (10 partitions)

Group commit in action: one caller is fsync-bound (~3.4k commits/s on this
SSD); adding callers grows batches instead of adding fsyncs, scaling to
**1.9 GB/s** of durable writes at 64 KB payloads.

```
payload   callers         ops/s      MB/s
256B      1                3.4k       0.8
256B      8                5.5k       1.3
256B      64              19.8k       4.8
256B      256             69.6k      17.0
4KB       1                3.5k      13.5
4KB       8                5.6k      21.7
4KB       64              19.1k      74.5
4KB       256             64.9k     253.5
64KB      1                3.2k     201.2
64KB      8                5.8k     362.7
64KB      64              17.3k    1081.4
64KB      256             30.2k    1888.0
```

Mixed append + stream (4 KB payload) — streaming costs writers little;
reads come largely from page cache:

```
writers  readers       w ops/s    w MB/s   r entries/s    r MB/s
1        1                3.3k      12.8        812.9k    3175.2
8        8                4.7k      18.3         1.58M    6186.5
64       8               13.8k      54.0        428.6k    1674.1
256      8               52.0k     203.1        462.2k    1805.6
```

Caller latency (4 KB payload):

```
parallel      average     median     99th %
1           294.41 μs  233.73 μs    1.87 ms
16            1.97 ms    1.91 ms    4.59 ms
128           3.39 ms    3.14 ms    8.28 ms
```

## Replica (2 replica nodes, ack :all, 10 partitions)

Real `:peer` nodes over Erlang distribution on the same host, so this
measures protocol overhead (serialize + ship + remote fsync + ack), not
network RTT. Every batch is durable on all three copies before the caller
returns.

```
payload   callers         ops/s      MB/s
256B      1                1.5k       0.4
256B      8                2.3k       0.6
256B      64               6.0k       1.5
256B      256             22.1k       5.4
4KB       1                1.3k       4.9
4KB       8                2.3k       9.0
4KB       64               5.9k      23.2
4KB       256             20.5k      80.1
64KB      1                 875      54.7
64KB      8                1.8k     115.4
64KB      64               5.1k     316.8
64KB      256              8.2k     513.8
```

Mixed append + stream (4 KB payload; reads served from the primary's WAL):

```
writers  readers       w ops/s    w MB/s   r entries/s    r MB/s
1        1                1.0k       4.0        426.3k    1665.2
8        8                1.8k       7.2        802.0k    3132.9
64       8                5.4k      20.9        373.1k    1457.5
256      8               23.0k      89.8        436.9k    1706.5
```

Caller latency (4 KB payload):

```
parallel      average     median     99th %
1           718.73 μs  653.05 μs    2.35 ms
16            6.69 ms    6.45 ms   14.07 ms
128          10.74 ms   10.30 ms   22.19 ms
```

## S3 (in-memory fake, 30 ms simulated PUT latency, 4 partitions)

The fake isolates the pipeline from network variance: a lone caller gets
exactly `1 / PUT latency` ≈ 32 ops/s, and group commit multiplies that by
~128× at 256 callers — each PUT carries a whole batch. Against real S3,
expect the same shape with real PUT latency (~10–100 ms) substituted.

```
payload   callers         ops/s      MB/s
1KB       1                  32       0.0
1KB       32                520       0.5
1KB       256              4.1k       4.1
16KB      1                  32       0.5
16KB      32                518       8.1
16KB      256              4.1k      64.8
```

Mixed append + stream (1 KB payload) — read MB/s is inflated here because
"S3" is an in-memory Agent; treat the write columns as the signal:

```
writers  readers       w ops/s    w MB/s   r entries/s    r MB/s
1        1                  32       0.0         39.2k      38.3
32       4                 520       0.5        324.5k     316.9
256      8                4.1k       4.0         2.91M    2842.4
```

Caller latency (1 KB payload):

```
parallel      average     median     99th %
1            31.00 ms   30.99 ms   31.63 ms
64           62.10 ms   62.01 ms   65.49 ms
```

At 64 parallel callers the median is ~2× PUT latency: a caller lands mid-PUT,
waits for it to finish, then rides the next group commit.
