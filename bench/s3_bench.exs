Code.require_file("support/bench_helper.exs", __DIR__)

bucket = System.get_env("S3_BENCH_BUCKET")

req_options =
  if bucket do
    IO.puts("s3 backend: real bucket #{bucket} (credentials/endpoint from AWS_* env vars)")
    []
  else
    Code.require_file("../test/support/fake_s3.ex", __DIR__)

    simulated_latency_ms = String.to_integer(System.get_env("S3_SIM_LATENCY_MS", "30"))
    {:ok, store} = DurableBuffer.Test.FakeS3.start_store()
    Req.Test.set_req_test_to_shared()

    Req.Test.stub(:s3_bench_stub, fn conn ->
      if conn.method == "PUT" do
        Process.sleep(simulated_latency_ms)
      end

      DurableBuffer.Test.FakeS3.call(conn, store)
    end)

    IO.puts(
      "s3 backend: in-memory fake with #{simulated_latency_ms}ms simulated PUT latency " <>
        "(set S3_BENCH_BUCKET to hit real S3)"
    )

    [plug: {Req.Test, :s3_bench_stub}, retry: false]
  end

partitions = String.to_integer(System.get_env("PARTITIONS", "4"))

{:ok, _pid} =
  DurableBuffer.start_link(
    name: :bench_s3,
    partitions: partitions,
    backend:
      {DurableBuffer.Backend.S3,
       bucket: bucket || "bench-bucket",
       prefix: "durable_buffer_bench/#{System.os_time(:second)}",
       req_options: req_options}
  )

IO.puts("partitions=#{partitions}")

DurableBuffer.Bench.throughput_grid(:bench_s3,
  payload_sizes: [1024, 16 * 1024],
  concurrencies: [1, 32, 256]
)

DurableBuffer.Bench.mixed_grid(:bench_s3,
  payload_size: 1024,
  combos: [{1, 1}, {32, 4}, {256, 8}]
)

DurableBuffer.Bench.latency(:bench_s3, payload_size: 1024, parallel_levels: [1, 64])

DurableBuffer.truncate_all(:bench_s3)
