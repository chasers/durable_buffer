import Bitwise

Code.require_file("support/bench_helper.exs", __DIR__)

alias DurableBuffer.Backend.Tiered
alias DurableBuffer.Bench

ack = System.get_env("ACK", "local") |> String.to_existing_atom()
partitions = String.to_integer(System.get_env("PARTITIONS", "4"))
bucket = System.get_env("S3_BENCH_BUCKET")
puts = :counters.new(1, [:write_concurrency])

compression =
  case System.get_env("COMPRESSION") do
    nil -> DurableBuffer.Compression.default()
    value -> String.to_existing_atom(value)
  end

log_payload = fn size ->
  line = fn ->
    ~s({"ts":"2026-09-16T14:#{:rand.uniform(59)}:#{:rand.uniform(59)}.#{:rand.uniform(999_999)}Z",) <>
      ~s("level":"info","event_message":"request completed","request_id":"#{:rand.uniform(1 <<< 60)}",) <>
      ~s("status":#{Enum.random([200, 201, 204, 404, 500])},"duration_ms":#{:rand.uniform(900)}}\n)
  end

  Stream.repeatedly(line)
  |> Enum.reduce_while("", fn chunk, acc ->
    if byte_size(acc) >= size, do: {:halt, acc}, else: {:cont, acc <> chunk}
  end)
  |> binary_part(0, size)
end

req_options =
  if bucket do
    IO.puts("tiered backend: real bucket #{bucket} (credentials/endpoint from AWS_* env vars)")
    []
  else
    Code.require_file("../test/support/fake_s3.ex", __DIR__)

    simulated_latency_ms = String.to_integer(System.get_env("S3_SIM_LATENCY_MS", "30"))
    {:ok, fake_store} = DurableBuffer.Test.FakeS3.start_store()
    Process.put(:fake_store, fake_store)
    Req.Test.set_req_test_to_shared()

    Req.Test.stub(:tiered_bench_stub, fn conn ->
      if conn.method == "PUT" do
        :counters.add(puts, 1, 1)
        Process.sleep(simulated_latency_ms)
      end

      DurableBuffer.Test.FakeS3.call(conn, fake_store)
    end)

    IO.puts(
      "tiered backend: in-memory fake with #{simulated_latency_ms}ms simulated PUT latency " <>
        "(set S3_BENCH_BUCKET to hit real S3)"
    )

    [plug: {Req.Test, :tiered_bench_stub}, retry: false]
  end

dir = Path.join(System.tmp_dir!(), "durable_buffer_bench_tiered_#{System.os_time(:second)}")

segment_opts =
  [
    segment_ms: System.get_env("SEGMENT_MS"),
    segment_bytes: System.get_env("SEGMENT_BYTES")
  ]
  |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  |> Enum.map(fn {key, value} -> {key, String.to_integer(value)} end)

{:ok, _pid} =
  DurableBuffer.start_link(
    name: :bench_tiered,
    partitions: partitions,
    backend:
      {Tiered,
       [
         dir: dir,
         ack: ack,
         compression: compression,
         bucket: bucket || "bench-bucket",
         prefix: "durable_buffer_bench/#{System.os_time(:second)}",
         req_options: req_options
       ] ++ segment_opts}
  )

%{backend: {Tiered, config}} = DurableBuffer.config(:bench_tiered)

IO.puts(
  "ack=#{ack} compression=#{config.s3.compression} partitions=#{partitions} fsync=#{config.fsync} segment_ms=#{config.segment_ms} " <>
    "segment_bytes=#{config.segment_bytes}"
)

duration_ms = String.to_integer(System.get_env("BENCH_DURATION_MS", "5000"))

append_loop = fn append_loop, caller, payload, deadline, count ->
  if System.monotonic_time(:millisecond) < deadline do
    {:ok, _offset} = DurableBuffer.append(:bench_tiered, caller, payload)
    append_loop.(append_loop, caller, payload, deadline, count + 1)
  else
    count
  end
end

IO.puts("\n== Throughput: bench_tiered ==")

IO.puts(
  String.pad_trailing("payload", 10) <>
    String.pad_trailing("callers", 9) <>
    String.pad_leading("ops/s", 12) <>
    String.pad_leading("MB/s", 10) <>
    String.pad_leading("PUTs/s", 10) <>
    String.pad_leading("entries/PUT", 13)
)

for payload_size <- [1024, 16 * 1024], concurrency <- [1, 32, 256] do
  payload = log_payload.(payload_size)
  :counters.put(puts, 1, 0)
  deadline = System.monotonic_time(:millisecond) + duration_ms

  ops =
    1..concurrency
    |> Enum.map(fn caller ->
      Task.async(fn -> append_loop.(append_loop, caller, payload, deadline, 0) end)
    end)
    |> Task.await_many(duration_ms + 60_000)
    |> Enum.sum()

  seconds = duration_ms / 1000
  put_count = :counters.get(puts, 1)
  entries_per_put = if put_count > 0, do: Bench.format_number(round(ops / put_count)), else: "-"

  IO.puts(
    String.pad_trailing(Bench.format_bytes(payload_size), 10) <>
      String.pad_trailing(Integer.to_string(concurrency), 9) <>
      String.pad_leading(Bench.format_number(round(ops / seconds)), 12) <>
      String.pad_leading(
        :erlang.float_to_binary(ops * payload_size / seconds / 1_048_576, decimals: 1),
        10
      ) <>
      String.pad_leading(
        if(bucket, do: "-", else: Bench.format_number(round(put_count / seconds))),
        10
      ) <>
      String.pad_leading(if(bucket, do: "-", else: entries_per_put), 13)
  )

  DurableBuffer.truncate_all(:bench_tiered)
end

IO.puts("\n== Upload lag: append return to S3 watermark, 1KB, 1 caller ==")

payload = log_payload.(1024)
index = DurableBuffer.partition_index(:bench_tiered, :lag)

await_watermark = fn await_watermark, target, started ->
  if Tiered.uploaded(config, index) >= target do
    System.monotonic_time(:microsecond) - started
  else
    Process.sleep(1)
    await_watermark.(await_watermark, target, started)
  end
end

lags =
  for _sample <- 1..5 do
    {:ok, offset} = DurableBuffer.append(:bench_tiered, :lag, payload)
    await_watermark.(await_watermark, offset + 1, System.monotonic_time(:microsecond))
  end
  |> Enum.map(&(&1 / 1000))
  |> Enum.sort()

IO.puts(
  "median #{:erlang.float_to_binary(Enum.at(lags, 2), decimals: 1)} ms, " <>
    "max #{:erlang.float_to_binary(List.last(lags), decimals: 1)} ms " <>
    "(0 means the append already waited for the upload)"
)

DurableBuffer.truncate_all(:bench_tiered)

if fake_store = Process.get(:fake_store) do
  IO.puts("\n== Compression: 2000 x 1KB log-like entries in one partition ==")

  entries = for _entry <- 1..2000, do: log_payload.(1024)
  {:ok, first.._last//_step} = DurableBuffer.append_batch(:bench_tiered, :ratio, entries)
  ratio_index = DurableBuffer.partition_index(:bench_tiered, :ratio)
  started = System.monotonic_time(:microsecond)

  await_watermark = fn await_watermark, target ->
    if Tiered.uploaded(config, ratio_index) >= target do
      :ok
    else
      Process.sleep(5)
      await_watermark.(await_watermark, target)
    end
  end

  await_watermark.(await_watermark, first + 2000)
  _waited = System.monotonic_time(:microsecond) - started

  stored =
    fake_store
    |> DurableBuffer.Test.FakeS3.objects()
    |> Enum.filter(fn {key, _body} -> String.contains?(key, "/p#{ratio_index}/") end)
    |> Enum.map(fn {_key, body} -> byte_size(body) end)
    |> Enum.sum()

  raw = 2000 * (1024 + 8)

  IO.puts(
    "raw #{Bench.format_bytes(raw)}, stored #{Bench.format_bytes(stored)}, " <>
      "ratio #{:erlang.float_to_binary(raw / max(stored, 1), decimals: 1)}x"
  )

  DurableBuffer.truncate_all(:bench_tiered)
end

Bench.latency(:bench_tiered, payload_size: 1024, parallel_levels: [1, 64])

DurableBuffer.truncate_all(:bench_tiered)
File.rm_rf!(dir)
