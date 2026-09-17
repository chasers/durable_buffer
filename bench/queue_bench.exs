Code.require_file("support/bench_helper.exs", __DIR__)
Code.require_file("../test/support/fake_s3.ex", __DIR__)

import Bitwise

alias DurableBuffer.Bench
alias DurableBuffer.Queue.Appender
alias DurableBuffer.Queue.Consumer
alias DurableBuffer.Queue.Store

latency_ms = String.to_integer(System.get_env("S3_SIM_LATENCY_MS", "30"))
partitions = String.to_integer(System.get_env("PARTITIONS", "4"))
duration_ms = String.to_integer(System.get_env("BENCH_DURATION_MS", "5000"))
nodes = String.to_integer(System.get_env("NODES", "1"))
counters = :counters.new(3, [:write_concurrency])

{:ok, fake_store} = DurableBuffer.Test.FakeS3.start_store()
Req.Test.set_req_test_to_shared()

Req.Test.stub(:queue_bench_stub, fn conn ->
  cond do
    conn.method == "PUT" and String.ends_with?(conn.request_path, ".batch") ->
      :counters.add(counters, 1, 1)

    conn.method == "PUT" ->
      :counters.add(counters, 2, 1)

    conn.method == "GET" ->
      :counters.add(counters, 3, 1)

    true ->
      :ok
  end

  Process.sleep(latency_ms)
  DurableBuffer.Test.FakeS3.call(conn, fake_store)
end)

queue_opts = [
  manifest_gap_ms: String.to_integer(System.get_env("MANIFEST_GAP_MS", "50")),
  bucket: "bench-bucket",
  prefix: "queue_bench/#{System.os_time(:second)}",
  req_options: [plug: {Req.Test, :queue_bench_stub}, retry: false]
]

config = Store.config(queue_opts)

{:ok, _pid} =
  DurableBuffer.start_link(
    name: :bench_queue,
    partitions: partitions,
    backend: {DurableBuffer.Backend.Queue, queue_opts}
  )

IO.puts(
  "queue backend: in-memory fake, #{latency_ms}ms per request (GET and PUT), " <>
    "partitions=#{partitions} compression=#{config.compression} nodes=#{nodes} " <>
    "manifest_gap_ms=#{config.manifest_gap_ms} " <>
    "(other nodes run the same caller count: a batch PUT, then a manifest append)"
)

log_payload = fn size ->
  line = fn ->
    ~s({"ts":"2026-09-17T14:#{:rand.uniform(59)}:#{:rand.uniform(59)}Z","level":"info",) <>
      ~s("event_message":"request completed","request_id":"#{:rand.uniform(1 <<< 60)}",) <>
      ~s("status":#{Enum.random([200, 404, 500])},"duration_ms":#{:rand.uniform(900)}}\n)
  end

  Stream.repeatedly(line)
  |> Enum.reduce_while("", fn chunk, acc ->
    if byte_size(acc) >= size, do: {:halt, acc}, else: {:cont, acc <> chunk}
  end)
  |> binary_part(0, size)
end

other_nodes =
  for _node <- 2..nodes//1 do
    {:ok, appender} = GenServer.start_link(Appender, config)
    appender
  end

append_loop = fn append_loop, caller, payload, deadline, count ->
  if System.monotonic_time(:millisecond) < deadline do
    {:ok, _offset} = DurableBuffer.append(:bench_queue, caller, payload)
    append_loop.(append_loop, caller, payload, deadline, count + 1)
  else
    count
  end
end

other_location = "#{config.prefix}/other-node.batch"
other_batch = DurableBuffer.Queue.Batch.encode(["other node"], :none)
:ok = Store.put_batch(Store.req(config), config, other_location, other_batch)

node_loop = fn node_loop, appender, deadline ->
  if System.monotonic_time(:millisecond) < deadline do
    Process.sleep(latency_ms)
    {:ok, _sequences} = Appender.append(appender, [{other_location, []}])
    node_loop.(node_loop, appender, deadline)
  end
end

IO.puts("\n== Produce: 1KB log-like payloads ==")

IO.puts(
  String.pad_trailing("callers", 9) <>
    String.pad_leading("ops/s", 10) <>
    String.pad_leading("batch PUTs/s", 14) <>
    String.pad_leading("manifest writes/s", 19) <>
    String.pad_leading("conflicts/s", 13) <>
    String.pad_leading("entries/batch", 15)
)

payload = log_payload.(1024)
local_appender = Appender.ensure_started(config)

for callers <- [1, 32, 256] do
  :counters.put(counters, 1, 0)
  :counters.put(counters, 2, 0)
  before = Appender.stats(local_appender)
  others_before = Enum.map(other_nodes, &Appender.stats/1)
  deadline = System.monotonic_time(:millisecond) + duration_ms

  node_tasks =
    for appender <- other_nodes, _caller <- 1..callers do
      Task.async(fn -> node_loop.(node_loop, appender, deadline) end)
    end

  ops =
    1..callers
    |> Enum.map(fn caller ->
      Task.async(fn -> append_loop.(append_loop, caller, payload, deadline, 0) end)
    end)
    |> Task.await_many(duration_ms + 60_000)
    |> Enum.sum()

  Task.await_many(node_tasks, 60_000)

  seconds = duration_ms / 1000
  now = Appender.stats(local_appender)

  other_conflicts =
    other_nodes
    |> Enum.map(&Appender.stats/1)
    |> Enum.zip(others_before)
    |> Enum.map(fn {after_stats, before_stats} ->
      after_stats.conflicts - before_stats.conflicts
    end)
    |> Enum.sum()

  batch_puts = :counters.get(counters, 1)

  IO.puts(
    String.pad_trailing(Integer.to_string(callers), 9) <>
      String.pad_leading(Bench.format_number(round(ops / seconds)), 10) <>
      String.pad_leading(Bench.format_number(round(batch_puts / seconds)), 14) <>
      String.pad_leading(Bench.format_number(round(:counters.get(counters, 2) / seconds)), 19) <>
      String.pad_leading(
        Bench.format_number(
          round((now.conflicts - before.conflicts + other_conflicts) / seconds)
        ),
        13
      ) <>
      String.pad_leading(Bench.format_number(round(ops / max(batch_puts, 1))), 15)
  )
end

IO.puts("\n== Consume: drain the manifest with read-ahead ==")

{:ok, consumer} = Consumer.open(config)
{:ok, manifest, _version} = Store.read_manifest(Store.req(config), config)

IO.puts(
  "queued entries: #{manifest.entry_count}, manifest size: #{Bench.format_bytes(byte_size(manifest.body) + 22)}"
)

drain = fn drain, consumer, batches, entries ->
  case Consumer.next_descriptors(consumer, 64) do
    {:ok, [], consumer} ->
      {consumer, batches, entries}

    {:ok, descriptors, consumer} ->
      fetched =
        descriptors
        |> Enum.map(fn descriptor -> Task.async(fn -> Consumer.fetch(config, descriptor) end) end)
        |> Task.await_many(60_000)

      count = Enum.reduce(fetched, 0, fn {:ok, batch}, total -> total + length(batch.entries) end)
      {:ok, consumer} = Consumer.ack_through(consumer, List.last(descriptors).sequence)
      drain.(drain, consumer, batches + length(descriptors), entries + count)
  end
end

started = System.monotonic_time(:millisecond)
{_consumer, batches, entries} = drain.(drain, consumer, 0, 0)
elapsed = max(System.monotonic_time(:millisecond) - started, 1) / 1000

IO.puts(
  "#{batches} batches, #{entries} entries in #{:erlang.float_to_binary(elapsed, decimals: 1)} s: " <>
    "#{Bench.format_number(round(entries / elapsed))} entries/s"
)

stored =
  fake_store
  |> DurableBuffer.Test.FakeS3.objects()
  |> Enum.filter(fn {key, _body} -> String.ends_with?(key, ".batch") end)
  |> Enum.map(fn {_key, body} -> byte_size(body) end)
  |> Enum.sum()

IO.puts(
  "batch bytes stored: #{Bench.format_bytes(stored)} for #{Bench.format_bytes(entries * 1024)} of payload"
)

Bench.latency(:bench_queue, payload_size: 1024, parallel_levels: [1, 64])
