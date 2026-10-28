defmodule WriterFramingBench.NoopBackend do
  @behaviour DurableBuffer.Backend

  @impl true
  def init_config(_opts), do: %{}

  @impl true
  def open(config, _partition_index), do: {:ok, config}

  @impl true
  def commit(state, _batch, _byte_size, _span), do: {:ok, state}

  @impl true
  def stream(_config, _partition_index), do: []

  @impl true
  def truncate(state, _next_offset), do: {:ok, state}

  @impl true
  def close(_state), do: :ok
end

callers = String.to_integer(System.get_env("CALLERS", "64"))
appends_per_caller = String.to_integer(System.get_env("APPENDS", "2000"))
payload_kb = String.to_integer(System.get_env("PAYLOAD_KB", "256"))

payload = for _ <- 1..payload_kb, do: :crypto.strong_rand_bytes(1024)

{:ok, writer} =
  DurableBuffer.Partition.start_link(
    name: :writer_framing_bench,
    backend: {WriterFramingBench.NoopBackend, %{}},
    partition_index: 0,
    max_batch_bytes: 8 * 1024 * 1024
  )

max_queue = :counters.new(1, [])

sampler =
  spawn_link(fn ->
    Stream.repeatedly(fn ->
      {:message_queue_len, len} = Process.info(writer, :message_queue_len)
      if len > :counters.get(max_queue, 1), do: :counters.put(max_queue, 1, len)
      Process.sleep(1)
    end)
    |> Stream.run()
  end)

{micros, _} =
  :timer.tc(fn ->
    1..callers
    |> Task.async_stream(
      fn _ ->
        for _ <- 1..appends_per_caller,
            do: {:ok, _} = DurableBuffer.Partition.append(writer, payload)
      end,
      max_concurrency: callers,
      timeout: :infinity
    )
    |> Stream.run()
  end)

Process.unlink(sampler)
Process.exit(sampler, :kill)

total = callers * appends_per_caller
seconds = micros / 1_000_000

IO.puts(
  "appends=#{total} payload=#{payload_kb}KB elapsed=#{Float.round(seconds, 2)}s " <>
    "throughput=#{round(total / seconds)} appends/s " <>
    "(#{Float.round(total * payload_kb / 1024 / seconds, 1)} MB/s) " <>
    "max_writer_queue=#{:counters.get(max_queue, 1)}"
)
