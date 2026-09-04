defmodule DurableBuffer.Bench do
  @moduledoc """
  Shared benchmark harness.

  `throughput_grid/2` measures true aggregate throughput: for each payload
  size × caller concurrency combination it runs a timed loop of concurrent
  callers issuing blocking `DurableBuffer.append/3` calls and reports total
  ops/s and MB/s. `latency/2` runs Benchee on single appends at several
  `parallel:` levels, reporting the caller-observed latency distribution
  (median / p99) under load.
  """

  @default_payload_sizes [256, 4 * 1024, 64 * 1024]
  @default_concurrencies [1, 8, 64, 256]

  def throughput_grid(name, opts \\ []) do
    payload_sizes = Keyword.get(opts, :payload_sizes, @default_payload_sizes)
    concurrencies = Keyword.get(opts, :concurrencies, @default_concurrencies)

    duration_ms =
      Keyword.get(
        opts,
        :duration_ms,
        String.to_integer(System.get_env("BENCH_DURATION_MS", "5000"))
      )

    warmup_ms =
      Keyword.get(opts, :warmup_ms, String.to_integer(System.get_env("BENCH_WARMUP_MS", "1000")))

    IO.puts("\n== Throughput: #{name} ==")

    header =
      String.pad_trailing("payload", 10) <>
        String.pad_trailing("callers", 9) <>
        String.pad_leading("ops/s", 12) <>
        String.pad_leading("MB/s", 10)

    IO.puts(header)

    for payload_size <- payload_sizes, concurrency <- concurrencies do
      result = measure(name, payload_size, concurrency, warmup_ms, duration_ms)

      IO.puts(
        String.pad_trailing(format_bytes(payload_size), 10) <>
          String.pad_trailing(Integer.to_string(concurrency), 9) <>
          String.pad_leading(format_number(result.ops_per_sec), 12) <>
          String.pad_leading(:erlang.float_to_binary(result.mb_per_sec, decimals: 1), 10)
      )

      DurableBuffer.truncate_all(name)
    end

    :ok
  end

  def measure(name, payload_size, concurrency, warmup_ms, duration_ms) do
    payload = :binary.copy("x", payload_size)

    run = fn ms ->
      deadline = System.monotonic_time(:millisecond) + ms

      1..concurrency
      |> Enum.map(fn caller ->
        Task.async(fn -> append_loop(name, caller, payload, deadline, 0) end)
      end)
      |> Task.await_many(ms + 60_000)
      |> Enum.sum()
    end

    run.(warmup_ms)
    DurableBuffer.truncate_all(name)

    ops = run.(duration_ms)
    seconds = duration_ms / 1000

    %{
      ops_per_sec: round(ops / seconds),
      mb_per_sec: ops * payload_size / seconds / 1_048_576
    }
  end

  defp append_loop(name, caller, payload, deadline, count) do
    if System.monotonic_time(:millisecond) < deadline do
      {:ok, _offset} = DurableBuffer.append(name, caller, payload)
      append_loop(name, caller, payload, deadline, count + 1)
    else
      count
    end
  end

  @default_batch_sizes [10, 100, 1000]
  @default_batch_concurrencies [1, 8, 64]

  def batch_grid(name, opts \\ []) do
    payload_size = Keyword.get(opts, :payload_size, 256)
    batch_sizes = Keyword.get(opts, :batch_sizes, @default_batch_sizes)
    concurrencies = Keyword.get(opts, :concurrencies, @default_batch_concurrencies)

    duration_ms =
      Keyword.get(
        opts,
        :duration_ms,
        String.to_integer(System.get_env("BENCH_DURATION_MS", "5000"))
      )

    warmup_ms =
      Keyword.get(opts, :warmup_ms, String.to_integer(System.get_env("BENCH_WARMUP_MS", "1000")))

    IO.puts("\n== append_batch throughput: #{name}, #{format_bytes(payload_size)} payload ==")

    header =
      String.pad_trailing("batch", 8) <>
        String.pad_trailing("callers", 9) <>
        String.pad_leading("entries/s", 13) <>
        String.pad_leading("MB/s", 10)

    IO.puts(header)

    for batch_size <- batch_sizes, concurrency <- concurrencies do
      result =
        measure_batch(name, payload_size, batch_size, concurrency, warmup_ms, duration_ms)

      IO.puts(
        String.pad_trailing(Integer.to_string(batch_size), 8) <>
          String.pad_trailing(Integer.to_string(concurrency), 9) <>
          String.pad_leading(format_number(result.entries_per_sec), 13) <>
          String.pad_leading(:erlang.float_to_binary(result.mb_per_sec, decimals: 1), 10)
      )

      DurableBuffer.truncate_all(name)
    end

    :ok
  end

  defp measure_batch(name, payload_size, batch_size, concurrency, warmup_ms, duration_ms) do
    payloads = List.duplicate(:binary.copy("x", payload_size), batch_size)

    run = fn ms ->
      deadline = System.monotonic_time(:millisecond) + ms

      1..concurrency
      |> Enum.map(fn caller ->
        Task.async(fn -> append_batch_loop(name, caller, payloads, batch_size, deadline, 0) end)
      end)
      |> Task.await_many(ms + 60_000)
      |> Enum.sum()
    end

    run.(warmup_ms)
    DurableBuffer.truncate_all(name)

    entries = run.(duration_ms)
    seconds = duration_ms / 1000

    %{
      entries_per_sec: round(entries / seconds),
      mb_per_sec: entries * payload_size / seconds / 1_048_576
    }
  end

  defp append_batch_loop(name, caller, payloads, batch_size, deadline, count) do
    if System.monotonic_time(:millisecond) < deadline do
      {:ok, _range} = DurableBuffer.append_batch(name, caller, payloads)
      append_batch_loop(name, caller, payloads, batch_size, deadline, count + batch_size)
    else
      count
    end
  end

  @default_mixed_combos [{1, 1}, {8, 8}, {64, 8}, {256, 8}]

  def mixed_grid(name, opts \\ []) do
    payload_size = Keyword.get(opts, :payload_size, 4 * 1024)
    combos = Keyword.get(opts, :combos, @default_mixed_combos)

    duration_ms =
      Keyword.get(
        opts,
        :duration_ms,
        String.to_integer(System.get_env("BENCH_DURATION_MS", "5000"))
      )

    warmup_ms =
      Keyword.get(opts, :warmup_ms, String.to_integer(System.get_env("BENCH_WARMUP_MS", "1000")))

    IO.puts("\n== Mixed append + stream: #{name}, #{format_bytes(payload_size)} payload ==")
    IO.puts("(readers repeatedly re-stream whole partitions while writers append)")

    header =
      String.pad_trailing("writers", 9) <>
        String.pad_trailing("readers", 9) <>
        String.pad_leading("w ops/s", 12) <>
        String.pad_leading("w MB/s", 10) <>
        String.pad_leading("r entries/s", 14) <>
        String.pad_leading("r MB/s", 10)

    IO.puts(header)

    for {writers, readers} <- combos do
      result = measure_mixed(name, payload_size, writers, readers, warmup_ms, duration_ms)

      IO.puts(
        String.pad_trailing(Integer.to_string(writers), 9) <>
          String.pad_trailing(Integer.to_string(readers), 9) <>
          String.pad_leading(format_number(result.write_ops_per_sec), 12) <>
          String.pad_leading(:erlang.float_to_binary(result.write_mb_per_sec, decimals: 1), 10) <>
          String.pad_leading(format_number(result.read_entries_per_sec), 14) <>
          String.pad_leading(:erlang.float_to_binary(result.read_mb_per_sec, decimals: 1), 10)
      )

      DurableBuffer.truncate_all(name)
    end

    :ok
  end

  defp measure_mixed(name, payload_size, writers, readers, warmup_ms, duration_ms) do
    payload = :binary.copy("x", payload_size)

    run = fn ms ->
      deadline = System.monotonic_time(:millisecond) + ms

      writer_tasks =
        for caller <- 1..writers do
          Task.async(fn -> append_loop(name, caller, payload, deadline, 0) end)
        end

      reader_tasks =
        for reader <- 1..readers do
          Task.async(fn -> read_loop(name, rem(reader, writers) + 1, deadline, 0, 0) end)
        end

      writes = writer_tasks |> Task.await_many(ms + 60_000) |> Enum.sum()

      {entries, bytes} =
        reader_tasks
        |> Task.await_many(ms + 60_000)
        |> Enum.reduce({0, 0}, fn {entries, bytes}, {total_entries, total_bytes} ->
          {total_entries + entries, total_bytes + bytes}
        end)

      {writes, entries, bytes}
    end

    run.(warmup_ms)
    DurableBuffer.truncate_all(name)

    {writes, entries, bytes} = run.(duration_ms)
    seconds = duration_ms / 1000

    %{
      write_ops_per_sec: round(writes / seconds),
      write_mb_per_sec: writes * payload_size / seconds / 1_048_576,
      read_entries_per_sec: round(entries / seconds),
      read_mb_per_sec: bytes / seconds / 1_048_576
    }
  end

  defp read_loop(name, key, deadline, entries, bytes) do
    if System.monotonic_time(:millisecond) < deadline do
      {new_entries, new_bytes} =
        name
        |> DurableBuffer.stream(key)
        |> Enum.reduce({entries, bytes}, fn payload, {entry_acc, byte_acc} ->
          {entry_acc + 1, byte_acc + byte_size(payload)}
        end)

      read_loop(name, key, deadline, new_entries, new_bytes)
    else
      {entries, bytes}
    end
  end

  @default_reader_counts [1, 8, 64]

  @doc """
  Read throughput with no writers at all.

  `mixed_grid/2` always measures reads against concurrent appends, so it
  cannot say what the read path costs on its own. This fills the partition
  once, then re-streams it with nothing else running.
  """
  def stream_grid(name, opts \\ []) do
    payload_size = Keyword.get(opts, :payload_size, 4 * 1024)
    entries = Keyword.get(opts, :entries, 20_000)
    reader_counts = Keyword.get(opts, :reader_counts, @default_reader_counts)

    duration_ms =
      Keyword.get(
        opts,
        :duration_ms,
        String.to_integer(System.get_env("BENCH_DURATION_MS", "5000"))
      )

    IO.puts("\n== Stream only: #{name}, #{format_bytes(payload_size)} payload ==")
    IO.puts("(no writers; #{format_number(entries)} entries per partition, re-streamed whole)")

    IO.puts(
      String.pad_trailing("readers", 9) <>
        String.pad_leading("entries/s", 13) <>
        String.pad_leading("MB/s", 10) <>
        String.pad_leading("full scans/s", 14)
    )

    for readers <- reader_counts do
      DurableBuffer.truncate_all(name)
      fill(name, payload_size, entries)
      result = measure_stream(name, readers, duration_ms, entries)

      IO.puts(
        String.pad_trailing(Integer.to_string(readers), 9) <>
          String.pad_leading(format_number(result.entries_per_sec), 13) <>
          String.pad_leading(:erlang.float_to_binary(result.mb_per_sec, decimals: 1), 10) <>
          String.pad_leading(:erlang.float_to_binary(result.scans_per_sec, decimals: 1), 14)
      )
    end

    DurableBuffer.truncate_all(name)
    :ok
  end

  defp measure_stream(name, readers, duration_ms, _entries) do
    deadline = System.monotonic_time(:millisecond) + duration_ms

    results =
      for reader <- 1..readers do
        Task.async(fn -> read_loop(name, reader, deadline, 0, 0) end)
      end
      |> Task.await_many(duration_ms + 60_000)

    entries = results |> Enum.map(&elem(&1, 0)) |> Enum.sum()
    bytes = results |> Enum.map(&elem(&1, 1)) |> Enum.sum()
    seconds = duration_ms / 1000

    %{
      entries_per_sec: round(entries / seconds),
      mb_per_sec: bytes / seconds / 1_048_576,
      scans_per_sec: entries / max(bytes, 1) * bytes / seconds / 20_000
    }
  end

  @doc """
  What `from:` costs at increasing depth into a partition.

  The sparse seek index exists so a resume does not rescan the log. This
  measures the time to the first entry of a `from:` read, which is where a
  scan would show up.
  """
  def seek_grid(name, opts \\ []) do
    payload_size = Keyword.get(opts, :payload_size, 256)
    entries = Keyword.get(opts, :entries, 200_000)
    samples = Keyword.get(opts, :samples, 200)

    IO.puts("\n== Seek: #{name}, #{format_bytes(payload_size)} payload ==")
    IO.puts("(time to the first entry of a from: read, #{format_number(entries)} entries)")

    IO.puts(
      String.pad_trailing("from", 12) <>
        String.pad_leading("us/seek", 12) <>
        String.pad_leading("entries/s", 13)
    )

    DurableBuffer.truncate_all(name)
    fill(name, payload_size, entries)
    %{first: first} = DurableBuffer.offsets(name, 1)

    for fraction <- [0.0, 0.25, 0.5, 0.9, 0.99] do
      from = first + round(entries * fraction)

      {microseconds, _} =
        :timer.tc(fn ->
          for _ <- 1..samples do
            [_first] = name |> DurableBuffer.stream(1, from: from) |> Enum.take(1)
          end
        end)

      per_seek = microseconds / samples

      IO.puts(
        String.pad_trailing(format_number(from - first), 12) <>
          String.pad_leading(:erlang.float_to_binary(per_seek, decimals: 1), 12) <>
          String.pad_leading(format_number(round(1_000_000 / per_seek)), 13)
      )
    end

    DurableBuffer.truncate_all(name)
    :ok
  end

  defp fill(name, payload_size, entries) do
    payload = :binary.copy("x", payload_size)
    chunk = 1000

    for _ <- 1..div(entries, chunk) do
      {:ok, _range} = DurableBuffer.append_batch(name, 1, List.duplicate(payload, chunk))
    end

    :ok
  end

  def latency(name, opts \\ []) do
    payload_size = Keyword.get(opts, :payload_size, 4 * 1024)
    parallel_levels = Keyword.get(opts, :parallel_levels, [1, 16, 128])
    time = Keyword.get(opts, :time, String.to_integer(System.get_env("BENCH_TIME", "5")))
    payload = :binary.copy("x", payload_size)

    for parallel <- parallel_levels do
      IO.puts(
        "\n== Caller latency: #{name}, #{format_bytes(payload_size)} payload, #{parallel} parallel caller(s) =="
      )

      Benchee.run(
        %{
          "append" => fn -> {:ok, _offset} = DurableBuffer.append(name, self(), payload) end
        },
        warmup: 1,
        time: time,
        parallel: parallel,
        print: [configuration: false]
      )

      DurableBuffer.truncate_all(name)
    end

    :ok
  end

  def format_bytes(bytes) when bytes >= 1024 * 1024, do: "#{div(bytes, 1024 * 1024)}MB"
  def format_bytes(bytes) when bytes >= 1024, do: "#{div(bytes, 1024)}KB"
  def format_bytes(bytes), do: "#{bytes}B"

  def format_number(number) when number >= 1_000_000, do: "#{Float.round(number / 1_000_000, 2)}M"
  def format_number(number) when number >= 1_000, do: "#{Float.round(number / 1_000, 1)}k"
  def format_number(number), do: Integer.to_string(number)
end
