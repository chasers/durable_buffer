defmodule DurableBuffer.PartitionTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Partition
  alias DurableBuffer.Test.SlowBackend

  defp start_partition(context_opts \\ []) do
    {:ok, recorder} = SlowBackend.start_recorder()

    backend_opts =
      Keyword.merge([recorder: recorder], Keyword.get(context_opts, :backend_opts, []))

    opts =
      context_opts
      |> Keyword.take([:max_batch_bytes, :max_batch_entries, :flush_delay_ms])
      |> Keyword.merge(
        name: :"partition_test_#{System.unique_integer([:positive])}",
        backend: DurableBuffer.Backend.normalize({SlowBackend, backend_opts}),
        partition_index: 0
      )

    pid = start_supervised!({Partition, opts})
    {pid, recorder}
  end

  test "append blocks until committed and preserves order" do
    {pid, recorder} = start_partition()

    assert {:ok, _} = Partition.append(pid, "a")
    assert {:ok, _} = Partition.append(pid, "b")

    assert List.flatten(SlowBackend.committed_batches(recorder)) == ["a", "b"]
  end

  test "concurrent appends coalesce into fewer commits than appends" do
    {pid, recorder} = start_partition()

    tasks =
      for index <- 1..50 do
        Task.async(fn -> Partition.append(pid, "entry-#{index}") end)
      end

    assert Enum.all?(Task.await_many(tasks, 5000), &match?({:ok, _}, &1))

    batches = SlowBackend.committed_batches(recorder)
    assert length(List.flatten(batches)) == 50
    assert length(batches) < 50
  end

  test "append_batch commits all payloads in order with one reply" do
    {pid, recorder} = start_partition()

    assert {:ok, _} = Partition.append_batch(pid, ["b1", "b2", "b3"])
    assert {:ok, _} = Partition.append(pid, "single")

    assert List.flatten(SlowBackend.committed_batches(recorder)) == ["b1", "b2", "b3", "single"]
  end

  test "append_batch with an empty list is a no-op" do
    {pid, recorder} = start_partition()

    assert {:ok, _} = Partition.append_batch(pid, [])
    assert SlowBackend.committed_batches(recorder) == []
  end

  test "append_batch interleaves with concurrent single appends in one commit" do
    {pid, recorder} = start_partition()

    tasks =
      [
        Task.async(fn -> Partition.append_batch(pid, Enum.map(1..50, &"batch-#{&1}")) end),
        Task.async(fn -> Partition.append(pid, "lone") end)
      ]

    assert [{:ok, batch_range}, {:ok, lone}] = Task.await_many(tasks, 5000)
    assert Enum.sort(Enum.to_list(batch_range) ++ [lone]) == Enum.to_list(0..50)

    committed = List.flatten(SlowBackend.committed_batches(recorder))
    assert length(committed) == 51
    assert "lone" in committed

    assert Enum.filter(committed, &String.starts_with?(&1, "batch-")) ==
             Enum.map(1..50, &"batch-#{&1}")
  end

  test "append_batch propagates commit errors" do
    {pid, _recorder} = start_partition(backend_opts: [fail_on: "poison"])

    assert {:error, :injected_failure} = Partition.append_batch(pid, ["ok-entry", "poison-pill"])
  end

  test "max_batch_entries counts individual batch payloads" do
    {pid, recorder} = start_partition(max_batch_entries: 10)

    assert {:ok, _} = Partition.append_batch(pid, Enum.map(1..25, &"forced-#{&1}"))
    :ok = Partition.sync(pid)

    batches = SlowBackend.committed_batches(recorder)
    assert length(List.flatten(batches)) == 25
  end

  test "flush_delay_ms coalesces a burst into one commit" do
    {pid, recorder} =
      start_partition(flush_delay_ms: 50, backend_opts: [commit_sleep: 0])

    for index <- 1..10 do
      :ok = Partition.append_async(pid, "delayed-#{index}")
    end

    Process.sleep(150)

    batches = SlowBackend.committed_batches(recorder)
    assert length(batches) == 1
    assert List.flatten(batches) == Enum.map(1..10, &"delayed-#{&1}")
  end

  test "sync flushes immediately without waiting out the flush delay" do
    {pid, recorder} =
      start_partition(flush_delay_ms: 60_000, backend_opts: [commit_sleep: 0])

    :ok = Partition.append_async(pid, "no-waiting")

    assert :ok = Partition.sync(pid)
    assert List.flatten(SlowBackend.committed_batches(recorder)) == ["no-waiting"]
  end

  test "append_async entries are durable after sync" do
    {pid, recorder} = start_partition()

    for index <- 1..20 do
      :ok = Partition.append_async(pid, "async-#{index}")
    end

    assert :ok = Partition.sync(pid)

    committed = List.flatten(SlowBackend.committed_batches(recorder))
    assert committed == Enum.map(1..20, &"async-#{&1}")
  end

  test "sync on an idle partition returns immediately" do
    {pid, _recorder} = start_partition()
    assert :ok = Partition.sync(pid)
  end

  test "max_batch_entries forces an immediate flush" do
    {pid, recorder} = start_partition(max_batch_entries: 1)

    for index <- 1..5 do
      :ok = Partition.append_async(pid, "forced-#{index}")
    end

    :ok = Partition.sync(pid)

    assert length(SlowBackend.committed_batches(recorder)) == 5
  end

  test "commit errors are propagated to every caller in the batch" do
    {pid, _recorder} = start_partition(backend_opts: [fail_on: "poison"])

    tasks =
      for _ <- 1..5 do
        Task.async(fn -> Partition.append(pid, "poison-pill") end)
      end

    assert Enum.all?(Task.await_many(tasks, 5000), &(&1 == {:error, :injected_failure}))
  end

  test "sync propagates a commit error for pending async entries" do
    {pid, _recorder} = start_partition(backend_opts: [fail_on: "poison"])

    :ok = Partition.append_async(pid, "poison-async")

    assert {:error, :injected_failure} = Partition.sync(pid)
  end

  test "truncate flushes pending entries then clears committed data" do
    {pid, recorder} = start_partition()

    :ok = Partition.append_async(pid, "pending")
    assert :ok = Partition.truncate(pid)

    assert SlowBackend.committed_batches(recorder) == []
  end

  test "acks are unsupported on a backend that does not track them" do
    {pid, _recorder} = start_partition()

    assert {:error, :unsupported} = Partition.acks(pid)
    assert {:error, :unsupported} = Partition.ack(pid, "worker-1", 0)
  end
end
