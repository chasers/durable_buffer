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
      |> Keyword.take([:max_batch_bytes, :max_batch_entries])
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

    assert :ok = Partition.append(pid, "a")
    assert :ok = Partition.append(pid, "b")

    assert List.flatten(SlowBackend.committed_batches(recorder)) == ["a", "b"]
  end

  test "concurrent appends coalesce into fewer commits than appends" do
    {pid, recorder} = start_partition()

    tasks =
      for index <- 1..50 do
        Task.async(fn -> Partition.append(pid, "entry-#{index}") end)
      end

    assert Enum.all?(Task.await_many(tasks, 5000), &(&1 == :ok))

    batches = SlowBackend.committed_batches(recorder)
    assert length(List.flatten(batches)) == 50
    assert length(batches) < 50
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
end
