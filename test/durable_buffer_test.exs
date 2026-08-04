defmodule DurableBufferTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  defp start_buffer(tmp_dir, opts \\ []) do
    name = :"buffer_#{System.unique_integer([:positive])}"

    opts =
      Keyword.merge(
        [
          name: name,
          backend: {DurableBuffer.Backend.Local, dir: tmp_dir},
          partitions: 4
        ],
        opts
      )

    start_supervised!({DurableBuffer, opts})
    name
  end

  test "append then stream round-trips through the keyed partition", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir)

    assert :ok = DurableBuffer.append(name, "user-1", "first")
    assert :ok = DurableBuffer.append(name, "user-1", "second")

    assert Enum.to_list(DurableBuffer.stream(name, "user-1")) == ["first", "second"]
  end

  test "keys hash to stable partitions and any term is a valid key", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir)

    for key <- ["string", :atom, 42, {:tuple, 1}, self()] do
      index = DurableBuffer.partition_index(name, key)
      assert index == DurableBuffer.partition_index(name, key)
      assert index in 0..3
      assert :ok = DurableBuffer.append(name, key, :erlang.term_to_binary(key))
    end
  end

  test "concurrent appends across keys land in their own partitions", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir)

    tasks =
      for key <- 1..20, index <- 1..10 do
        Task.async(fn -> DurableBuffer.append(name, key, "#{key}:#{index}") end)
      end

    assert Enum.all?(Task.await_many(tasks, 10_000), &(&1 == :ok))

    for key <- 1..20 do
      entries =
        name
        |> DurableBuffer.stream(key)
        |> Enum.filter(&String.starts_with?(&1, "#{key}:"))

      assert Enum.sort(entries) == Enum.sort(Enum.map(1..10, &"#{key}:#{&1}"))
    end
  end

  test "append_batch round-trips through the keyed partition", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir)

    assert :ok = DurableBuffer.append_batch(name, "user-9", ["one", "two", "three"])
    assert :ok = DurableBuffer.append_batch(name, "user-9", [])

    assert Enum.to_list(DurableBuffer.stream(name, "user-9")) == ["one", "two", "three"]
  end

  test "append_async then sync_all makes everything durable", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir)

    for key <- 1..8 do
      :ok = DurableBuffer.append_async(name, key, "async-#{key}")
    end

    assert :ok = DurableBuffer.sync_all(name)

    for key <- 1..8 do
      assert "async-#{key}" in Enum.to_list(DurableBuffer.stream(name, key))
    end
  end

  test "data survives a buffer restart", %{tmp_dir: tmp_dir} do
    name = :"restart_buffer_#{System.unique_integer([:positive])}"
    opts = [name: name, backend: {DurableBuffer.Backend.Local, dir: tmp_dir}, partitions: 2]

    {:ok, pid} = DurableBuffer.start_link(opts)
    :ok = DurableBuffer.append(name, "k", "persisted")
    :ok = Supervisor.stop(pid)

    {:ok, pid} = DurableBuffer.start_link(opts)
    :ok = DurableBuffer.append(name, "k", "after-restart")

    assert Enum.to_list(DurableBuffer.stream(name, "k")) == ["persisted", "after-restart"]
    :ok = Supervisor.stop(pid)
  end

  test "truncate clears one partition without touching others", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir, partitions: 2)

    {key_a, key_b} = distinct_partition_keys(name)

    :ok = DurableBuffer.append(name, key_a, "keep")
    :ok = DurableBuffer.append(name, key_b, "drop")

    assert :ok = DurableBuffer.truncate(name, key_b)

    assert Enum.to_list(DurableBuffer.stream(name, key_a)) == ["keep"]
    assert Enum.to_list(DurableBuffer.stream(name, key_b)) == []
  end

  defp distinct_partition_keys(name) do
    key_a = 1

    key_b =
      Enum.find(2..100, fn key ->
        DurableBuffer.partition_index(name, key) != DurableBuffer.partition_index(name, key_a)
      end)

    {key_a, key_b}
  end
end
