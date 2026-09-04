defmodule DurableBufferTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  defp start_buffer(tmp_dir, opts \\ []) do
    name = Keyword.get(opts, :name, :"buffer_#{System.unique_integer([:positive])}")

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

    assert {:ok, _} = DurableBuffer.append(name, "user-1", "first")
    assert {:ok, _} = DurableBuffer.append(name, "user-1", "second")

    assert Enum.to_list(DurableBuffer.stream(name, "user-1")) == ["first", "second"]
  end

  test "keys hash to stable partitions and any term is a valid key", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir)

    for key <- ["string", :atom, 42, {:tuple, 1}, self()] do
      index = DurableBuffer.partition_index(name, key)
      assert index == DurableBuffer.partition_index(name, key)
      assert index in 0..3
      assert {:ok, _} = DurableBuffer.append(name, key, :erlang.term_to_binary(key))
    end
  end

  test "concurrent appends across keys land in their own partitions", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir)

    tasks =
      for key <- 1..20, index <- 1..10 do
        Task.async(fn -> DurableBuffer.append(name, key, "#{key}:#{index}") end)
      end

    assert Enum.all?(Task.await_many(tasks, 10_000), &match?({:ok, _}, &1))

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

    assert {:ok, _} = DurableBuffer.append_batch(name, "user-9", ["one", "two", "three"])
    assert {:ok, _} = DurableBuffer.append_batch(name, "user-9", [])

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
    {:ok, _} = DurableBuffer.append(name, "k", "persisted")
    :ok = Supervisor.stop(pid)

    {:ok, pid} = DurableBuffer.start_link(opts)
    {:ok, _} = DurableBuffer.append(name, "k", "after-restart")

    assert Enum.to_list(DurableBuffer.stream(name, "k")) == ["persisted", "after-restart"]
    :ok = Supervisor.stop(pid)
  end

  test "truncate clears one partition without touching others", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir, partitions: 2)

    {key_a, key_b} = distinct_partition_keys(name)

    {:ok, _} = DurableBuffer.append(name, key_a, "keep")
    {:ok, _} = DurableBuffer.append(name, key_b, "drop")

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

  test "replica_status reports :unsupported for a non-replicated backend", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir)

    assert {:error, :unsupported} = DurableBuffer.replica_status(name, "user-1")
    assert {:error, :unsupported} = DurableBuffer.await_replicas(name, "user-1")
  end

  test "await_replicas returns the nodes that never adopted the epoch", %{tmp_dir: tmp_dir} do
    name =
      start_buffer(tmp_dir,
        backend:
          {DurableBuffer.Backend.Replica,
           dir: Path.join(tmp_dir, "primary"),
           replica_dir: Path.join(tmp_dir, "replica"),
           replicas: [:unreachable@nohost],
           ack: 1,
           rpc_timeout: 500,
           heal_timeout: 500},
        partitions: 1
      )

    assert {:error, {:not_adopted, [:unreachable@nohost]}} =
             DurableBuffer.await_replicas(name, "user-1", 200)
  end

  test "await_replicas succeeds once every replica is on the epoch", %{tmp_dir: tmp_dir} do
    name =
      start_buffer(tmp_dir,
        backend:
          {DurableBuffer.Backend.Replica,
           dir: Path.join(tmp_dir, "primary"),
           replica_dir: Path.join(tmp_dir, "replica"),
           replicas: [node()]},
        partitions: 1
      )

    assert {:ok, _} = DurableBuffer.append(name, "user-1", "replicated")
    assert :ok = DurableBuffer.await_replicas(name, "user-1")

    assert :ok = DurableBuffer.truncate(name, "user-1")
    assert :ok = DurableBuffer.await_replicas(name, "user-1")

    {:ok, status} = DurableBuffer.replica_status(name, "user-1")
    assert %{adopted_epoch: 1, epoch: 1, promotable?: true} = Map.fetch!(status, node())
  end

  describe "reads gated at the durable offset" do
    test "hides a batch that never met the ack policy", %{tmp_dir: tmp_dir} do
      name =
        start_buffer(tmp_dir,
          backend:
            {DurableBuffer.Backend.Replica,
             dir: Path.join(tmp_dir, "primary"),
             replica_dir: Path.join(tmp_dir, "replica"),
             replicas: [:unreachable@nohost],
             ack: :all,
             rpc_timeout: 300,
             heal_timeout: 300},
          partitions: 1
        )

      assert {:error, {:insufficient_acks, 1, 2}} =
               DurableBuffer.append(name, "user-1", "never-acked")

      assert Enum.to_list(DurableBuffer.stream(name, "user-1")) == []
      assert Enum.to_list(DurableBuffer.stream(name, "user-1", dirty: true)) == ["never-acked"]
    end

    test "shows a batch once the ack policy is met", %{tmp_dir: tmp_dir} do
      name =
        start_buffer(tmp_dir,
          backend:
            {DurableBuffer.Backend.Replica,
             dir: Path.join(tmp_dir, "primary"),
             replica_dir: Path.join(tmp_dir, "replica"),
             replicas: [node()],
             ack: :all},
          partitions: 1
        )

      assert {:ok, _} = DurableBuffer.append(name, "user-1", "acked")
      assert Enum.to_list(DurableBuffer.stream(name, "user-1")) == ["acked"]
    end

    test "returns every entry across chunk boundaries", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)
      payload = String.duplicate("x", 1000)

      for _ <- 1..200 do
        assert {:ok, _} = DurableBuffer.append(name, "user-1", payload)
      end

      assert length(Enum.to_list(DurableBuffer.stream(name, "user-1"))) == 200
    end

    test "reads nothing back after a truncate", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)

      assert {:ok, _} = DurableBuffer.append(name, "user-1", "before")
      assert :ok = DurableBuffer.truncate(name, "user-1")

      assert Enum.to_list(DurableBuffer.stream(name, "user-1")) == []

      assert {:ok, _} = DurableBuffer.append(name, "user-1", "after")
      assert Enum.to_list(DurableBuffer.stream(name, "user-1")) == ["after"]
    end
  end

  describe "logical offsets" do
    test "are contiguous across group commits and batches", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)

      assert {:ok, 0} = DurableBuffer.append(name, "k", "a")
      assert {:ok, 1..3} = DurableBuffer.append_batch(name, "k", ["b", "c", "d"])
      assert {:ok, 4} = DurableBuffer.append(name, "k", "e")
      assert {:ok, []} = DurableBuffer.append_batch(name, "k", [])
      assert {:ok, 5} = DurableBuffer.append(name, "k", "f")
    end

    test "pair with their payloads", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)
      {:ok, 0..2} = DurableBuffer.append_batch(name, "k", ["a", "b", "c"])

      assert Enum.to_list(DurableBuffer.stream(name, "k", with_offsets: true)) ==
               [{0, "a"}, {1, "b"}, {2, "c"}]
    end

    test "from: returns exactly the suffix", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)
      {:ok, 0..4} = DurableBuffer.append_batch(name, "k", ["a", "b", "c", "d", "e"])

      assert Enum.to_list(DurableBuffer.stream(name, "k", from: 0)) == ~w(a b c d e)
      assert Enum.to_list(DurableBuffer.stream(name, "k", from: 3)) == ~w(d e)
      assert Enum.to_list(DurableBuffer.stream(name, "k", from: 5)) == []
      assert Enum.to_list(DurableBuffer.stream(name, "k", from: 99)) == []
    end

    test "survive a restart", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)
      {:ok, 0..1} = DurableBuffer.append_batch(name, "k", ["a", "b"])
      stop_supervised!({DurableBuffer, name})

      name = start_buffer(tmp_dir, name: name, partitions: 1)
      assert {:ok, 2} = DurableBuffer.append(name, "k", "c")

      assert Enum.to_list(DurableBuffer.stream(name, "k", with_offsets: true)) ==
               [{0, "a"}, {1, "b"}, {2, "c"}]
    end

    test "stay monotonic across a truncate", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)
      {:ok, 0..1} = DurableBuffer.append_batch(name, "k", ["a", "b"])

      :ok = DurableBuffer.truncate(name, "k")

      assert %{first: 2, next: 2} = DurableBuffer.offsets(name, "k")
      assert {:ok, 2} = DurableBuffer.append(name, "k", "c")
      assert Enum.to_list(DurableBuffer.stream(name, "k", with_offsets: true)) == [{2, "c"}]
    end

    test "stay monotonic across a truncate and a restart", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)
      {:ok, 0..1} = DurableBuffer.append_batch(name, "k", ["a", "b"])
      :ok = DurableBuffer.truncate(name, "k")
      stop_supervised!({DurableBuffer, name})

      name = start_buffer(tmp_dir, name: name, partitions: 1)
      assert {:ok, 2} = DurableBuffer.append(name, "k", "c")
    end

    test "offsets/2 reports first, durable and next", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)
      assert DurableBuffer.offsets(name, "k") == %{first: 0, durable: 0, next: 0}

      {:ok, 0..2} = DurableBuffer.append_batch(name, "k", ["a", "b", "c"])
      assert DurableBuffer.offsets(name, "k") == %{first: 0, durable: 3, next: 3}
    end

    test "a commit that never met the ack policy does not advance durable", %{tmp_dir: tmp_dir} do
      name =
        start_buffer(tmp_dir,
          backend:
            {DurableBuffer.Backend.Replica,
             dir: Path.join(tmp_dir, "primary"),
             replica_dir: Path.join(tmp_dir, "replica"),
             replicas: [:unreachable@nohost],
             ack: :all,
             rpc_timeout: 300,
             heal_timeout: 300},
          partitions: 1
        )

      assert {:error, {:insufficient_acks, 1, 2}} = DurableBuffer.append(name, "k", "stuck")

      assert %{first: 0, durable: 0, next: 1} = DurableBuffer.offsets(name, "k")
      assert Enum.to_list(DurableBuffer.stream(name, "k")) == []
    end
  end
end
