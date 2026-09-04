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

  describe "seeking with from:" do
    setup %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)

      for chunk <- Enum.chunk_every(0..29, 3) do
        {:ok, _range} =
          DurableBuffer.append_batch(name, "k", Enum.map(chunk, &"entry-#{&1}"))
      end

      %{name: name, dir: tmp_dir}
    end

    test "lands on a batch boundary", %{name: name} do
      assert Enum.take(DurableBuffer.stream(name, "k", from: 9), 2) == ["entry-9", "entry-10"]
    end

    test "lands mid-batch", %{name: name} do
      assert Enum.take(DurableBuffer.stream(name, "k", from: 10), 2) == ["entry-10", "entry-11"]
    end

    test "labels the suffix with the right offsets", %{name: name} do
      assert Enum.take(DurableBuffer.stream(name, "k", from: 25, with_offsets: true), 2) ==
               [{25, "entry-25"}, {26, "entry-26"}]
    end

    test "returns the whole suffix from every offset", %{name: name} do
      for from <- 0..30 do
        assert Enum.to_list(DurableBuffer.stream(name, "k", from: from)) ==
                 Enum.map(from..29//1, &"entry-#{&1}")
      end
    end

    test "is correct without an index", %{name: name, dir: dir} do
      File.rm!(Path.join(dir, "p0.idx"))

      assert Enum.to_list(DurableBuffer.stream(name, "k", from: 25)) ==
               Enum.map(25..29, &"entry-#{&1}")
    end

    test "is correct with a corrupt index", %{name: name, dir: dir} do
      File.write!(Path.join(dir, "p0.idx"), :crypto.strong_rand_bytes(200))

      assert Enum.to_list(DurableBuffer.stream(name, "k", from: 25)) ==
               Enum.map(25..29, &"entry-#{&1}")
    end

    test "is correct with a truncated index", %{name: name, dir: dir} do
      path = Path.join(dir, "p0.idx")
      contents = File.read!(path)
      File.write!(path, binary_part(contents, 0, 30))

      assert Enum.to_list(DurableBuffer.stream(name, "k", from: 25)) ==
               Enum.map(25..29, &"entry-#{&1}")
    end
  end

  test "an index ahead of the WAL is trimmed on open", %{tmp_dir: tmp_dir} do
    name = start_buffer(tmp_dir, partitions: 1)
    {:ok, 0..2} = DurableBuffer.append_batch(name, "k", ["a", "b", "c"])
    stop_supervised!({DurableBuffer, name})

    index = Path.join(tmp_dir, "p0.idx")
    before = File.stat!(index).size
    dangling = <<99::64-big, 999_999::64-big>>

    File.write!(index, [File.read!(index), dangling, <<:erlang.crc32(dangling)::32-big>>], [
      :binary
    ])

    name = start_buffer(tmp_dir, name: name, partitions: 1)
    assert File.stat!(index).size == before

    assert Enum.to_list(DurableBuffer.stream(name, "k", from: 1)) == ["b", "c"]
  end

  describe "retention" do
    setup %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)
      {:ok, 0..9} = DurableBuffer.append_batch(name, "k", Enum.map(0..9, &"e#{&1}"))
      %{name: name, dir: tmp_dir}
    end

    test "trims to an explicit point", %{name: name} do
      assert :ok = DurableBuffer.trim(name, "k", upto: 6)

      assert %{first: 6, next: 10} = DurableBuffer.offsets(name, "k")
      assert Enum.to_list(DurableBuffer.stream(name, "k")) == Enum.map(6..9, &"e#{&1}")
    end

    test "keeps offsets stable across a trim", %{name: name} do
      :ok = DurableBuffer.trim(name, "k", upto: 6)

      assert Enum.take(DurableBuffer.stream(name, "k", with_offsets: true), 2) ==
               [{6, "e6"}, {7, "e7"}]

      assert {:ok, 10} = DurableBuffer.append(name, "k", "e10")
      assert Enum.to_list(DurableBuffer.stream(name, "k", from: 9)) == ["e9", "e10"]
    end

    test "seeks correctly after a trim", %{name: name} do
      :ok = DurableBuffer.trim(name, "k", upto: 4)

      for from <- 4..10 do
        assert Enum.to_list(DurableBuffer.stream(name, "k", from: from)) ==
                 Enum.map(from..9//1, &"e#{&1}")
      end
    end

    test "survives a restart", %{name: name, dir: dir} do
      :ok = DurableBuffer.trim(name, "k", upto: 6)
      stop_supervised!({DurableBuffer, name})

      name = start_buffer(dir, name: name, partitions: 1)

      assert %{first: 6, next: 10} = DurableBuffer.offsets(name, "k")
      assert Enum.to_list(DurableBuffer.stream(name, "k")) == Enum.map(6..9, &"e#{&1}")
      assert {:ok, 10} = DurableBuffer.append(name, "k", "e10")
    end

    test "a trim past every entry keeps offsets monotonic", %{name: name} do
      assert :ok = DurableBuffer.trim(name, "k", upto: 10)

      assert %{first: 10, next: 10} = DurableBuffer.offsets(name, "k")
      assert Enum.to_list(DurableBuffer.stream(name, "k")) == []
      assert {:ok, 10} = DurableBuffer.append(name, "k", "after")
    end

    test "refuses a trim past the durable offset", %{name: name} do
      assert {:error, :not_durable} = DurableBuffer.trim(name, "k", upto: 11)
      assert %{first: 0} = DurableBuffer.offsets(name, "k")
    end

    test "a trim below the base is a no-op", %{name: name} do
      :ok = DurableBuffer.trim(name, "k", upto: 6)
      assert :ok = DurableBuffer.trim(name, "k", upto: 2)
      assert %{first: 6} = DurableBuffer.offsets(name, "k")
    end
  end

  test "a replica keeps replicating across a primary trim", %{tmp_dir: tmp_dir} do
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

    {:ok, 0..4} = DurableBuffer.append_batch(name, "k", ~w(a b c d e))
    assert :ok = DurableBuffer.trim(name, "k", upto: 3)

    assert {:ok, 5} = DurableBuffer.append(name, "k", "f")
    assert %{first: 3, next: 6} = DurableBuffer.offsets(name, "k")
    assert Enum.to_list(DurableBuffer.stream(name, "k")) == ~w(d e f)

    assert %{epoch: 0} = DurableBuffer.replica_status(name, "k") |> elem(1) |> Map.fetch!(node())
  end

  describe "review regressions" do
    test "a pre-trim index never mislabels offsets", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)

      for chunk <- Enum.chunk_every(0..19, 2) do
        {:ok, _range} = DurableBuffer.append_batch(name, "k", Enum.map(chunk, &"e#{&1}"))
      end

      index = Path.join(tmp_dir, "p0.idx")
      stale = File.read!(index)

      :ok = DurableBuffer.trim(name, "k", upto: 10)
      File.write!(index, stale)

      assert Enum.take(DurableBuffer.stream(name, "k", from: 5, with_offsets: true), 2) ==
               [{10, "e10"}, {11, "e11"}]

      assert Enum.take(DurableBuffer.stream(name, "k", from: 15, with_offsets: true), 2) ==
               [{15, "e15"}, {16, "e16"}]
    end

    test "a stale index is dropped when the partition reopens", %{tmp_dir: tmp_dir} do
      name = start_buffer(tmp_dir, partitions: 1)

      for chunk <- Enum.chunk_every(0..19, 2) do
        {:ok, _range} = DurableBuffer.append_batch(name, "k", Enum.map(chunk, &"e#{&1}"))
      end

      index = Path.join(tmp_dir, "p0.idx")
      stale = File.read!(index)
      :ok = DurableBuffer.trim(name, "k", upto: 10)
      stop_supervised!({DurableBuffer, name})
      File.write!(index, stale)

      name = start_buffer(tmp_dir, name: name, partitions: 1)

      assert Enum.take(DurableBuffer.stream(name, "k", from: 12, with_offsets: true), 2) ==
               [{12, "e12"}, {13, "e13"}]
    end

    test "trim settles the pipeline before judging the trim point", %{tmp_dir: tmp_dir} do
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

      for i <- 0..4, do: :ok = DurableBuffer.append_async(name, "k", "e#{i}")

      assert :ok = DurableBuffer.trim(name, "k", upto: 5)
      assert %{first: 5, next: 5} = DurableBuffer.offsets(name, "k")
    end

    test "reading replica status does not block behind a busy committer", %{tmp_dir: tmp_dir} do
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

      {:ok, 0..2} = DurableBuffer.append_batch(name, "k", ~w(a b c))

      writers =
        for i <- 1..20 do
          Task.async(fn -> DurableBuffer.append(name, "k", "concurrent-#{i}") end)
        end

      assert {:ok, status} = DurableBuffer.replica_status(name, "k")
      assert %{epoch: 0} = Map.fetch!(status, node())
      assert Enum.all?(Task.await_many(writers, 5000), &match?({:ok, _}, &1))
    end
  end
end
