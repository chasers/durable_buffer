defmodule DurableBuffer.Backend.ReplicaTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Backend.Local
  alias DurableBuffer.Backend.Replica
  alias DurableBuffer.WAL

  @moduletag :tmp_dir

  defp dirs(tmp_dir) do
    {Path.join(tmp_dir, "primary"), Path.join(tmp_dir, "replica")}
  end

  defp encode_batch(payloads) do
    entries = Enum.map(payloads, &elem(WAL.encode(&1), 0))
    {entries, IO.iodata_length(entries)}
  end

  test "commit writes locally and to the replica dir on the replica node", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    config =
      Replica.init_config(dir: primary_dir, replica_dir: replica_dir, replicas: [node()])

    {:ok, state} = Replica.open(config, 0)
    {batch, bytes} = encode_batch(["replicated-entry"])

    assert {:ok, state} = Replica.commit(state, batch, bytes)

    assert Enum.to_list(Replica.stream(config, 0)) == ["replicated-entry"]

    assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) == [
             "replicated-entry"
           ]

    assert :ok = Replica.close(state)
  end

  test "ack :all fails when a replica is unreachable", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    config =
      Replica.init_config(
        dir: primary_dir,
        replica_dir: replica_dir,
        replicas: [:unreachable@nohost],
        ack: :all,
        rpc_timeout: 1000
      )

    {:ok, state} = Replica.open(config, 0)
    {batch, bytes} = encode_batch(["never-acked"])

    assert {:error, {:insufficient_acks, 1, 2}, state} = Replica.commit(state, batch, bytes)

    assert Enum.to_list(Replica.stream(config, 0)) == ["never-acked"]
    assert :ok = Replica.close(state)
  end

  test "integer ack tolerates an unreachable replica", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    config =
      Replica.init_config(
        dir: primary_dir,
        replica_dir: replica_dir,
        replicas: [node(), :unreachable@nohost],
        ack: 2,
        rpc_timeout: 1000
      )

    {:ok, state} = Replica.open(config, 0)
    {batch, bytes} = encode_batch(["quorum-entry"])

    assert {:ok, state} = Replica.commit(state, batch, bytes)
    assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) == ["quorum-entry"]
    assert :ok = Replica.close(state)
  end

  test "quorum needs a majority of local plus replicas", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    config =
      Replica.init_config(
        dir: primary_dir,
        replica_dir: replica_dir,
        replicas: [node()],
        ack: :quorum
      )

    assert config.needed_acks == 2

    {:ok, state} = Replica.open(config, 0)
    {batch, bytes} = encode_batch(["majority"])
    assert {:ok, state} = Replica.commit(state, batch, bytes)
    assert :ok = Replica.close(state)
  end

  test "truncate clears local and replica copies", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    config =
      Replica.init_config(dir: primary_dir, replica_dir: replica_dir, replicas: [node()])

    {:ok, state} = Replica.open(config, 0)
    {batch, bytes} = encode_batch(["to-be-dropped"])
    {:ok, state} = Replica.commit(state, batch, bytes)

    assert {:ok, state} = Replica.truncate(state)

    assert Enum.to_list(Replica.stream(config, 0)) == []
    assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) == []
    assert :ok = Replica.close(state)
  end

  test "fsync defaults to false and propagates when enabled", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    assert Replica.init_config(dir: primary_dir).fsync == false

    config =
      Replica.init_config(
        dir: primary_dir,
        replica_dir: replica_dir,
        replicas: [node()],
        fsync: true
      )

    assert config.fsync == true

    {:ok, state} = Replica.open(config, 0)
    assert state.local.fsync == true

    {batch, bytes} = encode_batch(["synced-everywhere"])
    assert {:ok, state} = Replica.commit(state, batch, bytes)

    writer = DurableBuffer.Replica.writer_pid(replica_dir, 0, true)
    assert :sys.get_state(writer).local.fsync == true

    assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) ==
             ["synced-everywhere"]

    assert :ok = Replica.close(state)
  end

  test "pipelined fsync: true commits stay ordered and converge", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)
    name = :"fsync_pipeline_#{System.unique_integer([:positive])}"

    start_supervised!(
      {DurableBuffer,
       name: name,
       partitions: 1,
       max_inflight_commits: 8,
       backend:
         {DurableBuffer.Backend.Replica,
          dir: primary_dir, replica_dir: replica_dir, replicas: [node()], fsync: true}}
    )

    for index <- 1..100 do
      DurableBuffer.append_async(name, :key, "synced-#{index}")
    end

    assert :ok = DurableBuffer.sync(name, :key)

    expected = Enum.map(1..100, &"synced-#{&1}")
    assert Enum.to_list(DurableBuffer.stream(name, :key)) == expected

    assert File.read!(Path.join(primary_dir, "p0.wal")) ==
             File.read!(Path.join(replica_dir, "p0.wal"))
  end

  test "commit records durability watermarks per member", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    config =
      Replica.init_config(dir: primary_dir, replica_dir: replica_dir, replicas: [node()])

    {:ok, state} = Replica.open(config, 0)
    {batch, bytes} = encode_batch(["watermarked"])
    {:ok, state} = Replica.commit(state, batch, bytes)

    assert state.watermarks == %{:local => {0, bytes}, node() => {0, bytes}}

    {batch2, bytes2} = encode_batch(["again"])
    {:ok, state} = Replica.commit(state, batch2, bytes2)

    assert state.watermarks == %{
             :local => {0, bytes + bytes2},
             node() => {0, bytes + bytes2}
           }

    assert :ok = Replica.close(state)
  end

  test "writer rejects batches that do not land at its tail", %{tmp_dir: tmp_dir} do
    {_primary_dir, replica_dir} = dirs(tmp_dir)
    {batch1, bytes1} = encode_batch(["first"])
    {batch2, _bytes2} = encode_batch(["second"])
    bin1 = IO.iodata_to_binary(batch1)
    bin2 = IO.iodata_to_binary(batch2)

    assert {:ok, {0, ^bytes1}} = DurableBuffer.Replica.commit(replica_dir, 0, 0, 0, bin1)

    assert {:error, {:sequence_mismatch, %{expected: {0, ^bytes1}, got: {0, 0}}}} =
             DurableBuffer.Replica.commit(replica_dir, 0, 0, 0, bin2)

    assert {:error, {:sequence_mismatch, %{expected: {0, ^bytes1}, got: {0, gap}}}} =
             DurableBuffer.Replica.commit(replica_dir, 0, 0, bytes1 + 100, bin2)

    assert gap == bytes1 + 100

    assert {:error, {:sequence_mismatch, %{expected: {0, ^bytes1}, got: {9, ^bytes1}}}} =
             DurableBuffer.Replica.commit(replica_dir, 0, 9, bytes1, bin2)

    assert {:ok, {0, watermark}} = DurableBuffer.Replica.commit(replica_dir, 0, 0, bytes1, bin2)
    assert watermark == bytes1 + byte_size(bin2)

    assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) ==
             ["first", "second"]
  end

  @tag capture_log: true
  test "a replica that missed batches is resynced from the primary WAL", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    solo_config = Replica.init_config(dir: primary_dir, replica_dir: replica_dir, replicas: [])
    {:ok, solo_state} = Replica.open(solo_config, 0)
    {batch1, bytes1} = encode_batch(["missed-by-replica"])
    {:ok, solo_state} = Replica.commit(solo_state, batch1, bytes1)
    :ok = Replica.close(solo_state)

    config =
      Replica.init_config(
        dir: primary_dir,
        replica_dir: replica_dir,
        replicas: [node()],
        ack: :all
      )

    {:ok, state} = Replica.open(config, 0)
    {batch2, bytes2} = encode_batch(["after-the-gap"])

    assert {:ok, state} = Replica.commit(state, batch2, bytes2)

    assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) ==
             ["missed-by-replica", "after-the-gap"]

    assert File.read!(Path.join(primary_dir, "p0.wal")) ==
             File.read!(Path.join(replica_dir, "p0.wal"))

    assert :ok = Replica.close(state)
  end

  test "truncate bumps the epoch, persists it, and replicas adopt it", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    config =
      Replica.init_config(dir: primary_dir, replica_dir: replica_dir, replicas: [node()])

    {:ok, state} = Replica.open(config, 0)
    assert state.epoch == 0

    {batch, bytes} = encode_batch(["pre-truncate"])
    {:ok, state} = Replica.commit(state, batch, bytes)

    {:ok, state} = Replica.truncate(state)
    assert state.epoch == 1
    assert DurableBuffer.Epoch.load(primary_dir, 0) == 1
    assert DurableBuffer.Epoch.load(replica_dir, 0) == 1

    {batch, bytes} = encode_batch(["post-truncate"])
    assert {:ok, state} = Replica.commit(state, batch, bytes)

    assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) ==
             ["post-truncate"]

    assert :ok = Replica.close(state)
  end

  @tag capture_log: true
  test "a replica that missed a truncate is truncated and resynced", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    {batch0, bytes0} = encode_batch(["old-epoch-data"])

    {:ok, {0, ^bytes0}} =
      DurableBuffer.Replica.commit(replica_dir, 0, 0, 0, IO.iodata_to_binary(batch0))

    solo_config = Replica.init_config(dir: primary_dir, replica_dir: replica_dir, replicas: [])
    {:ok, solo_state} = Replica.open(solo_config, 0)
    {:ok, solo_state} = Replica.truncate(solo_state)
    assert solo_state.epoch == 1
    {new_batch, new_bytes} = encode_batch(["new-epoch-data"])
    {:ok, solo_state} = Replica.commit(solo_state, new_batch, new_bytes)
    :ok = Replica.close(solo_state)

    config =
      Replica.init_config(
        dir: primary_dir,
        replica_dir: replica_dir,
        replicas: [node()],
        ack: :all
      )

    {:ok, state} = Replica.open(config, 0)
    {batch, bytes} = encode_batch(["after-heal"])

    assert {:ok, state} = Replica.commit(state, batch, bytes)

    assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) ==
             ["new-epoch-data", "after-heal"]

    assert DurableBuffer.Epoch.load(replica_dir, 0) == 1
    assert :ok = Replica.close(state)
  end

  test "the epoch survives a primary restart", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    config = Replica.init_config(dir: primary_dir, replica_dir: replica_dir, replicas: [])
    {:ok, state} = Replica.open(config, 0)
    {:ok, state} = Replica.truncate(state)
    {:ok, state} = Replica.truncate(state)
    assert state.epoch == 2
    :ok = Replica.close(state)

    {:ok, state} = Replica.open(config, 0)
    assert state.epoch == 2
    assert :ok = Replica.close(state)
  end

  test "writer re-acks duplicate batches idempotently", %{tmp_dir: tmp_dir} do
    {_primary_dir, replica_dir} = dirs(tmp_dir)
    {batch, bytes} = encode_batch(["dup"])
    bin = IO.iodata_to_binary(batch)

    assert {:ok, {0, ^bytes}} = DurableBuffer.Replica.commit(replica_dir, 0, 0, 0, bin)
    assert {:ok, {0, ^bytes}} = DurableBuffer.Replica.commit(replica_dir, 0, 0, 0, bin)

    assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) == ["dup"]
  end

  test "pipelined commits stay ordered and byte-identical on the replica", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)
    name = :"pipelined_buffer_#{System.unique_integer([:positive])}"

    start_supervised!(
      {DurableBuffer,
       name: name,
       partitions: 1,
       max_inflight_commits: 8,
       backend:
         {DurableBuffer.Backend.Replica,
          dir: primary_dir, replica_dir: replica_dir, replicas: [node()]}}
    )

    for index <- 1..200 do
      DurableBuffer.append_async(name, :key, "entry-#{index}")
    end

    assert :ok = DurableBuffer.sync(name, :key)

    expected = Enum.map(1..200, &"entry-#{&1}")
    assert Enum.to_list(DurableBuffer.stream(name, :key)) == expected

    primary_wal = File.read!(Path.join(primary_dir, "p0.wal"))
    replica_wal = File.read!(Path.join(replica_dir, "p0.wal"))
    assert primary_wal == replica_wal
  end

  @tag capture_log: true
  test "a replica killed mid-load rejoins and converges", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)
    name = :"rejoin_buffer_#{System.unique_integer([:positive])}"

    start_supervised!(
      {DurableBuffer,
       name: name,
       partitions: 1,
       backend:
         {DurableBuffer.Backend.Replica,
          dir: primary_dir, replica_dir: replica_dir, replicas: [node()]}}
    )

    for index <- 1..50 do
      DurableBuffer.append_async(name, :key, "before-#{index}")
    end

    assert :ok = DurableBuffer.sync(name, :key)

    writer = DurableBuffer.Replica.writer_pid(replica_dir, 0, false)
    :ok = GenServer.stop(writer)

    for index <- 1..50 do
      DurableBuffer.append_async(name, :key, "after-#{index}")
    end

    assert :ok = DurableBuffer.sync(name, :key)

    expected = Enum.map(1..50, &"before-#{&1}") ++ Enum.map(1..50, &"after-#{&1}")
    assert Enum.to_list(DurableBuffer.stream(name, :key)) == expected

    assert File.read!(Path.join(primary_dir, "p0.wal")) ==
             File.read!(Path.join(replica_dir, "p0.wal"))
  end

  test "works end to end through DurableBuffer", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)
    name = :"replica_buffer_#{System.unique_integer([:positive])}"

    start_supervised!(
      {DurableBuffer,
       name: name,
       partitions: 2,
       backend:
         {DurableBuffer.Backend.Replica,
          dir: primary_dir, replica_dir: replica_dir, replicas: [node()]}}
    )

    tasks =
      for index <- 1..20 do
        Task.async(fn -> DurableBuffer.append(name, index, "entry-#{index}") end)
      end

    assert Enum.all?(Task.await_many(tasks, 10_000), &(&1 == :ok))

    primary_config = Local.init_config(dir: primary_dir)

    local_entries =
      Enum.sort(
        Enum.to_list(Local.stream(primary_config, 0)) ++
          Enum.to_list(Local.stream(primary_config, 1))
      )

    replica_config = Local.init_config(dir: replica_dir)

    replica_entries =
      Enum.sort(
        Enum.to_list(Local.stream(replica_config, 0)) ++
          Enum.to_list(Local.stream(replica_config, 1))
      )

    expected = Enum.sort(Enum.map(1..20, &"entry-#{&1}"))
    assert local_entries == expected
    assert replica_entries == expected
  end

  describe "healing a primary that lost WAL bytes a replica still holds" do
    test "recovers the whole tail", %{tmp_dir: tmp_dir} do
      {primary_dir, replica_dir} = dirs(tmp_dir)

      config =
        Replica.init_config(
          dir: primary_dir,
          replica_dir: replica_dir,
          replicas: [node()],
          fsync: false
        )

      {:ok, state} = Replica.open(config, 0)
      {batch, bytes} = encode_batch(["acked-then-lost"])
      assert {:ok, state} = Replica.commit(state, batch, bytes)
      assert :ok = Replica.close(state)

      crash_primary(primary_dir, 0)
      assert Enum.to_list(Replica.stream(config, 0)) == []

      {:ok, state} = Replica.open(config, 0)
      assert Enum.to_list(Replica.stream(config, 0)) == ["acked-then-lost"]
      assert :ok = Replica.close(state)
    end

    test "recovers only the missing suffix", %{tmp_dir: tmp_dir} do
      {primary_dir, replica_dir} = dirs(tmp_dir)

      config =
        Replica.init_config(
          dir: primary_dir,
          replica_dir: replica_dir,
          replicas: [node()],
          fsync: false
        )

      {:ok, state} = Replica.open(config, 0)

      state =
        Enum.reduce(["one", "two", "three"], state, fn payload, state ->
          {batch, bytes} = encode_batch([payload])
          {:ok, state} = Replica.commit(state, batch, bytes)
          state
        end)

      assert :ok = Replica.close(state)

      {kept, _bytes} = encode_batch(["one"])
      crash_primary(primary_dir, IO.iodata_length(kept))
      assert Enum.to_list(Replica.stream(config, 0)) == ["one"]

      {:ok, state} = Replica.open(config, 0)
      assert Enum.to_list(Replica.stream(config, 0)) == ["one", "two", "three"]
      assert :ok = Replica.close(state)
    end

    test "appends nothing when the primary is level with the replica", %{tmp_dir: tmp_dir} do
      {primary_dir, replica_dir} = dirs(tmp_dir)

      config =
        Replica.init_config(dir: primary_dir, replica_dir: replica_dir, replicas: [node()])

      {:ok, state} = Replica.open(config, 0)
      {batch, bytes} = encode_batch(["level"])
      assert {:ok, state} = Replica.commit(state, batch, bytes)
      assert :ok = Replica.close(state)

      {:ok, state} = Replica.open(config, 0)
      assert Enum.to_list(Replica.stream(config, 0)) == ["level"]
      assert :ok = Replica.close(state)
    end

    test "opens without healing when every replica is unreachable", %{tmp_dir: tmp_dir} do
      {primary_dir, replica_dir} = dirs(tmp_dir)

      config =
        Replica.init_config(
          dir: primary_dir,
          replica_dir: replica_dir,
          replicas: [:unreachable@nohost],
          heal_timeout: 500
        )

      assert {:ok, state} = Replica.open(config, 0)
      assert Enum.to_list(Replica.stream(config, 0)) == []
      assert :ok = Replica.close(state)
    end

    test "ignores a replica on an older epoch", %{tmp_dir: tmp_dir} do
      {primary_dir, replica_dir} = dirs(tmp_dir)

      config =
        Replica.init_config(
          dir: primary_dir,
          replica_dir: replica_dir,
          replicas: [node()],
          fsync: false
        )

      {:ok, state} = Replica.open(config, 0)
      {batch, bytes} = encode_batch(["old-epoch"])
      assert {:ok, state} = Replica.commit(state, batch, bytes)
      assert :ok = Replica.close(state)

      crash_primary(primary_dir, 0)
      DurableBuffer.Epoch.store!(primary_dir, 0, 7)

      {:ok, state} = Replica.open(config, 0)
      assert Enum.to_list(Replica.stream(config, 0)) == []
      assert :ok = Replica.close(state)
    end
  end

  defp crash_primary(primary_dir, keep_bytes) do
    path = Local.wal_path(primary_dir, 0)
    contents = File.read!(path)
    File.write!(path, binary_part(contents, 0, keep_bytes))
  end
end
