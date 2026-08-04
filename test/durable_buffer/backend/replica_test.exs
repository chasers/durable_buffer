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
end
