defmodule DurableBuffer.TransportDistributedTest do
  @moduledoc """
  The replication data path over two real nodes, once per transport.

  Excluded from `mix test`. Starting distribution renames this node, which
  would break every other test holding `node()`, so CI runs this module on
  its own with `mix test --only distributed`.

  Both nodes run gen_rpc with `port_discovery: :stateless`, which derives
  the listening port from the trailing integer in the node name. That is
  what lets two gen_rpc nodes share one host: `db_transport1@127.0.0.1`
  listens on 5371 and `db_transport2@127.0.0.1` on 5372.
  """

  use ExUnit.Case, async: false

  alias DurableBuffer.Backend.Local
  alias DurableBuffer.Backend.Replica
  alias DurableBuffer.Transport
  alias DurableBuffer.WAL

  @moduletag :distributed
  @moduletag :tmp_dir
  @moduletag capture_log: true
  @moduletag timeout: 120_000

  @transports [Transport.Distribution, Transport.GenRPC]

  setup_all do
    System.cmd("epmd", ["-daemon"])

    unless Node.alive?() do
      {:ok, _pid} = :net_kernel.start([:"db_transport1@127.0.0.1", :longnames])
    end

    :ok = stateless_gen_rpc()

    peer_args = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)

    {:ok, peer, node} =
      :peer.start_link(%{
        name: :db_transport2,
        host: ~c"127.0.0.1",
        longnames: true,
        args: peer_args
      })

    {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:durable_buffer])
    _ = :erpc.call(node, Application, :stop, [:gen_rpc])
    :ok = :erpc.call(node, Application, :put_env, [:gen_rpc, :port_discovery, :stateless])
    {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:gen_rpc])

    on_exit(fn -> :peer.stop(peer) end)

    %{replica_node: node}
  end

  defp stateless_gen_rpc do
    _ = Application.stop(:gen_rpc)
    Application.put_env(:gen_rpc, :port_discovery, :stateless)
    {:ok, _apps} = Application.ensure_all_started(:gen_rpc)
    :ok
  end

  defp encode_batch(payloads) do
    entries = Enum.map(payloads, &elem(WAL.encode(&1), 0))
    {entries, IO.iodata_length(entries)}
  end

  defp commit!(state, payloads) do
    {batch, bytes} = encode_batch(payloads)
    span = {Replica.offsets(state).next, length(batch)}
    {:ok, state} = Replica.commit(state, batch, bytes, span)
    state
  end

  defp replica_entries(replica_node, replica_dir) do
    stream = Local.stream(Local.init_config(dir: replica_dir), 0)
    :erpc.call(replica_node, Enum, :to_list, [stream])
  end

  defp open(ctx, transport, suffix, opts \\ []) do
    primary_dir = Path.join([ctx.tmp_dir, suffix, "primary"])
    replica_dir = Path.join([ctx.tmp_dir, suffix, "replica"])

    config =
      Replica.init_config(
        Keyword.merge(
          [
            dir: primary_dir,
            replica_dir: replica_dir,
            replicas: [ctx.replica_node],
            transport: transport
          ],
          opts
        )
      )

    {:ok, state} = Replica.open(config, 0)
    {state, replica_dir}
  end

  defp await_replica(replica_node, replica_dir, count, attempts \\ 100) do
    if length(replica_entries(replica_node, replica_dir)) >= count or attempts == 0 do
      :ok
    else
      Process.sleep(50)
      await_replica(replica_node, replica_dir, count, attempts - 1)
    end
  end

  for transport <- @transports do
    describe "#{inspect(transport)}" do
      @transport transport

      test "a commit reaches the replica node", ctx do
        {state, replica_dir} = open(ctx, @transport, "single")

        state = commit!(state, ["one"])

        assert replica_entries(ctx.replica_node, replica_dir) == ["one"]
        assert :ok = Replica.close(state)
      end

      test "a deep pipeline of batches arrives in the order it was sent", ctx do
        {state, replica_dir} = open(ctx, @transport, "ordered", ack: 1)

        payloads = for index <- 1..500, do: "entry-#{index}"

        state =
          Enum.reduce(payloads, state, fn payload, state -> commit!(state, [payload]) end)

        await_replica(ctx.replica_node, replica_dir, length(payloads))

        assert replica_entries(ctx.replica_node, replica_dir) == payloads
        assert :ok = Replica.close(state)
      end

      test "a replica that starts behind is resynced from the primary WAL", ctx do
        primary_dir = Path.join([ctx.tmp_dir, "resync", "primary"])
        replica_dir = Path.join([ctx.tmp_dir, "resync", "replica"])

        solo =
          Replica.init_config(dir: primary_dir, replica_dir: replica_dir, replicas: [])

        {:ok, state} = Replica.open(solo, 0)
        state = commit!(state, ["before-one"])
        state = commit!(state, ["before-two"])
        :ok = Replica.close(state)

        config =
          Replica.init_config(
            dir: primary_dir,
            replica_dir: replica_dir,
            replicas: [ctx.replica_node],
            transport: @transport
          )

        {:ok, state} = Replica.open(config, 0)
        state = commit!(state, ["after"])

        assert replica_entries(ctx.replica_node, replica_dir) ==
                 ["before-one", "before-two", "after"]

        assert :ok = Replica.close(state)
      end
    end
  end
end
