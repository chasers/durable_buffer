Code.require_file("support/bench_helper.exs", __DIR__)

defmodule DurableBuffer.TransportBench do
  @moduledoc """
  Append throughput and caller latency for each replication transport, with
  and without an unrelated load on the Erlang distribution channel between
  the same node pair.

  On an idle loopback the two transports should look alike. The point of
  gen_rpc is that distribution is one TCP connection per node pair, so
  everything else on that pair competes with replication. The hog is what
  makes that visible: processes shipping large binaries to the replica node
  over `:erpc`, which is traffic replication has nothing to do with.

  Both nodes run gen_rpc with `port_discovery: :stateless`, which derives
  the port from the trailing integer in the node name, so two gen_rpc nodes
  can share one host.
  """

  @payload_bytes 64 * 1024
  @throughput_callers 64
  @latency_callers 8
  @latency_samples 2000
  @hog_bytes 4 * 1024 * 1024

  def run(replica_node, transports, hog_counts) do
    for {label, transport} <- transports, hogs <- hog_counts do
      measure_one(replica_node, label, transport, hogs)
    end
  end

  defp measure_one(replica_node, label, transport, hogs) do
    name = :"transport_bench_#{label}_#{hogs}"
    base_dir = Path.join(System.tmp_dir!(), "#{name}_#{System.os_time(:second)}")

    {:ok, pid} =
      DurableBuffer.start_link(
        name: name,
        partitions: 8,
        backend:
          {DurableBuffer.Backend.Replica,
           dir: Path.join(base_dir, "primary"),
           replica_dir: Path.join(base_dir, "replica"),
           replicas: [replica_node],
           transport: transport}
      )

    hog_pids = start_hogs(replica_node, hogs)

    throughput =
      DurableBuffer.Bench.measure(name, @payload_bytes, @throughput_callers, 1000, 5000)

    DurableBuffer.truncate_all(name)
    latency = latency(name)

    Enum.each(hog_pids, &Process.exit(&1, :kill))
    Supervisor.stop(pid)
    File.rm_rf!(base_dir)

    report(label, hogs, throughput, latency)
  end

  defp start_hogs(_replica_node, 0), do: []

  defp start_hogs(replica_node, count) do
    payload = :binary.copy("h", @hog_bytes)

    for _index <- 1..count do
      spawn(fn -> hog_loop(replica_node, payload) end)
    end
  end

  defp hog_loop(replica_node, payload) do
    _ = :erpc.call(replica_node, :erlang, :byte_size, [payload], 30_000)
    hog_loop(replica_node, payload)
  catch
    _kind, _reason -> hog_loop(replica_node, payload)
  end

  defp latency(name) do
    payload = :binary.copy("x", @payload_bytes)
    per_caller = div(@latency_samples, @latency_callers)

    samples =
      1..@latency_callers
      |> Enum.map(fn caller ->
        Task.async(fn ->
          for _index <- 1..per_caller do
            started = System.monotonic_time(:microsecond)
            {:ok, _offset} = DurableBuffer.append(name, caller, payload)
            System.monotonic_time(:microsecond) - started
          end
        end)
      end)
      |> Task.await_many(120_000)
      |> List.flatten()
      |> Enum.sort()

    %{median: percentile(samples, 0.50), p99: percentile(samples, 0.99)}
  end

  defp percentile(sorted, fraction) do
    index = min(round(fraction * length(sorted)), length(sorted) - 1)
    Enum.at(sorted, index)
  end

  def header do
    IO.puts("\n== Replication transport: 64 KiB payload, 8 partitions ==")
    IO.puts("(hogs ship 4 MiB :erpc payloads to the replica node, unrelated to replication)")

    IO.puts(
      String.pad_trailing("transport", 16) <>
        String.pad_trailing("hogs", 6) <>
        String.pad_leading("ops/s", 12) <>
        String.pad_leading("MB/s", 10) <>
        String.pad_leading("p50 us", 10) <>
        String.pad_leading("p99 us", 10)
    )
  end

  defp report(label, hogs, throughput, latency) do
    IO.puts(
      String.pad_trailing(label, 16) <>
        String.pad_trailing(Integer.to_string(hogs), 6) <>
        String.pad_leading(Integer.to_string(throughput.ops_per_sec), 12) <>
        String.pad_leading(:erlang.float_to_binary(throughput.mb_per_sec, decimals: 1), 10) <>
        String.pad_leading(Integer.to_string(latency.median), 10) <>
        String.pad_leading(Integer.to_string(latency.p99), 10)
    )
  end
end

System.cmd("epmd", ["-daemon"])

unless Node.alive?() do
  {:ok, _pid} = :net_kernel.start([:"durable_buffer_bench1@127.0.0.1", :longnames])
end

_ = Application.stop(:gen_rpc)
Application.put_env(:gen_rpc, :port_discovery, :stateless)
{:ok, _apps} = Application.ensure_all_started(:gen_rpc)

peer_args = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)

{:ok, _peer, replica_node} =
  :peer.start_link(%{
    name: :durable_buffer_bench2,
    host: ~c"127.0.0.1",
    longnames: true,
    args: peer_args
  })

{:ok, _apps} = :erpc.call(replica_node, Application, :ensure_all_started, [:durable_buffer])
_ = :erpc.call(replica_node, Application, :stop, [:gen_rpc])
:ok = :erpc.call(replica_node, Application, :put_env, [:gen_rpc, :port_discovery, :stateless])
{:ok, _apps} = :erpc.call(replica_node, Application, :ensure_all_started, [:gen_rpc])

hog_counts =
  System.get_env("HOGS", "0,4")
  |> String.split(",")
  |> Enum.map(&String.to_integer/1)

IO.puts("note: both nodes run on this host, so this measures contention, not network RTT")
DurableBuffer.TransportBench.header()

DurableBuffer.TransportBench.run(
  replica_node,
  [
    {"distribution", DurableBuffer.Transport.Distribution},
    {"gen_rpc", DurableBuffer.Transport.GenRPC}
  ],
  hog_counts
)
