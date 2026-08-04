Code.require_file("support/bench_helper.exs", __DIR__)

System.cmd("epmd", ["-daemon"])

unless Node.alive?() do
  {:ok, _pid} = :net_kernel.start([:durable_buffer_bench, :shortnames])
end

replica_count = String.to_integer(System.get_env("REPLICAS", "2"))

ack =
  case System.get_env("ACK", "all") do
    "all" -> :all
    "quorum" -> :quorum
    count -> String.to_integer(count)
  end

peer_args = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)

replicas =
  for index <- 1..replica_count do
    {:ok, _pid, node} =
      :peer.start_link(%{name: :"durable_buffer_bench_replica_#{index}", args: peer_args})

    {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:durable_buffer])
    node
  end

base_dir = Path.join(System.tmp_dir!(), "durable_buffer_bench_replica_#{System.os_time(:second)}")
primary_dir = Path.join(base_dir, "primary")
replica_dir = Path.join(base_dir, "replica")
partitions = String.to_integer(System.get_env("PARTITIONS", "#{System.schedulers_online()}"))

{:ok, _pid} =
  DurableBuffer.start_link(
    name: :bench_replica,
    partitions: partitions,
    backend:
      {DurableBuffer.Backend.Replica,
       dir: primary_dir, replica_dir: replica_dir, replicas: replicas, ack: ack}
  )

IO.puts(
  "replica backend: replicas=#{inspect(replicas)} ack=#{inspect(ack)} " <>
    "partitions=#{partitions} dir=#{base_dir}"
)

IO.puts("note: peers run on this host, so this measures protocol overhead, not network RTT")

DurableBuffer.Bench.throughput_grid(:bench_replica)
DurableBuffer.Bench.mixed_grid(:bench_replica)
DurableBuffer.Bench.latency(:bench_replica)

File.rm_rf!(base_dir)
