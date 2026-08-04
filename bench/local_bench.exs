Code.require_file("support/bench_helper.exs", __DIR__)

dir = Path.join(System.tmp_dir!(), "durable_buffer_bench_local_#{System.os_time(:second)}")
partitions = String.to_integer(System.get_env("PARTITIONS", "#{System.schedulers_online()}"))

{:ok, _pid} =
  DurableBuffer.start_link(
    name: :bench_local,
    partitions: partitions,
    backend: {DurableBuffer.Backend.Local, dir: dir}
  )

IO.puts("local backend: dir=#{dir} partitions=#{partitions}")

DurableBuffer.Bench.throughput_grid(:bench_local)
DurableBuffer.Bench.batch_grid(:bench_local)
DurableBuffer.Bench.mixed_grid(:bench_local)
DurableBuffer.Bench.latency(:bench_local)

File.rm_rf!(dir)
