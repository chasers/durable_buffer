defmodule DurableBuffer.Replica do
  @moduledoc """
  Replica-side entry points, invoked over `:erpc` by
  `DurableBuffer.Backend.Replica` on the primary.

  Any node running the `:durable_buffer` application can serve as a replica:
  writers are started on demand under a `PartitionSupervisor` and keyed by
  `{replica_dir, partition_index}`, so one replica node can host replicas of
  many buffers and partitions concurrently.
  """

  alias DurableBuffer.Replica.Writer

  @doc """
  Durably appends an already-framed batch to the replica WAL for
  `{dir, partition_index}`, starting the writer if needed.
  """
  @spec commit(Path.t(), non_neg_integer(), binary()) :: :ok | {:error, term()}
  def commit(dir, partition_index, batch) do
    Writer.commit(ensure_writer(dir, partition_index), batch)
  end

  @doc """
  Discards all replicated data for `{dir, partition_index}`.
  """
  @spec truncate(Path.t(), non_neg_integer()) :: :ok
  def truncate(dir, partition_index) do
    Writer.truncate(ensure_writer(dir, partition_index))
  end

  defp ensure_writer(dir, partition_index) do
    key = {dir, partition_index}
    name = Writer.name(dir, partition_index)

    case Registry.lookup(DurableBuffer.Registry, {:replica_writer, dir, partition_index}) do
      [{pid, _value}] ->
        pid

      [] ->
        supervisor = {:via, PartitionSupervisor, {DurableBuffer.Replica.WriterSupervisors, key}}

        case DynamicSupervisor.start_child(
               supervisor,
               {Writer, dir: dir, partition_index: partition_index, name: name}
             ) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
  end
end
