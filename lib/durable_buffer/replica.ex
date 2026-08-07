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

  `epoch` and `offset` identify where the batch belongs: the primary's
  current epoch and the WAL byte offset at which the batch starts. The
  writer rejects any batch that does not land exactly at its tail. On
  success the reply carries the writer's new durability watermark.
  """
  @spec commit(Path.t(), non_neg_integer(), non_neg_integer(), non_neg_integer(), binary()) ::
          {:ok, DurableBuffer.Backend.Replica.watermark()} | {:error, term()}
  def commit(dir, partition_index, epoch, offset, batch) do
    Writer.commit(ensure_writer(dir, partition_index), epoch, offset, batch)
  end

  @doc """
  Discards all replicated data for `{dir, partition_index}` and adopts the
  primary's post-truncate epoch.
  """
  @spec truncate(Path.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def truncate(dir, partition_index, epoch) do
    Writer.truncate(ensure_writer(dir, partition_index), epoch)
  end

  @doc """
  Returns the pid of the writer for `{dir, partition_index}`, starting it if
  needed. `fsync` applies only when this call starts the writer (an
  already-running writer keeps its setting).
  """
  @spec writer_pid(Path.t(), non_neg_integer(), boolean()) :: pid()
  def writer_pid(dir, partition_index, fsync) do
    ensure_writer(dir, partition_index, fsync)
  end

  @doc """
  Returns the writer pid and its tail `{epoch, offset}` in one call. Called
  over `:erpc` by `DurableBuffer.Replica.Sender` when establishing its
  channel, so it can decide between resuming live traffic and resyncing the
  replica from the primary's WAL.
  """
  @spec attach(Path.t(), non_neg_integer(), boolean()) ::
          {pid(), DurableBuffer.Backend.Replica.watermark()}
  def attach(dir, partition_index, fsync) do
    pid = ensure_writer(dir, partition_index, fsync)
    {pid, Writer.tail(pid)}
  end

  defp ensure_writer(dir, partition_index, fsync \\ true) do
    key = {dir, partition_index}
    name = Writer.name(dir, partition_index)

    case Registry.lookup(DurableBuffer.Registry, {:replica_writer, dir, partition_index}) do
      [{pid, _value}] ->
        pid

      [] ->
        supervisor = {:via, PartitionSupervisor, {DurableBuffer.Replica.WriterSupervisors, key}}

        case DynamicSupervisor.start_child(
               supervisor,
               {Writer, dir: dir, partition_index: partition_index, name: name, fsync: fsync}
             ) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
  end
end
