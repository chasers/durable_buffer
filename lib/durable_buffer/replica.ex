defmodule DurableBuffer.Replica do
  @moduledoc """
  Replica-side entry points, invoked by `DurableBuffer.Backend.Replica` on
  the primary — over `:erpc` for the control path, and over the configured
  `DurableBuffer.Transport` for `replicate/7`.

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
  primary's post-truncate epoch and logical byte base.
  """
  @spec truncate(Path.t(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  def truncate(dir, partition_index, epoch, base_byte) do
    Writer.truncate(ensure_writer(dir, partition_index), epoch, base_byte)
  end

  @doc """
  Drops every byte below `base_byte` from the replica WAL for
  `{dir, partition_index}`, passing on a trim the primary already applied.
  """
  @spec trim(Path.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def trim(dir, partition_index, base_byte) do
    Writer.trim(ensure_writer(dir, partition_index), base_byte)
  end

  @doc """
  Reads `length` bytes of the replica WAL for `{dir, partition_index}`
  starting at `offset`.

  Called over `:erpc` by `DurableBuffer.Backend.Replica` when a restarted
  primary heals the WAL tail it lost from a replica that still holds it.
  """
  @spec read_range(Path.t(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_range(dir, partition_index, offset, length) do
    Writer.read_range(ensure_writer(dir, partition_index), offset, length)
  end

  @doc """
  Hands one replicated batch to the already-running writer for
  `{dir, partition_index}`.

  This is the replica-side end of a `DurableBuffer.Transport` that has no
  pid-to-pid messaging, such as `DurableBuffer.Transport.GenRPC`. It sends
  the same `:replicate` message the writer would receive over distribution,
  so the writer's group-commit drain is unaffected, and the ack goes
  straight back to `reply_to` over distribution.

  It does **not** start a writer. `attach/3` starts one, with the primary's
  configured `fsync`, and a sender attaches before it sends anything. A
  batch that arrives with no writer running therefore means the writer died
  after the attach, so dropping it is right: the sender's monitor fires and
  it re-attaches, which starts the writer with the correct setting and
  resyncs. Starting one here would pick the `fsync: true` default and
  silently datasync every batch against an explicit `fsync: false`.

  It does the least possible work on purpose. `:gen_rpc.ordered_cast/4`
  blocks its acceptor until this returns, and that acceptor is what keeps
  batches in order for the whole node pair.
  """
  @spec replicate(
          Path.t(),
          non_neg_integer(),
          reference(),
          non_neg_integer(),
          non_neg_integer(),
          binary(),
          pid()
        ) :: :ok
  def replicate(dir, partition_index, ref, epoch, offset, batch, reply_to) do
    case Registry.lookup(DurableBuffer.Registry, {:replica_writer, dir, partition_index}) do
      [{writer, _value}] ->
        send(writer, {:replicate, ref, epoch, offset, batch, reply_to})

      [] ->
        :ok
    end

    :ok
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
