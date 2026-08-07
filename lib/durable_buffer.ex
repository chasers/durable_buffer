defmodule DurableBuffer do
  @moduledoc """
  A partitioned, group-committed buffer durable to local disk, N replica
  nodes, or S3.

  Start a buffer under your supervision tree:

      children = [
        {DurableBuffer,
         name: :events,
         backend: {DurableBuffer.Backend.Local, dir: "/var/lib/events"},
         partitions: 8}
      ]

  Then append from any process:

      :ok = DurableBuffer.append(:events, user_id, payload)

  `append/3` returns once the payload is durable per the backend's guarantee.
  All appends that arrive at a partition while a commit is in flight are
  group-committed together, so the per-write fsync/replication/PUT cost is
  amortized across concurrent callers, and commits are pipelined — the next
  batch forms while the previous one is being committed. For small payloads
  prefer `append_batch/4`, which moves a whole list of payloads in one call.
  The `partition_key` is hashed to one of a fixed set of partitions; each
  partition commits independently and in parallel.

  Backends:

    * `DurableBuffer.Backend.Local` — append-only WAL + `datasync` per commit
    * `DurableBuffer.Backend.Replica` — local WAL + parallel replication to
      `replicas: [node()]` with a configurable ack policy
    * `DurableBuffer.Backend.S3` — one immutable S3 object per group commit
      via `Req` + `ReqS3`
  """

  alias DurableBuffer.Partition

  @doc """
  Returns a child spec for a buffer instance.

  Options:

    * `:name` (required) — atom identifying the buffer
    * `:backend` (required) — `{module, opts}` backend spec
    * `:partitions` — number of partition writers, default `System.schedulers_online()`
    * `:max_batch_bytes` — force a flush when a pending batch reaches this size, default 8 MiB
    * `:max_batch_entries` — force a flush at this many pending entries, default 5000
    * `:flush_delay_ms` — dwell time before committing a batch that started
      while the partition was idle. The default 0 is adaptive: batches
      normally commit as soon as possible, but when commits are completing
      slowly (an fsync or PUT is the bottleneck) and batches are concurrent,
      a dwell of up to 2 ms is applied automatically so batches fill. An
      explicit value fixes the dwell instead, trading median latency for
      throughput; size caps and `sync/3` still flush immediately
    * `:max_inflight_commits` — for backends that support pipelined commits
      (currently `DurableBuffer.Backend.Replica`), how many batches may be
      committing concurrently per partition, default 32. Callers are always
      replied to in order
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :name)},
      start: {DurableBuffer.Supervisor, :start_link, [opts]},
      type: :supervisor
    }
  end

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    DurableBuffer.Supervisor.start_link(opts)
  end

  @doc """
  Appends a payload to the partition selected by `partition_key`, blocking
  until it is durable.
  """
  @spec append(atom(), term(), iodata(), timeout()) :: :ok | {:error, term()}
  def append(name, partition_key, payload, timeout \\ :infinity) do
    name
    |> partition_server(partition_key)
    |> Partition.append(payload, timeout)
  end

  @doc """
  Appends a list of payloads to the partition selected by `partition_key` in
  a single call, blocking until all of them are durable.

  Far cheaper than N `append/3` calls for small payloads: the messaging and
  reply cost is paid once per list, and the whole list joins one group
  commit. An empty list is a no-op.
  """
  @spec append_batch(atom(), term(), [iodata()], timeout()) :: :ok | {:error, term()}
  def append_batch(name, partition_key, payloads, timeout \\ :infinity) do
    name
    |> partition_server(partition_key)
    |> Partition.append_batch(payloads, timeout)
  end

  @doc """
  Enqueues a payload without waiting for durability. Use `sync/2` to await
  durability of everything enqueued so far.
  """
  @spec append_async(atom(), term(), iodata()) :: :ok
  def append_async(name, partition_key, payload) do
    name
    |> partition_server(partition_key)
    |> Partition.append_async(payload)
  end

  @doc """
  Blocks until every payload enqueued to `partition_key`'s partition is durable.
  """
  @spec sync(atom(), term(), timeout()) :: :ok | {:error, term()}
  def sync(name, partition_key, timeout \\ :infinity) do
    name
    |> partition_server(partition_key)
    |> Partition.sync(timeout)
  end

  @doc """
  Blocks until every enqueued payload in every partition is durable.
  """
  @spec sync_all(atom(), timeout()) :: :ok
  def sync_all(name, timeout \\ :infinity) do
    %{partitions: partitions} = config(name)

    for index <- 0..(partitions - 1) do
      Partition.sync(partition_name(name, index), timeout)
    end

    :ok
  end

  @doc """
  Lazily streams committed payloads for `partition_key`'s partition, oldest
  first.
  """
  @spec stream(atom(), term()) :: Enumerable.t()
  def stream(name, partition_key) do
    %{backend: {backend, backend_config}} = config(name)
    backend.stream(backend_config, partition_index(name, partition_key))
  end

  @doc """
  Discards all committed and pending data for `partition_key`'s partition.
  """
  @spec truncate(atom(), term(), timeout()) :: :ok
  def truncate(name, partition_key, timeout \\ :infinity) do
    name
    |> partition_server(partition_key)
    |> Partition.truncate(timeout)
  end

  @doc """
  Discards all committed and pending data in every partition.
  """
  @spec truncate_all(atom(), timeout()) :: :ok
  def truncate_all(name, timeout \\ :infinity) do
    %{partitions: partitions} = config(name)

    for index <- 0..(partitions - 1) do
      Partition.truncate(partition_name(name, index), timeout)
    end

    :ok
  end

  @doc """
  Returns the partition index `partition_key` hashes to.
  """
  @spec partition_index(atom(), term()) :: non_neg_integer()
  def partition_index(name, partition_key) do
    %{partitions: partitions} = config(name)
    :erlang.phash2(partition_key, partitions)
  end

  @doc """
  Returns the via-tuple name of the writer for partition `index`.
  """
  @spec partition_name(atom(), non_neg_integer()) :: GenServer.name()
  def partition_name(name, index) do
    {:via, Registry, {DurableBuffer.Registry, {name, index}}}
  end

  @doc """
  Returns the buffer's resolved configuration.
  """
  @spec config(atom()) :: %{partitions: pos_integer(), backend: {module(), map()}}
  def config(name) do
    :persistent_term.get({DurableBuffer, name})
  end

  defp partition_server(name, partition_key) do
    partition_name(name, partition_index(name, partition_key))
  end
end
