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

  alias DurableBuffer.Backend
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
  Lazily streams durable payloads for `partition_key`'s partition, oldest
  first.

  A reader sees only data that has met the backend's durability guarantee:
  the ack policy for `DurableBuffer.Backend.Replica`, a returned `datasync`
  for `DurableBuffer.Backend.Local`. The limit is re-read as the stream
  advances, so a consumer that keeps pulling picks up data that becomes
  durable while it runs. Like any read of a file that is still being
  written, the stream ends at the current end of durable data.

  Pass `dirty: true` to read the whole local WAL instead, including batches
  that have not met the policy and may still fail. Recovery tooling wants
  this; ordinary consumers do not.
  """
  @spec stream(atom(), term(), keyword()) :: Enumerable.t()
  def stream(name, partition_key, opts \\ []) do
    %{backend: {backend, backend_config}} = buffer = config(name)
    index = partition_index(name, partition_key)
    limit = read_limit(buffer, index)

    if Keyword.get(opts, :dirty, false) or limit == nil or not Backend.gates_reads?(backend) do
      backend.stream(backend_config, index)
    else
      backend.stream(backend_config, index, limit)
    end
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
  Reports each replica's replication state for `partition_key`'s partition.

  Only `DurableBuffer.Backend.Replica` tracks one; the other backends return
  `{:error, :unsupported}`. Each entry carries the epoch that replica has
  confirmed, the watermark the primary last saw from it, and two flags:

    * `promotable?` — the replica is on the primary's epoch. A replica that
      missed a truncate still holds pre-truncate data, and is not safe to
      promote until its sender re-attaches and truncates it.
    * `caught_up?` — `promotable?`, and its watermark is at the primary's
      WAL tail.
  """
  @spec replica_status(atom(), term(), timeout()) :: {:ok, map()} | {:error, term()}
  def replica_status(name, partition_key, timeout \\ 5_000) do
    name
    |> partition_server(partition_key)
    |> Partition.replica_status(timeout)
  end

  @doc """
  Blocks until every replica of `partition_key`'s partition is on the
  primary's epoch, or until `timeout`.

  `truncate/3` does not wait: it bumps the epoch, resets the senders, and
  returns. A replica its `:erpc` did not reach converges shortly afterwards,
  when its sender re-attaches. Call this when you need the stronger
  guarantee — before you promote a follower, say.
  """
  @spec await_replicas(atom(), term(), timeout()) ::
          :ok | {:error, {:not_adopted, [node()]} | term()}
  def await_replicas(name, partition_key, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_adoption(name, partition_key, deadline)
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
  @spec config(atom()) :: %{
          partitions: pos_integer(),
          backend: {module(), map()},
          durable_offsets: :atomics.atomics_ref()
        }
  def config(name) do
    :persistent_term.get({DurableBuffer, name})
  end

  defp read_limit(buffer, index) do
    case Map.get(buffer, :durable_offsets) do
      nil -> nil
      offsets -> fn -> :atomics.get(offsets, index + 1) end
    end
  end

  defp await_adoption(name, partition_key, deadline) do
    case replica_status(name, partition_key) do
      {:ok, status} ->
        pending = for {node, %{promotable?: false}} <- status, do: node

        cond do
          pending == [] ->
            :ok

          System.monotonic_time(:millisecond) >= deadline ->
            {:error, {:not_adopted, pending}}

          true ->
            Process.sleep(25)
            await_adoption(name, partition_key, deadline)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp partition_server(name, partition_key) do
    partition_name(name, partition_index(name, partition_key))
  end
end
