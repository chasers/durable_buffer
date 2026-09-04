defmodule DurableBuffer.Partition do
  @moduledoc """
  Per-partition group-commit writer.

  Appends are calls that do not reply immediately: entries accumulate in a
  pending batch, and batches are handed to a linked
  `DurableBuffer.Partition.Committer` that owns the backend and replies to
  callers once their data is durable. Commits are pipelined: while one batch
  is being committed (fsync / replication / PUT), the writer keeps draining
  its mailbox into the next batch, and `:commit_done` triggers the next
  handoff — so batch formation overlaps commit latency instead of stopping
  for it.

  When the partition is idle a lone append pays exactly one commit of
  latency (plus `flush_delay_ms`, if configured, which lets batches fill at
  moderate load). `max_batch_bytes` / `max_batch_entries` force an immediate
  handoff under heavy load.

  With the default `flush_delay_ms: 0` the dwell is adaptive: the committer
  reports how long commits are taking to complete via `{:dwell_hint, ms}`
  messages, and a batch that starts while the partition is idle waits that
  long (0–2 ms) before handoff — but only when the previous batch was
  concurrent (≥ 2 entries), so a lone caller never pays it. This grows
  batches exactly when an expensive durability step (fsync, S3 PUT) is the
  bottleneck and stays at zero otherwise. An explicit `flush_delay_ms`
  fixes the dwell instead.
  """

  use GenServer

  alias DurableBuffer.Partition.Committer
  alias DurableBuffer.WAL

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Appends a payload and blocks until it is durable.
  """
  @spec append(GenServer.server(), iodata(), timeout()) :: :ok | {:error, term()}
  def append(server, payload, timeout \\ :infinity) do
    GenServer.call(server, {:append, payload}, timeout)
  end

  @doc """
  Appends a list of payloads in one call and blocks until all of them are
  durable.

  The whole list is framed as consecutive WAL entries and joins the current
  group commit together, so the per-call messaging cost is paid once for the
  entire list — the main lever for small-payload throughput.
  """
  @spec append_batch(GenServer.server(), [iodata()], timeout()) :: :ok | {:error, term()}
  def append_batch(server, payloads, timeout \\ :infinity)

  def append_batch(_server, [], _timeout), do: :ok

  def append_batch(server, payloads, timeout) do
    GenServer.call(server, {:append_batch, payloads}, timeout)
  end

  @doc """
  Enqueues a payload without waiting for durability.
  """
  @spec append_async(GenServer.server(), iodata()) :: :ok
  def append_async(server, payload) do
    GenServer.cast(server, {:append, payload})
  end

  @doc """
  Blocks until every previously enqueued payload is durable.

  Returns `{:error, reason}` if any commit containing `append_async/2`
  entries has failed since the last sync or truncate — those entries had no
  caller to receive the error, so it is surfaced here.
  """
  @spec sync(GenServer.server(), timeout()) :: :ok | {:error, term()}
  def sync(server, timeout \\ :infinity) do
    GenServer.call(server, :sync, timeout)
  end

  @doc """
  Discards all committed data for the partition.
  """
  @spec truncate(GenServer.server(), timeout()) :: :ok
  def truncate(server, timeout \\ :infinity) do
    GenServer.call(server, :truncate, timeout)
  end

  @doc """
  Reports the backend's per-replica replication state.
  """
  @spec replica_status(GenServer.server(), timeout()) :: {:ok, map()} | {:error, term()}
  def replica_status(server, timeout \\ 5_000) do
    GenServer.call(server, :replica_status, timeout)
  end

  @impl GenServer
  def init(opts) do
    {backend, config} = Keyword.fetch!(opts, :backend)
    partition_index = Keyword.fetch!(opts, :partition_index)

    {:ok, committer} =
      Committer.start_link(
        backend,
        config,
        partition_index,
        Keyword.take(opts, [:max_inflight_commits])
      )

    {:ok,
     %{
       committer: committer,
       pending: [],
       pending_bytes: 0,
       pending_count: 0,
       in_flight: 0,
       flush_scheduled?: false,
       max_batch_bytes: Keyword.get(opts, :max_batch_bytes, 8 * 1024 * 1024),
       max_batch_entries: Keyword.get(opts, :max_batch_entries, 5000),
       flush_delay_ms: Keyword.get(opts, :flush_delay_ms, 0),
       hinted_dwell_ms: 0,
       last_batch_count: 0
     }}
  end

  @impl GenServer
  def handle_call({:append, payload}, from, state) do
    {:noreply, enqueue(state, from, encode_one(payload))}
  end

  def handle_call({:append_batch, payloads}, from, state) do
    {:noreply, enqueue(state, from, encode_many(payloads))}
  end

  def handle_call(:sync, from, %{pending: []} = state) do
    Committer.request_sync(state.committer, from)
    {:noreply, state}
  end

  def handle_call(:sync, from, state) do
    state =
      %{state | pending: [{:sync, from} | state.pending]}
      |> handoff()

    {:noreply, state}
  end

  def handle_call(:replica_status, _from, state) do
    {:reply, Committer.replica_status(state.committer), state}
  end

  def handle_call(:truncate, from, state) do
    state = if state.pending == [], do: state, else: handoff(state)
    Committer.request_truncate(state.committer, from)
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:append, payload}, state) do
    {:noreply, enqueue(state, nil, encode_one(payload))}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    state = %{state | flush_scheduled?: false}

    if state.pending != [] and state.in_flight == 0 do
      {:noreply, handoff(state)}
    else
      {:noreply, state}
    end
  end

  def handle_info(:commit_done, state) do
    state = %{state | in_flight: state.in_flight - 1}

    if state.pending != [] and state.in_flight == 0 do
      {:noreply, handoff(state)}
    else
      {:noreply, state}
    end
  end

  def handle_info({:dwell_hint, dwell_ms}, state) do
    {:noreply, %{state | hinted_dwell_ms: dwell_ms}}
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  defp encode_one(payload) do
    {entry, entry_size} = WAL.encode(payload)
    {entry, 1, entry_size}
  end

  defp encode_many(payloads) do
    {entries, count, bytes} =
      Enum.reduce(payloads, {[], 0, 0}, fn payload, {entries, count, bytes} ->
        {entry, entry_size} = WAL.encode(payload)
        {[entry | entries], count + 1, bytes + entry_size}
      end)

    {Enum.reverse(entries), count, bytes}
  end

  defp enqueue(state, from, {entries, count, bytes}) do
    state = %{
      state
      | pending: [{:entries, from, entries} | state.pending],
        pending_bytes: state.pending_bytes + bytes,
        pending_count: state.pending_count + count
    }

    cond do
      state.pending_bytes >= state.max_batch_bytes or
          state.pending_count >= state.max_batch_entries ->
        handoff(state)

      state.flush_scheduled? or state.in_flight > 0 ->
        state

      true ->
        schedule_flush(state)
        %{state | flush_scheduled?: true}
    end
  end

  defp handoff(state) do
    Committer.commit(state.committer, Enum.reverse(state.pending), state.pending_bytes)

    %{
      state
      | pending: [],
        pending_bytes: 0,
        pending_count: 0,
        last_batch_count: state.pending_count,
        in_flight: state.in_flight + 1
    }
  end

  defp schedule_flush(state) do
    case flush_delay(state) do
      0 -> send(self(), :flush)
      delay -> Process.send_after(self(), :flush, delay)
    end
  end

  defp flush_delay(%{flush_delay_ms: 0} = state) do
    if state.last_batch_count > 1, do: state.hinted_dwell_ms, else: 0
  end

  defp flush_delay(state), do: state.flush_delay_ms
end
