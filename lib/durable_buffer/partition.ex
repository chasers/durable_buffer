defmodule DurableBuffer.Partition do
  @moduledoc """
  Per-partition group-commit writer.

  Appends are calls that do not reply immediately: entries accumulate in a
  pending batch and the first entry into an empty batch self-sends `:flush`.
  Every append the mailbox delivers before `:flush` joins the batch, so a
  single backend commit (one write + fsync / replicate / PUT) covers all
  concurrent callers, and each caller is replied to only once its data is
  durable. When the buffer is idle a lone append pays exactly one commit of
  latency. `max_batch_bytes` / `max_batch_entries` force an immediate flush
  under heavy load.
  """

  use GenServer

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

  @impl GenServer
  def init(opts) do
    {backend, config} = Keyword.fetch!(opts, :backend)
    partition_index = Keyword.fetch!(opts, :partition_index)
    {:ok, backend_state} = backend.open(config, partition_index)

    {:ok,
     %{
       backend: backend,
       backend_state: backend_state,
       pending: [],
       pending_bytes: 0,
       pending_count: 0,
       flush_scheduled?: false,
       async_error: nil,
       max_batch_bytes: Keyword.get(opts, :max_batch_bytes, 8 * 1024 * 1024),
       max_batch_entries: Keyword.get(opts, :max_batch_entries, 5000)
     }}
  end

  @impl GenServer
  def handle_call({:append, payload}, from, state) do
    {:noreply, enqueue(state, from, payload)}
  end

  def handle_call(:sync, _from, %{pending: []} = state) do
    {:reply, sync_reply(:ok, state), %{state | async_error: nil}}
  end

  def handle_call(:sync, from, state) do
    {reply, state} = flush(state)
    GenServer.reply(from, sync_reply(reply, state))
    {:noreply, %{state | async_error: nil}}
  end

  def handle_call(:truncate, _from, state) do
    {_reply, state} = if state.pending == [], do: {:ok, state}, else: flush(state)
    {:ok, backend_state} = state.backend.truncate(state.backend_state)
    {:reply, :ok, %{state | backend_state: backend_state, async_error: nil}}
  end

  @impl GenServer
  def handle_cast({:append, payload}, state) do
    {:noreply, enqueue(state, nil, payload)}
  end

  @impl GenServer
  def handle_info(:flush, %{pending: []} = state) do
    {:noreply, %{state | flush_scheduled?: false}}
  end

  def handle_info(:flush, state) do
    {_reply, state} = flush(%{state | flush_scheduled?: false})
    {:noreply, state}
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    state.backend.close(state.backend_state)
  end

  defp enqueue(state, from, payload) do
    {entry, entry_size} = WAL.encode(payload)

    state = %{
      state
      | pending: [{from, entry} | state.pending],
        pending_bytes: state.pending_bytes + entry_size,
        pending_count: state.pending_count + 1
    }

    cond do
      state.pending_bytes >= state.max_batch_bytes or
          state.pending_count >= state.max_batch_entries ->
        {_reply, state} = flush(state)
        state

      state.flush_scheduled? ->
        state

      true ->
        send(self(), :flush)
        %{state | flush_scheduled?: true}
    end
  end

  defp flush(state) do
    pending = Enum.reverse(state.pending)
    batch = Enum.map(pending, fn {_from, entry} -> entry end)

    {reply, backend_state} =
      case state.backend.commit(state.backend_state, batch, state.pending_bytes) do
        {:ok, backend_state} -> {:ok, backend_state}
        {:error, reason, backend_state} -> {{:error, reason}, backend_state}
      end

    Enum.each(pending, fn
      {nil, _entry} -> :ok
      {from, _entry} -> GenServer.reply(from, reply)
    end)

    {reply,
     %{
       state
       | backend_state: backend_state,
         pending: [],
         pending_bytes: 0,
         pending_count: 0,
         async_error: async_error(reply, pending, state.async_error)
     }}
  end

  defp async_error({:error, reason}, pending, previous) do
    if Enum.any?(pending, fn {from, _entry} -> from == nil end) do
      reason
    else
      previous
    end
  end

  defp async_error(:ok, _pending, previous), do: previous

  defp sync_reply(reply, %{async_error: nil}), do: reply
  defp sync_reply(:ok, state), do: {:error, state.async_error}
  defp sync_reply(error, _state), do: error
end
