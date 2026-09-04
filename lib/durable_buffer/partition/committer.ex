defmodule DurableBuffer.Partition.Committer do
  @moduledoc """
  Owns a partition's backend state and executes its commits, so the
  `DurableBuffer.Partition` writer can keep draining its mailbox while a
  commit (fsync / replication / PUT) is in flight.

  Started by and linked to its writer. Work arrives as casts, which
  preserves order: entry batches are committed and their callers replied to,
  sync requests are answered after everything cast before them, and truncate
  clears the backend. Traps exits so the backend is closed when the writer
  goes down.

  Backends implementing the asynchronous commit contract
  (`DurableBuffer.Backend.commit_async/4` + `handle_message/2`) get
  pipelining: up to `:max_inflight_commits` batches are submitted before
  their predecessors settle, callers are replied to strictly in submission
  order, and the writer is credited with `:commit_done` at submission time
  while the pipeline has capacity — so batch formation overlaps the local
  write only, not the full replication round trip. Synchronous backends keep
  the one-commit-at-a-time path unchanged.

  The async path also tracks an exponential moving average of commit
  completion latency (submission to settle) and tells the writer how much
  batching would pay via `{:dwell_hint, ms}` messages — 0 below 2 ms, 1 ms
  from 2 ms, 2 ms from 4 ms — sent only when the recommendation changes.
  The writer uses the hint as its adaptive flush dwell.

  After every commit, completion and truncate the committer publishes the
  backend's durable byte offset into the buffer's `:atomics` array, at the
  slot for its partition. `DurableBuffer.stream/3` reads that slot to gate
  reads without sending the committer a message.
  """

  use GenServer

  alias DurableBuffer.Backend

  def start_link(backend, config, partition_index, opts \\ []) do
    GenServer.start_link(__MODULE__, {backend, config, partition_index, opts, self()})
  end

  @doc """
  Hands a batch of pending units over for commit. Units are
  `{:entries, from | nil, entries_iodata}` and `{:sync, from}` in arrival
  order; the committer replies to every `from` once the batch is durable and
  then notifies the writer with `:commit_done`.
  """
  @spec commit(GenServer.server(), [tuple()], non_neg_integer()) :: :ok
  def commit(server, units, byte_size) do
    GenServer.cast(server, {:commit, units, byte_size})
  end

  @spec request_sync(GenServer.server(), GenServer.from()) :: :ok
  def request_sync(server, from) do
    GenServer.cast(server, {:sync, from})
  end

  @spec request_truncate(GenServer.server(), GenServer.from()) :: :ok
  def request_truncate(server, from) do
    GenServer.cast(server, {:truncate, from})
  end

  @doc """
  Reports the backend's per-replica replication state, for backends that
  track one. Returns `{:error, :unsupported}` for the others.
  """
  @spec replica_status(GenServer.server(), timeout()) :: {:ok, map()} | {:error, term()}
  def replica_status(server, timeout \\ 5_000) do
    GenServer.call(server, :replica_status, timeout)
  end

  @impl GenServer
  def handle_call(:replica_status, _from, state) do
    reply =
      if function_exported?(state.backend, :status, 1) do
        {:ok, state.backend.status(state.backend_state)}
      else
        {:error, :unsupported}
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def init({backend, config, partition_index, opts, writer}) do
    Process.flag(:trap_exit, true)
    {:ok, backend_state} = backend.open(config, partition_index)

    state = %{
      backend: backend,
      backend_state: backend_state,
      writer: writer,
      async_error: nil,
      async?: Backend.async?(backend),
      max_inflight: Keyword.get(opts, :max_inflight_commits, 32),
      partition_index: partition_index,
      durable_offsets: Keyword.get(opts, :durable_offsets),
      publishes_offset?:
        Keyword.get(opts, :durable_offsets) != nil and
          function_exported?(backend, :durable_offset, 1),
      pending: :queue.new(),
      completed: %{},
      deferred_credits: 0,
      completion_ewma_ms: 0.0,
      dwell_hint: 0
    }

    {:ok, publish_offset(state)}
  end

  @impl GenServer
  def handle_cast({:commit, units, byte_size}, %{async?: true} = state) do
    batch = for {:entries, _from, entries} <- units, do: entries
    tag = make_ref()

    state =
      case state.backend.commit_async(state.backend_state, batch, byte_size, tag) do
        {:done, result, backend_state} ->
          %{
            state
            | backend_state: backend_state,
              completed: Map.put(state.completed, tag, result)
          }

        {:pending, backend_state} ->
          %{state | backend_state: backend_state}
      end

    submitted_at = System.monotonic_time(:millisecond)
    state = %{state | pending: :queue.in({:commit, tag, units, submitted_at}, state.pending)}

    state =
      if :queue.len(state.pending) <= state.max_inflight do
        send(state.writer, :commit_done)
        state
      else
        %{state | deferred_credits: state.deferred_credits + 1}
      end

    {:noreply, publish_offset(flush(state))}
  end

  def handle_cast({:commit, units, byte_size}, state) do
    batch = for {:entries, _from, entries} <- units, do: entries

    {reply, backend_state} =
      case state.backend.commit(state.backend_state, batch, byte_size) do
        {:ok, backend_state} -> {:ok, backend_state}
        {:error, reason, backend_state} -> {{:error, reason}, backend_state}
      end

    state = reply_units(%{state | backend_state: backend_state}, units, reply)
    send(state.writer, :commit_done)
    {:noreply, publish_offset(state)}
  end

  def handle_cast({:sync, from}, %{async?: true} = state) do
    if :queue.is_empty(state.pending) do
      GenServer.reply(from, sync_reply(:ok, state))
      {:noreply, %{state | async_error: nil}}
    else
      {:noreply, %{state | pending: :queue.in({:sync_mark, from}, state.pending)}}
    end
  end

  def handle_cast({:sync, from}, state) do
    GenServer.reply(from, sync_reply(:ok, state))
    {:noreply, %{state | async_error: nil}}
  end

  def handle_cast({:truncate, from}, state) do
    state = drain(state)
    {:ok, backend_state} = state.backend.truncate(state.backend_state)
    GenServer.reply(from, :ok)
    {:noreply, publish_offset(%{state | backend_state: backend_state, async_error: nil})}
  end

  @impl GenServer
  def handle_info({:backend, message}, state) do
    {:noreply, publish_offset(handle_backend_message(state, message))}
  end

  def handle_info({:EXIT, pid, reason}, %{writer: pid} = state) do
    {:stop, reason, state}
  end

  def handle_info({:EXIT, _pid, :normal}, state) do
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    state.backend.close(state.backend_state)
  end

  defp publish_offset(%{publishes_offset?: false} = state), do: state

  defp publish_offset(state) do
    :atomics.put(
      state.durable_offsets,
      state.partition_index + 1,
      state.backend.durable_offset(state.backend_state)
    )

    state
  end

  defp handle_backend_message(state, message) do
    {completions, backend_state} =
      state.backend.handle_message(message, state.backend_state)

    %{
      state
      | backend_state: backend_state,
        completed: Enum.into(completions, state.completed)
    }
    |> flush()
  end

  defp flush(state) do
    case :queue.peek(state.pending) do
      {:value, {:commit, tag, units, submitted_at}} ->
        case Map.pop(state.completed, tag) do
          {nil, _completed} ->
            state

          {result, completed} ->
            {_head, pending} = :queue.out(state.pending)

            %{state | pending: pending, completed: completed}
            |> observe_completion(submitted_at)
            |> reply_units(units, result)
            |> release_credit()
            |> flush()
        end

      {:value, {:sync_mark, from}} ->
        GenServer.reply(from, sync_reply(:ok, state))
        {_head, pending} = :queue.out(state.pending)
        flush(%{state | pending: pending, async_error: nil})

      :empty ->
        state
    end
  end

  defp release_credit(%{deferred_credits: 0} = state), do: state

  defp release_credit(state) do
    send(state.writer, :commit_done)
    %{state | deferred_credits: state.deferred_credits - 1}
  end

  defp observe_completion(state, submitted_at) do
    latency = System.monotonic_time(:millisecond) - submitted_at
    ewma = state.completion_ewma_ms * 0.8 + latency * 0.2

    hint =
      cond do
        ewma >= 4 -> 2
        ewma >= 2 -> 1
        true -> 0
      end

    if hint != state.dwell_hint do
      send(state.writer, {:dwell_hint, hint})
    end

    %{state | completion_ewma_ms: ewma, dwell_hint: hint}
  end

  defp drain(state) do
    if :queue.is_empty(state.pending) do
      state
    else
      receive do
        {:backend, message} -> state |> handle_backend_message(message) |> drain()
      end
    end
  end

  defp reply_units(state, units, reply) do
    Enum.each(units, fn
      {:entries, nil, _entries} -> :ok
      {:entries, from, _entries} -> GenServer.reply(from, reply)
      {:sync, _from} -> :ok
    end)

    state = %{state | async_error: async_error(reply, units, state.async_error)}
    reply_to_syncs(units, reply, state)
  end

  defp async_error({:error, reason}, units, previous) do
    if Enum.any?(units, &match?({:entries, nil, _entries}, &1)) do
      reason
    else
      previous
    end
  end

  defp async_error(:ok, _units, previous), do: previous

  defp reply_to_syncs(units, reply, state) do
    syncs = for {:sync, from} <- units, do: from

    if syncs == [] do
      state
    else
      Enum.each(syncs, &GenServer.reply(&1, sync_reply(reply, state)))
      %{state | async_error: nil}
    end
  end

  defp sync_reply(reply, %{async_error: nil}), do: reply
  defp sync_reply(:ok, state), do: {:error, state.async_error}
  defp sync_reply(error, _state), do: error
end
