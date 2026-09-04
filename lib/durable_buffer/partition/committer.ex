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

  The committer also assigns every entry a logical offset. Offsets are
  handed out in commit-submission order, which is also caller-reply order,
  so the offset an append returns is the entry's true log position. The
  counter is seeded from the backend at open and after every truncate.

  After every commit, completion and truncate it publishes four numbers into
  the buffer's `:atomics` array, in the four slots for its partition: the
  durable byte offset, the durable logical offset, the next logical offset,
  and the base offset. `DurableBuffer.stream/3` and `offsets/2` read those
  slots without sending the committer a message.
  """

  use GenServer

  require Logger

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
  Drops every entry below `upto`, or applies the retention policy when
  `upto` is `:policy`. Ordered against commits and truncates like every
  other unit of work here, so a trim takes its turn rather than pre-empting
  a commit.

  A `nil` `from` is the partition's own retention timer, which has nobody to
  answer. Failures are logged instead of returned.
  """
  @spec request_trim(GenServer.server(), GenServer.from() | nil, non_neg_integer() | :policy) ::
          :ok
  def request_trim(server, from, upto) do
    GenServer.cast(server, {:trim, from, upto})
  end

  @doc """
  Reports what retention has to work with, for backends that can say.
  Returns `{:error, :unsupported}` for the others.
  """
  @spec request_retention_status(GenServer.server(), GenServer.from()) :: :ok
  def request_retention_status(server, from) do
    GenServer.cast(server, {:retention_status, from})
  end

  @doc """
  Reports the backend's per-replica replication state, for backends that
  track one. Returns `{:error, :unsupported}` for the others.
  """
  @spec request_replica_status(GenServer.server(), GenServer.from()) :: :ok
  def request_replica_status(server, from) do
    GenServer.cast(server, {:replica_status, from})
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
      retention: Keyword.get(opts, :retention, %{ms: nil, bytes: nil}),
      publishes_offset?: Keyword.get(opts, :durable_offsets) != nil,
      next_offset: seed_next_offset(backend, backend_state),
      durable_logical: seed_next_offset(backend, backend_state),
      base_offset: seed_base_offset(backend, backend_state),
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
    assigned_from = state.next_offset
    {units, next_offset} = assign_offsets(units, assigned_from)
    state = %{state | next_offset: next_offset}
    batch = for {:entries, _from, entries, _count, _shape, _first} <- units, do: entries
    tag = make_ref()

    state =
      case state.backend.commit_async(
             state.backend_state,
             batch,
             byte_size,
             {assigned_from, next_offset - assigned_from},
             tag
           ) do
        {:done, result, backend_state} ->
          %{
            state
            | backend_state: backend_state,
              completed: Map.put(state.completed, tag, result),
              next_offset: rewind(result, assigned_from, next_offset)
          }

        {:pending, backend_state} ->
          %{state | backend_state: backend_state}
      end

    submitted_at = System.monotonic_time(:millisecond)

    state = %{
      state
      | pending: :queue.in({:commit, tag, units, submitted_at, next_offset}, state.pending)
    }

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
    assigned_from = state.next_offset
    {units, next_offset} = assign_offsets(units, assigned_from)
    state = %{state | next_offset: next_offset}
    batch = for {:entries, _from, entries, _count, _shape, _first} <- units, do: entries

    {reply, backend_state} =
      case state.backend.commit(
             state.backend_state,
             batch,
             byte_size,
             {assigned_from, next_offset - assigned_from}
           ) do
        {:ok, backend_state} -> {:ok, backend_state}
        {:error, reason, backend_state} -> {{:error, reason}, backend_state}
      end

    state = %{state | backend_state: backend_state}

    state =
      if reply == :ok do
        %{state | durable_logical: next_offset}
      else
        %{state | next_offset: rewind(reply, assigned_from, next_offset)}
      end

    state = reply_units(state, units, reply)
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

  def handle_cast({:retention_status, from}, state) do
    reply =
      if Backend.applies_retention?(state.backend) do
        {:ok, state.backend.retention_status(state.backend_state)}
      else
        {:error, :unsupported}
      end

    GenServer.reply(from, reply)
    {:noreply, state}
  end

  def handle_cast({:replica_status, from}, state) do
    reply =
      if function_exported?(state.backend, :status, 1) do
        {:ok, state.backend.status(state.backend_state)}
      else
        {:error, :unsupported}
      end

    GenServer.reply(from, reply)
    {:noreply, state}
  end

  def handle_cast({:trim, from, :policy}, state) do
    state = if function_exported?(state.backend, :trim, 2), do: drain(state), else: state

    cond do
      not Backend.applies_retention?(state.backend) or
          not function_exported?(state.backend, :trim, 2) ->
        answer(from, {:error, :unsupported})
        {:noreply, state}

      state.retention.ms == nil and state.retention.bytes == nil ->
        answer(from, {:error, :no_retention_policy})
        {:noreply, state}

      true ->
        case state.backend.retention_point(state.backend_state, state.retention) do
          :none ->
            answer(from, :ok)
            {:noreply, state}

          {:ok, upto} ->
            apply_trim(state, from, min(upto, state.durable_logical))
        end
    end
  end

  def handle_cast({:trim, from, upto}, state) do
    state = if function_exported?(state.backend, :trim, 2), do: drain(state), else: state

    cond do
      not function_exported?(state.backend, :trim, 2) ->
        answer(from, {:error, :unsupported})
        {:noreply, state}

      upto > state.durable_logical ->
        answer(from, {:error, :not_durable})
        {:noreply, state}

      true ->
        apply_trim(state, from, upto)
    end
  end

  def handle_cast({:truncate, from}, state) do
    state = drain(state)
    {:ok, backend_state} = state.backend.truncate(state.backend_state, state.next_offset)

    state =
      publish_offset(%{
        state
        | backend_state: backend_state,
          async_error: nil,
          next_offset: seed_next_offset(state.backend, backend_state),
          durable_logical: seed_next_offset(state.backend, backend_state),
          base_offset: seed_base_offset(state.backend, backend_state)
      })

    GenServer.reply(from, :ok)
    {:noreply, state}
  end

  defp apply_trim(state, from, upto) do
    {reply, backend_state} =
      case state.backend.trim(state.backend_state, upto) do
        {:ok, backend_state} -> {:ok, backend_state}
        {:error, reason, backend_state} -> {{:error, reason}, backend_state}
      end

    state =
      publish_offset(%{
        state
        | backend_state: backend_state,
          base_offset: seed_base_offset(state.backend, backend_state)
      })

    answer(from, reply)
    {:noreply, state}
  end

  defp answer(nil, :ok), do: :ok

  defp answer(nil, {:error, reason}) do
    Logger.warning("durable_buffer: automatic retention trim failed: #{inspect(reason)}")
  end

  defp answer(from, reply), do: GenServer.reply(from, reply)

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
    slot = state.partition_index * 4

    :atomics.put(state.durable_offsets, slot + 1, byte_durable_offset(state))
    :atomics.put(state.durable_offsets, slot + 2, state.durable_logical)
    :atomics.put(state.durable_offsets, slot + 3, state.next_offset)
    :atomics.put(state.durable_offsets, slot + 4, state.base_offset)

    state
  end

  defp byte_durable_offset(state) do
    if function_exported?(state.backend, :durable_offset, 1) do
      state.backend.durable_offset(state.backend_state)
    else
      0
    end
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
      {:value, {:commit, tag, units, submitted_at, logical_end}} ->
        case Map.pop(state.completed, tag) do
          {nil, _completed} ->
            state

          {result, completed} ->
            {_head, pending} = :queue.out(state.pending)
            state = %{state | pending: pending, completed: completed}
            state = if result == :ok, do: %{state | durable_logical: logical_end}, else: state

            state
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

  defp rewind({:error, _reason}, assigned_from, _next), do: assigned_from
  defp rewind(_ok, _assigned_from, next), do: next

  defp assign_offsets(units, next) do
    Enum.map_reduce(units, next, fn
      {:entries, from, entries, count, shape}, offset ->
        {{:entries, from, entries, count, shape, offset}, offset + count}

      other, offset ->
        {other, offset}
    end)
  end

  defp seed_next_offset(backend, backend_state) do
    if DurableBuffer.Backend.tracks_offsets?(backend) do
      backend.offsets(backend_state).next
    else
      0
    end
  end

  defp seed_base_offset(backend, backend_state) do
    if DurableBuffer.Backend.tracks_offsets?(backend) do
      backend.offsets(backend_state).first
    else
      0
    end
  end

  defp unit_reply({:error, _reason} = error, _shape, _first, _count), do: error
  defp unit_reply(:ok, :offset, first, _count), do: {:ok, first}
  defp unit_reply(:ok, :range, first, count), do: {:ok, first..(first + count - 1)}

  defp reply_units(state, units, reply) do
    state = publish_offset(state)

    Enum.each(units, fn
      {:entries, nil, _entries, _count, _shape, _first} ->
        :ok

      {:entries, from, _entries, count, shape, first} ->
        GenServer.reply(from, unit_reply(reply, shape, first, count))

      {:sync, _from} ->
        :ok
    end)

    state = %{state | async_error: async_error(reply, units, state.async_error)}
    reply_to_syncs(units, reply, state)
  end

  defp async_error({:error, reason}, units, previous) do
    if Enum.any?(units, &match?({:entries, nil, _entries, _count, _shape, _first}, &1)) do
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
