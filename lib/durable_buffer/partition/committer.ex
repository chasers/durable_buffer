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
  """

  use GenServer

  def start_link(backend, config, partition_index) do
    GenServer.start_link(__MODULE__, {backend, config, partition_index, self()})
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

  @impl GenServer
  def init({backend, config, partition_index, writer}) do
    Process.flag(:trap_exit, true)
    {:ok, backend_state} = backend.open(config, partition_index)
    {:ok, %{backend: backend, backend_state: backend_state, writer: writer, async_error: nil}}
  end

  @impl GenServer
  def handle_cast({:commit, units, byte_size}, state) do
    batch = for {:entries, _from, entries} <- units, do: entries

    {reply, backend_state} =
      case state.backend.commit(state.backend_state, batch, byte_size) do
        {:ok, backend_state} -> {:ok, backend_state}
        {:error, reason, backend_state} -> {{:error, reason}, backend_state}
      end

    state = %{
      state
      | backend_state: backend_state,
        async_error: async_error(reply, units, state.async_error)
    }

    Enum.each(units, fn
      {:entries, nil, _entries} -> :ok
      {:entries, from, _entries} -> GenServer.reply(from, reply)
      {:sync, _from} -> :ok
    end)

    state = reply_to_syncs(units, reply, state)

    send(state.writer, :commit_done)
    {:noreply, state}
  end

  def handle_cast({:sync, from}, state) do
    GenServer.reply(from, sync_reply(:ok, state))
    {:noreply, %{state | async_error: nil}}
  end

  def handle_cast({:truncate, from}, state) do
    {:ok, backend_state} = state.backend.truncate(state.backend_state)
    GenServer.reply(from, :ok)
    {:noreply, %{state | backend_state: backend_state, async_error: nil}}
  end

  @impl GenServer
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
