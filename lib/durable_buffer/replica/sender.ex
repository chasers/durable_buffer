defmodule DurableBuffer.Replica.Sender do
  @moduledoc """
  Primary-side replication channel to one replica node for one partition.

  Owns a long-lived, ordered channel to the remote
  `DurableBuffer.Replica.Writer` and pipelines batches over it without
  waiting for acks, so a slow or dead replica never blocks the commit path —
  it only stalls its own channel (sends block this process when the
  distribution buffer to the replica is full, which is the intended
  backpressure). Acks come back as durability watermarks and are forwarded
  to the owner as `{:backend, {:watermark, node, watermark}}` messages.

  On every (re)connect the sender attaches with the remote writer's tail
  `{epoch, offset}` and reconciles: a replica already at the expected
  position resumes live traffic; a replica that is behind gets the missing
  suffix streamed straight from the primary's WAL file in chunks before
  live traffic resumes; a replica on an older epoch (it missed a truncate)
  or ahead of the primary (it holds bytes the primary lost) is truncated
  and re-replicated from offset zero. Catch-up therefore needs no separate
  bookkeeping: the WAL is the queue, and every failure — writer death,
  rejected batch, unacked-queue overflow, ack stall — heals by
  re-attaching.

  Unacked batches are kept in a bounded in-memory queue (`:max_sender_bytes`)
  so the common reconnect case avoids re-reading the WAL; the writer
  re-acknowledges duplicates idempotently, so re-sends are safe.
  """

  use GenServer

  require Logger

  @connect_retry_ms 1000
  @resync_chunk_bytes 1024 * 1024

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Enqueues a batch for replication. The batch must already be written to
  the primary WAL. Never blocks the caller.
  """
  @spec commit(GenServer.server(), non_neg_integer(), non_neg_integer(), binary()) :: :ok
  def commit(sender, epoch, offset, binary) do
    GenServer.cast(sender, {:commit, epoch, offset, binary})
  end

  @doc """
  Clears the queue and adopts `epoch` with an empty primary WAL. Called
  after a truncate, once the pipeline is drained.
  """
  @spec reset(GenServer.server(), non_neg_integer()) :: :ok
  def reset(sender, epoch) do
    GenServer.call(sender, {:reset, epoch}, :infinity)
  end

  @spec stop(GenServer.server()) :: :ok
  def stop(sender) do
    GenServer.stop(sender)
  end

  @impl GenServer
  def init(opts) do
    send(self(), :connect)

    {:ok,
     %{
       owner: Keyword.fetch!(opts, :owner),
       node: Keyword.fetch!(opts, :node),
       dir: Keyword.fetch!(opts, :dir),
       partition_index: Keyword.fetch!(opts, :partition_index),
       primary_dir: Keyword.fetch!(opts, :primary_dir),
       primary_tail: Keyword.fetch!(opts, :primary_tail),
       epoch: Keyword.fetch!(opts, :epoch),
       rpc_timeout: Keyword.fetch!(opts, :rpc_timeout),
       max_bytes: Keyword.fetch!(opts, :max_bytes),
       fsync: Keyword.get(opts, :fsync, false),
       writer: nil,
       monitor: nil,
       mode: :live,
       resync_fd: nil,
       resync_cursor: nil,
       queue: :queue.new(),
       queued_bytes: 0,
       watermark: {0, 0},
       progress_check: nil
     }}
  end

  @impl GenServer
  def handle_cast({:commit, epoch, offset, binary}, state) do
    state = %{state | epoch: epoch, primary_tail: offset + byte_size(binary)}

    cond do
      state.queued_bytes + byte_size(binary) > state.max_bytes ->
        state = %{state | queue: :queue.new(), queued_bytes: 0}
        {:noreply, if(state.mode == :resync, do: state, else: force_reattach(state))}

      true ->
        entry = {epoch, offset, binary}

        state = %{
          state
          | queue: :queue.in(entry, state.queue),
            queued_bytes: state.queued_bytes + byte_size(binary)
        }

        state =
          if state.mode == :live do
            state |> send_entry(entry) |> ensure_progress_check()
          else
            state
          end

        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_call({:reset, epoch}, _from, state) do
    state = close_resync(state)

    {:reply, :ok,
     %{
       state
       | epoch: epoch,
         primary_tail: 0,
         queue: :queue.new(),
         queued_bytes: 0,
         mode: :live
     }}
  end

  @impl GenServer
  def handle_info(:connect, %{writer: nil} = state) do
    case attach(state) do
      {:ok, writer, remote_tail} ->
        monitor = Process.monitor(writer)
        state = %{state | writer: writer, monitor: monitor}
        {:noreply, reconcile(state, remote_tail)}

      :error ->
        Process.send_after(self(), :connect, @connect_retry_ms)
        {:noreply, state}
    end
  end

  def handle_info(:connect, state) do
    {:noreply, state}
  end

  def handle_info(:resync_step, %{mode: :resync, writer: writer} = state) when writer != nil do
    target = next_needed(state)
    cursor = state.resync_cursor

    cond do
      cursor >= target ->
        state = close_resync(state)
        state = resend_queue(%{state | mode: :live})
        {:noreply, ensure_progress_check(state)}

      true ->
        case :file.pread(state.resync_fd, cursor, min(@resync_chunk_bytes, target - cursor)) do
          {:ok, data} when byte_size(data) > 0 ->
            send(state.writer, {:replicate, state.epoch, cursor, data, self()})
            send(self(), :resync_step)
            {:noreply, %{state | resync_cursor: cursor + byte_size(data)}}

          _eof_or_error ->
            {:noreply, force_reattach(state)}
        end
    end
  end

  def handle_info(:resync_step, state) do
    {:noreply, state}
  end

  def handle_info({:replica_ack, watermark}, state) do
    send(state.owner, {:backend, {:watermark, state.node, watermark}})
    {queue, queued_bytes} = drop_acked(state.queue, state.queued_bytes, watermark)

    state = %{
      state
      | watermark: max(state.watermark, watermark),
        queue: queue,
        queued_bytes: queued_bytes
    }

    {:noreply, state}
  end

  def handle_info({:replica_nack, {:sequence_mismatch, %{got: {epoch, _offset}}} = reason}, state)
      when epoch >= state.epoch do
    Logger.warning(
      "DurableBuffer replica sender to #{inspect(state.node)} " <>
        "(#{state.dir} p#{state.partition_index}) rejected: #{inspect(reason)}; resyncing"
    )

    {:noreply, force_reattach(state)}
  end

  def handle_info({:replica_nack, {:sequence_mismatch, _details}}, state) do
    {:noreply, state}
  end

  def handle_info({:replica_nack, reason}, state) do
    Logger.warning(
      "DurableBuffer replica sender to #{inspect(state.node)} " <>
        "(#{state.dir} p#{state.partition_index}) commit failed: #{inspect(reason)}; retrying"
    )

    {:noreply, force_reattach(state)}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{monitor: monitor} = state) do
    {:noreply, force_reattach(%{state | monitor: nil})}
  end

  def handle_info({:check_progress, watermark_at_send}, state) do
    state = %{state | progress_check: nil}

    if not :queue.is_empty(state.queue) and state.watermark == watermark_at_send and
         state.mode == :live and state.writer != nil do
      {:noreply, force_reattach(state)}
    else
      {:noreply, ensure_progress_check(state)}
    end
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  defp attach(state) do
    {pid, remote_tail} =
      :erpc.call(
        state.node,
        DurableBuffer.Replica,
        :attach,
        [state.dir, state.partition_index, state.fsync],
        state.rpc_timeout
      )

    {:ok, pid, remote_tail}
  catch
    _kind, _reason -> :error
  end

  defp reconcile(state, {remote_epoch, remote_offset} = remote_tail) do
    expected = {state.epoch, next_needed(state)}

    cond do
      remote_tail == expected ->
        state = resend_queue(%{state | mode: :live})
        ensure_progress_check(state)

      remote_epoch == state.epoch and remote_offset < next_needed(state) ->
        start_resync(state, remote_offset)

      true ->
        case truncate_remote(state) do
          :ok -> start_resync(state, 0)
          :error -> force_reattach(state)
        end
    end
  end

  defp next_needed(state) do
    case :queue.peek(state.queue) do
      {:value, {_epoch, offset, _binary}} -> offset
      :empty -> state.primary_tail
    end
  end

  defp start_resync(state, cursor) do
    path = DurableBuffer.Backend.Local.wal_path(state.primary_dir, state.partition_index)

    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        Logger.info(
          "DurableBuffer replica sender resyncing #{inspect(state.node)} " <>
            "(#{state.dir} p#{state.partition_index}) from offset #{cursor}"
        )

        send(self(), :resync_step)
        %{state | mode: :resync, resync_fd: fd, resync_cursor: cursor}

      {:error, :enoent} when cursor == 0 ->
        state = resend_queue(%{state | mode: :live})
        ensure_progress_check(state)

      {:error, _reason} ->
        force_reattach(state)
    end
  end

  defp truncate_remote(state) do
    :erpc.call(
      state.node,
      DurableBuffer.Replica,
      :truncate,
      [state.dir, state.partition_index, state.epoch],
      state.rpc_timeout
    )
  catch
    _kind, _reason -> :error
  end

  defp force_reattach(state) do
    if state.monitor, do: Process.demonitor(state.monitor, [:flush])
    state = close_resync(state)
    Process.send_after(self(), :connect, @connect_retry_ms)
    %{state | writer: nil, monitor: nil, mode: :live}
  end

  defp close_resync(%{resync_fd: nil} = state), do: state

  defp close_resync(state) do
    :ok = :file.close(state.resync_fd)
    %{state | resync_fd: nil, resync_cursor: nil}
  end

  defp send_entry(%{writer: nil} = state, _entry), do: state

  defp send_entry(state, {epoch, offset, binary}) do
    send(state.writer, {:replicate, epoch, offset, binary, self()})
    state
  end

  defp resend_queue(state) do
    Enum.reduce(:queue.to_list(state.queue), state, &send_entry(&2, &1))
  end

  defp drop_acked(queue, queued_bytes, watermark) do
    case :queue.out(queue) do
      {{:value, {epoch, offset, binary}}, rest} ->
        if {epoch, offset + byte_size(binary)} <= watermark do
          drop_acked(rest, queued_bytes - byte_size(binary), watermark)
        else
          {queue, queued_bytes}
        end

      {:empty, _rest} ->
        {queue, queued_bytes}
    end
  end

  defp ensure_progress_check(%{progress_check: nil} = state) do
    if :queue.is_empty(state.queue) do
      state
    else
      ref = Process.send_after(self(), {:check_progress, state.watermark}, state.rpc_timeout)

      %{state | progress_check: ref}
    end
  end

  defp ensure_progress_check(state), do: state
end
