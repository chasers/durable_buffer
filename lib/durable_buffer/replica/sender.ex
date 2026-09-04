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
  `{epoch, offset}` and reconciles against the primary's own WAL, not
  against its unacked queue: a replica holding a prefix of the primary WAL
  resumes live traffic, and anything it already has is dropped from the
  queue and counted as the watermark it is; a replica that is behind gets
  the missing suffix streamed straight from the primary's WAL file in chunks
  first; a replica on an older epoch (it missed a truncate) is truncated and
  re-replicated from offset zero. A replica *ahead of the primary* is
  truncated too, but that is a last resort and it is logged:
  `DurableBuffer.Backend.Replica.open/2` heals the primary from such a
  replica before the partition serves, so a sender only sees one when the
  replica was unreachable at open. Catch-up therefore needs no separate
  bookkeeping: the WAL is the queue, and every failure — writer death,
  rejected batch, unacked-queue overflow, ack stall — heals by
  re-attaching.

  Unacked batches are kept in a bounded in-memory queue (`:max_sender_bytes`)
  so the common reconnect case avoids re-reading the WAL; the writer
  re-acknowledges duplicates idempotently, so re-sends are safe.

  Every attach mints a reference that stamps the batches it sends, and the
  writer echoes it on each ack. Acks carrying any other reference are
  dropped. Without that, an ack still in flight when the sender re-attaches
  and truncates the replica would be applied afterwards, and the primary
  would count a replica as durable through an offset it no longer holds.
  """

  use GenServer

  require Logger

  alias DurableBuffer.Meta

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
  Clears the queue, adopts `epoch` with an empty primary WAL, and re-attaches
  at once. Called after a truncate, once the pipeline is drained.

  The re-attach is what makes a truncate converge. `Backend.Replica.truncate/1`
  sends each replica an `:erpc` truncate that may fail, and a replica that
  misses it keeps the old epoch and its pre-truncate data. Re-attaching makes
  the sender compare epochs immediately, truncate the replica itself, and
  keep retrying on its reconnect timer until the replica confirms — rather
  than waiting for the next commit to be rejected.
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
       primary_base: 0,
       epoch: Keyword.fetch!(opts, :epoch),
       rpc_timeout: Keyword.fetch!(opts, :rpc_timeout),
       max_bytes: Keyword.fetch!(opts, :max_bytes),
       fsync: Keyword.get(opts, :fsync, false),
       writer: nil,
       monitor: nil,
       attach_ref: nil,
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
    state = %{
      state
      | epoch: epoch,
        primary_tail: 0,
        primary_base: 0,
        queue: :queue.new(),
        queued_bytes: 0,
        watermark: {0, 0}
    }

    {:reply, :ok, reattach(state, 0)}
  end

  @impl GenServer
  def handle_info(:connect, %{writer: nil} = state) do
    case attach(state) do
      {:ok, writer, remote_tail} ->
        monitor = Process.monitor(writer)

        state = %{
          state
          | writer: writer,
            monitor: monitor,
            attach_ref: make_ref(),
            primary_base: primary_base(state)
        }

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
        case :file.pread(
               state.resync_fd,
               cursor - state.primary_base,
               min(@resync_chunk_bytes, target - cursor)
             ) do
          {:ok, data} when byte_size(data) > 0 ->
            send(state.writer, {:replicate, state.attach_ref, state.epoch, cursor, data, self()})
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

  def handle_info({:replica_ack, ref, watermark}, %{attach_ref: ref} = state) do
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

  def handle_info(
        {:replica_nack, ref, {:sequence_mismatch, %{got: {epoch, _offset}}} = reason},
        %{attach_ref: ref} = state
      )
      when epoch >= state.epoch do
    Logger.warning(
      "DurableBuffer replica sender to #{inspect(state.node)} " <>
        "(#{state.dir} p#{state.partition_index}) rejected: #{inspect(reason)}; resyncing"
    )

    {:noreply, force_reattach(state)}
  end

  def handle_info(
        {:replica_nack, ref, {:sequence_mismatch, _details}},
        %{attach_ref: ref} = state
      ) do
    {:noreply, state}
  end

  def handle_info({:replica_nack, ref, reason}, %{attach_ref: ref} = state) do
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
    cond do
      remote_epoch != state.epoch ->
        wipe_and_resync(state)

      remote_offset > state.primary_tail ->
        Logger.warning(
          "DurableBuffer replica sender to #{inspect(state.node)} " <>
            "(#{state.dir} p#{state.partition_index}) found the replica " <>
            "#{remote_offset - state.primary_tail} bytes ahead of the primary and " <>
            "is discarding them. The primary heals at open, so reaching this means " <>
            "the replica was unreachable then."
        )

        wipe_and_resync(state)

      remote_offset < state.primary_base ->
        Logger.info(
          "DurableBuffer replica sender to #{inspect(state.node)} " <>
            "(#{state.dir} p#{state.partition_index}) is below the primary's trimmed " <>
            "base; discarding it and resyncing from offset #{state.primary_base}"
        )

        wipe_and_resync(state)

      remote_offset < next_needed(state) ->
        state |> note_adopted() |> start_resync(remote_offset)

      true ->
        state = state |> note_adopted() |> adopt_remote_tail(remote_tail)
        state = resend_queue(%{state | mode: :live})
        ensure_progress_check(state)
    end
  end

  defp adopt_remote_tail(state, watermark) do
    send(state.owner, {:backend, {:watermark, state.node, watermark}})
    {queue, queued_bytes} = drop_acked(state.queue, state.queued_bytes, watermark)

    %{
      state
      | watermark: max(state.watermark, watermark),
        queue: queue,
        queued_bytes: queued_bytes
    }
  end

  defp wipe_and_resync(state) do
    case truncate_remote(state) do
      :ok -> state |> note_adopted() |> start_resync(state.primary_base)
      :error -> force_reattach(state)
    end
  end

  defp primary_base(state) do
    Meta.load(state.primary_dir, state.partition_index).base_byte_offset
  end

  defp note_adopted(state) do
    send(state.owner, {:backend, {:adopted, state.node, state.epoch}})
    state
  end

  defp next_needed(state) do
    case :queue.peek(state.queue) do
      {:value, {_epoch, offset, _binary}} -> offset
      :empty -> state.primary_tail
    end
  end

  defp start_resync(state, cursor) do
    path = DurableBuffer.Backend.Local.wal_path(state.primary_dir, state.partition_index)
    base = primary_base(state)
    cursor = max(cursor, base)
    state = %{state | primary_base: base}

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
      [state.dir, state.partition_index, state.epoch, state.primary_base],
      state.rpc_timeout
    )
  catch
    _kind, _reason -> :error
  end

  defp force_reattach(state), do: reattach(state, @connect_retry_ms)

  defp reattach(state, delay) do
    if state.monitor, do: Process.demonitor(state.monitor, [:flush])
    state = close_resync(state)
    Process.send_after(self(), :connect, delay)
    %{state | writer: nil, monitor: nil, attach_ref: nil, mode: :live}
  end

  defp close_resync(%{resync_fd: nil} = state), do: state

  defp close_resync(state) do
    :ok = :file.close(state.resync_fd)
    %{state | resync_fd: nil, resync_cursor: nil}
  end

  defp send_entry(%{writer: nil} = state, _entry), do: state

  defp send_entry(state, {epoch, offset, binary}) do
    send(state.writer, {:replicate, state.attach_ref, epoch, offset, binary, self()})
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
