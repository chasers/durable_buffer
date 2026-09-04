defmodule DurableBuffer.Replica.Writer do
  @moduledoc """
  Replica-side WAL writer for one `{dir, partition_index}`.

  Holds an open raw fd and performs one write (+ `datasync`, when started
  with `fsync: true`) per replicated batch. Batches arrive already
  group-committed by the primary and stamped
  with `{epoch, offset}` — the primary's epoch and the WAL byte offset at
  which the batch starts. A batch is appended only when it lands exactly at
  this writer's tail (same epoch, offset equal to the local WAL size). A
  batch the writer already has (same epoch, ends at or before the tail) is
  re-acknowledged idempotently, so the primary's sender can re-send unacked
  batches after a reconnect. Anything else is rejected, so a replica that
  missed a batch or a truncate refuses to diverge silently instead of
  appending after a gap.

  Batches arrive either as synchronous calls (`commit/4`, used by `:erpc`)
  or as pipelined `{:replicate, epoch, offset, batch, from}` messages from a
  `DurableBuffer.Replica.Sender`, answered asynchronously with
  `{:replica_ack, ref, watermark}` / `{:replica_nack, ref, reason}` sent to
  `from`. The `ref` is the sender's attach reference, echoed back untouched
  so the sender can drop an ack that belongs to an earlier attach. A
  successful append acknowledges with the writer's new durability watermark
  `{epoch, offset}` — everything up to `offset` in `epoch` is on disk here.

  Pipelined batches group-commit: every `:replicate` message queued in the
  mailbox is drained, the contiguous run is appended with a single write +
  `datasync`, and one ack carrying the final watermark covers them all — so
  a deep pipeline of small batches costs one fsync, not one per batch.

  The epoch is persisted next to the WAL (see `DurableBuffer.Meta`) and
  adopted from the primary on truncate.
  """

  use GenServer

  alias DurableBuffer.Backend.Local
  alias DurableBuffer.Meta

  @mirrored {0, 0}

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Returns the via-tuple name for the writer of `{dir, partition_index}`.
  """
  @spec name(Path.t(), non_neg_integer()) :: GenServer.name()
  def name(dir, partition_index) do
    {:via, Registry, {DurableBuffer.Registry, {:replica_writer, dir, partition_index}}}
  end

  @spec commit(GenServer.server(), non_neg_integer(), non_neg_integer(), binary()) ::
          {:ok, DurableBuffer.Backend.Replica.watermark()} | {:error, term()}
  def commit(server, epoch, offset, batch) do
    GenServer.call(server, {:commit, epoch, offset, batch}, :infinity)
  end

  @spec truncate(GenServer.server(), non_neg_integer(), non_neg_integer()) :: :ok
  def truncate(server, epoch, base_byte) do
    GenServer.call(server, {:truncate, epoch, base_byte}, :infinity)
  end

  @doc """
  Drops every byte below the logical byte offset `base_byte`, passing on a
  trim the primary already applied.
  """
  @spec trim(GenServer.server(), non_neg_integer()) :: :ok
  def trim(server, base_byte) do
    GenServer.call(server, {:trim, base_byte}, :infinity)
  end

  @doc """
  Reads `length` bytes of this writer's WAL starting at `offset`.

  Serves the primary's heal path, so it is answered in order against the
  writer's own appends.
  """
  @spec read_range(GenServer.server(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_range(server, offset, length) do
    GenServer.call(server, {:read_range, offset, length}, :infinity)
  end

  @doc """
  Returns this writer's tail `{epoch, offset}` — where the next batch must
  land.
  """
  @spec tail(GenServer.server()) :: DurableBuffer.Backend.Replica.watermark()
  def tail(server) do
    GenServer.call(server, :tail, :infinity)
  end

  @impl GenServer
  def init(opts) do
    dir = Keyword.fetch!(opts, :dir)
    partition_index = Keyword.fetch!(opts, :partition_index)
    config = Local.init_config(dir: dir, fsync: Keyword.get(opts, :fsync, true), index: false)
    {:ok, local} = Local.open(config, partition_index)

    {:ok,
     %{
       local: local,
       dir: dir,
       partition_index: partition_index,
       epoch: Meta.epoch(dir, partition_index)
     }}
  end

  @impl GenServer
  def handle_call({:commit, epoch, offset, batch}, _from, state) do
    {reply, state} = do_commit(state, epoch, offset, batch)
    {:reply, reply, state}
  end

  def handle_call({:truncate, epoch, base_byte}, _from, state) do
    {:ok, local} = Local.truncate(state.local, Local.offsets(state.local).next)
    {:ok, local} = Local.reset_to(local, base_byte)
    Meta.update!(state.dir, state.partition_index, &%{&1 | epoch: epoch})
    {:reply, :ok, %{state | local: local, epoch: epoch}}
  end

  def handle_call({:trim, base_byte}, _from, state) do
    case Local.trim_bytes(state.local, base_byte) do
      {:ok, local} -> {:reply, :ok, %{state | local: local}}
      {:error, _reason, local} -> {:reply, :ok, %{state | local: local}}
    end
  end

  def handle_call({:read_range, offset, length}, _from, state) do
    {:reply, Local.read_range(state.local, offset, length), state}
  end

  def handle_call(:tail, _from, state) do
    {:reply, {state.epoch, Local.offset(state.local)}, state}
  end

  @impl GenServer
  def handle_info({:replicate, ref, epoch, offset, batch, from}, state) do
    messages = drain_replicates([{ref, epoch, offset, batch, from}])
    {:noreply, replicate_group(state, messages)}
  end

  @impl GenServer
  def terminate(_reason, state) do
    Local.close(state.local)
  end

  defp do_commit(state, epoch, offset, batch) do
    tail = Local.offset(state.local)

    cond do
      {epoch, offset} == {state.epoch, tail} ->
        case Local.commit(state.local, batch, byte_size(batch), @mirrored) do
          {:ok, local} ->
            {{:ok, {state.epoch, Local.offset(local)}}, %{state | local: local}}

          {:error, reason, local} ->
            {{:error, {:commit_failed, reason}}, %{state | local: local}}
        end

      epoch == state.epoch and offset + byte_size(batch) <= tail ->
        {{:ok, {state.epoch, tail}}, state}

      true ->
        {{:error, {:sequence_mismatch, %{expected: {state.epoch, tail}, got: {epoch, offset}}}},
         state}
    end
  end

  defp drain_replicates(acc) do
    receive do
      {:replicate, ref, epoch, offset, batch, from} ->
        drain_replicates([{ref, epoch, offset, batch, from} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp replicate_group(state, messages) do
    tail = Local.offset(state.local)

    {batches, bytes, appended, others} =
      Enum.reduce(messages, {[], 0, [], []}, fn {ref, epoch, offset, batch, from},
                                                {batches, bytes, appended, others} ->
        running_tail = tail + bytes

        cond do
          epoch == state.epoch and offset == running_tail ->
            {[batches, batch], bytes + byte_size(batch), [{from, ref} | appended], others}

          epoch == state.epoch and offset + byte_size(batch) <= running_tail ->
            {batches, bytes, appended, [{{from, ref}, :duplicate} | others]}

          true ->
            nack =
              {:sequence_mismatch, %{expected: {state.epoch, running_tail}, got: {epoch, offset}}}

            {batches, bytes, appended, [{{from, ref}, {:nack, nack}} | others]}
        end
      end)

    {state, watermark} =
      if bytes == 0 do
        {state, {state.epoch, tail}}
      else
        case Local.commit(state.local, batches, bytes, @mirrored) do
          {:ok, local} ->
            watermark = {state.epoch, Local.offset(local)}
            reply_each(appended, &{:replica_ack, &1, watermark})
            {%{state | local: local}, watermark}

          {:error, reason, local} ->
            reply_each(appended, &{:replica_nack, &1, {:commit_failed, reason}})
            {%{state | local: local}, {state.epoch, tail}}
        end
      end

    duplicates = for {recipient, :duplicate} <- others, do: recipient
    reply_each(duplicates, &{:replica_ack, &1, watermark})

    for {{from, ref}, {:nack, nack}} <- others do
      send(from, {:replica_nack, ref, nack})
    end

    state
  end

  defp reply_each(recipients, build_message) do
    recipients
    |> Enum.uniq()
    |> Enum.each(fn {from, ref} -> send(from, build_message.(ref)) end)
  end
end
