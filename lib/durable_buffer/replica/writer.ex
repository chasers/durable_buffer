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
  `{:replica_ack, watermark}` / `{:replica_nack, reason}` sent to `from`. A
  successful append acknowledges with the writer's new durability watermark
  `{epoch, offset}` — everything up to `offset` in `epoch` is on disk here.

  Pipelined batches group-commit: every `:replicate` message queued in the
  mailbox is drained, the contiguous run is appended with a single write +
  `datasync`, and one ack carrying the final watermark covers them all — so
  a deep pipeline of small batches costs one fsync, not one per batch.

  The epoch is persisted next to the WAL (see `DurableBuffer.Epoch`) and
  adopted from the primary on truncate.
  """

  use GenServer

  alias DurableBuffer.Backend.Local
  alias DurableBuffer.Epoch

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

  @spec truncate(GenServer.server(), non_neg_integer()) :: :ok
  def truncate(server, epoch) do
    GenServer.call(server, {:truncate, epoch}, :infinity)
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
    config = Local.init_config(dir: dir, fsync: Keyword.get(opts, :fsync, true))
    {:ok, local} = Local.open(config, partition_index)

    {:ok,
     %{
       local: local,
       dir: dir,
       partition_index: partition_index,
       epoch: Epoch.load(dir, partition_index)
     }}
  end

  @impl GenServer
  def handle_call({:commit, epoch, offset, batch}, _from, state) do
    {reply, state} = do_commit(state, epoch, offset, batch)
    {:reply, reply, state}
  end

  def handle_call({:truncate, epoch}, _from, state) do
    {:ok, local} = Local.truncate(state.local)
    Epoch.store!(state.dir, state.partition_index, epoch)
    {:reply, :ok, %{state | local: local, epoch: epoch}}
  end

  def handle_call(:tail, _from, state) do
    {:reply, {state.epoch, Local.offset(state.local)}, state}
  end

  @impl GenServer
  def handle_info({:replicate, epoch, offset, batch, from}, state) do
    messages = drain_replicates([{epoch, offset, batch, from}])
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
        case Local.commit(state.local, batch, byte_size(batch)) do
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
      {:replicate, epoch, offset, batch, from} ->
        drain_replicates([{epoch, offset, batch, from} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp replicate_group(state, messages) do
    tail = Local.offset(state.local)

    {batches, bytes, appended, others} =
      Enum.reduce(messages, {[], 0, [], []}, fn {epoch, offset, batch, from},
                                                {batches, bytes, appended, others} ->
        running_tail = tail + bytes

        cond do
          epoch == state.epoch and offset == running_tail ->
            {[batches, batch], bytes + byte_size(batch), [from | appended], others}

          epoch == state.epoch and offset + byte_size(batch) <= running_tail ->
            {batches, bytes, appended, [{from, :duplicate} | others]}

          true ->
            nack =
              {:sequence_mismatch, %{expected: {state.epoch, running_tail}, got: {epoch, offset}}}

            {batches, bytes, appended, [{from, {:nack, nack}} | others]}
        end
      end)

    {state, watermark} =
      if bytes == 0 do
        {state, {state.epoch, tail}}
      else
        case Local.commit(state.local, batches, bytes) do
          {:ok, local} ->
            watermark = {state.epoch, Local.offset(local)}
            reply_each(appended, {:replica_ack, watermark})
            {%{state | local: local}, watermark}

          {:error, reason, local} ->
            reply_each(appended, {:replica_nack, {:commit_failed, reason}})
            {%{state | local: local}, {state.epoch, tail}}
        end
      end

    duplicates = for {from, :duplicate} <- others, do: from
    reply_each(duplicates, {:replica_ack, watermark})

    for {from, {:nack, nack}} <- others do
      send(from, {:replica_nack, nack})
    end

    state
  end

  defp reply_each(froms, message) do
    froms
    |> Enum.uniq()
    |> Enum.each(&send(&1, message))
  end
end
