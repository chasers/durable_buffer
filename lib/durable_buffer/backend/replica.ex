defmodule DurableBuffer.Backend.Replica do
  @moduledoc """
  Replicated-disk backend.

  Each group commit is written to the local WAL and then pipelined to every
  node in `replicas:` over a long-lived per-replica
  `DurableBuffer.Replica.Sender` channel, where a
  `DurableBuffer.Replica.Writer` appends it (replicas therefore never hold
  bytes the primary WAL doesn't). Through the asynchronous commit contract,
  commits overlap each other: while one batch's acks are still in flight,
  the next batches are already written locally and on the wire. A slow or
  dead replica stalls only its own channel, never the commit path.

  The `ack:` policy decides when a commit counts as durable (the local write
  is one ack):

    * `:all` (default) — local + every replica
    * `:quorum` — a majority of `1 + length(replicas)`
    * a positive integer — that many acks

  A commit that reaches its ack target completes as soon as the target is
  met; stragglers keep replicating in the background. Reads (`stream/2`)
  are served from the local WAL. A failed local write is always an error
  regardless of policy, since reads depend on the local copy.

  By default this backend does **not** fsync (`fsync: false`): durability is
  the ack policy — data is durable because N machines hold it in their page
  caches and WALs, not because any one disk flushed it. This is the stance
  RabbitMQ streams take. Correlated power loss across an ack-quorum of
  nodes can lose acked writes; per-node crashes are already handled by CRC
  torn-tail recovery. Pass `fsync: true` to `datasync` every commit on the
  primary and every replica before it is acked, at a significant throughput
  cost when many partitions share a disk.

  Every batch is stamped with `{epoch, offset}` — the partition's epoch
  (bumped on truncate, persisted via `DurableBuffer.Epoch`) and the WAL byte
  offset at which the batch starts. Replica writers append a batch only when
  it lands exactly at their tail, so a replica that missed a batch or a
  truncate rejects everything after the gap instead of diverging silently.
  Rejections, disconnects, and ack stalls all heal the same way: the sender
  re-attaches, learns the replica's tail, and streams the missing WAL
  suffix (truncating the replica first when it missed an epoch bump) before
  resuming live traffic — a replica that was down for an hour catches up
  automatically.

  Healing runs in the other direction too. A primary opened with
  `fsync: false` can come back from a crash having lost WAL bytes a replica
  still holds and already acked. `open/2` therefore asks every replica for
  its tail, and pulls back anything it is missing from the furthest one
  before the partition serves a single append. Pulled bytes are CRC-checked
  frame by frame and `datasync`ed whatever the `fsync:` setting is. A
  replica unreachable at open is not consulted, so `heal_timeout:` (5s)
  bounds how long a dead node delays startup. Until it does, its
  acks are missing, which surfaces as `:insufficient_acks` errors when the
  ack policy needs it. Primary and replica nodes must run the same
  `:durable_buffer` version, since batches cross nodes with this framing.

  Acks are durability watermarks, not per-batch confirmations: each replica
  replies with `{epoch, offset}` meaning "everything up to `offset` in
  `epoch` is on my disk", and the primary keeps the highest watermark seen
  per member (monotonic, so duplicate or reordered acks are harmless). A
  batch counts as committed once at least `ack:` members — the primary
  included — have watermarks at or past the batch's end.
  """

  @behaviour DurableBuffer.Backend

  require Logger

  alias DurableBuffer.Backend.Local
  alias DurableBuffer.Epoch
  alias DurableBuffer.Replica.Sender
  alias DurableBuffer.WAL

  @heal_chunk_bytes 1024 * 1024

  @typedoc """
  A member's durability watermark: everything up to `offset` in `epoch` is
  on that member's disk. Watermarks order correctly as plain term
  comparisons, since the epoch dominates the offset.
  """
  @type watermark :: {epoch :: non_neg_integer(), offset :: non_neg_integer()}

  @impl DurableBuffer.Backend
  def init_config(opts) do
    dir = Keyword.fetch!(opts, :dir)
    replicas = Keyword.get(opts, :replicas, [])

    %{
      dir: dir,
      replicas: replicas,
      replica_dir: Keyword.get(opts, :replica_dir, dir),
      needed_acks: needed_acks(Keyword.get(opts, :ack, :all), replicas),
      rpc_timeout: Keyword.get(opts, :rpc_timeout, 15_000),
      max_sender_bytes: Keyword.get(opts, :max_sender_bytes, 64 * 1024 * 1024),
      heal_timeout: Keyword.get(opts, :heal_timeout, 5_000),
      fsync: Keyword.get(opts, :fsync, false)
    }
  end

  @impl DurableBuffer.Backend
  def open(config, partition_index) do
    {:ok, local} = Local.open(local_config(config), partition_index)
    epoch = Epoch.load(config.dir, partition_index)
    local = heal(config, partition_index, epoch, local)

    senders =
      Map.new(config.replicas, fn node ->
        {:ok, sender} =
          Sender.start_link(
            owner: self(),
            node: node,
            dir: config.replica_dir,
            partition_index: partition_index,
            primary_dir: config.dir,
            primary_tail: Local.offset(local),
            epoch: epoch,
            rpc_timeout: config.rpc_timeout,
            max_bytes: config.max_sender_bytes,
            fsync: config.fsync
          )

        {node, sender}
      end)

    {:ok,
     %{
       local: local,
       config: config,
       partition_index: partition_index,
       epoch: epoch,
       watermarks: %{},
       senders: senders,
       pending: [],
       timeout_armed?: false
     }}
  end

  @impl DurableBuffer.Backend
  def commit(state, batch, byte_size) do
    tag = make_ref()

    case commit_async(state, batch, byte_size, tag) do
      {:done, :ok, state} -> {:ok, state}
      {:done, {:error, reason}, state} -> {:error, reason, state}
      {:pending, state} -> await_commit(state, tag)
    end
  end

  @impl DurableBuffer.Backend
  def commit_async(state, batch, byte_size, tag) do
    config = state.config
    binary = IO.iodata_to_binary(batch)
    epoch = state.epoch
    offset = Local.offset(state.local)
    target = {epoch, offset + byte_size}

    case Local.commit(state.local, binary, byte_size) do
      {:ok, local} ->
        Enum.each(state.senders, fn {_node, sender} ->
          Sender.commit(sender, epoch, offset, binary)
        end)

        watermarks = advance(state.watermarks, :local, target)
        state = %{state | local: local, watermarks: watermarks}

        if durable_count(watermarks, target) >= config.needed_acks do
          {:done, :ok, state}
        else
          deadline = System.monotonic_time(:millisecond) + config.rpc_timeout
          state = %{state | pending: state.pending ++ [{tag, target, deadline}]}
          {:pending, arm_timeout(state)}
        end

      {:error, reason, local} ->
        {:done, {:error, {:local_commit_failed, reason}}, %{state | local: local}}
    end
  end

  @impl DurableBuffer.Backend
  def handle_message({:watermark, node, watermark}, state) do
    watermarks = advance(state.watermarks, node, watermark)

    {completed, pending} =
      Enum.split_while(state.pending, fn {_tag, target, _deadline} ->
        durable_count(watermarks, target) >= state.config.needed_acks
      end)

    completions = for {tag, _target, _deadline} <- completed, do: {tag, :ok}
    {completions, %{state | watermarks: watermarks, pending: pending}}
  end

  def handle_message(:check_timeouts, state) do
    now = System.monotonic_time(:millisecond)

    {overdue, pending} =
      Enum.split_while(state.pending, fn {_tag, _target, deadline} -> deadline <= now end)

    completions =
      for {tag, target, _deadline} <- overdue do
        acks = durable_count(state.watermarks, target)
        {tag, {:error, {:insufficient_acks, acks, state.config.needed_acks}}}
      end

    state = %{state | pending: pending, timeout_armed?: false}
    {completions, arm_timeout(state)}
  end

  @impl DurableBuffer.Backend
  def stream(config, partition_index) do
    Local.stream(local_config(config), partition_index)
  end

  @impl DurableBuffer.Backend
  def truncate(state) do
    epoch = state.epoch + 1
    Epoch.store!(state.config.dir, state.partition_index, epoch)
    {:ok, local} = Local.truncate(state.local)

    Enum.each(state.senders, fn {_node, sender} -> Sender.reset(sender, epoch) end)

    for node <- state.config.replicas do
      try do
        :erpc.call(
          node,
          DurableBuffer.Replica,
          :truncate,
          [state.config.replica_dir, state.partition_index, epoch],
          state.config.rpc_timeout
        )
      catch
        _kind, _reason -> :ok
      end
    end

    {:ok, %{state | local: local, epoch: epoch}}
  end

  @impl DurableBuffer.Backend
  def close(state) do
    Enum.each(state.senders, fn {_node, sender} -> Sender.stop(sender) end)
    Local.close(state.local)
  end

  defp await_commit(state, tag) do
    receive do
      {:backend, message} ->
        {completions, state} = handle_message(message, state)

        case List.keyfind(completions, tag, 0) do
          {^tag, :ok} -> {:ok, state}
          {^tag, {:error, reason}} -> {:error, reason, state}
          nil -> await_commit(state, tag)
        end
    end
  end

  defp heal(%{replicas: []}, _partition_index, _epoch, local), do: local

  defp heal(config, partition_index, epoch, local) do
    case ahead_replica(config, partition_index, epoch, Local.offset(local)) do
      nil -> local
      {node, remote_offset} -> pull(config, partition_index, node, local, remote_offset)
    end
  end

  defp ahead_replica(config, partition_index, epoch, local_offset) do
    candidates =
      for node <- config.replicas,
          {^epoch, offset} <- [remote_tail(config, partition_index, node)],
          offset > local_offset,
          do: {node, offset}

    case candidates do
      [] -> nil
      list -> Enum.max_by(list, fn {_node, offset} -> offset end)
    end
  end

  defp remote_tail(config, partition_index, node) do
    {_pid, tail} =
      :erpc.call(
        node,
        DurableBuffer.Replica,
        :attach,
        [config.replica_dir, partition_index, config.fsync],
        config.heal_timeout
      )

    tail
  catch
    _kind, _reason -> nil
  end

  defp pull(config, partition_index, node, local, remote_offset) do
    Logger.warning(
      "DurableBuffer primary #{config.dir} p#{partition_index} is " <>
        "#{remote_offset - Local.offset(local)} bytes behind #{inspect(node)}; " <>
        "healing from it before the partition serves"
    )

    local = pull_chunks(config, partition_index, node, local, remote_offset, <<>>)
    :ok = Local.datasync(local)
    local
  end

  defp pull_chunks(config, partition_index, node, local, remote_offset, leftover) do
    cursor = Local.offset(local) + byte_size(leftover)

    if cursor >= remote_offset do
      local
    else
      length = min(@heal_chunk_bytes, remote_offset - cursor)

      case read_remote(config, partition_index, node, cursor, length) do
        {:ok, data} when byte_size(data) > 0 ->
          buffer = leftover <> data
          {_payloads, valid, rest} = WAL.decode_all(buffer)
          local = append_healed(local, buffer, valid)
          pull_chunks(config, partition_index, node, local, remote_offset, rest)

        _empty_or_error ->
          local
      end
    end
  end

  defp append_healed(local, _buffer, 0), do: local

  defp append_healed(local, buffer, valid) do
    {:ok, local} = Local.commit(local, binary_part(buffer, 0, valid), valid)
    local
  end

  defp read_remote(config, partition_index, node, offset, length) do
    :erpc.call(
      node,
      DurableBuffer.Replica,
      :read_range,
      [config.replica_dir, partition_index, offset, length],
      config.heal_timeout
    )
  catch
    _kind, _reason -> :error
  end

  defp local_config(config) do
    Local.init_config(dir: config.dir, fsync: config.fsync)
  end

  defp needed_acks(:all, replicas), do: 1 + length(replicas)
  defp needed_acks(:quorum, replicas), do: div(1 + length(replicas), 2) + 1
  defp needed_acks(count, _replicas) when is_integer(count) and count > 0, do: count

  defp arm_timeout(%{timeout_armed?: true} = state), do: state
  defp arm_timeout(%{pending: []} = state), do: state

  defp arm_timeout(%{pending: [{_tag, _target, deadline} | _rest]} = state) do
    delay = max(deadline - System.monotonic_time(:millisecond), 0)
    Process.send_after(self(), {:backend, :check_timeouts}, delay)
    %{state | timeout_armed?: true}
  end

  defp advance(watermarks, member, watermark) do
    Map.update(watermarks, member, watermark, &max(&1, watermark))
  end

  defp durable_count(watermarks, target) do
    Enum.count(watermarks, fn {_member, watermark} -> watermark >= target end)
  end
end
