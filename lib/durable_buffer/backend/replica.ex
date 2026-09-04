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
  met; stragglers keep replicating in the background. Reads are served from
  the local WAL, gated at `durable_offset/1` so a reader never sees a batch
  that has not met the ack policy. A failed local write is always an error
  regardless of policy, since reads depend on the local copy.

  By default this backend does **not** fsync (`fsync: false`): durability is
  the ack policy — data is durable because N machines hold it in their page
  caches and WALs, not because any one disk flushed it. This is the stance
  RabbitMQ streams take. Correlated power loss across an ack-quorum of
  nodes can lose acked writes; per-node crashes are already handled by CRC
  torn-tail recovery. Pass `fsync: true` to `datasync` every commit on the
  primary and every replica before it is acked, at a significant throughput
  cost when many partitions share a disk.

  Batches reach replicas over `transport:`, which defaults to
  `DurableBuffer.Transport.Distribution` — plain Erlang distribution, the
  behaviour every earlier version had. Only the batches use it. The control
  path (attach, truncate, trim, remote tail, remote read) and the acks stay
  on distribution whatever `transport:` says, because they are small and
  because the sender's liveness check is a `Process.monitor/1` on the remote
  writer. See `DurableBuffer.Transport`.

  Every batch is stamped with `{epoch, offset}` — the partition's epoch
  (bumped on truncate, persisted via `DurableBuffer.Meta`) and the WAL byte
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
  alias DurableBuffer.Meta
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
      fsync: Keyword.get(opts, :fsync, false),
      transport: transport!(Keyword.get(opts, :transport, DurableBuffer.Transport.Distribution))
    }
  end

  defp transport!(module) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :send_batch, 6) and
         function_exported?(module, :channel, 4) do
      module
    else
      raise ArgumentError,
            "transport: #{inspect(module)} does not implement DurableBuffer.Transport. " <>
              "Pass a module exporting channel/4 and send_batch/6, or omit the option " <>
              "to use DurableBuffer.Transport.Distribution."
    end
  end

  defp transport!(other) do
    raise ArgumentError, "transport: expects a module, got #{inspect(other)}"
  end

  @impl DurableBuffer.Backend
  def open(config, partition_index) do
    {:ok, local} = Local.open(local_config(config), partition_index)
    epoch = Meta.epoch(config.dir, partition_index)
    tails = remote_tails(config, partition_index)
    local = heal(config, partition_index, epoch, local, tails)
    adopted = for {node, {^epoch, _offset}} <- tails, into: %{}, do: {node, epoch}

    watermarks =
      for {node, {^epoch, _offset} = tail} <- tails,
          into: %{local: {epoch, Local.offset(local)}},
          do: {node, tail}

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
            fsync: config.fsync,
            transport: config.transport
          )

        {node, sender}
      end)

    {:ok,
     %{
       local: local,
       config: config,
       partition_index: partition_index,
       epoch: epoch,
       watermarks: watermarks,
       adopted: adopted,
       senders: senders,
       pending: [],
       timeout_armed?: false
     }}
  end

  @impl DurableBuffer.Backend
  def commit(state, batch, byte_size, span) do
    tag = make_ref()

    case commit_async(state, batch, byte_size, span, tag) do
      {:done, :ok, state} -> {:ok, state}
      {:done, {:error, reason}, state} -> {:error, reason, state}
      {:pending, state} -> await_commit(state, tag)
    end
  end

  @impl DurableBuffer.Backend
  def commit_async(state, batch, byte_size, span, tag) do
    config = state.config
    binary = IO.iodata_to_binary(batch)
    epoch = state.epoch
    offset = Local.offset(state.local)
    target = {epoch, offset + byte_size}

    case Local.commit(state.local, binary, byte_size, span) do
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

  def handle_message({:adopted, node, epoch}, state) do
    {[], %{state | adopted: Map.update(state.adopted, node, epoch, &max(&1, epoch))}}
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
  def stream(config, partition_index, opts) do
    Local.stream(local_config(config), partition_index, opts)
  end

  @doc """
  Logical entry offsets as of `open/2` or the last `truncate/1`, from the
  local WAL.
  """
  @impl DurableBuffer.Backend
  @spec offsets(map()) :: %{first: non_neg_integer(), next: non_neg_integer()}
  def offsets(state), do: Local.offsets(state.local)

  @doc """
  The primary's own retention point. Replicas mirror bytes and follow the
  base the primary propagates, so the policy is decided in one place.
  """
  @impl DurableBuffer.Backend
  @spec retention_point(map(), map()) :: {:ok, non_neg_integer()} | :none
  def retention_point(state, policy), do: Local.retention_point(state.local, policy)

  @impl DurableBuffer.Backend
  @spec retention_status(map()) :: %{oldest_ms: integer() | nil, bytes: non_neg_integer()}
  def retention_status(state), do: Local.retention_status(state.local)

  @doc """
  Drops every entry below `upto` locally, then passes the resulting logical
  byte base on to every replica.

  A trim never reaches the replication wire as a data change: logical byte
  offsets do not move, so a replica that has not yet trimmed still accepts
  the same batches at the same offsets. Propagation only reclaims space: a
  replica that misses it keeps more data than it needs, and the next trim
  carries a base that supersedes the one it missed.

  So propagation is a **cast**, not a call. It runs inside the committer,
  and retention applies itself on a timer, so a blocking round trip per
  replica would put every replica's latency — and `rpc_timeout` for a hung
  one — in front of the primary's commits, on a schedule. A truncate still
  blocks, because `await_replicas/3` is a guarantee callers depend on and a
  truncate is rare.
  """
  @impl DurableBuffer.Backend
  @spec trim(map(), non_neg_integer()) :: {:ok, map()} | {:error, term(), map()}
  def trim(state, upto) do
    case Local.trim(state.local, upto) do
      {:ok, local} ->
        base_byte = Local.base_byte(local)
        Enum.each(state.config.replicas, &trim_replica(state, &1, base_byte))
        {:ok, %{state | local: local}}

      {:error, reason, local} ->
        {:error, reason, %{state | local: local}}
    end
  end

  @doc """
  Byte offset through which the ack policy is met.

  Take every member watermark on the current epoch, sort them descending,
  and read off the `needed_acks`-th. That is the furthest offset `ack:`
  members agree on, which is exactly what a commit waits for. Reads are
  gated here, so a reader never sees a batch that has not met the policy —
  one that may still fail with `:insufficient_acks`, or that a primary crash
  can erase.
  """
  @impl DurableBuffer.Backend
  @spec durable_offset(map()) :: non_neg_integer()
  def durable_offset(state) do
    offsets =
      for {_member, {epoch, offset}} <- state.watermarks, epoch == state.epoch, do: offset

    case Enum.sort(offsets, :desc) do
      sorted when length(sorted) >= state.config.needed_acks ->
        Enum.at(sorted, state.config.needed_acks - 1)

      _too_few ->
        0
    end
  end

  @impl DurableBuffer.Backend
  def truncate(state, next) do
    epoch = state.epoch + 1
    Meta.update!(state.config.dir, state.partition_index, &%{&1 | epoch: epoch})
    {:ok, local} = Local.truncate(state.local, next)

    Enum.each(state.senders, fn {_node, sender} -> Sender.reset(sender, epoch) end)

    adopted =
      Enum.reduce(state.config.replicas, %{}, fn node, adopted ->
        case truncate_replica(state, node, epoch) do
          :ok ->
            Map.put(adopted, node, epoch)

          :error ->
            Logger.warning(
              "DurableBuffer #{state.config.dir} p#{state.partition_index} could not " <>
                "truncate #{inspect(node)}; it holds pre-truncate data until its sender " <>
                "re-attaches, and is not safe to promote until then"
            )

            adopted
        end
      end)

    {:ok, %{state | local: local, epoch: epoch, adopted: adopted, watermarks: %{}}}
  end

  @doc """
  Reports each replica's replication state.

  `adopted_epoch` is the newest epoch that replica has confirmed. A replica
  behind the primary's epoch still holds pre-truncate data and is not safe
  to promote, so `promotable?` is false until its sender re-attaches and
  truncates it. `caught_up?` additionally requires its watermark to be at
  the primary's WAL tail.
  """
  @spec status(map()) :: %{node() => map()}
  def status(state) do
    tail = {state.epoch, Local.offset(state.local)}

    Map.new(state.config.replicas, fn node ->
      adopted = Map.get(state.adopted, node)
      watermark = Map.get(state.watermarks, node)

      {node,
       %{
         epoch: state.epoch,
         adopted_epoch: adopted,
         watermark: watermark,
         tail: tail,
         promotable?: adopted == state.epoch,
         caught_up?: adopted == state.epoch and watermark == tail
       }}
    end)
  end

  defp truncate_replica(state, node, epoch) do
    :erpc.call(
      node,
      DurableBuffer.Replica,
      :truncate,
      [state.config.replica_dir, state.partition_index, epoch, 0],
      state.config.rpc_timeout
    )
  catch
    _kind, _reason -> :error
  end

  defp trim_replica(state, node, base_byte) do
    :erpc.cast(
      node,
      DurableBuffer.Replica,
      :trim,
      [state.config.replica_dir, state.partition_index, base_byte]
    )
  catch
    _kind, _reason -> :error
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

  defp heal(config, partition_index, epoch, local, tails) do
    case ahead_replica(tails, epoch, Local.offset(local)) do
      nil -> local
      {node, remote_offset} -> pull(config, partition_index, node, local, remote_offset)
    end
  end

  defp ahead_replica(tails, epoch, local_offset) do
    candidates =
      for {node, {^epoch, offset}} <- tails, offset > local_offset, do: {node, offset}

    case candidates do
      [] -> nil
      list -> Enum.max_by(list, fn {_node, offset} -> offset end)
    end
  end

  defp remote_tails(config, partition_index) do
    for node <- config.replicas,
        tail = remote_tail(config, partition_index, node),
        tail != nil,
        into: %{},
        do: {node, tail}
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
          {payloads, valid, rest} = WAL.decode_all(buffer)
          local = append_healed(local, buffer, length(payloads), valid)
          pull_chunks(config, partition_index, node, local, remote_offset, rest)

        _empty_or_error ->
          local
      end
    end
  end

  defp append_healed(local, _buffer, _count, 0), do: local

  defp append_healed(local, buffer, count, valid) do
    span = {Local.offsets(local).next, count}
    {:ok, local} = Local.commit(local, binary_part(buffer, 0, valid), valid, span)
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
