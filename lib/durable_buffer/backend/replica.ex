defmodule DurableBuffer.Backend.Replica do
  @moduledoc """
  Replicated-disk backend.

  Each group commit is written to the local WAL and, in parallel, shipped over
  `:erpc` to every node in `replicas:`, where a `DurableBuffer.Replica.Writer`
  appends and `datasync`s it. Replication overlaps the local write, so with
  fast replicas a commit costs roughly `max(local fsync, network round trip +
  replica fsync)` — and group commit amortizes that cost across all concurrent
  callers.

  The `ack:` policy decides when a commit counts as durable (the local write
  is one ack):

    * `:all` (default) — local + every replica
    * `:quorum` — a majority of `1 + length(replicas)`
    * a positive integer — that many acks

  A commit that reaches its ack target returns as soon as the target is met;
  stragglers finish in the background. Reads (`stream/2`) are served from the
  local WAL. A failed local write is always an error regardless of policy,
  since reads depend on the local copy.
  """

  @behaviour DurableBuffer.Backend

  alias DurableBuffer.Backend.Local

  @impl DurableBuffer.Backend
  def init_config(opts) do
    dir = Keyword.fetch!(opts, :dir)
    replicas = Keyword.get(opts, :replicas, [])

    %{
      dir: dir,
      replicas: replicas,
      replica_dir: Keyword.get(opts, :replica_dir, dir),
      needed_acks: needed_acks(Keyword.get(opts, :ack, :all), replicas),
      rpc_timeout: Keyword.get(opts, :rpc_timeout, 15_000)
    }
  end

  @impl DurableBuffer.Backend
  def open(config, partition_index) do
    {:ok, local} = Local.open(local_config(config), partition_index)
    {:ok, %{local: local, config: config, partition_index: partition_index}}
  end

  @impl DurableBuffer.Backend
  def commit(state, batch, byte_size) do
    config = state.config
    binary = IO.iodata_to_binary(batch)

    tasks =
      for node <- config.replicas do
        Task.async(fn ->
          replicate(node, config.replica_dir, state.partition_index, binary, config.rpc_timeout)
        end)
      end

    case Local.commit(state.local, binary, byte_size) do
      {:ok, local} ->
        acks = 1 + await_acks(tasks, config.needed_acks - 1, config.rpc_timeout)
        state = %{state | local: local}

        if acks >= config.needed_acks do
          {:ok, state}
        else
          {:error, {:insufficient_acks, acks, config.needed_acks}, state}
        end

      {:error, reason, local} ->
        {:error, {:local_commit_failed, reason}, %{state | local: local}}
    end
  end

  @impl DurableBuffer.Backend
  def stream(config, partition_index) do
    Local.stream(local_config(config), partition_index)
  end

  @impl DurableBuffer.Backend
  def truncate(state) do
    {:ok, local} = Local.truncate(state.local)

    for node <- state.config.replicas do
      try do
        :erpc.call(
          node,
          DurableBuffer.Replica,
          :truncate,
          [state.config.replica_dir, state.partition_index],
          state.config.rpc_timeout
        )
      catch
        _kind, _reason -> :ok
      end
    end

    {:ok, %{state | local: local}}
  end

  @impl DurableBuffer.Backend
  def close(state) do
    Local.close(state.local)
  end

  defp local_config(config) do
    Local.init_config(dir: config.dir)
  end

  defp needed_acks(:all, replicas), do: 1 + length(replicas)
  defp needed_acks(:quorum, replicas), do: div(1 + length(replicas), 2) + 1
  defp needed_acks(count, _replicas) when is_integer(count) and count > 0, do: count

  defp replicate(node, replica_dir, partition_index, binary, timeout) do
    :erpc.call(
      node,
      DurableBuffer.Replica,
      :commit,
      [replica_dir, partition_index, binary],
      timeout
    )
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp await_acks(_tasks, needed, _timeout) when needed <= 0, do: 0

  defp await_acks(tasks, needed, timeout) do
    refs = Map.new(tasks, fn task -> {task.ref, true} end)
    deadline = System.monotonic_time(:millisecond) + timeout
    collect_acks(refs, 0, needed, deadline)
  end

  defp collect_acks(refs, acked, needed, _deadline)
       when acked >= needed
       when map_size(refs) == 0 do
    acked
  end

  defp collect_acks(refs, acked, needed, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {ref, result} when is_map_key(refs, ref) ->
        Process.demonitor(ref, [:flush])
        collect_acks(Map.delete(refs, ref), acked + ack_value(result), needed, deadline)

      {:DOWN, ref, :process, _pid, _reason} when is_map_key(refs, ref) ->
        collect_acks(Map.delete(refs, ref), acked, needed, deadline)
    after
      remaining -> acked
    end
  end

  defp ack_value(:ok), do: 1
  defp ack_value(_result), do: 0
end
