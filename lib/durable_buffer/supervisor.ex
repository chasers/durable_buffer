defmodule DurableBuffer.Supervisor do
  @moduledoc """
  Supervision tree for one buffer instance: a fixed set of partition writers,
  one per partition, each owning its backend state.

  Also owns the `:atomics` array the partitions publish their offsets into,
  four slots each: durable byte offset, durable logical offset, next logical
  offset, base offset. `DurableBuffer.stream/3` and `offsets/2` read it
  lock-free, so a reader needs no message to the partition.
  """

  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}.Supervisor")
  end

  @impl Supervisor
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    partitions = Keyword.get(opts, :partitions, System.schedulers_online())
    backend = DurableBuffer.Backend.normalize(Keyword.fetch!(opts, :backend))

    durable_offsets = :atomics.new(partitions * 4, signed: false)
    retention = retention(opts)
    _validated = retention_interval!(opts)
    :ok = check_retention_supported!(retention, backend)

    :persistent_term.put({DurableBuffer, name}, %{
      name: name,
      partitions: partitions,
      backend: backend,
      durable_offsets: durable_offsets,
      retention: retention
    })

    children =
      for index <- 0..(partitions - 1) do
        partition_opts =
          opts
          |> Keyword.take([
            :max_batch_bytes,
            :max_batch_entries,
            :flush_delay_ms,
            :max_inflight_commits,
            :retention_interval_ms
          ])
          |> Keyword.merge(
            name: DurableBuffer.partition_name(name, index),
            backend: backend,
            partition_index: index,
            durable_offsets: durable_offsets,
            retention: retention
          )

        Supervisor.child_spec({DurableBuffer.Partition, partition_opts},
          id: {DurableBuffer.Partition, index}
        )
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp retention(opts) do
    %{
      ms: bound!(opts, :retention_ms),
      bytes: bound!(opts, :retention_bytes)
    }
  end

  defp retention_interval!(opts) do
    case Keyword.get(opts, :retention_interval_ms, 60_000) do
      :infinity ->
        :infinity

      value when is_integer(value) and value > 0 ->
        value

      other ->
        raise ArgumentError,
              ":retention_interval_ms must be a positive integer or :infinity, " <>
                "got #{inspect(other)}"
    end
  end

  defp check_retention_supported!(%{ms: nil, bytes: nil}, _backend), do: :ok

  defp check_retention_supported!(_retention, {DurableBuffer.Backend.Local, %{index: false}}) do
    raise ArgumentError,
          "retention needs the seek index, which carries the commit times and byte " <>
            "positions both bounds resolve through. Remove `index: false`, or drop " <>
            ":retention_ms and :retention_bytes and call trim/3 with an explicit upto:."
  end

  defp check_retention_supported!(_retention, _backend), do: :ok

  defp bound!(opts, key) do
    case Keyword.get(opts, key) do
      nil ->
        nil

      value when is_integer(value) and value > 0 ->
        value

      other ->
        raise ArgumentError, "#{inspect(key)} must be a positive integer, got #{inspect(other)}"
    end
  end
end
