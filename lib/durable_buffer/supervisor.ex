defmodule DurableBuffer.Supervisor do
  @moduledoc """
  Supervision tree for one buffer instance: a fixed set of partition writers,
  one per partition, each owning its backend state.

  Also owns the `:atomics` array the partitions publish their durable byte
  offsets into, one slot per partition. `DurableBuffer.stream/3` reads it
  lock-free to gate reads, so a reader needs no message to the partition.
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

    durable_offsets = :atomics.new(partitions, signed: false)

    :persistent_term.put({DurableBuffer, name}, %{
      partitions: partitions,
      backend: backend,
      durable_offsets: durable_offsets
    })

    children =
      for index <- 0..(partitions - 1) do
        partition_opts =
          opts
          |> Keyword.take([
            :max_batch_bytes,
            :max_batch_entries,
            :flush_delay_ms,
            :max_inflight_commits
          ])
          |> Keyword.merge(
            name: DurableBuffer.partition_name(name, index),
            backend: backend,
            partition_index: index,
            durable_offsets: durable_offsets
          )

        Supervisor.child_spec({DurableBuffer.Partition, partition_opts},
          id: {DurableBuffer.Partition, index}
        )
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
