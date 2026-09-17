defmodule DurableBuffer.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: DurableBuffer.Registry},
      {PartitionSupervisor,
       child_spec: DynamicSupervisor, name: DurableBuffer.Replica.WriterSupervisors},
      {DynamicSupervisor, name: DurableBuffer.Queue.AppenderSupervisor, strategy: :one_for_one}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: DurableBuffer.AppSupervisor)
  end
end
