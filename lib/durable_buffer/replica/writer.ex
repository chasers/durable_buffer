defmodule DurableBuffer.Replica.Writer do
  @moduledoc """
  Replica-side WAL writer for one `{dir, partition_index}`.

  Holds an open raw fd and performs one write + `datasync` per replicated
  batch. Batches arrive already group-committed by the primary, so each call
  is one durable append.
  """

  use GenServer

  alias DurableBuffer.Backend.Local

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

  @spec commit(GenServer.server(), binary()) :: :ok | {:error, term()}
  def commit(server, batch) do
    GenServer.call(server, {:commit, batch}, :infinity)
  end

  @spec truncate(GenServer.server()) :: :ok
  def truncate(server) do
    GenServer.call(server, :truncate, :infinity)
  end

  @impl GenServer
  def init(opts) do
    config = Local.init_config(dir: Keyword.fetch!(opts, :dir))
    Local.open(config, Keyword.fetch!(opts, :partition_index))
  end

  @impl GenServer
  def handle_call({:commit, batch}, _from, state) do
    case Local.commit(state, batch, byte_size(batch)) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:truncate, _from, state) do
    {:ok, state} = Local.truncate(state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    Local.close(state)
  end
end
