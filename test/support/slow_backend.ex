defmodule DurableBuffer.Test.SlowBackend do
  @moduledoc """
  Test backend that records every committed batch in an Agent and sleeps
  during `commit/3`, so concurrent appends observably coalesce into group
  commits.
  """

  @behaviour DurableBuffer.Backend

  alias DurableBuffer.WAL

  def start_recorder do
    Agent.start_link(fn -> [] end)
  end

  def committed_batches(recorder) do
    recorder
    |> Agent.get(&Enum.reverse/1)
    |> Enum.map(fn batch -> elem(WAL.decode_all(batch), 0) end)
  end

  @impl DurableBuffer.Backend
  def init_config(opts) do
    %{
      recorder: Keyword.fetch!(opts, :recorder),
      commit_sleep: Keyword.get(opts, :commit_sleep, 20),
      fail_on: Keyword.get(opts, :fail_on)
    }
  end

  @impl DurableBuffer.Backend
  def open(config, _partition_index) do
    {:ok, config}
  end

  @impl DurableBuffer.Backend
  def commit(state, batch, _byte_size, _span) do
    Process.sleep(state.commit_sleep)
    binary = IO.iodata_to_binary(batch)

    case state.fail_on do
      nil ->
        Agent.update(state.recorder, &[binary | &1])
        {:ok, state}

      pattern ->
        if binary =~ pattern do
          {:error, :injected_failure, state}
        else
          Agent.update(state.recorder, &[binary | &1])
          {:ok, state}
        end
    end
  end

  @impl DurableBuffer.Backend
  def stream(config, _partition_index) do
    config.recorder |> committed_batches() |> List.flatten()
  end

  @impl DurableBuffer.Backend
  def truncate(state, _next) do
    Agent.update(state.recorder, fn _batches -> [] end)
    {:ok, state}
  end

  @impl DurableBuffer.Backend
  def close(_state) do
    :ok
  end
end
