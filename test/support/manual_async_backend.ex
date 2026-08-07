defmodule DurableBuffer.ManualAsyncBackend do
  @moduledoc """
  Test backend implementing the asynchronous commit contract with externally
  controlled completions: every `commit_async/4` notifies `owner:` with
  `{:submitted, tag, committer}` and stays pending until the test sends the
  committer `{:backend, {:complete, tag}}`.
  """

  @behaviour DurableBuffer.Backend

  @impl DurableBuffer.Backend
  def init_config(opts) do
    %{owner: Keyword.fetch!(opts, :owner)}
  end

  @impl DurableBuffer.Backend
  def open(config, _partition_index) do
    {:ok, %{owner: config.owner}}
  end

  @impl DurableBuffer.Backend
  def commit(_state, _batch, _byte_size) do
    raise "ManualAsyncBackend only supports the async commit contract"
  end

  @impl DurableBuffer.Backend
  def commit_async(state, _batch, _byte_size, tag) do
    send(state.owner, {:submitted, tag, self()})
    {:pending, state}
  end

  @impl DurableBuffer.Backend
  def handle_message({:complete, tag}, state) do
    {[{tag, :ok}], state}
  end

  @impl DurableBuffer.Backend
  def stream(_config, _partition_index), do: []

  @impl DurableBuffer.Backend
  def truncate(state), do: {:ok, state}

  @impl DurableBuffer.Backend
  def close(_state), do: :ok
end
