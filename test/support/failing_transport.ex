defmodule DurableBuffer.FailingTransport do
  @moduledoc """
  A `DurableBuffer.Transport` for tests that fails on demand, so the sender's
  healing can be exercised without a real broken wire.

  Set the mode with `set/2` before opening the partition:

    * `:ok` — delegate to `DurableBuffer.Transport.Distribution`
    * `:error` — every `send_batch/6` returns `{:error, :simulated}`
    * `:raise` — every `send_batch/6` raises
    * `:no_channel` — `channel/4` returns `{:error, :simulated}`
    * `:nil_channel` — `channel/4` succeeds with `nil`, which is a legal
      opaque channel value and must not be read as "not attached"
  """

  @behaviour DurableBuffer.Transport

  alias DurableBuffer.Transport.Distribution

  @spec set(Path.t(), atom()) :: :ok
  def set(dir, mode) do
    :persistent_term.put({__MODULE__, dir}, mode)
  end

  defp mode(dir), do: :persistent_term.get({__MODULE__, dir}, :ok)

  @impl DurableBuffer.Transport
  def channel(node, dir, partition_index, writer) do
    case mode(dir) do
      :no_channel ->
        {:error, :simulated}

      :nil_channel ->
        {:ok, nil}

      _other ->
        {:ok, inner} = Distribution.channel(node, dir, partition_index, writer)
        {:ok, {dir, inner}}
    end
  end

  @impl DurableBuffer.Transport
  def send_batch(channel, ref, epoch, offset, batch, reply_to) do
    dir =
      case channel do
        {dir, _inner} -> dir
        nil -> nil
      end

    case dir && mode(dir) do
      :error -> {:error, :simulated}
      :raise -> raise "simulated transport failure"
      _other -> forward(channel, ref, epoch, offset, batch, reply_to)
    end
  end

  defp forward(nil, _ref, _epoch, _offset, _batch, _reply_to), do: :ok

  defp forward({_dir, inner}, ref, epoch, offset, batch, reply_to) do
    Distribution.send_batch(inner, ref, epoch, offset, batch, reply_to)
  end
end
