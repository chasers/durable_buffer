defmodule DurableBuffer.RecordingTransport do
  @moduledoc """
  A `DurableBuffer.Transport` for tests that reports every batch to a
  subscribed process and then delegates to
  `DurableBuffer.Transport.Distribution`.

  Replication keeps working, so a test can assert on both the wire and the
  replicated data. Call `subscribe/1` with the replica dir before opening
  the partition; each batch arrives as
  `{:transport_batch, dir, epoch, offset, byte_size}`.
  """

  @behaviour DurableBuffer.Transport

  alias DurableBuffer.Transport.Distribution

  @spec subscribe(Path.t()) :: {:ok, pid()}
  def subscribe(dir) do
    Registry.register(DurableBuffer.Registry, {:recording_transport, dir}, nil)
  end

  @impl DurableBuffer.Transport
  def channel(node, dir, partition_index, writer) do
    {:ok, inner} = Distribution.channel(node, dir, partition_index, writer)
    {:ok, {dir, inner}}
  end

  @impl DurableBuffer.Transport
  def send_batch({dir, writer}, ref, epoch, offset, batch, reply_to) do
    Registry.dispatch(DurableBuffer.Registry, {:recording_transport, dir}, fn subscribers ->
      for {pid, _value} <- subscribers do
        send(pid, {:transport_batch, dir, epoch, offset, byte_size(batch)})
      end
    end)

    Distribution.send_batch(writer, ref, epoch, offset, batch, reply_to)
  end
end
