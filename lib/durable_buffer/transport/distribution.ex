defmodule DurableBuffer.Transport.Distribution do
  @moduledoc """
  The default `DurableBuffer.Transport`: batches ride the Erlang
  distribution channel as plain messages to the remote writer pid.

  This is the transport every earlier version used, and it needs no
  configuration, no extra port and no extra dependency. Its channel is the
  writer pid itself.

  `send/2` to a remote pid blocks this process when the distribution buffer
  to that node fills, which is the sender's backpressure: a slow replica
  stalls its own channel and nothing else. The cost is the reason
  `DurableBuffer.Transport` exists — those bytes share one TCP connection
  with every other process on the node pair.
  """

  @behaviour DurableBuffer.Transport

  @impl DurableBuffer.Transport
  def channel(_node, _dir, _partition_index, writer), do: writer

  @impl DurableBuffer.Transport
  def send_batch(writer, ref, epoch, offset, batch, reply_to) do
    send(writer, {:replicate, ref, epoch, offset, batch, reply_to})
    :ok
  end
end
