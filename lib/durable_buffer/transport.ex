defmodule DurableBuffer.Transport do
  @moduledoc """
  The wire a `DurableBuffer.Replica.Sender` puts replicated batches on.

  Only the data path goes through here. The batches are the large payload —
  up to `max_sender_bytes` in flight per sender, per partition — and shipping
  them over the Erlang distribution channel head-of-line blocks everything
  else on the node pair, including the cluster heartbeat that replication
  exists to survive.

  The control path does not go through here. The six `:erpc` sites (attach,
  truncate, trim, remote tail, remote read) are small request/reply round
  trips that block nothing, so they stay on distribution. That is also what
  keeps liveness simple: distribution stays connected between the pair, so
  the sender's `Process.monitor/1` on the remote writer pid works whatever
  transport carries the bytes.

  Acks do not go through here either. A `{:replica_ack, ref, watermark}` is
  tens of bytes, and it travels back to the `reply_to` pid over
  distribution.

  A transport resolves a channel once per attach and then puts batches on
  it. The channel is opaque: `DurableBuffer.Transport.Distribution` uses the
  writer pid, and a transport that has no pid-to-pid messaging uses whatever
  names the remote writer for it.
  """

  @typedoc """
  A transport's own handle on one replica's writer, built by `c:channel/4`
  and passed back to `c:send_batch/6`. Opaque to the sender.
  """
  @type channel :: term()

  @doc """
  Resolves the writer of `partition_index` under `dir` on `node` to a
  channel.

  `writer` is the pid the sender got from its `:erpc` attach. A transport
  that addresses the writer by pid returns it; one that addresses it by name
  ignores it.
  """
  @callback channel(node(), Path.t(), non_neg_integer(), pid()) :: channel()

  @doc """
  Puts one already-framed batch on `channel`.

  The batch starts at WAL byte `offset` in `epoch`. `ref` is the sender's
  attach reference, which the writer echoes on the ack so the sender can
  drop an ack that belongs to an earlier attach. The writer answers
  `reply_to` over distribution.

  Batches must arrive in the order they are sent. The writer appends a batch
  only when it lands exactly at its tail, so a reordered pair costs a full
  resync.
  """
  @callback send_batch(
              channel(),
              reference(),
              non_neg_integer(),
              non_neg_integer(),
              binary(),
              pid()
            ) :: :ok
end
