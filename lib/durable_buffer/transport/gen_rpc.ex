defmodule DurableBuffer.Transport.GenRPC do
  @moduledoc """
  A `DurableBuffer.Transport` that ships replicated batches over
  [gen_rpc](https://github.com/emqx/gen_rpc), which gives each node pair a
  dedicated TCP socket outside Erlang distribution.

  Replication traffic then cannot starve the cluster heartbeat, `:global`,
  or any other application message on the pair. That is the whole point:
  distribution is one connection per pair, and a sender pushing batches
  through it head-of-line blocks everything else there.

  ## The dependency is yours to add

  `:durable_buffer` does not declare `gen_rpc`. The maintained fork is not
  published on Hex, so it can only be a git dependency, and Hex forbids a
  git dependency in a published package. Add it to your own application:

      {:gen_rpc, git: "https://github.com/emqx/gen_rpc.git", tag: "3.6.1"}

  `DurableBuffer.Backend.Replica.init_config/1` raises when this transport
  is configured and `:gen_rpc` is not loaded, so a missing dependency is an
  argument error at startup rather than a failure on the first commit.

  ## What it costs

    * **A port.** gen_rpc listens on its own TCP port on every node, primary
      and replica alike. Open it between the nodes.
    * **A TLS decision.** gen_rpc speaks plain TCP by default. Its own
      `ssl_server_options` / `ssl_client_options` configure TLS, and
      distribution's TLS settings do not apply to it.
    * **A different place for backpressure.** `send/2` to a remote pid
      blocks the sender when the distribution buffer fills.
      `:gen_rpc.ordered_cast/4` does not block the caller: it hands the
      payload to gen_rpc's client process, whose mailbox is unbounded, and
      the TCP send blocks that process instead. In-flight bytes stay bounded
      — `max_sender_bytes` still caps the unacked queue and the sender drops
      it and re-attaches on overflow — but the bytes wait somewhere else.

  ## Ordering

  This uses `ordered_cast/4`, not `cast/4`. `cast/4` spawns a process per
  message on the receiving side, so concurrent casts race; `ordered_cast/4`
  runs them on the acceptor and blocks it until each returns, which
  serialises delivery. A replica appends a batch only when it lands exactly
  at its WAL tail, so a reordered pair costs a full resync.

  `ordered_cast/4` refuses a bare node and demands a `{node, tag}`
  destination, because one queue per node would put every caller behind
  every other. The tag is `{dir, partition_index}`, so each partition gets
  its own connection, its own acceptor and its own order. Partitions
  therefore never block each other, and ordering holds where it is needed —
  within one partition's channel, which is the only place the writer's tail
  check cares about.

  Because the acceptor blocks, `DurableBuffer.Replica.replicate/7` does the
  least possible work: it resolves the writer and sends it one message.
  """

  @behaviour DurableBuffer.Transport

  @doc """
  Returns whether `:gen_rpc` is available in this runtime.
  """
  @spec available?() :: boolean()
  def available? do
    Code.ensure_loaded?(:gen_rpc) and function_exported?(:gen_rpc, :ordered_cast, 4)
  end

  @impl DurableBuffer.Transport
  def channel(node, dir, partition_index, _writer), do: {node, dir, partition_index}

  @impl DurableBuffer.Transport
  def send_batch({node, dir, partition_index}, ref, epoch, offset, batch, reply_to) do
    :gen_rpc.ordered_cast(
      {node, {dir, partition_index}},
      DurableBuffer.Replica,
      :replicate,
      [dir, partition_index, ref, epoch, offset, batch, reply_to]
    )

    :ok
  end
end
