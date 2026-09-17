defmodule DurableBuffer.Queue.Consumer do
  @moduledoc """
  The single consumer of a queue manifest, as in Open Data Buffer.

  `open/2` increments the manifest's epoch. From then on every manifest read
  and write checks it, and returns `{:error, :fenced}` once another consumer
  has opened the manifest. So a consumer that outlives a failover can never
  ack entries away from its replacement.

  Two ways to read:

    * **Serial.** `next_batch/1` fetches the next batch after the read cursor.
      `ack/2` acknowledges exactly the next sequence. Every `:ack_interval`
      acks (default 100) the acked entries leave the manifest in one
      compare-and-set write; `flush/1` does it at once.
    * **Read-ahead.** `next_descriptors/2` hands out up to `max` entries
      without fetching. `fetch/2` downloads one, and any process may call it.
      `ack_through/2` removes every entry through a sequence in one write.
      Ack only the highest sequence below which everything is processed.

  Acks that were not flushed are lost if the consumer stops, and those
  batches are delivered again: delivery is at-least-once. The consumer never
  deletes batch objects; `DurableBuffer.Queue.GC` does, after a grace period.
  """

  alias DurableBuffer.Queue.Batch
  alias DurableBuffer.Queue.Manifest
  alias DurableBuffer.Queue.Store

  defstruct [
    :config,
    :req,
    :epoch,
    :last_acked,
    :handed_out,
    ack_interval: 100,
    unflushed: 0
  ]

  @type t :: %__MODULE__{}

  @type descriptor :: Manifest.entry()

  @type batch :: %{
          sequence: non_neg_integer(),
          location: String.t(),
          metadata: [Manifest.metadata()],
          entries: [binary()]
        }

  @doc """
  Opens the manifest as its only consumer, fencing any earlier one.

  Options:

    * `:last_acked` — resume after this sequence. Without it the consumer
      starts at the oldest entry in the manifest.
    * `:ack_interval` — acks between manifest writes, default 100.
  """
  @spec open(map(), keyword()) :: {:ok, t()} | {:error, term()}
  def open(config, opts \\ []) do
    req = Store.req(config)

    result =
      Store.update_manifest(req, config, fn manifest ->
        epoch = Manifest.next_epoch(manifest)
        {:write, Manifest.set_epoch(manifest, epoch), epoch}
      end)

    with {:ok, epoch, _conflicts} <- result do
      last_acked = Keyword.get(opts, :last_acked)

      {:ok,
       %__MODULE__{
         config: config,
         req: req,
         epoch: epoch,
         last_acked: last_acked,
         handed_out: last_acked,
         ack_interval: Keyword.get(opts, :ack_interval, 100)
       }}
    end
  end

  @doc """
  Fetches the batch after the read cursor. Returns `nil` when the queue holds
  nothing more. The cursor moves only when the fetch succeeds.
  """
  @spec next_batch(t()) :: {:ok, batch() | nil, t()} | {:error, term()}
  def next_batch(consumer) do
    with {:ok, descriptors} <- descriptors_after(consumer, consumer.handed_out, 1) do
      case descriptors do
        [] ->
          {:ok, nil, consumer}

        [descriptor] ->
          with {:ok, batch} <- fetch(consumer, descriptor) do
            {:ok, batch, hand_out(consumer, [descriptor])}
          end
      end
    end
  end

  @doc """
  Hands out up to `max` entries after the read cursor, without fetching them.
  """
  @spec next_descriptors(t(), pos_integer()) :: {:ok, [descriptor()], t()} | {:error, term()}
  def next_descriptors(consumer, max) do
    with {:ok, descriptors} <- descriptors_after(consumer, consumer.handed_out, max) do
      {:ok, descriptors, hand_out(consumer, descriptors)}
    end
  end

  @doc """
  Downloads and decodes the batch a descriptor names. It touches no consumer
  state, so read-ahead workers may call it concurrently. The first argument
  is a consumer or a queue config.
  """
  @spec fetch(t() | map(), descriptor()) :: {:ok, batch()} | {:error, term()}
  def fetch(%__MODULE__{config: config, req: req}, descriptor), do: fetch(config, req, descriptor)
  def fetch(config, descriptor), do: fetch(config, Store.req(config), descriptor)

  defp fetch(config, req, descriptor) do
    with {:ok, body} <- Store.get_batch(req, config, descriptor.location),
         {:ok, entries} <- Batch.decode(body) do
      {:ok, Map.put(descriptor, :entries, entries)}
    end
  end

  @doc """
  Acknowledges `sequence`, which must be the one right after the last ack and
  already handed out. Writes the manifest every `:ack_interval` acks.
  """
  @spec ack(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  def ack(consumer, sequence) do
    expected = if consumer.last_acked, do: consumer.last_acked + 1, else: sequence

    cond do
      sequence != expected ->
        {:error, {:out_of_order_ack, expected, sequence}}

      consumer.handed_out == nil or sequence > consumer.handed_out ->
        {:error, {:not_handed_out, sequence}}

      true ->
        consumer = %{consumer | last_acked: sequence, unflushed: consumer.unflushed + 1}

        if consumer.unflushed >= consumer.ack_interval, do: flush(consumer), else: {:ok, consumer}
    end
  end

  @doc """
  Acknowledges every entry through `sequence` and removes them from the
  manifest in one write.
  """
  @spec ack_through(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  def ack_through(consumer, sequence) do
    cond do
      consumer.last_acked != nil and sequence <= consumer.last_acked ->
        {:error, {:non_monotonic_ack, consumer.last_acked, sequence}}

      consumer.handed_out == nil or sequence > consumer.handed_out ->
        {:error, {:not_handed_out, sequence}}

      true ->
        with {:ok, _removed} <- dequeue(consumer, sequence) do
          {:ok, %{consumer | last_acked: sequence, unflushed: 0}}
        end
    end
  end

  @doc """
  Removes every acked entry from the manifest now.
  """
  @spec flush(t()) :: {:ok, t()} | {:error, term()}
  def flush(%{unflushed: 0} = consumer), do: {:ok, consumer}

  def flush(consumer) do
    with {:ok, _removed} <- dequeue(consumer, consumer.last_acked) do
      {:ok, %{consumer | unflushed: 0}}
    end
  end

  defp dequeue(consumer, through) do
    result =
      Store.update_manifest(consumer.req, consumer.config, fn manifest ->
        if manifest.epoch == consumer.epoch do
          {manifest, removed} = Manifest.dequeue(manifest, through)
          {:write, manifest, removed}
        else
          {:error, :fenced}
        end
      end)

    with {:ok, removed, _conflicts} <- result, do: {:ok, removed}
  end

  defp descriptors_after(consumer, after_sequence, max) do
    with {:ok, manifest, _version} <- Store.read_manifest(consumer.req, consumer.config) do
      if manifest.epoch == consumer.epoch do
        {:ok,
         manifest
         |> Manifest.entries()
         |> Enum.drop_while(&(after_sequence != nil and &1.sequence <= after_sequence))
         |> Enum.take(max)}
      else
        {:error, :fenced}
      end
    end
  end

  defp hand_out(consumer, []), do: consumer

  defp hand_out(consumer, descriptors) do
    first = hd(descriptors).sequence
    last = List.last(descriptors).sequence
    last_acked = if consumer.last_acked == nil, do: first - 1, else: consumer.last_acked
    %{consumer | handed_out: last, last_acked: last_acked}
  end
end
