defmodule DurableBuffer.Queue.Manifest do
  @moduledoc """
  The queue manifest, in the Open Data Buffer version 1 binary format.

  A manifest is a run of entries followed by a 22-byte footer:

      entry:  entry_len u32 LE | sequence u64 LE | location_len u16 LE | location
              | metadata_count u32 LE
              | metadata*: start_index u32 LE | ingestion_time_ms i64 LE
                           | payload_len u32 LE | payload
      footer: entry_count u32 LE | next_sequence u64 LE | epoch u64 LE | version u16 LE

  `append/2` adds entries without decoding the existing ones, so an append
  costs the same whatever the queue length. Sequences are contiguous and
  assigned on append. The epoch fences consumers: `2^64 - 1` means no
  consumer has initialized the manifest.
  """

  import Bitwise

  defstruct body: <<>>, entry_count: 0, next_sequence: 0, epoch: 0xFFFFFFFFFFFFFFFF

  @version 1
  @footer_size 22
  @uninitialized_epoch 0xFFFFFFFFFFFFFFFF

  @type metadata :: %{
          start_index: non_neg_integer(),
          ingestion_time_ms: integer(),
          payload: binary()
        }
  @type entry :: %{sequence: non_neg_integer(), location: String.t(), metadata: [metadata()]}
  @type t :: %__MODULE__{
          body: binary(),
          entry_count: non_neg_integer(),
          next_sequence: non_neg_integer(),
          epoch: non_neg_integer()
        }

  @doc """
  An empty manifest with no initialized consumer.
  """
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  The epoch value that means no consumer has initialized the manifest.
  """
  @spec uninitialized_epoch() :: non_neg_integer()
  def uninitialized_epoch, do: @uninitialized_epoch

  @doc """
  Parses a manifest's footer and keeps its entries as raw bytes.
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, term()}
  def decode(binary) when byte_size(binary) >= @footer_size do
    body_size = byte_size(binary) - @footer_size
    footer = binary_part(binary, body_size, @footer_size)

    case footer do
      <<count::32-little, next::64-little, epoch::64-little, @version::16-little>> ->
        {:ok,
         %__MODULE__{
           body: binary_part(binary, 0, body_size),
           entry_count: count,
           next_sequence: next,
           epoch: epoch
         }}

      <<_fields::binary-size(20), version::16-little>> ->
        {:error, {:unsupported_manifest_version, version}}
    end
  end

  def decode(_binary), do: {:error, :truncated_manifest}

  @doc """
  Serializes the manifest.
  """
  @spec encode(t()) :: iodata()
  def encode(manifest) do
    [
      manifest.body,
      <<manifest.entry_count::32-little, manifest.next_sequence::64-little,
        manifest.epoch::64-little, @version::16-little>>
    ]
  end

  @doc """
  Appends one entry per `{location, metadata}`, assigning each the next
  sequence. Returns the manifest and the sequences assigned, in order.
  """
  @spec append(t(), [{String.t(), [metadata()]}]) :: {t(), [non_neg_integer()]}
  def append(manifest, items) do
    {encoded, next} =
      Enum.map_reduce(items, manifest.next_sequence, fn {location, metadata}, sequence ->
        {encode_entry(sequence, location, metadata), sequence + 1}
      end)

    sequences = Enum.to_list(manifest.next_sequence..(next - 1)//1)

    {%{
       manifest
       | body: IO.iodata_to_binary([manifest.body | encoded]),
         entry_count: manifest.entry_count + length(items),
         next_sequence: next
     }, sequences}
  end

  @doc """
  Decodes every entry, oldest first.
  """
  @spec entries(t()) :: [entry()]
  def entries(manifest), do: decode_entries(manifest.body, [])

  @doc """
  Removes every entry with a sequence at or below `through`. Returns the
  manifest and the removed entries.
  """
  @spec dequeue(t(), non_neg_integer()) :: {t(), [entry()]}
  def dequeue(manifest, through) do
    {removed, rest} = split_through(manifest.body, through, [])

    {%{manifest | body: rest, entry_count: manifest.entry_count - length(removed)}, removed}
  end

  @doc """
  Sets the epoch.
  """
  @spec set_epoch(t(), non_neg_integer()) :: t()
  def set_epoch(manifest, epoch), do: %{manifest | epoch: epoch}

  @doc """
  The epoch a new consumer takes: one above the current epoch, skipping the
  uninitialized value on wrap.
  """
  @spec next_epoch(t()) :: non_neg_integer()
  def next_epoch(manifest) do
    case manifest.epoch + 1 &&& @uninitialized_epoch do
      @uninitialized_epoch -> 0
      epoch -> epoch
    end
  end

  defp encode_entry(sequence, location, metadata) do
    items =
      for item <- metadata do
        <<item.start_index::32-little, item.ingestion_time_ms::64-signed-little,
          byte_size(item.payload)::32-little, item.payload::binary>>
      end

    rest = [
      <<sequence::64-little, byte_size(location)::16-little>>,
      location,
      <<length(metadata)::32-little>>
      | items
    ]

    [<<IO.iodata_length(rest)::32-little>> | rest]
  end

  defp split_through(
         <<len::32-little, entry::binary-size(len), rest::binary>> = body,
         through,
         acc
       ) do
    <<sequence::64-little, _tail::binary>> = entry

    if sequence <= through do
      split_through(rest, through, [decode_entry(entry) | acc])
    else
      {Enum.reverse(acc), body}
    end
  end

  defp split_through(<<>>, _through, acc), do: {Enum.reverse(acc), <<>>}

  defp decode_entries(<<len::32-little, entry::binary-size(len), rest::binary>>, acc) do
    decode_entries(rest, [decode_entry(entry) | acc])
  end

  defp decode_entries(<<>>, acc), do: Enum.reverse(acc)

  defp decode_entry(
         <<sequence::64-little, location_len::16-little, location::binary-size(location_len),
           count::32-little, items::binary>>
       ) do
    %{sequence: sequence, location: location, metadata: decode_metadata(items, count, [])}
  end

  defp decode_metadata(_items, 0, acc), do: Enum.reverse(acc)

  defp decode_metadata(
         <<start::32-little, time::64-signed-little, len::32-little, payload::binary-size(len),
           rest::binary>>,
         count,
         acc
       ) do
    item = %{start_index: start, ingestion_time_ms: time, payload: payload}
    decode_metadata(rest, count - 1, [item | acc])
  end
end
