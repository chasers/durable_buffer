defmodule DurableBuffer.Queue.Batch do
  @moduledoc """
  Data batch objects, in the Open Data Buffer version 1 binary format.

      record block (optionally compressed): (len u32 LE | data)*
      footer (7 bytes, never compressed):   compression u8 | record_count u32 LE | version u16 LE

  Compression code 0 is none and 1 is zstd. The format has no code for gzip.
  """

  alias DurableBuffer.Compression

  @version 1

  @type codec :: :zstd | :none

  @doc """
  Encodes `records` as a batch compressed with `codec`.
  """
  @spec encode([binary()], codec()) :: binary()
  def encode(records, codec) do
    block = for record <- records, do: [<<byte_size(record)::32-little>>, record]
    body = Compression.compress(block, codec)
    <<body::binary, code(codec)::8, length(records)::32-little, @version::16-little>>
  end

  @doc """
  Decodes a batch into its records.
  """
  @spec decode(binary()) :: {:ok, [binary()]} | {:error, term()}
  def decode(binary) when byte_size(binary) >= 7 do
    body = binary_part(binary, 0, byte_size(binary) - 7)
    <<code::8, count::32-little, version::16-little>> = binary_part(binary, byte_size(binary), -7)

    with :ok <- check_version(version),
         {:ok, codec} <- codec(code),
         records = decode_records(Compression.decompress(body, codec), []),
         :ok <- check_count(records, count) do
      {:ok, records}
    end
  end

  def decode(_binary), do: {:error, :truncated_batch}

  @doc """
  Validates a codec for this format.
  """
  @spec validate!(term()) :: codec()
  def validate!(codec) when codec in [:zstd, :none], do: Compression.validate!(codec)

  def validate!(other) do
    raise ArgumentError,
          ":compression must be :zstd or :none for the queue backend, got #{inspect(other)}"
  end

  defp code(:none), do: 0
  defp code(:zstd), do: 1

  defp codec(0), do: {:ok, :none}
  defp codec(1), do: {:ok, :zstd}
  defp codec(code), do: {:error, {:unsupported_compression, code}}

  defp check_version(@version), do: :ok
  defp check_version(version), do: {:error, {:unsupported_batch_version, version}}

  defp check_count(records, count) when length(records) == count, do: :ok
  defp check_count(records, count), do: {:error, {:record_count_mismatch, length(records), count}}

  defp decode_records(<<len::32-little, record::binary-size(len), rest::binary>>, acc) do
    decode_records(rest, [record | acc])
  end

  defp decode_records(_rest, acc), do: Enum.reverse(acc)
end
