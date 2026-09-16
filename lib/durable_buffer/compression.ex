defmodule DurableBuffer.Compression do
  @moduledoc """
  Codecs for segment objects stored in S3.

  The codec is part of the object key: `<offset>.wal` is plain,
  `<offset>.wal.zst` is zstd and `<offset>.wal.gz` is gzip. A reader picks
  the decoder from the key, so objects written with different codecs can sit
  side by side under one prefix.

  `:zstd` needs the `:zstd` module that ships with OTP 28 and later. It runs
  at level 1: on log-like data that is about twice as fast as zstd's default
  level 3 and compresses as well or better.
  """

  @type codec :: :zstd | :gzip | :none

  @codecs [:zstd, :gzip, :none]
  @zstd_level 1

  @doc """
  The default codec: `:zstd` when the runtime has it, `:gzip` otherwise.
  """
  @spec default() :: :zstd | :gzip
  def default do
    if zstd_available?(), do: :zstd, else: :gzip
  end

  @doc """
  Returns `codec`, or raises `ArgumentError` when it is unknown or the
  runtime cannot use it.
  """
  @spec validate!(term()) :: codec()
  def validate!(:zstd) do
    unless zstd_available?() do
      raise ArgumentError,
            "compression: :zstd needs the :zstd module from OTP 28 or later. " <>
              "Use compression: :gzip or compression: :none on this runtime."
    end

    :zstd
  end

  def validate!(codec) when codec in @codecs, do: codec

  def validate!(other) do
    raise ArgumentError,
          ":compression must be one of #{inspect(@codecs)}, got #{inspect(other)}"
  end

  @doc """
  Compresses `data` with `codec`.
  """
  @spec compress(iodata(), codec()) :: binary()
  def compress(data, :none), do: IO.iodata_to_binary(data)
  def compress(data, :gzip), do: :zlib.gzip(data)

  def compress(data, :zstd) do
    data |> :zstd.compress(%{compressionLevel: @zstd_level}) |> IO.iodata_to_binary()
  end

  @doc """
  Reads the file at `path` in chunks of `chunk_bytes` and compresses it with
  `codec`.

  Each chunk is one short call into the codec, so a large segment never holds
  a scheduler for the whole file.
  """
  @spec compress_file(Path.t(), codec(), pos_integer()) :: {:ok, binary()} | {:error, term()}
  def compress_file(path, codec, chunk_bytes \\ 1_048_576) do
    with {:ok, fd} <- :file.open(path, [:read, :raw, :binary]) do
      try do
        encoder = open_encoder(codec)
        result = encode_chunks(fd, encoder, chunk_bytes, [])
        close_encoder(encoder)
        result
      after
        :file.close(fd)
      end
    end
  end

  defp encode_chunks(fd, encoder, chunk_bytes, acc) do
    case :file.read(fd, chunk_bytes) do
      {:ok, chunk} -> encode_chunks(fd, encoder, chunk_bytes, [acc, encode(encoder, chunk)])
      :eof -> {:ok, IO.iodata_to_binary([acc, finish(encoder)])}
      {:error, reason} -> {:error, reason}
    end
  end

  defp open_encoder(:none), do: :none

  defp open_encoder(:gzip) do
    z = :zlib.open()
    :ok = :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)
    {:gzip, z}
  end

  defp open_encoder(:zstd) do
    {:ok, context} = :zstd.context(:compress, %{compressionLevel: @zstd_level})
    {:zstd, context}
  end

  defp encode(:none, chunk), do: chunk
  defp encode({:gzip, z}, chunk), do: :zlib.deflate(z, chunk)
  defp encode({:zstd, context}, chunk), do: zstd_stream(context, chunk, [])

  defp zstd_stream(context, input, acc) do
    case :zstd.stream(context, input) do
      {:continue, output} -> [acc, output]
      {:continue, remaining, output} -> zstd_stream(context, remaining, [acc, output])
    end
  end

  defp finish(:none), do: []
  defp finish({:gzip, z}), do: :zlib.deflate(z, [], :finish)
  defp finish({:zstd, context}), do: zstd_finish(context, [])

  defp zstd_finish(context, acc) do
    {:done, output} = :zstd.finish(context, [])
    [acc, output]
  end

  defp close_encoder({:gzip, z}), do: :zlib.close(z)
  defp close_encoder({:zstd, context}), do: :zstd.close(context)
  defp close_encoder(:none), do: :ok

  @doc """
  Decompresses `data` that was written with `codec`.
  """
  @spec decompress(binary(), codec()) :: binary()
  def decompress(data, :none), do: data
  def decompress(data, :gzip), do: :zlib.gunzip(data)
  def decompress(data, :zstd), do: data |> :zstd.decompress() |> IO.iodata_to_binary()

  @doc """
  The object key suffix for `codec`.
  """
  @spec extension(codec()) :: String.t()
  def extension(:none), do: ".wal"
  def extension(:gzip), do: ".wal.gz"
  def extension(:zstd), do: ".wal.zst"

  @doc """
  Splits a segment object key into its offset and codec, or returns `:error`
  when the key does not name a segment.
  """
  @spec parse_key(String.t()) :: {:ok, non_neg_integer(), codec()} | :error
  def parse_key(key) do
    name = Path.basename(key)

    with [digits, suffix] <- String.split(name, ".", parts: 2),
         {:ok, codec} <- codec_for("." <> suffix),
         {offset, ""} <- Integer.parse(digits) do
      {:ok, offset, codec}
    else
      _not_a_segment -> :error
    end
  end

  defp codec_for(".wal"), do: {:ok, :none}
  defp codec_for(".wal.gz"), do: {:ok, :gzip}
  defp codec_for(".wal.zst"), do: {:ok, :zstd}
  defp codec_for(_suffix), do: :error

  defp zstd_available? do
    Code.ensure_loaded?(:zstd) and function_exported?(:zstd, :compress, 1)
  end
end
