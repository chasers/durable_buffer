defmodule DurableBuffer.Compression do
  @moduledoc """
  Codecs for data written to object storage.

  `:zstd` needs the `:zstd` module that ships with OTP 28 and later. It runs
  at level 1: on log-like data that is about twice as fast as zstd's default
  level 3 and compresses as well or better.
  """

  @type codec :: :zstd | :gzip | :none

  @codecs [:zstd, :gzip, :none]
  @zstd_level 1

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
  Decompresses `data` that was written with `codec`.
  """
  @spec decompress(binary(), codec()) :: binary()
  def decompress(data, :none), do: data
  def decompress(data, :gzip), do: :zlib.gunzip(data)
  def decompress(data, :zstd), do: data |> :zstd.decompress() |> IO.iodata_to_binary()

  defp zstd_available? do
    Code.ensure_loaded?(:zstd) and function_exported?(:zstd, :compress, 1)
  end
end
