defmodule DurableBuffer.WAL do
  @moduledoc """
  Write-ahead-log entry framing.

  Each entry is `<<len::32-big, crc32::32-big, payload::binary-size(len)>>`
  where `crc32` is `:erlang.crc32/1` of the payload. Decoding validates the
  CRC and treats any short or corrupt trailing bytes as a torn write.
  """

  @header_size 8

  @doc """
  Encodes a payload into a framed WAL entry.
  """
  @spec encode(iodata()) :: {iodata(), pos_integer()}
  def encode(payload) do
    bin = IO.iodata_to_binary(payload)
    len = byte_size(bin)
    {[<<len::32-big, :erlang.crc32(bin)::32-big>>, bin], @header_size + len}
  end

  @doc """
  Decodes as many complete, CRC-valid entries as possible from the front of
  the binary.

  Returns `{payloads, valid_byte_size, rest}` where `valid_byte_size` is the
  number of bytes occupied by the decoded entries and `rest` is the remaining
  undecodable tail (empty when the log is clean).
  """
  @spec decode_all(binary()) :: {[binary()], non_neg_integer(), binary()}
  def decode_all(binary) do
    decode_all(binary, [], 0)
  end

  defp decode_all(
         <<len::32-big, crc::32-big, payload::binary-size(len), rest::binary>> = bin,
         acc,
         valid
       ) do
    if :erlang.crc32(payload) == crc do
      decode_all(rest, [payload | acc], valid + @header_size + len)
    else
      {Enum.reverse(acc), valid, bin}
    end
  end

  defp decode_all(rest, acc, valid) do
    {Enum.reverse(acc), valid, rest}
  end

  @doc """
  Reads a WAL file, truncating any torn tail in place.

  Returns the byte offset at which the next entry should be appended. Missing
  files are treated as empty logs.
  """
  @spec recover!(Path.t()) :: non_neg_integer()
  def recover!(path) do
    case File.read(path) do
      {:ok, contents} ->
        {_payloads, valid, rest} = decode_all(contents)

        if rest != "" do
          truncate!(path, valid)
        end

        valid

      {:error, :enoent} ->
        0
    end
  end

  defp truncate!(path, valid) do
    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    {:ok, _} = :file.position(fd, valid)
    :ok = :file.truncate(fd)
    :ok = :file.close(fd)
  end
end
