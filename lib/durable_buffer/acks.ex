defmodule DurableBuffer.Acks do
  @moduledoc """
  Per-partition consumer ack positions, stored as `p<index>.acks` next to the
  partition WAL.

  A map of consumer id to the offset that consumer has processed through,
  inclusive, framed as `<<crc32::32, term_to_binary(map)>>`. The whole map is
  rewritten on every ack; it holds one small entry per named consumer, and
  consumers ack at their own cadence rather than per entry.

  A missing or corrupt file reads as an empty map. That is the safe
  direction: a lost ack replays entries a consumer already handled, which a
  consumer must tolerate anyway, whereas an invented ack would drop them.
  """

  @type t :: %{term() => non_neg_integer()}

  @doc """
  Loads the ack map for `{dir, partition_index}`, defaulting to empty.
  """
  @spec load(Path.t(), non_neg_integer()) :: t()
  def load(dir, partition_index) do
    with {:ok, <<crc::32-big, body::binary>>} <- File.read(path(dir, partition_index)),
         true <- :erlang.crc32(body) == crc,
         {:ok, acks} <- decode(body) do
      acks
    else
      _unusable -> %{}
    end
  end

  @doc """
  Writes the ack map for `{dir, partition_index}`.

  `fsync: false` skips the `datasync`, matching the buffer's durability
  stance: a buffer that does not sync its data has no reason to sync a
  position into it.
  """
  @spec store!(Path.t(), non_neg_integer(), t(), boolean()) :: :ok
  def store!(dir, partition_index, acks, fsync?) do
    path = path(dir, partition_index)
    File.mkdir_p!(Path.dirname(path))
    body = :erlang.term_to_binary(acks)
    {:ok, fd} = :file.open(path, [:write, :raw, :binary])
    :ok = :file.write(fd, [<<:erlang.crc32(body)::32-big>>, body])
    if fsync?, do: :ok = :file.datasync(fd)
    :ok = :file.close(fd)
  end

  @doc """
  Discards every ack for `{dir, partition_index}`.
  """
  @spec reset(Path.t(), non_neg_integer()) :: :ok
  def reset(dir, partition_index) do
    _ignored = File.rm(path(dir, partition_index))
    :ok
  end

  defp decode(body) do
    {:ok, :erlang.binary_to_term(body, [:safe])}
  rescue
    ArgumentError -> :error
  end

  defp path(dir, partition_index) do
    Path.join(dir, "p#{partition_index}.acks")
  end
end
