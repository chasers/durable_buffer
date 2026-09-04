defmodule DurableBuffer.Meta do
  @moduledoc """
  Per-partition metadata sidecar, stored as `p<index>.meta` next to the
  partition WAL.

  Holds three numbers:

    * `epoch` — monotonic counter bumped on every truncate. Replicated
      batches are stamped with it, so a replica that missed a truncate
      rejects appends from the new epoch instead of silently mixing data
      across truncations.
    * `base_offset` — the first logical entry offset still retained. It
      advances on trim and on truncate, so an offset never means two
      different entries over the life of a partition.
    * `base_byte_offset` — the WAL byte offset the retained data starts at.
      Replication stamps batches with *logical* byte offsets, so trimming
      the primary's head does not shift them on the wire.

  Framed as `<<epoch::64, base_offset::64, base_byte_offset::64, crc32::32>>`.

  A missing or corrupt file reads as all zeroes — the safe direction, since
  a stale epoch is rejected by peers rather than accepted. The 12-byte
  `<<epoch::64, crc32::32>>` record written before 0.4.0 is still read, so
  an upgrade keeps its epoch instead of resyncing every replica.
  """

  defstruct epoch: 0, base_offset: 0, base_byte_offset: 0

  @type t :: %__MODULE__{
          epoch: non_neg_integer(),
          base_offset: non_neg_integer(),
          base_byte_offset: non_neg_integer()
        }

  @doc """
  Loads the metadata for `{dir, partition_index}`, defaulting to zeroes.
  """
  @spec load(Path.t(), non_neg_integer()) :: t()
  def load(dir, partition_index) do
    case File.read(path(dir, partition_index)) do
      {:ok, binary} -> decode(binary)
      {:error, _reason} -> %__MODULE__{}
    end
  end

  @doc """
  Returns just the epoch for `{dir, partition_index}`.
  """
  @spec epoch(Path.t(), non_neg_integer()) :: non_neg_integer()
  def epoch(dir, partition_index), do: load(dir, partition_index).epoch

  @doc """
  Durably persists `meta` for `{dir, partition_index}`.
  """
  @spec store!(Path.t(), non_neg_integer(), t()) :: :ok
  def store!(dir, partition_index, %__MODULE__{} = meta) do
    path = path(dir, partition_index)
    File.mkdir_p!(Path.dirname(path))
    body = encode(meta)
    {:ok, fd} = :file.open(path, [:write, :raw, :binary])
    :ok = :file.write(fd, [body, <<:erlang.crc32(body)::32-big>>])
    :ok = :file.datasync(fd)
    :ok = :file.close(fd)
  end

  @doc """
  Loads the metadata, applies `fun` to it, and persists the result.
  """
  @spec update!(Path.t(), non_neg_integer(), (t() -> t())) :: t()
  def update!(dir, partition_index, fun) do
    meta = dir |> load(partition_index) |> fun.()
    :ok = store!(dir, partition_index, meta)
    meta
  end

  defp encode(%__MODULE__{} = meta) do
    <<meta.epoch::64-big, meta.base_offset::64-big, meta.base_byte_offset::64-big>>
  end

  defp decode(<<body::binary-size(24), crc::32-big>>) do
    <<epoch::64-big, base_offset::64-big, base_byte_offset::64-big>> = body

    if :erlang.crc32(body) == crc do
      %__MODULE__{
        epoch: epoch,
        base_offset: base_offset,
        base_byte_offset: base_byte_offset
      }
    else
      %__MODULE__{}
    end
  end

  defp decode(<<body::binary-size(8), crc::32-big>>) do
    <<epoch::64-big>> = body

    if :erlang.crc32(body) == crc, do: %__MODULE__{epoch: epoch}, else: %__MODULE__{}
  end

  defp decode(_other), do: %__MODULE__{}

  defp path(dir, partition_index) do
    Path.join(dir, "p#{partition_index}.meta")
  end
end
