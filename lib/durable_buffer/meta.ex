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

  A missing or corrupt file reads as all zeroes. For the epoch alone that is
  the safe direction, since peers reject a stale epoch rather than accept it.
  It is *not* safe for the two bases: a reset base renumbers the partition
  from zero, which is the "same offset, different data" hazard logical
  offsets exist to prevent. So the record is never written in place —
  `store!/3` writes a sibling and renames, and a crash leaves the previous
  record whole rather than a zero-byte file.

  A trim spans two durable steps, the WAL rewrite and this record. The
  sidecar written by `store_pending!/3` names the base a trim is moving to,
  so `DurableBuffer.Backend.Local` can finish or discard an interrupted one
  when the partition opens.
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
    write_framed!(path, encode(meta))
  end

  @doc """
  Records the base a trim is moving to, before the WAL rewrite that gets it
  there.

  A trim renames the retained suffix over the WAL and then writes the new
  base here, which is two durable steps with a window between them. This
  sidecar closes it: `pending/2` reports an interrupted trim when the
  partition opens, and the presence of the WAL's temporary file says which
  side of the rename the crash fell on.
  """
  @spec store_pending!(Path.t(), non_neg_integer(), t()) :: :ok
  def store_pending!(dir, partition_index, %__MODULE__{} = meta) do
    path = pending_path(dir, partition_index)
    File.mkdir_p!(Path.dirname(path))
    write_framed!(path, encode(meta))
  end

  @doc """
  The base an interrupted trim was moving to, or `nil` when no trim is
  pending. A corrupt sidecar reads as `nil`: the trim then did not happen,
  which keeps data rather than dropping it.
  """
  @spec pending(Path.t(), non_neg_integer()) :: t() | nil
  def pending(dir, partition_index) do
    case File.read(pending_path(dir, partition_index)) do
      {:ok, binary} -> decode_pending(binary)
      {:error, _reason} -> nil
    end
  end

  @doc """
  Discards the pending-trim sidecar.
  """
  @spec clear_pending(Path.t(), non_neg_integer()) :: :ok
  def clear_pending(dir, partition_index) do
    _ignored = File.rm(pending_path(dir, partition_index))
    :ok
  end

  defp write_framed!(path, body) do
    temp = path <> ".new"
    {:ok, fd} = :file.open(temp, [:write, :raw, :binary])
    :ok = :file.write(fd, [body, <<:erlang.crc32(body)::32-big>>])
    :ok = :file.datasync(fd)
    :ok = :file.close(fd)
    :ok = File.rename(temp, path)
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

  defp decode(_other), do: %__MODULE__{}

  defp decode_pending(<<body::binary-size(24), crc::32-big>>) do
    if :erlang.crc32(body) == crc, do: decode(<<body::binary, crc::32-big>>)
  end

  defp decode_pending(_other), do: nil

  defp path(dir, partition_index) do
    Path.join(dir, "p#{partition_index}.meta")
  end

  defp pending_path(dir, partition_index) do
    Path.join(dir, "p#{partition_index}.trim")
  end
end
