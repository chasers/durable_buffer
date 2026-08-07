defmodule DurableBuffer.Epoch do
  @moduledoc """
  Per-partition epoch persistence for the replicated backend.

  The epoch is a monotonic counter bumped on every truncate and stored in a
  `p<index>.meta` sidecar file next to the partition WAL, framed as
  `<<epoch::64-big, crc32::32-big>>`. Replicated batches are stamped with the
  epoch, so a replica that missed a truncate rejects appends from the new
  epoch instead of silently mixing data across truncations. A missing or
  corrupt meta file reads as epoch 0 — the safe direction, since a stale
  epoch is rejected by peers rather than accepted.
  """

  @doc """
  Loads the persisted epoch for `{dir, partition_index}`, defaulting to 0.
  """
  @spec load(Path.t(), non_neg_integer()) :: non_neg_integer()
  def load(dir, partition_index) do
    with {:ok, <<epoch::64-big, crc::32-big>>} <- File.read(path(dir, partition_index)),
         true <- :erlang.crc32(<<epoch::64-big>>) == crc do
      epoch
    else
      _other -> 0
    end
  end

  @doc """
  Durably persists the epoch for `{dir, partition_index}`.
  """
  @spec store!(Path.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def store!(dir, partition_index, epoch) do
    path = path(dir, partition_index)
    File.mkdir_p!(Path.dirname(path))
    bin = <<epoch::64-big>>
    {:ok, fd} = :file.open(path, [:write, :raw, :binary])
    :ok = :file.write(fd, [bin, <<:erlang.crc32(bin)::32-big>>])
    :ok = :file.datasync(fd)
    :ok = :file.close(fd)
  end

  defp path(dir, partition_index) do
    Path.join(dir, "p#{partition_index}.meta")
  end
end
