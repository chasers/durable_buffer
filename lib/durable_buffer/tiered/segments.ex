defmodule DurableBuffer.Tiered.Segments do
  @moduledoc """
  Local segment files for `DurableBuffer.Backend.Tiered`.

  A partition keeps its segments under `<dir>/p<index>/`. Each segment is a
  file of framed `DurableBuffer.WAL` entries, named by the logical offset of
  its first entry and zero-padded to the width `DurableBuffer.Backend.S3`
  uses for its keys, so a local file name and its S3 key name the same
  segment.

  The `uploaded` file beside them holds the offset through which S3 has the
  partition's data. It is written to a sibling and renamed, so a crash leaves
  the previous watermark whole.
  """

  @offset_width 12

  @type segment :: %{offset: non_neg_integer(), path: Path.t()}

  @doc """
  The directory that holds `partition_index`'s segments under `dir`.
  """
  @spec partition_dir(Path.t(), non_neg_integer()) :: Path.t()
  def partition_dir(dir, partition_index) do
    Path.join(dir, "p#{partition_index}")
  end

  @doc """
  The path of the segment whose first entry is `offset`.
  """
  @spec path(Path.t(), non_neg_integer()) :: Path.t()
  def path(partition_dir, offset) do
    padded = offset |> Integer.to_string() |> String.pad_leading(@offset_width, "0")
    Path.join(partition_dir, padded <> ".wal")
  end

  @doc """
  Lists the partition's segments in offset order.
  """
  @spec list(Path.t()) :: [segment()]
  def list(partition_dir) do
    case File.ls(partition_dir) do
      {:ok, names} ->
        names
        |> Enum.filter(&String.ends_with?(&1, ".wal"))
        |> Enum.flat_map(&describe(partition_dir, &1))
        |> Enum.sort_by(& &1.offset)

      {:error, :enoent} ->
        []
    end
  end

  defp describe(partition_dir, name) do
    case name |> Path.basename(".wal") |> Integer.parse() do
      {offset, ""} -> [%{offset: offset, path: Path.join(partition_dir, name)}]
      _not_a_segment -> []
    end
  end

  @doc """
  Pairs each segment with the offset after its last entry: the next
  segment's offset, or `last_end` for the highest one.
  """
  @spec with_ends([segment()], non_neg_integer() | :infinity) :: [
          %{offset: non_neg_integer(), end: non_neg_integer() | :infinity, path: Path.t()}
        ]
  def with_ends(segments, last_end) do
    ends = Enum.map(Enum.drop(segments, 1), & &1.offset) ++ [last_end]

    Enum.zip_with(segments, ends, fn segment, segment_end ->
      Map.put(segment, :end, segment_end)
    end)
  end

  @doc """
  Reads the upload watermark, or `nil` when none was written.
  """
  @spec read_watermark(Path.t()) :: non_neg_integer() | nil
  def read_watermark(partition_dir) do
    with {:ok, body} <- File.read(watermark_path(partition_dir)),
         {offset, ""} <- body |> String.trim() |> Integer.parse() do
      offset
    else
      _missing_or_corrupt -> nil
    end
  end

  @doc """
  Durably stores the upload watermark.
  """
  @spec store_watermark!(Path.t(), non_neg_integer()) :: :ok
  def store_watermark!(partition_dir, offset) do
    path = watermark_path(partition_dir)
    temporary = path <> ".tmp"
    {:ok, fd} = :file.open(temporary, [:write, :raw, :binary])
    :ok = :file.write(fd, Integer.to_string(offset))
    :ok = :file.datasync(fd)
    :ok = :file.close(fd)
    File.rename!(temporary, path)
  end

  defp watermark_path(partition_dir), do: Path.join(partition_dir, "uploaded")
end
