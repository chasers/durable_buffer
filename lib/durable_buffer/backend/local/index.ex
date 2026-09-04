defmodule DurableBuffer.Backend.Local.Index do
  @moduledoc """
  Sparse seek index for a local WAL, stored as `p<index>.idx`.

  One fixed-size record per group commit,
  `<<first_offset::64, byte_pos::64, crc32::32>>`: the logical offset of the
  batch's first entry, and the WAL byte position the batch starts at. A seek
  binary-searches for the last record at or before the wanted offset, starts
  the read there, and skips the few entries in between.

  The index is a pure cache. It is written on the commit path but never
  `datasync`ed, and any record that disagrees with the WAL is dropped when
  the partition opens. A missing, stale, torn or corrupt index costs a scan
  from the start of the log — never a wrong answer.
  """

  @record_size 20

  @type handle :: %{fd: :file.fd() | nil, path: Path.t()}

  @doc """
  Opens the index for appending, first dropping any record the WAL does not
  back — records past `wal_size`, and a torn partial record at the end.
  """
  @spec open(Path.t(), non_neg_integer(), non_neg_integer()) :: handle()
  def open(dir, partition_index, wal_size) do
    path = path(dir, partition_index)
    File.mkdir_p!(Path.dirname(path))
    trim(path, wal_size)

    case :file.open(path, [:append, :raw, :binary]) do
      {:ok, fd} -> %{fd: fd, path: path}
      {:error, _reason} -> %{fd: nil, path: path}
    end
  end

  @doc """
  Records that the batch starting at `byte_pos` begins at `first_offset`.

  Failures are ignored: the index is a cache, and a partition must not fail
  a commit because its index could not be written.
  """
  @spec append(handle(), non_neg_integer(), non_neg_integer()) :: :ok
  def append(%{fd: nil}, _first_offset, _byte_pos), do: :ok

  def append(%{fd: fd}, first_offset, byte_pos) do
    body = <<first_offset::64-big, byte_pos::64-big>>
    _ignored = :file.write(fd, [body, <<:erlang.crc32(body)::32-big>>])
    :ok
  end

  @doc """
  Returns `{byte_pos, first_offset}` for the last batch starting at or
  before `from`, or `nil` when the index cannot answer.
  """
  @spec seek(Path.t(), non_neg_integer(), non_neg_integer()) ::
          {non_neg_integer(), non_neg_integer()} | nil
  def seek(dir, partition_index, from) do
    path = path(dir, partition_index)

    with {:ok, %{size: size}} <- File.stat(path),
         records when records > 0 <- div(size, @record_size),
         {:ok, fd} <- :file.open(path, [:read, :raw, :binary]) do
      found = floor_record(fd, 0, records - 1, from, nil)
      :ok = :file.close(fd)
      found
    else
      _unusable -> nil
    end
  end

  @doc """
  Discards the index, then reopens it empty.
  """
  @spec reset(handle()) :: handle()
  def reset(%{path: nil} = handle), do: handle

  def reset(handle) do
    close(handle)
    _ignored = File.rm(handle.path)

    case :file.open(handle.path, [:append, :raw, :binary]) do
      {:ok, fd} -> %{handle | fd: fd}
      {:error, _reason} -> %{handle | fd: nil}
    end
  end

  @spec close(handle()) :: :ok
  def close(%{fd: nil}), do: :ok
  def close(%{fd: fd}), do: :file.close(fd)

  defp floor_record(_fd, low, high, _from, best) when low > high, do: best

  defp floor_record(fd, low, high, from, best) do
    middle = div(low + high, 2)

    case read_record(fd, middle) do
      {:ok, {first_offset, byte_pos}} when first_offset <= from ->
        floor_record(fd, middle + 1, high, from, {byte_pos, first_offset})

      {:ok, _later} ->
        floor_record(fd, low, middle - 1, from, best)

      :error ->
        best
    end
  end

  defp read_record(fd, position) do
    case :file.pread(fd, position * @record_size, @record_size) do
      {:ok, <<first_offset::64-big, byte_pos::64-big, crc::32-big>>} ->
        body = <<first_offset::64-big, byte_pos::64-big>>

        if :erlang.crc32(body) == crc do
          {:ok, {first_offset, byte_pos}}
        else
          :error
        end

      _torn_or_missing ->
        :error
    end
  end

  defp trim(path, wal_size) do
    case File.stat(path) do
      {:ok, %{size: size}} -> trim_to(path, div(size, @record_size), wal_size)
      {:error, _reason} -> :ok
    end
  end

  defp trim_to(_path, 0, _wal_size), do: :ok

  defp trim_to(path, records, wal_size) do
    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    keep = backed_records(fd, 0, records - 1, wal_size, 0)
    {:ok, _position} = :file.position(fd, keep * @record_size)
    :ok = :file.truncate(fd)
    :ok = :file.close(fd)
  end

  defp backed_records(_fd, low, high, _wal_size, keep) when low > high, do: keep

  defp backed_records(fd, low, high, wal_size, keep) do
    middle = div(low + high, 2)

    case read_record(fd, middle) do
      {:ok, {_first_offset, byte_pos}} when byte_pos < wal_size ->
        backed_records(fd, middle + 1, high, wal_size, middle + 1)

      {:ok, _past_the_wal} ->
        backed_records(fd, low, middle - 1, wal_size, keep)

      :error ->
        keep
    end
  end

  defp path(dir, partition_index) do
    Path.join(dir, "p#{partition_index}.idx")
  end
end
