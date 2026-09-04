defmodule DurableBuffer.Backend.Local.Index do
  @moduledoc """
  Sparse seek index for a local WAL, stored as `p<index>.idx`.

  One fixed-size record per group commit,
  `<<first_offset::64, byte_pos::64, commit_ms::64, crc32::32>>`: the logical
  offset of the batch's first entry, the WAL byte position the batch starts
  at, and the wall-clock millisecond it committed. A seek binary-searches
  for the last record at or before the wanted offset, starts the read there,
  and skips the few entries in between. All three fields rise together,
  because commits are ordered, so a search by time or by byte position is
  the same binary search.

  For reads the index is a pure cache: it is written on the commit path, and
  any record that disagrees with the WAL is dropped when the partition
  opens. A missing, stale, torn or corrupt index costs a scan from the start
  of the log — never a wrong answer.

  Retention weakens that to "losing it delays retention", never "drops data
  early". Two things bound the damage:

    * A range the index cannot date is backfilled as *just written* when the
      partition opens, so undated data looks young rather than expired.
    * `sync/1` `datasync`s on an interval rather than per commit, so a crash
      leaves only the commits since the last sync undated.

  The record grew from 20 bytes to 28 in 0.4.0. An index written before that
  fails its CRC on the first read and is discarded, which costs one scan.
  """

  @record_size 28

  @type handle :: %{fd: :file.fd() | nil, path: Path.t()}

  @doc """
  Opens the index for appending, first dropping every record the WAL does
  not back: records past its tail, records below its retained base, and a
  torn partial record at the end.

  Dropping below the base matters because `trim/3` writes without a
  `datasync` while the metadata beside it is synced. A crash between the two
  can leave the base advanced and stale records on disk, and a record
  pointing into trimmed bytes would otherwise label a read with offsets
  lower than the entries actually carry.

  Retained data the surviving records do not date is then backfilled as
  written now. That errs toward keeping data: a lost index makes old data
  look young, never the reverse.

  Both steps read only the first record on the clean-restart path, where
  nothing is below the base and the head is already dated. A full read and
  rewrite happens only when there is something to change.
  """
  @spec open(
          Path.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: handle()
  def open(dir, partition_index, wal_tail, base_offset \\ 0, base_byte \\ 0) do
    path = path(dir, partition_index)
    File.mkdir_p!(Path.dirname(path))
    drop_unbacked(path, wal_tail)
    trim_path(path, base_offset, base_byte)
    backfill(path, base_offset, base_byte, wal_tail)

    case :file.open(path, [:append, :raw, :binary]) do
      {:ok, fd} -> %{fd: fd, path: path}
      {:error, _reason} -> %{fd: nil, path: path}
    end
  end

  @doc """
  Records that the batch starting at `byte_pos` begins at `first_offset` and
  committed at `commit_ms`.

  Failures are ignored: the index is a cache, and a partition must not fail
  a commit because its index could not be written.
  """
  @spec append(handle(), non_neg_integer(), non_neg_integer(), integer()) :: :ok
  def append(%{fd: nil}, _first_offset, _byte_pos, _commit_ms), do: :ok

  def append(%{fd: fd}, first_offset, byte_pos, commit_ms) do
    _ignored = :file.write(fd, encode({first_offset, byte_pos, commit_ms}))
    :ok
  end

  @doc """
  Flushes the index to disk.

  Called on an interval rather than per commit. The index is append-only, so
  one `datasync` covers every record written since the last one, and the
  cost stays off the per-commit path.
  """
  @spec sync(handle()) :: :ok
  def sync(%{fd: nil}), do: :ok

  def sync(%{fd: fd}) do
    _ignored = :file.datasync(fd)
    :ok
  end

  @doc """
  Returns `{byte_pos, first_offset}` for the last batch starting at or
  before `from`, or `nil` when the index cannot answer.
  """
  @spec seek(Path.t(), non_neg_integer(), non_neg_integer()) ::
          {non_neg_integer(), non_neg_integer()} | nil
  def seek(dir, partition_index, from) do
    with_records(dir, partition_index, nil, fn fd, records ->
      case floor_record(fd, 0, records - 1, from, nil) do
        {first_offset, byte_pos, _commit_ms} -> {byte_pos, first_offset}
        nil -> nil
      end
    end)
  end

  @doc """
  Returns `{:ok, first_offset}` for the oldest batch committed at or after
  `cutoff_ms` — the point a trim by age should cut at, since every entry
  below it committed earlier.

  When every dated batch is older than the cutoff, the last record's
  `first_offset` is returned instead of the log's tail. Batches the index
  does not cover are the newest ones, and dropping them on the strength of
  an older record's timestamp would discard data that may be young.

  Returns `:unknown` when the index holds no usable record.
  """
  @spec seek_time(Path.t(), non_neg_integer(), integer()) ::
          {:ok, non_neg_integer()} | :unknown
  def seek_time(dir, partition_index, cutoff_ms) do
    ceiling(dir, partition_index, cutoff_ms, 2)
  end

  @doc """
  Returns `{:ok, first_offset}` for the oldest batch starting at or after the
  logical byte position `target_byte` — the point a trim by size should cut
  at to leave no more than the wanted bytes.

  Falls back to the last record and to `:unknown` exactly as `seek_time/3`
  does, and for the same reason.
  """
  @spec seek_byte(Path.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | :unknown
  def seek_byte(dir, partition_index, target_byte) do
    ceiling(dir, partition_index, target_byte, 1)
  end

  @doc """
  The commit timestamp of the oldest record, or `:unknown` when the index
  holds none.
  """
  @spec oldest(Path.t(), non_neg_integer()) :: {:ok, integer()} | :unknown
  def oldest(dir, partition_index) do
    with_records(dir, partition_index, :unknown, fn fd, _records ->
      case read_record(fd, 0) do
        {:ok, {_first_offset, _byte_pos, commit_ms}} -> {:ok, commit_ms}
        :error -> :unknown
      end
    end)
  end

  @doc """
  Drops every record for data below `upto`, keeping the rest untouched.

  Record positions are logical byte offsets, and a trim does not move
  those, so the surviving records stay correct without being rewritten.

  When no surviving record starts exactly at `upto`, one is written for the
  new head carrying the timestamp of the batch it fell inside. The head
  keeps its real age across a trim rather than resetting to now.
  """
  @spec trim(handle(), non_neg_integer(), non_neg_integer()) :: handle()
  def trim(%{path: nil} = handle, _upto, _base_byte), do: handle

  def trim(handle, upto, base_byte) do
    close(handle)
    trim_path(handle.path, upto, base_byte)
    reopen(handle)
  end

  @doc """
  Discards the index, then reopens it empty.
  """
  @spec reset(handle()) :: handle()
  def reset(%{path: nil} = handle), do: handle

  def reset(handle) do
    close(handle)
    _ignored = File.rm(handle.path)
    reopen(handle)
  end

  @spec close(handle()) :: :ok
  def close(%{fd: nil}), do: :ok
  def close(%{fd: fd}), do: :file.close(fd)

  defp ceiling(dir, partition_index, value, element) do
    with_records(dir, partition_index, :unknown, fn fd, records ->
      case ceiling_record(fd, 0, records - 1, value, element, nil) do
        {first_offset, _byte_pos, _commit_ms} -> {:ok, first_offset}
        nil -> last_first_offset(fd, records)
      end
    end)
  end

  defp last_first_offset(fd, records) do
    case read_record(fd, records - 1) do
      {:ok, {first_offset, _byte_pos, _commit_ms}} -> {:ok, first_offset}
      :error -> :unknown
    end
  end

  defp with_records(dir, partition_index, empty, fun) do
    path = path(dir, partition_index)

    with {:ok, %{size: size}} <- File.stat(path),
         records when records > 0 <- div(size, @record_size),
         {:ok, fd} <- :file.open(path, [:read, :raw, :binary]) do
      found = fun.(fd, records)
      :ok = :file.close(fd)
      found
    else
      _unusable -> empty
    end
  end

  defp trim_path(path, upto, base_byte) do
    case first_record(path) do
      {:ok, {first_offset, _byte_pos, _commit_ms}} when first_offset < upto ->
        {dropped, kept} =
          path
          |> read_all()
          |> Enum.split_while(fn {offset, _byte_pos, _commit_ms} -> offset < upto end)

        File.write!(path, Enum.map(head_dated(kept, dropped, upto, base_byte), &encode/1))

      _nothing_below_upto ->
        :ok
    end
  end

  defp head_dated([{upto, _byte_pos, _commit_ms} | _rest] = kept, _dropped, upto, _base_byte) do
    kept
  end

  defp head_dated([], _dropped, _upto, _base_byte), do: []
  defp head_dated(kept, [], _upto, _base_byte), do: kept

  defp head_dated(kept, dropped, upto, base_byte) do
    {_first_offset, _byte_pos, commit_ms} = List.last(dropped)
    [{upto, base_byte, commit_ms} | kept]
  end

  defp backfill(path, base_offset, base_byte, wal_tail) do
    if wal_tail > base_byte do
      case first_record(path) do
        {:ok, {^base_offset, _byte_pos, _commit_ms}} ->
          :ok

        _undated_head ->
          records = read_all(path)
          File.write!(path, Enum.map([{base_offset, base_byte, now_ms()} | records], &encode/1))
      end
    end

    :ok
  end

  defp first_record(path) do
    with {:ok, %{size: size}} <- File.stat(path),
         records when records > 0 <- div(size, @record_size),
         {:ok, fd} <- :file.open(path, [:read, :raw, :binary]) do
      found = read_record(fd, 0)
      :ok = :file.close(fd)
      found
    else
      _unusable -> :error
    end
  end

  defp reopen(handle) do
    case :file.open(handle.path, [:append, :raw, :binary]) do
      {:ok, fd} -> %{handle | fd: fd}
      {:error, _reason} -> %{handle | fd: nil}
    end
  end

  defp now_ms, do: System.system_time(:millisecond)

  defp encode({first_offset, byte_pos, commit_ms}) do
    body = <<first_offset::64-big, byte_pos::64-big, commit_ms::64-big>>
    [body, <<:erlang.crc32(body)::32-big>>]
  end

  defp read_all(path) do
    with {:ok, %{size: size}} <- File.stat(path),
         records when records > 0 <- div(size, @record_size),
         {:ok, fd} <- :file.open(path, [:read, :raw, :binary]) do
      found =
        for position <- 0..(records - 1),
            {:ok, record} <- [read_record(fd, position)],
            do: record

      :ok = :file.close(fd)
      found
    else
      _unusable -> []
    end
  end

  defp floor_record(_fd, low, high, _from, best) when low > high, do: best

  defp floor_record(fd, low, high, from, best) do
    middle = div(low + high, 2)

    case read_record(fd, middle) do
      {:ok, {first_offset, _byte_pos, _commit_ms} = record} when first_offset <= from ->
        floor_record(fd, middle + 1, high, from, record)

      {:ok, _later} ->
        floor_record(fd, low, middle - 1, from, best)

      :error ->
        best
    end
  end

  defp ceiling_record(_fd, low, high, _value, _element, best) when low > high, do: best

  defp ceiling_record(fd, low, high, value, element, best) do
    middle = div(low + high, 2)

    case read_record(fd, middle) do
      {:ok, record} ->
        if elem(record, element) >= value do
          ceiling_record(fd, low, middle - 1, value, element, record)
        else
          ceiling_record(fd, middle + 1, high, value, element, best)
        end

      :error ->
        best
    end
  end

  defp read_record(fd, position) do
    case :file.pread(fd, position * @record_size, @record_size) do
      {:ok, <<first_offset::64-big, byte_pos::64-big, commit_ms::64-big, crc::32-big>>} ->
        body = <<first_offset::64-big, byte_pos::64-big, commit_ms::64-big>>

        if :erlang.crc32(body) == crc do
          {:ok, {first_offset, byte_pos, commit_ms}}
        else
          :error
        end

      _torn_or_missing ->
        :error
    end
  end

  defp drop_unbacked(path, wal_size) do
    case File.stat(path) do
      {:ok, %{size: size}} -> drop_unbacked_to(path, div(size, @record_size), wal_size)
      {:error, _reason} -> :ok
    end
  end

  defp drop_unbacked_to(_path, 0, _wal_size), do: :ok

  defp drop_unbacked_to(path, records, wal_size) do
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
      {:ok, {_first_offset, byte_pos, _commit_ms}} when byte_pos < wal_size ->
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
