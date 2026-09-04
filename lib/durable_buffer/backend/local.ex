defmodule DurableBuffer.Backend.Local do
  @moduledoc """
  Local-disk backend.

  Each partition owns an append-only WAL file opened in raw binary mode.
  A group commit is one `:file.write/2` of the whole batch followed by one
  `:file.datasync/1`, so the fsync cost is shared by every entry in the batch.
  Torn tails are truncated on open.

  Reads are gated at `durable_offset/1`, so a stream never returns bytes
  whose `datasync` has not yet returned.

  `fsync: false` skips the `datasync`, making a commit durable only to the
  page cache — data survives a BEAM crash but not an OS crash or power
  loss. The default is `true` here; `DurableBuffer.Backend.Replica` opens
  its WALs with the buffer's `fsync:` setting, which defaults to `false`
  there because replication is the durability mechanism.
  """

  @behaviour DurableBuffer.Backend

  alias DurableBuffer.Backend.Local.Index
  alias DurableBuffer.Meta
  alias DurableBuffer.WAL

  @read_chunk_size 65_536
  @header_size 8

  @impl DurableBuffer.Backend
  def init_config(opts) do
    %{
      dir: Keyword.fetch!(opts, :dir),
      fsync: Keyword.get(opts, :fsync, true),
      index: Keyword.get(opts, :index, true),
      index_sync_ms: Keyword.get(opts, :index_sync_ms, 5_000)
    }
  end

  @impl DurableBuffer.Backend
  def open(config, partition_index) do
    path = wal_path(config.dir, partition_index)
    File.mkdir_p!(Path.dirname(path))
    {physical, entry_count} = WAL.recover!(path)
    meta = Meta.load(config.dir, partition_index)
    {:ok, fd} = :file.open(path, [:append, :raw, :binary])

    {:ok,
     %{
       fd: fd,
       path: path,
       offset: meta.base_byte_offset + physical,
       base_byte: meta.base_byte_offset,
       fsync: config.fsync,
       dir: config.dir,
       partition_index: partition_index,
       base_offset: meta.base_offset,
       entry_count: entry_count,
       index: open_index(config, partition_index, meta, physical),
       index_sync_ms: config.index_sync_ms,
       index_synced_at: System.monotonic_time(:millisecond)
     }}
  end

  defp open_index(%{index: false}, _partition_index, _meta, _physical) do
    %{fd: nil, path: nil}
  end

  defp open_index(config, partition_index, meta, physical) do
    Index.open(
      config.dir,
      partition_index,
      meta.base_byte_offset + physical,
      meta.base_offset,
      meta.base_byte_offset
    )
  end

  @doc """
  The partition's logical entry offset bounds.
  """
  @impl DurableBuffer.Backend
  @spec offsets(map()) :: %{first: non_neg_integer(), next: non_neg_integer()}
  def offsets(state) do
    %{first: state.base_offset, next: state.base_offset + state.entry_count}
  end

  @doc """
  The offset a retention policy would cut at, or `:none` when neither bound
  is exceeded.

  Both bounds resolve through the seek index, so both cut on a batch
  boundary at or below the exact point. When they disagree the higher point
  wins: whichever bound binds first is the one that decides.

  A bound the index cannot answer for is skipped rather than guessed. Time
  retention therefore stalls while the index is being rebuilt, and size
  retention keeps bounding the disk in the meantime.
  """
  @impl DurableBuffer.Backend
  @spec retention_point(map(), map()) :: {:ok, non_neg_integer()} | :none
  def retention_point(state, policy) do
    case Enum.reject(
           [time_point(state, policy[:ms]), size_point(state, policy[:bytes])],
           &is_nil/1
         ) do
      [] -> :none
      points -> {:ok, Enum.max(points)}
    end
  end

  defp time_point(_state, nil), do: nil

  defp time_point(state, ms) do
    cutoff = System.system_time(:millisecond) - ms

    case Index.seek_time(state.dir, state.partition_index, cutoff) do
      {:ok, upto} -> upto
      :unknown -> nil
    end
  end

  defp size_point(_state, nil), do: nil

  defp size_point(state, bytes) do
    if state.offset - state.base_byte > bytes do
      case Index.seek_byte(state.dir, state.partition_index, state.offset - bytes) do
        {:ok, upto} -> upto
        :unknown -> nil
      end
    end
  end

  @doc """
  What retention has to work with: the commit time of the oldest retained
  batch, and the bytes retained.

  `oldest_ms` is `nil` for an empty partition, and for one whose index
  cannot date its head — which is what a stalled time retention looks like
  from outside.
  """
  @impl DurableBuffer.Backend
  @spec retention_status(map()) :: %{oldest_ms: integer() | nil, bytes: non_neg_integer()}
  def retention_status(state) do
    %{oldest_ms: oldest_ms(state), bytes: state.offset - state.base_byte}
  end

  defp oldest_ms(%{entry_count: 0}), do: nil

  defp oldest_ms(state) do
    case Index.oldest(state.dir, state.partition_index) do
      {:ok, commit_ms} -> commit_ms
      :unknown -> nil
    end
  end

  @impl DurableBuffer.Backend
  def commit(state, batch, byte_size, {first_offset, count}) do
    with :ok <- :file.write(state.fd, batch),
         :ok <- sync(state) do
      Index.append(state.index, first_offset, state.offset, System.system_time(:millisecond))

      state = sync_index(state)

      {:ok, %{state | offset: state.offset + byte_size, entry_count: state.entry_count + count}}
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp sync_index(%{index_sync_ms: :infinity} = state), do: state

  defp sync_index(state) do
    now = System.monotonic_time(:millisecond)

    if now - state.index_synced_at >= state.index_sync_ms do
      :ok = Index.sync(state.index)
      %{state | index_synced_at: now}
    else
      state
    end
  end

  defp sync(%{fsync: false}), do: :ok
  defp sync(state), do: :file.datasync(state.fd)

  @doc """
  Byte offset at which the next commit will be appended.

  This is a *logical* offset: it counts from the first byte ever written to
  the partition, not from the start of the file. Trimming the head of the
  WAL moves `base_byte_offset` and leaves every logical offset alone, so a
  trim never shifts what replication has already stamped on the wire. The
  physical file position is `logical - base_byte_offset`.
  """
  @spec offset(map()) :: non_neg_integer()
  def offset(state), do: state.offset

  @doc """
  Reads `length` bytes of the WAL starting at `offset`.

  Returns fewer bytes near the end of the file, and an empty binary at or
  past it.
  """
  @spec read_range(map(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_range(state, offset, length) do
    case :file.open(state.path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        result = :file.pread(fd, offset - state.base_byte, length)
        :ok = :file.close(fd)

        case result do
          {:ok, data} -> {:ok, data}
          :eof -> {:ok, <<>>}
          {:error, reason} -> {:error, reason}
        end

      {:error, :enoent} ->
        {:ok, <<>>}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Logical byte offset the retained data starts at.
  """
  @spec base_byte(map()) :: non_neg_integer()
  def base_byte(state), do: state.base_byte

  @doc """
  Empties the WAL and restarts its logical byte offsets at `base_byte`.

  A replica that fell behind a primary's trim needs the bytes the primary
  dropped, and nobody has them. It discards what it holds and adopts the
  primary's base instead, so the next batch lands where the primary says it
  should.
  """
  @spec reset_to(map(), non_neg_integer()) :: {:ok, map()}
  def reset_to(state, base_byte) do
    :ok = :file.close(state.fd)
    :ok = File.rm(state.path)

    Meta.update!(state.dir, state.partition_index, fn meta ->
      %{meta | base_byte_offset: base_byte}
    end)

    {:ok, fd} = :file.open(state.path, [:append, :raw, :binary])

    {:ok,
     %{
       state
       | fd: fd,
         offset: base_byte,
         base_byte: base_byte,
         entry_count: 0,
         index: Index.reset(state.index)
     }}
  end

  @doc """
  Forces a `datasync` whatever the backend's `fsync:` setting is.
  """
  @spec datasync(map()) :: :ok | {:error, term()}
  def datasync(state), do: :file.datasync(state.fd)

  @doc """
  Path of the WAL file for `partition_index` under `dir`.
  """
  @spec wal_path(Path.t(), non_neg_integer()) :: Path.t()
  def wal_path(dir, partition_index) when is_binary(dir) do
    Path.join(dir, "p#{partition_index}.wal")
  end

  @impl DurableBuffer.Backend
  def stream(config, partition_index) do
    path = wal_path(config.dir, partition_index)
    stream_file(path)
  end

  @impl DurableBuffer.Backend
  def stream(config, partition_index, opts) do
    meta = Meta.load(config.dir, partition_index)
    from = Keyword.get(opts, :from)
    {start_byte, start_offset} = start_at(config.dir, partition_index, meta, from)

    config.dir
    |> wal_path(partition_index)
    |> stream_file(
      limit: physical_limit(Keyword.get(opts, :limit), meta.base_byte_offset),
      start: max(start_byte - meta.base_byte_offset, 0)
    )
    |> project(start_offset, from, Keyword.get(opts, :with_offsets, false))
  end

  defp physical_limit(nil, _base_byte), do: nil
  defp physical_limit(limit_fun, base_byte), do: fn -> limit_fun.() - base_byte end

  defp start_at(_dir, _partition_index, meta, nil) do
    {meta.base_byte_offset, meta.base_offset}
  end

  defp start_at(dir, partition_index, meta, from) do
    case Index.seek(dir, partition_index, from) do
      {byte_pos, first_offset}
      when byte_pos >= meta.base_byte_offset and first_offset >= meta.base_offset ->
        {byte_pos, first_offset}

      _missing_or_trimmed_away ->
        {meta.base_byte_offset, meta.base_offset}
    end
  end

  defp project(stream, _base, nil, false), do: stream

  defp project(stream, base, from, with_offsets?) do
    stream
    |> Stream.with_index(base)
    |> drop_below(from)
    |> Stream.map(fn {payload, offset} ->
      if with_offsets?, do: {offset, payload}, else: payload
    end)
  end

  defp drop_below(stream, nil), do: stream

  defp drop_below(stream, from) do
    Stream.drop_while(stream, fn {_payload, offset} -> offset < from end)
  end

  @doc """
  Byte offset through which commits are durable. The write and the
  `datasync` both succeed before `offset` advances, so it is exactly the
  readable prefix.
  """
  @impl DurableBuffer.Backend
  @spec durable_offset(map()) :: non_neg_integer()
  def durable_offset(state), do: state.offset

  @impl DurableBuffer.Backend
  def truncate(state, next) do
    :ok = :file.close(state.fd)
    :ok = File.rm(state.path)

    Meta.update!(state.dir, state.partition_index, fn meta ->
      %{meta | base_offset: next, base_byte_offset: 0}
    end)

    {:ok, fd} = :file.open(state.path, [:append, :raw, :binary])

    {:ok,
     %{
       state
       | fd: fd,
         offset: 0,
         base_byte: 0,
         base_offset: next,
         entry_count: 0,
         index: Index.reset(state.index)
     }}
  end

  @doc """
  Drops every entry below the logical offset `upto`.

  The retained suffix is copied to a sibling file, `datasync`ed and renamed
  over the WAL, so a crash mid-trim leaves the original intact. Cost is
  proportional to the bytes *kept*, which is cheap exactly when trimming is
  routine.

  Logical byte offsets do not move: `base_byte_offset` advances by the bytes
  dropped, so replication keeps stamping the same numbers and a trim never
  reaches the wire. The seek index needs no rebuild for the same reason —
  its surviving records are still correct.

  Trimming past the last entry keeps offsets monotonic rather than resetting
  them, exactly as `truncate/2` does.
  """
  @impl DurableBuffer.Backend
  @spec trim(map(), non_neg_integer()) :: {:ok, map()} | {:error, term(), map()}
  def trim(state, upto) do
    next = state.base_offset + state.entry_count

    cond do
      upto <= state.base_offset -> {:ok, state}
      upto >= next -> rewrite(state, state.offset - state.base_byte, next, 0)
      true -> trim_to(state, upto)
    end
  end

  @doc """
  Drops every byte below the logical byte offset `base_byte`.

  The replicated backend uses this to pass a primary's trim on to its
  replicas, which mirror bytes and have no view of entry offsets. A logical
  byte offset is a frame boundary on every member, since every member holds
  the same bytes at the same logical positions.
  """
  @spec trim_bytes(map(), non_neg_integer()) :: {:ok, map()} | {:error, term(), map()}
  def trim_bytes(state, base_byte) do
    cond do
      base_byte <= state.base_byte ->
        {:ok, state}

      base_byte > state.offset ->
        {:error, :beyond_tail, state}

      true ->
        {:ok, state} =
          rewrite(state, base_byte - state.base_byte, state.base_offset, state.entry_count)

        {_physical, retained} = WAL.recover!(state.path)
        dropped = state.entry_count - retained

        {:ok, %{state | base_offset: state.base_offset + dropped, entry_count: retained}}
    end
  end

  defp trim_to(state, upto) do
    case cut_position(state, upto) do
      {:ok, cut} ->
        rewrite(state, cut, upto, state.entry_count - (upto - state.base_offset))

      :error ->
        {:error, :cannot_locate_offset, state}
    end
  end

  defp cut_position(state, upto) do
    {start, offset} =
      case Index.seek(state.dir, state.partition_index, upto) do
        nil -> {0, state.base_offset}
        {logical, first_offset} -> {logical - state.base_byte, first_offset}
      end

    case :file.open(state.path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        found = advance(fd, start, offset, upto)
        :ok = :file.close(fd)
        found

      {:error, _reason} ->
        :error
    end
  end

  defp advance(_fd, position, offset, upto) when offset >= upto, do: {:ok, position}

  defp advance(fd, position, offset, upto) do
    case :file.pread(fd, position, @header_size) do
      {:ok, <<length::32-big, _crc::32-big>>} ->
        advance(fd, position + @header_size + length, offset + 1, upto)

      _short_or_error ->
        :error
    end
  end

  defp rewrite(state, cut, base_offset, entry_count) do
    temp = state.path <> ".trim"
    :ok = :file.close(state.fd)
    :ok = copy_suffix(state.path, temp, cut)
    :ok = File.rename(temp, state.path)
    base_byte = state.base_byte + cut

    Meta.update!(state.dir, state.partition_index, fn meta ->
      %{meta | base_offset: base_offset, base_byte_offset: base_byte}
    end)

    {:ok, fd} = :file.open(state.path, [:append, :raw, :binary])

    {:ok,
     %{
       state
       | fd: fd,
         base_byte: base_byte,
         base_offset: base_offset,
         entry_count: entry_count,
         index: Index.trim(state.index, base_offset, base_byte)
     }}
  end

  defp copy_suffix(source, destination, cut) do
    {:ok, from} = :file.open(source, [:read, :raw, :binary])
    {:ok, to} = :file.open(destination, [:write, :raw, :binary])
    {:ok, _position} = :file.position(from, cut)
    :ok = copy_all(from, to)
    :ok = :file.datasync(to)
    :ok = :file.close(to)
    :ok = :file.close(from)
  end

  defp copy_all(from, to) do
    case :file.read(from, @read_chunk_size) do
      {:ok, data} ->
        :ok = :file.write(to, data)
        copy_all(from, to)

      :eof ->
        :ok
    end
  end

  @impl DurableBuffer.Backend
  def close(state) do
    :ok = Index.close(state.index)
    :ok = :file.close(state.fd)
  end

  @doc """
  Lazily streams CRC-valid WAL payloads from a file, reading in chunks.
  """
  @spec stream_file(Path.t(), keyword()) :: Enumerable.t()
  def stream_file(path, opts \\ []) do
    limit_fun = Keyword.get(opts, :limit)
    start = Keyword.get(opts, :start, 0)

    Stream.resource(
      fn ->
        case :file.open(path, [:read, :raw, :binary, {:read_ahead, @read_chunk_size}]) do
          {:ok, fd} -> position(fd, start)
          {:error, :enoent} -> :done
        end
      end,
      fn
        :done ->
          {:halt, :done}

        {fd, buffer, position} = acc ->
          case readable_bytes(limit_fun, position) do
            0 ->
              {:halt, acc}

            count ->
              case :file.read(fd, count) do
                {:ok, chunk} ->
                  {payloads, _valid, rest} = WAL.decode_all(buffer <> chunk)
                  {payloads, {fd, rest, position + byte_size(chunk)}}

                :eof ->
                  {:halt, acc}
              end
          end
      end,
      fn
        :done -> :ok
        {fd, _buffer, _position} -> :file.close(fd)
      end
    )
  end

  defp position(fd, 0), do: {fd, <<>>, 0}

  defp position(fd, start) do
    case :file.position(fd, start) do
      {:ok, _position} -> {fd, <<>>, start}
      {:error, _reason} -> {fd, <<>>, 0}
    end
  end

  defp readable_bytes(nil, _position), do: @read_chunk_size

  defp readable_bytes(limit_fun, position) do
    limit_fun.() |> Kernel.-(position) |> max(0) |> min(@read_chunk_size)
  end
end
