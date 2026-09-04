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

  alias DurableBuffer.Meta
  alias DurableBuffer.WAL

  @read_chunk_size 65_536

  @impl DurableBuffer.Backend
  def init_config(opts) do
    %{dir: Keyword.fetch!(opts, :dir), fsync: Keyword.get(opts, :fsync, true)}
  end

  @impl DurableBuffer.Backend
  def open(config, partition_index) do
    path = wal_path(config.dir, partition_index)
    File.mkdir_p!(Path.dirname(path))
    {offset, entry_count} = WAL.recover!(path)
    meta = Meta.load(config.dir, partition_index)
    {:ok, fd} = :file.open(path, [:append, :raw, :binary])

    {:ok,
     %{
       fd: fd,
       path: path,
       offset: offset,
       fsync: config.fsync,
       dir: config.dir,
       partition_index: partition_index,
       base_offset: meta.base_offset,
       entry_count: entry_count
     }}
  end

  @doc """
  Logical entry offsets as of `open/2` or the last `truncate/1`.

  `DurableBuffer.Partition.Committer` seeds its offset counter from this and
  assigns every offset after it, so these do not track later commits.
  """
  @impl DurableBuffer.Backend
  @spec offsets(map()) :: %{first: non_neg_integer(), next: non_neg_integer()}
  def offsets(state) do
    %{first: state.base_offset, next: state.base_offset + state.entry_count}
  end

  @impl DurableBuffer.Backend
  def commit(state, batch, byte_size) do
    with :ok <- :file.write(state.fd, batch),
         :ok <- sync(state) do
      {:ok, %{state | offset: state.offset + byte_size}}
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp sync(%{fsync: false}), do: :ok
  defp sync(state), do: :file.datasync(state.fd)

  @doc """
  Byte offset at which the next commit will be appended — the current WAL
  size.
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
        result = :file.pread(fd, offset, length)
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
    base = Meta.load(config.dir, partition_index).base_offset

    config.dir
    |> wal_path(partition_index)
    |> stream_file(Keyword.get(opts, :limit))
    |> project(base, Keyword.get(opts, :from), Keyword.get(opts, :with_offsets, false))
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
    {:ok, %{state | fd: fd, offset: 0, base_offset: next, entry_count: 0}}
  end

  @impl DurableBuffer.Backend
  def close(state) do
    :ok = :file.close(state.fd)
  end

  @doc """
  Lazily streams CRC-valid WAL payloads from a file, reading in chunks.
  """
  @spec stream_file(Path.t(), DurableBuffer.Backend.limit_fun() | nil) :: Enumerable.t()
  def stream_file(path, limit_fun \\ nil) do
    Stream.resource(
      fn ->
        case :file.open(path, [:read, :raw, :binary, {:read_ahead, @read_chunk_size}]) do
          {:ok, fd} -> {fd, <<>>, 0}
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

  defp readable_bytes(nil, _position), do: @read_chunk_size

  defp readable_bytes(limit_fun, position) do
    limit_fun.() |> Kernel.-(position) |> max(0) |> min(@read_chunk_size)
  end
end
