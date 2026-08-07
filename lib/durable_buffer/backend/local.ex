defmodule DurableBuffer.Backend.Local do
  @moduledoc """
  Local-disk backend.

  Each partition owns an append-only WAL file opened in raw binary mode.
  A group commit is one `:file.write/2` of the whole batch followed by one
  `:file.datasync/1`, so the fsync cost is shared by every entry in the batch.
  Torn tails are truncated on open.

  `fsync: false` skips the `datasync`, making a commit durable only to the
  page cache — data survives a BEAM crash but not an OS crash or power
  loss. The default is `true` here; `DurableBuffer.Backend.Replica` opens
  its WALs with the buffer's `fsync:` setting, which defaults to `false`
  there because replication is the durability mechanism.
  """

  @behaviour DurableBuffer.Backend

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
    offset = WAL.recover!(path)
    {:ok, fd} = :file.open(path, [:append, :raw, :binary])
    {:ok, %{fd: fd, path: path, offset: offset, fsync: config.fsync}}
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
  def truncate(state) do
    :ok = :file.close(state.fd)
    :ok = File.rm(state.path)
    {:ok, fd} = :file.open(state.path, [:append, :raw, :binary])
    {:ok, %{state | fd: fd, offset: 0}}
  end

  @impl DurableBuffer.Backend
  def close(state) do
    :ok = :file.close(state.fd)
  end

  @doc """
  Lazily streams CRC-valid WAL payloads from a file, reading in chunks.
  """
  @spec stream_file(Path.t()) :: Enumerable.t()
  def stream_file(path) do
    Stream.resource(
      fn ->
        case :file.open(path, [:read, :raw, :binary, {:read_ahead, @read_chunk_size}]) do
          {:ok, fd} -> {fd, <<>>}
          {:error, :enoent} -> :done
        end
      end,
      fn
        :done ->
          {:halt, :done}

        {fd, buffer} ->
          case :file.read(fd, @read_chunk_size) do
            {:ok, chunk} ->
              {payloads, _valid, rest} = WAL.decode_all(buffer <> chunk)
              {payloads, {fd, rest}}

            :eof ->
              {:halt, {fd, buffer}}
          end
      end,
      fn
        :done -> :ok
        {fd, _buffer} -> :file.close(fd)
      end
    )
  end
end
