defmodule DurableBuffer.Backend.Local do
  @moduledoc """
  Local-disk backend.

  Each partition owns an append-only WAL file opened in raw binary mode.
  A group commit is one `:file.write/2` of the whole batch followed by one
  `:file.datasync/1`, so the fsync cost is shared by every entry in the batch.
  Torn tails are truncated on open.
  """

  @behaviour DurableBuffer.Backend

  alias DurableBuffer.WAL

  @read_chunk_size 65_536

  @impl DurableBuffer.Backend
  def init_config(opts) do
    %{dir: Keyword.fetch!(opts, :dir)}
  end

  @impl DurableBuffer.Backend
  def open(config, partition_index) do
    path = wal_path(config, partition_index)
    File.mkdir_p!(Path.dirname(path))
    WAL.recover!(path)
    {:ok, fd} = :file.open(path, [:append, :raw, :binary])
    {:ok, %{fd: fd, path: path}}
  end

  @impl DurableBuffer.Backend
  def commit(state, batch, _byte_size) do
    with :ok <- :file.write(state.fd, batch),
         :ok <- :file.datasync(state.fd) do
      {:ok, state}
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  @impl DurableBuffer.Backend
  def stream(config, partition_index) do
    path = wal_path(config, partition_index)
    stream_file(path)
  end

  @impl DurableBuffer.Backend
  def truncate(state) do
    :ok = :file.close(state.fd)
    :ok = File.rm(state.path)
    {:ok, fd} = :file.open(state.path, [:append, :raw, :binary])
    {:ok, %{state | fd: fd}}
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

  defp wal_path(config, partition_index) do
    Path.join(config.dir, "p#{partition_index}.wal")
  end
end
