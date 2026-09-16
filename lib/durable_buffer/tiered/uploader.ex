defmodule DurableBuffer.Tiered.Uploader do
  @moduledoc """
  Uploads sealed segments of one `DurableBuffer.Backend.Tiered` partition to
  S3, strictly in offset order.

  Started and linked by `DurableBuffer.Backend.Tiered.open/2`, which runs in
  the partition's committer. After each upload it stores the watermark in
  the partition's `uploaded` file and sends `{:backend, {:uploaded, through}}`
  to that owner, which settles `ack: :remote` commits and releases backlog.

  An upload is one `DurableBuffer.Backend.S3.commit/4` of the whole segment,
  so the S3 object has the same key and framing as a segment written by the
  S3 backend. A failed upload retries with exponential backoff and never
  skips a segment, so S3 always holds a contiguous prefix of the log. A
  sealed file never changes, so a retry or a second upload after a crash
  sends the same bytes to the same key.

  A segment is uploaded exactly when its offset is below the watermark:
  uploads run in order, and the active segment always starts at or above the
  end of every sealed one. With `local_retention: :uploaded` a segment's file
  is deleted once its watermark is stored. With a byte bound, uploaded segments are deleted
  oldest first while the partition's local files exceed it.
  """

  use GenServer

  require Logger

  alias DurableBuffer.Backend.S3
  alias DurableBuffer.Tiered.Segments

  @min_backoff_ms 100
  @max_backoff_ms 30_000

  @doc """
  Starts an uploader linked to the caller.

  Options: `:owner`, `:s3` (an open `DurableBuffer.Backend.S3` state whose
  `next_offset` is the watermark), `:dir` (the partition directory),
  `:queue` (sealed segments as `%{offset:, end:, path:}`, in offset order)
  and `:local_retention`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Queues a sealed segment for upload.
  """
  @spec sealed(pid(), %{offset: non_neg_integer(), end: non_neg_integer(), path: Path.t()}) ::
          :ok
  def sealed(uploader, segment) do
    GenServer.cast(uploader, {:sealed, segment})
  end

  @doc """
  Stops the uploader at once, abandoning any upload in flight.

  The caller's link is removed first, so the stop does not reach it.
  """
  @spec stop(pid()) :: :ok
  def stop(uploader) do
    Process.unlink(uploader)
    ref = Process.monitor(uploader)
    Process.exit(uploader, :kill)

    receive do
      {:DOWN, ^ref, :process, ^uploader, _reason} -> :ok
    end
  end

  @impl GenServer
  def init(opts) do
    state = %{
      owner: Keyword.fetch!(opts, :owner),
      s3: Keyword.fetch!(opts, :s3),
      dir: Keyword.fetch!(opts, :dir),
      queue: :queue.from_list(Keyword.get(opts, :queue, [])),
      local_retention: Keyword.get(opts, :local_retention, :uploaded),
      backoff_ms: 0,
      busy?: false
    }

    {:ok, kick(state)}
  end

  @impl GenServer
  def handle_cast({:sealed, segment}, state) do
    {:noreply, kick(%{state | queue: :queue.in(segment, state.queue)})}
  end

  @impl GenServer
  def handle_info(:upload, state) do
    state = %{state | busy?: false}

    case :queue.peek(state.queue) do
      :empty -> {:noreply, state}
      {:value, segment} -> {:noreply, upload(state, segment)}
    end
  end

  defp kick(%{busy?: true} = state), do: state

  defp kick(state) do
    if :queue.is_empty(state.queue) do
      state
    else
      send(self(), :upload)
      %{state | busy?: true}
    end
  end

  defp upload(state, segment) do
    case put(state.s3, segment) do
      {:ok, s3} ->
        :ok = Segments.store_watermark!(state.dir, segment.end)
        :ok = apply_local_retention(state, segment.end)
        send(state.owner, {:backend, {:uploaded, segment.end}})
        {_segment, queue} = :queue.out(state.queue)
        kick(%{state | s3: s3, queue: queue, backoff_ms: 0})

      {:error, reason} ->
        backoff_ms = min(max(state.backoff_ms * 2, @min_backoff_ms), @max_backoff_ms)

        Logger.warning(
          "durable_buffer: upload of #{segment.path} failed, retrying in " <>
            "#{backoff_ms} ms: #{inspect(reason)}"
        )

        Process.send_after(self(), :upload, backoff_ms)
        %{state | backoff_ms: backoff_ms, busy?: true}
    end
  end

  defp put(s3, segment) do
    case File.read(segment.path) do
      {:ok, binary} ->
        case S3.commit(
               s3,
               binary,
               byte_size(binary),
               {segment.offset, segment.end - segment.offset}
             ) do
          {:ok, s3} -> {:ok, s3}
          {:error, reason, _s3} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, {:read_failed, reason}}
    end
  rescue
    exception -> {:error, exception}
  end

  defp apply_local_retention(%{local_retention: :uploaded} = state, through) do
    state.dir
    |> Segments.list()
    |> Enum.filter(&(&1.offset < through))
    |> Enum.each(&File.rm(&1.path))
  end

  defp apply_local_retention(%{local_retention: bound} = state, through)
       when is_integer(bound) do
    sized =
      for segment <- Segments.list(state.dir),
          {:ok, %{size: size}} <- [File.stat(segment.path)],
          do: Map.put(segment, :size, size)

    total = sized |> Enum.map(& &1.size) |> Enum.sum()

    Enum.reduce_while(sized, total, fn segment, total ->
      if total > bound and segment.offset < through do
        _ignored = File.rm(segment.path)
        {:cont, total - segment.size}
      else
        {:halt, total}
      end
    end)

    :ok
  end
end
