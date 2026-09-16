defmodule DurableBuffer.Backend.Tiered do
  @moduledoc """
  Experimental two-tier backend: segment files on local disk, uploaded to S3.

  Each partition appends group commits to an active segment file under
  `<dir>/p<index>/`. The segment is sealed when it reaches `:segment_bytes`
  or when it is `:segment_ms` old. A `DurableBuffer.Tiered.Uploader`
  uploads each sealed segment as one S3 object, strictly in offset order.

  Objects use the key layout of `DurableBuffer.Backend.S3`, and this backend
  drives the S3 tier through that module. The uploader compresses each
  segment before its PUT, `:zstd` by default. Local files stay uncompressed,
  so compression costs nothing on the commit path. So a processor on any node reads
  the uploaded log with `DurableBuffer.Backend.S3.stream/3` and needs no
  running buffer.

  `:ack` sets when a commit settles:

    * `:local` (default) — after the local write, and its `datasync` when
      `fsync: true` (the default in this mode). A node that is lost before
      its segments upload loses them.
    * `:remote` — after the uploader stores the segment that holds the
      commit. `fsync` defaults to `false`: sealing always `datasync`s the
      segment, and nothing in an unsealed segment is acked yet.

  With `ack: :remote` the segment is also sealed whenever the uploader is
  idle. This is group commit one level up: while a PUT is in flight, new
  commits collect in the active segment, and the upload's completion seals
  them into the next PUT. A lone append pays one PUT, and segments grow with
  load instead of waiting out `:segment_ms`.

  Reads are gated by entry offset, not by byte offset: `durable_offset/1`
  is the end of the local log for `:local` and the upload watermark for
  `:remote`. `stream/3` reads entries below the lowest local segment from
  S3 and the rest from local files, falling back to S3 for a local segment
  that is deleted before the reader opens it.

  On open every existing segment is treated as sealed: the highest file's
  torn tail is truncated and `datasync`ed, sealed segments above the upload
  watermark are queued for upload, and the next commit opens a new segment.

  Options:

    * `:dir` (required)
    * `:bucket` (required), `:prefix`, `:req_options` — as for
      `DurableBuffer.Backend.S3`
    * `:compression` — `:zstd`, `:gzip` or `:none`; default
      `DurableBuffer.Compression.default/0`: `:zstd` on OTP 28 and later,
      `:gzip` before
    * `:ack` — `:local` or `:remote`, default `:local`
    * `:fsync` — default `true` for `:local`, `false` for `:remote`
    * `:segment_bytes` — default 64 MiB
    * `:segment_ms` — default 60 s
    * `:local_retention` — `:uploaded` (default) deletes a segment's file
      once it is uploaded; a byte count keeps uploaded segments until the
      partition's local files exceed it
    * `:max_local_bytes` — refuse commits with `{:error, :upload_backlog}`
      while more bytes than this are not yet uploaded; default `:infinity`
  """

  @behaviour DurableBuffer.Backend

  alias DurableBuffer.Backend.S3
  alias DurableBuffer.Compression
  alias DurableBuffer.Tiered.Segments
  alias DurableBuffer.Tiered.Uploader
  alias DurableBuffer.WAL

  @impl DurableBuffer.Backend
  def init_config(opts) do
    ack = Keyword.get(opts, :ack, :local)

    unless ack in [:local, :remote] do
      raise ArgumentError, ":ack must be :local or :remote, got #{inspect(ack)}"
    end

    %{
      dir: Keyword.fetch!(opts, :dir),
      s3:
        opts
        |> Keyword.take([:bucket, :prefix, :req_options])
        |> Keyword.put(:compression, Keyword.get(opts, :compression, Compression.default()))
        |> S3.init_config(),
      ack: ack,
      fsync: Keyword.get(opts, :fsync, ack == :local),
      segment_bytes: Keyword.get(opts, :segment_bytes, 64 * 1024 * 1024),
      segment_ms: Keyword.get(opts, :segment_ms, 60_000),
      local_retention: Keyword.get(opts, :local_retention, :uploaded),
      max_local_bytes: Keyword.get(opts, :max_local_bytes, :infinity)
    }
  end

  @impl DurableBuffer.Backend
  def open(config, partition_index) do
    dir = Segments.partition_dir(config.dir, partition_index)
    File.mkdir_p!(dir)
    {:ok, s3} = S3.open(config.s3, partition_index)

    {segments, local_next} = recover_segments(dir)
    uploaded = max(Segments.read_watermark(dir) || 0, s3.next_offset)
    next = max(local_next || 0, uploaded)

    ended = Segments.with_ends(segments, next)
    {uploaded_segments, sealed} = Enum.split_with(ended, &(&1.offset < uploaded))

    if config.local_retention == :uploaded do
      Enum.each(uploaded_segments, &File.rm(&1.path))
    end

    local_first =
      case ended do
        [] -> next
        [lowest | _rest] -> lowest.offset
      end

    s3 = %{s3 | next_offset: uploaded}

    {:ok, uploader} =
      Uploader.start_link(
        owner: self(),
        s3: s3,
        dir: dir,
        queue: sealed,
        local_retention: config.local_retention
      )

    {:ok,
     %{
       config: config,
       partition_index: partition_index,
       dir: dir,
       s3: s3,
       uploader: uploader,
       active: nil,
       first_offset: min(s3.first_offset, local_first),
       next_offset: next,
       uploaded: uploaded,
       sealed: Enum.map(sealed, &%{end: &1.end, bytes: file_size(&1.path)}),
       pending: :queue.new()
     }}
  end

  defp recover_segments(dir) do
    case Segments.list(dir) do
      [] ->
        {[], nil}

      segments ->
        highest = List.last(segments)
        {_bytes, count} = WAL.recover!(highest.path)

        if count == 0 do
          File.rm!(highest.path)
          lower = Enum.drop(segments, -1)
          {lower, if(lower == [], do: nil, else: highest.offset)}
        else
          {:ok, fd} = :file.open(highest.path, [:read, :write, :raw, :binary])
          :ok = :file.datasync(fd)
          :ok = :file.close(fd)
          {segments, highest.offset + count}
        end
    end
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      {:error, _reason} -> 0
    end
  end

  @doc """
  The offset through which S3 holds `partition_index`'s data, as stored by
  its uploader. A processor reading from S3 sees entries below it.
  """
  @spec uploaded(map(), non_neg_integer()) :: non_neg_integer()
  def uploaded(config, partition_index) do
    config.dir
    |> Segments.partition_dir(partition_index)
    |> Segments.read_watermark()
    |> Kernel.||(0)
  end

  @impl DurableBuffer.Backend
  @spec offsets(map()) :: %{first: non_neg_integer(), next: non_neg_integer()}
  def offsets(state) do
    %{first: state.first_offset, next: state.next_offset}
  end

  @doc """
  The entry offset readers are gated at: the end of the local log for
  `ack: :local`, the upload watermark for `ack: :remote`.
  """
  @impl DurableBuffer.Backend
  @spec durable_offset(map()) :: non_neg_integer()
  def durable_offset(%{config: %{ack: :remote}} = state),
    do: max(state.uploaded, state.first_offset)

  def durable_offset(state), do: state.next_offset

  @impl DurableBuffer.Backend
  def commit(state, batch, byte_size, span) do
    tag = make_ref()

    case commit_async(state, batch, byte_size, span, tag) do
      {:done, :ok, state} -> {:ok, state}
      {:done, {:error, reason}, state} -> {:error, reason, state}
      {:pending, state} -> await_commit(state, tag)
    end
  end

  defp await_commit(state, tag) do
    receive do
      {:backend, message} ->
        {completions, state} = handle_message(message, state)

        case List.keyfind(completions, tag, 0) do
          {^tag, :ok} -> {:ok, state}
          {^tag, {:error, reason}} -> {:error, reason, state}
          nil -> await_commit(state, tag)
        end
    end
  end

  @impl DurableBuffer.Backend
  def commit_async(state, batch, byte_size, {first_offset, count}, tag) do
    if backlog_bytes(state) >= state.config.max_local_bytes do
      {:done, {:error, :upload_backlog}, state}
    else
      state = ensure_active(state, first_offset)

      case write(state.active, batch, state.config.fsync) do
        :ok ->
          next = first_offset + count

          active = %{state.active | bytes: state.active.bytes + byte_size}

          state = %{state | active: active, next_offset: next}
          settle(state, tag, next)

        {:error, reason} ->
          {:done, {:error, reason}, discard_partial_write(state)}
      end
    end
  end

  defp settle(%{config: %{ack: :remote}} = state, tag, next) do
    state = maybe_seal(%{state | pending: :queue.in({tag, next}, state.pending)})
    {:pending, state}
  end

  defp settle(state, _tag, _next) do
    {:done, :ok, maybe_seal(state)}
  end

  defp write(active, batch, fsync?) do
    with :ok <- :file.write(active.fd, batch) do
      if fsync?, do: :file.datasync(active.fd), else: :ok
    end
  end

  defp discard_partial_write(state) do
    fd = state.active.fd
    _position = :file.position(fd, state.active.bytes)
    _truncated = :file.truncate(fd)
    state
  end

  defp backlog_bytes(state) do
    active_bytes = if state.active, do: state.active.bytes, else: 0
    Enum.reduce(state.sealed, active_bytes, &(&1.bytes + &2))
  end

  defp ensure_active(%{active: nil} = state, first_offset) do
    path = Segments.path(state.dir, first_offset)
    {:ok, fd} = :file.open(path, [:write, :raw, :binary])

    if state.config.segment_ms != :infinity do
      Process.send_after(self(), {:backend, {:rotate, first_offset}}, state.config.segment_ms)
    end

    %{state | active: %{fd: fd, path: path, offset: first_offset, bytes: 0}}
  end

  defp ensure_active(state, _first_offset), do: state

  defp maybe_seal(state) do
    if state.active.bytes >= state.config.segment_bytes or uploader_idle?(state) do
      seal(state)
    else
      state
    end
  end

  defp uploader_idle?(%{config: %{ack: :remote}, sealed: []}), do: true
  defp uploader_idle?(_state), do: false

  defp seal(state) do
    active = state.active
    :ok = :file.datasync(active.fd)
    :ok = :file.close(active.fd)

    :ok =
      Uploader.sealed(state.uploader, %{
        offset: active.offset,
        end: state.next_offset,
        path: active.path
      })

    %{
      state
      | active: nil,
        sealed: state.sealed ++ [%{end: state.next_offset, bytes: active.bytes}]
    }
  end

  @impl DurableBuffer.Backend
  def handle_message({:rotate, offset}, %{active: %{offset: offset}} = state) do
    {[], seal(state)}
  end

  def handle_message({:rotate, _offset}, state) do
    {[], state}
  end

  def handle_message({:uploaded, through}, %{uploaded: uploaded} = state)
      when through > uploaded do
    sealed = Enum.drop_while(state.sealed, &(&1.end <= through))
    state = %{state | uploaded: through, sealed: sealed, s3: %{state.s3 | next_offset: through}}
    {completions, state} = settle_uploaded(state, through)

    if state.active != nil and uploader_idle?(state) do
      {completions, seal(state)}
    else
      {completions, state}
    end
  end

  def handle_message({:uploaded, _through}, state) do
    {[], state}
  end

  defp settle_uploaded(state, through) do
    {settled, pending} =
      state.pending
      |> :queue.to_list()
      |> Enum.split_while(fn {_tag, commit_end} -> commit_end <= through end)

    completions = for {tag, _commit_end} <- settled, do: {tag, :ok}
    {completions, %{state | pending: :queue.from_list(pending)}}
  end

  @impl DurableBuffer.Backend
  def stream(config, partition_index) do
    stream(config, partition_index, [])
  end

  @doc """
  Streams the partition's payloads across both tiers, oldest first.

  Entries below the lowest local segment come from S3; the rest come from
  local segment files. `:limit` is an entry offset, re-read before every
  entry, so a reader of `ack: :remote` data never sees an entry S3 does not
  hold yet.
  """
  @impl DurableBuffer.Backend
  def stream(config, partition_index, opts) do
    Stream.flat_map([:build], fn :build -> build_stream(config, partition_index, opts) end)
  end

  defp build_stream(config, partition_index, opts) do
    from = Keyword.get(opts, :from)
    limit = Keyword.get(opts, :limit)
    dir = Segments.partition_dir(config.dir, partition_index)
    local = Segments.with_ends(Segments.list(dir), :infinity)

    lowest_local =
      case local do
        [] -> :infinity
        [lowest | _rest] -> lowest.offset
      end

    remote =
      if from != nil and from >= lowest_local do
        []
      else
        config.s3
        |> S3.stream(partition_index, from: from || 0, with_offsets: true)
        |> Stream.take_while(fn {offset, _payload} -> offset < lowest_local end)
      end

    local_entries =
      local
      |> Stream.reject(&(from != nil and &1.end <= from))
      |> Stream.flat_map(&segment_entries(config, partition_index, &1))

    remote
    |> Stream.concat(local_entries)
    |> Stream.drop_while(fn {offset, _payload} -> from != nil and offset < from end)
    |> gate(limit)
    |> Stream.map(fn {offset, payload} ->
      if Keyword.get(opts, :with_offsets, false), do: {offset, payload}, else: payload
    end)
  end

  defp segment_entries(config, partition_index, segment) do
    case File.read(segment.path) do
      {:ok, binary} ->
        {payloads, _valid, _rest} = WAL.decode_all(binary)
        Enum.with_index(payloads, fn payload, index -> {segment.offset + index, payload} end)

      {:error, :enoent} ->
        config.s3
        |> S3.stream(partition_index, from: segment.offset, with_offsets: true)
        |> Stream.take_while(fn {offset, _payload} -> offset < segment.end end)
    end
  end

  defp gate(stream, nil), do: stream

  defp gate(stream, limit_fun) do
    Stream.take_while(stream, fn {offset, _payload} -> offset < limit_fun.() end)
  end

  @impl DurableBuffer.Backend
  def retention_point(state, policy) do
    S3.retention_point(state.s3, policy)
  end

  @impl DurableBuffer.Backend
  def retention_status(state) do
    S3.retention_status(state.s3)
  end

  @doc """
  Drops segments that lie entirely below `upto`, in both tiers.

  The point is clamped to the upload watermark first, so a trim never
  deletes a segment that exists only on local disk.
  """
  @impl DurableBuffer.Backend
  def trim(state, upto) do
    upto = min(upto, state.uploaded)
    {:ok, s3} = S3.trim(state.s3, upto)

    kept =
      state.dir
      |> Segments.list()
      |> Segments.with_ends(state.next_offset)
      |> Enum.reject(fn segment ->
        segment.end <= upto and segment.offset < state.uploaded and File.rm(segment.path) == :ok
      end)

    local_first =
      case kept do
        [] -> state.next_offset
        [lowest | _rest] -> lowest.offset
      end

    {:ok, %{state | s3: s3, first_offset: min(s3.first_offset, local_first)}}
  end

  @impl DurableBuffer.Backend
  def truncate(state, next) do
    :ok = Uploader.stop(state.uploader)
    state = close_active(state)

    state.dir |> Segments.list() |> Enum.each(&File.rm!(&1.path))
    {:ok, s3} = S3.truncate(state.s3, next)
    :ok = Segments.store_watermark!(state.dir, next)

    {:ok, uploader} =
      Uploader.start_link(
        owner: self(),
        s3: s3,
        dir: state.dir,
        queue: [],
        local_retention: state.config.local_retention
      )

    {:ok,
     %{
       state
       | s3: s3,
         uploader: uploader,
         first_offset: next,
         next_offset: next,
         uploaded: next,
         sealed: [],
         pending: :queue.new()
     }}
  end

  @impl DurableBuffer.Backend
  def close(state) do
    _state = close_active(state)
    Uploader.stop(state.uploader)
  end

  defp close_active(%{active: nil} = state), do: state

  defp close_active(state) do
    _closed = :file.close(state.active.fd)
    %{state | active: nil}
  end
end
