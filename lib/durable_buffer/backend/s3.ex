defmodule DurableBuffer.Backend.S3 do
  @moduledoc """
  S3 backend using `Req` + `ReqS3`.

  Each group commit uploads one immutable segment object
  `<prefix>/p<partition>/<offset>.wal` containing the framed batch, so a
  commit is durable exactly when the PUT succeeds — there is no fsync
  equivalent to manage and no torn-write recovery. Reads LIST the partition's
  segments in key order and GET them lazily. Reads need no durability gate:
  an object exists only once its PUT succeeded.

  The key is the segment's first logical entry offset, zero-padded so keys
  sort in offset order. That makes the key list its own seek index: a
  `from:` read picks the last segment starting at or before the wanted
  offset and skips the few entries inside it, with no extra state to keep.
  Offsets resume from a LIST plus one GET of the newest segment on open, and
  a truncate records the new base in a sibling `base` object so offsets stay
  monotonic across it.

  `open/2` reads the newest segment, so it needs more than a LIST to
  succeed. A transient failure there raises rather than guessing: an
  under-counted newest segment would re-issue offsets that already name
  entries. Failing lets the supervisor retry.


  S3 PUT latency is high relative to disk, which makes group commit the whole
  ballgame: while one PUT is in flight every new append queues into the next
  batch, so throughput is bounded by `batch size × partitions / PUT latency`,
  not by caller count.

  Options:

    * `:bucket` (required)
    * `:prefix` — key prefix, default `"durable_buffer"`
    * `:req_options` — extra options merged into the `Req` request; use
      `aws_sigv4: [access_key_id: ..., secret_access_key: ...]` for
      credentials (or rely on `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`),
      `aws_endpoint_url_s3:` for MinIO and other S3-compatibles, and
      `plug: {Req.Test, ...}` in tests
  """

  @behaviour DurableBuffer.Backend

  alias DurableBuffer.WAL

  @offset_width 12

  @impl DurableBuffer.Backend
  def init_config(opts) do
    %{
      bucket: Keyword.fetch!(opts, :bucket),
      prefix: Keyword.get(opts, :prefix, "durable_buffer"),
      req_options: Keyword.get(opts, :req_options, [])
    }
  end

  @impl DurableBuffer.Backend
  def open(config, partition_index) do
    req = build_req(config)
    base = load_base(req, config, partition_index)

    {first, next} =
      case list_keys(req, config, partition_index) do
        [] ->
          {base, base}

        keys ->
          last = List.last(keys)

          {offset_from_key(List.first(keys)),
           offset_from_key(last) + count_entries(req, config, last)}
      end

    {:ok,
     %{
       req: req,
       config: config,
       partition_index: partition_index,
       first_offset: first,
       next_offset: next
     }}
  end

  @doc """
  Logical entry offsets as of `open/2` or the last `truncate/1`.

  Segment keys are the segment's first entry offset, so the bounds come from
  a LIST plus one GET of the newest segment to count its entries.
  """
  @impl DurableBuffer.Backend
  @spec offsets(map()) :: %{first: non_neg_integer(), next: non_neg_integer()}
  def offsets(state) do
    %{first: state.first_offset, next: state.next_offset}
  end

  @impl DurableBuffer.Backend
  def commit(state, batch, _byte_size, {first_offset, count}) do
    key = object_key(state.config, state.partition_index, first_offset)
    binary = IO.iodata_to_binary(batch)

    case Req.put(state.req, url: "s3://#{state.config.bucket}/#{key}", body: binary) do
      {:ok, %Req.Response{status: status}} when status in 200..299 ->
        {:ok, %{state | next_offset: first_offset + count}}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:unexpected_status, status}, state}

      {:error, exception} ->
        {:error, exception, state}
    end
  end

  @impl DurableBuffer.Backend
  def stream(config, partition_index) do
    req = build_req(config)
    stream_keys(req, config, list_keys(req, config, partition_index))
  end

  defp stream_keys(req, config, keys) do
    Stream.resource(
      fn -> {req, keys} end,
      fn
        {_req, []} ->
          {:halt, :done}

        {req, [key | rest]} ->
          %Req.Response{status: 200, body: body} =
            Req.get!(req, url: "s3://#{config.bucket}/#{key}", decode_body: false)

          {payloads, _valid, _rest} = WAL.decode_all(body)
          {payloads, {req, rest}}
      end,
      fn _acc -> :ok end
    )
  end

  @impl DurableBuffer.Backend
  def stream(config, partition_index, opts) do
    req = build_req(config)
    keys = list_keys(req, config, partition_index)
    from = Keyword.get(opts, :from)
    {keys, base} = seek(keys, from, req, config, partition_index)

    req
    |> stream_keys(config, keys)
    |> project(base, from, Keyword.get(opts, :with_offsets, false))
  end

  defp seek(keys, nil, req, config, partition_index) do
    {keys, base_offset(keys, req, config, partition_index)}
  end

  defp seek(keys, from, req, config, partition_index) do
    case Enum.split_while(keys, &(offset_from_key(&1) <= from)) do
      {[], after_from} ->
        {after_from, base_offset(keys, req, config, partition_index)}

      {at_or_before, after_from} ->
        floor_key = List.last(at_or_before)
        {[floor_key | after_from], offset_from_key(floor_key)}
    end
  end

  defp base_offset([], req, config, partition_index) do
    load_base(req, config, partition_index)
  end

  defp base_offset([first | _rest], _req, _config, _partition_index) do
    offset_from_key(first)
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
  The offset a retention policy would cut at, or `:none` when neither bound
  is exceeded.

  Segments are objects, so the LIST that `trim/2` already does carries both
  bounds: `LastModified` dates a segment and `Size` measures it. No extra
  request, and no state of our own to keep.

  Age comes from when a segment was written, not from when its entries were
  produced. The two differ by at most one group commit.
  """
  @impl DurableBuffer.Backend
  @spec retention_point(map(), map()) :: {:ok, non_neg_integer()} | :none
  def retention_point(state, policy) do
    segments = list_objects(state.req, state.config, state.partition_index)

    case Enum.reject(
           [time_point(segments, policy[:ms]), size_point(segments, policy[:bytes])],
           &is_nil/1
         ) do
      [] -> :none
      points -> {:ok, Enum.max(points)}
    end
  end

  defp time_point([], _ms), do: nil
  defp time_point(_segments, nil), do: nil

  defp time_point(segments, ms) do
    cutoff = System.system_time(:millisecond) - ms

    case Enum.find(segments, &(&1.modified_ms != nil and &1.modified_ms >= cutoff)) do
      nil -> offset_from_key(List.last(segments).key)
      segment -> offset_from_key(segment.key)
    end
  end

  defp size_point([], _bytes), do: nil
  defp size_point(_segments, nil), do: nil

  defp size_point(segments, bytes) do
    case Enum.find(Enum.zip(segments, suffix_sizes(segments)), fn {_segment, kept} ->
           kept <= bytes
         end) do
      nil -> offset_from_key(List.last(segments).key)
      {segment, _kept} -> offset_from_key(segment.key)
    end
  end

  defp suffix_sizes(segments) do
    segments
    |> Enum.reverse()
    |> Enum.scan(0, fn segment, total -> total + segment.size end)
    |> Enum.reverse()
  end

  @impl DurableBuffer.Backend
  @spec retention_status(map()) :: %{oldest_ms: integer() | nil, bytes: non_neg_integer()}
  def retention_status(state) do
    segments = list_objects(state.req, state.config, state.partition_index)

    %{
      oldest_ms: oldest_ms(segments),
      bytes: segments |> Enum.map(& &1.size) |> Enum.sum()
    }
  end

  defp oldest_ms([segment | _rest]), do: segment.modified_ms
  defp oldest_ms([]), do: nil

  @doc """
  Deletes every segment that lies entirely below `upto`.

  Segments are immutable objects, so a partly-covered one is kept whole.
  `first` therefore lands on a segment boundary at or below `upto`, and a
  reader may still see a few entries under the requested trim point. The
  local backend cuts exactly, since it can rewrite its file.
  """
  @impl DurableBuffer.Backend
  @spec trim(map(), non_neg_integer()) :: {:ok, map()}
  def trim(state, upto) do
    keys = list_keys(state.req, state.config, state.partition_index)

    dropped =
      for {key, segment_end} <- segment_ends(keys, state.next_offset),
          segment_end <= upto,
          do: key

    for key <- dropped do
      %Req.Response{status: status} =
        Req.delete!(state.req, url: "s3://#{state.config.bucket}/#{key}")

      true = status in 200..299
    end

    first =
      case keys -- dropped do
        [] -> state.next_offset
        [kept | _rest] -> offset_from_key(kept)
      end

    :ok = store_base(state.req, state.config, state.partition_index, first)
    {:ok, %{state | first_offset: first}}
  end

  defp describe(content) do
    %{
      key: Map.fetch!(content, "Key"),
      modified_ms: modified_ms(Map.get(content, "LastModified")),
      size: content |> Map.get("Size", "0") |> String.to_integer()
    }
  end

  defp modified_ms(nil), do: nil

  defp modified_ms(stamp) do
    case DateTime.from_iso8601(stamp) do
      {:ok, datetime, _offset} -> DateTime.to_unix(datetime, :millisecond)
      {:error, _reason} -> nil
    end
  end

  defp segment_ends(keys, next_offset) do
    offsets = Enum.map(keys, &offset_from_key/1)
    Enum.zip(keys, Enum.drop(offsets, 1) ++ [next_offset])
  end

  @impl DurableBuffer.Backend
  def truncate(state, next) do
    for key <- list_keys(state.req, state.config, state.partition_index) do
      %Req.Response{status: status} =
        Req.delete!(state.req, url: "s3://#{state.config.bucket}/#{key}")

      true = status in 200..299
    end

    :ok = store_base(state.req, state.config, state.partition_index, next)

    {:ok, %{state | first_offset: next, next_offset: next}}
  end

  @impl DurableBuffer.Backend
  def close(_state) do
    :ok
  end

  defp build_req(config) do
    config.req_options
    |> Req.new()
    |> ReqS3.attach()
  end

  defp partition_prefix(config, partition_index) do
    "#{config.prefix}/p#{partition_index}/"
  end

  defp object_key(config, partition_index, offset) do
    padded = offset |> Integer.to_string() |> String.pad_leading(@offset_width, "0")
    "#{partition_prefix(config, partition_index)}#{padded}.wal"
  end

  defp offset_from_key(key) do
    key
    |> Path.basename(".wal")
    |> String.to_integer()
  end

  defp base_key(config, partition_index) do
    "#{partition_prefix(config, partition_index)}base"
  end

  defp load_base(req, config, partition_index) do
    case Req.get(req,
           url: "s3://#{config.bucket}/#{base_key(config, partition_index)}",
           decode_body: false
         ) do
      {:ok, %Req.Response{status: 200, body: body}} when is_binary(body) -> parse_base(body)
      _missing_or_error -> 0
    end
  end

  defp parse_base(body) do
    case body |> String.trim() |> Integer.parse() do
      {base, ""} -> base
      _not_a_base -> 0
    end
  end

  defp store_base(req, config, partition_index, base) do
    %Req.Response{status: status} =
      Req.put!(req,
        url: "s3://#{config.bucket}/#{base_key(config, partition_index)}",
        body: Integer.to_string(base)
      )

    true = status in 200..299
    :ok
  end

  defp count_entries(req, config, key) do
    {payloads, _valid, _rest} = WAL.decode_all(fetch!(req, config, key))
    length(payloads)
  end

  defp fetch!(req, config, key) do
    case Req.get(req, url: "s3://#{config.bucket}/#{key}", decode_body: false) do
      {:ok, %Req.Response{status: 200, body: body}} ->
        body

      {:ok, %Req.Response{status: status}} ->
        raise "DurableBuffer could not open #{key}: S3 answered #{status}"

      {:error, exception} ->
        raise "DurableBuffer could not open #{key}: #{Exception.message(exception)}"
    end
  end

  defp list_keys(req, config, partition_index) do
    req
    |> list_objects(config, partition_index)
    |> Enum.map(& &1.key)
  end

  defp list_objects(req, config, partition_index) do
    req
    |> list_prefix(config, partition_prefix(config, partition_index), nil, [])
    |> Enum.filter(&String.ends_with?(&1.key, ".wal"))
  end

  defp list_prefix(req, config, prefix, continuation_token, acc) do
    params =
      [
        {"list-type", "2"},
        {"prefix", prefix}
      ] ++
        if continuation_token do
          [{"continuation-token", continuation_token}]
        else
          []
        end

    %Req.Response{status: 200, body: body} =
      Req.get!(req, url: "s3://#{config.bucket}?#{URI.encode_query(params)}")

    %{"ListBucketResult" => result} = body

    objects =
      result
      |> Map.get("Contents", [])
      |> List.wrap()
      |> Enum.map(&describe/1)

    acc = acc ++ objects

    case result do
      %{"IsTruncated" => "true", "NextContinuationToken" => token} ->
        list_prefix(req, config, prefix, token, acc)

      _result ->
        Enum.sort_by(acc, & &1.key)
    end
  end
end
