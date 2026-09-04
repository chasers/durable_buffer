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
  sort in offset order. Offsets therefore resume from a LIST plus one GET of
  the newest segment on open, and a truncate records the new base in a
  sibling `base` object so offsets stay monotonic across it.

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
  def commit(state, batch, _byte_size) do
    key = object_key(state.config, state.partition_index, state.next_offset)
    binary = IO.iodata_to_binary(batch)
    {payloads, _valid, _rest} = WAL.decode_all(binary)

    case Req.put(state.req, url: "s3://#{state.config.bucket}/#{key}", body: binary) do
      {:ok, %Req.Response{status: status}} when status in 200..299 ->
        {:ok, %{state | next_offset: state.next_offset + length(payloads)}}

      {:ok, %Req.Response{status: status}} ->
        {:error, {:unexpected_status, status}, state}

      {:error, exception} ->
        {:error, exception, state}
    end
  end

  @impl DurableBuffer.Backend
  def stream(config, partition_index) do
    Stream.resource(
      fn ->
        req = build_req(config)
        {req, list_keys(req, config, partition_index)}
      end,
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

    base =
      case list_keys(req, config, partition_index) do
        [] -> load_base(req, config, partition_index)
        [first | _rest] -> offset_from_key(first)
      end

    config
    |> stream(partition_index)
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
    %Req.Response{status: 200, body: body} =
      Req.get!(req, url: "s3://#{config.bucket}/#{key}", decode_body: false)

    {payloads, _valid, _rest} = WAL.decode_all(body)
    length(payloads)
  end

  defp list_keys(req, config, partition_index) do
    list_keys(req, config, partition_index, nil, [])
  end

  defp list_keys(req, config, partition_index, continuation_token, acc) do
    params =
      [
        {"list-type", "2"},
        {"prefix", partition_prefix(config, partition_index)}
      ] ++
        if continuation_token do
          [{"continuation-token", continuation_token}]
        else
          []
        end

    %Req.Response{status: 200, body: body} =
      Req.get!(req, url: "s3://#{config.bucket}?#{URI.encode_query(params)}")

    %{"ListBucketResult" => result} = body

    keys =
      result
      |> Map.get("Contents", [])
      |> List.wrap()
      |> Enum.map(&Map.fetch!(&1, "Key"))
      |> Enum.filter(&String.ends_with?(&1, ".wal"))

    acc = acc ++ keys

    case result do
      %{"IsTruncated" => "true", "NextContinuationToken" => token} ->
        list_keys(req, config, partition_index, token, acc)

      _result ->
        Enum.sort(acc)
    end
  end
end
