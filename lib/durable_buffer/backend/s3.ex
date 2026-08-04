defmodule DurableBuffer.Backend.S3 do
  @moduledoc """
  S3 backend using `Req` + `ReqS3`.

  Each group commit uploads one immutable segment object
  `<prefix>/p<partition>/<sequence>.wal` containing the framed batch, so a
  commit is durable exactly when the PUT succeeds — there is no fsync
  equivalent to manage and no torn-write recovery. The sequence resumes from
  a LIST on open. Reads LIST the partition's segments in key order and GET
  them lazily.

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

  @seq_width 12

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

    seq =
      case list_keys(req, config, partition_index) do
        [] -> 0
        keys -> keys |> List.last() |> seq_from_key() |> Kernel.+(1)
      end

    {:ok, %{req: req, config: config, partition_index: partition_index, seq: seq}}
  end

  @impl DurableBuffer.Backend
  def commit(state, batch, _byte_size) do
    key = object_key(state.config, state.partition_index, state.seq)

    case Req.put(state.req,
           url: "s3://#{state.config.bucket}/#{key}",
           body: IO.iodata_to_binary(batch)
         ) do
      {:ok, %Req.Response{status: status}} when status in 200..299 ->
        {:ok, %{state | seq: state.seq + 1}}

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
  def truncate(state) do
    for key <- list_keys(state.req, state.config, state.partition_index) do
      %Req.Response{status: status} =
        Req.delete!(state.req, url: "s3://#{state.config.bucket}/#{key}")

      true = status in 200..299
    end

    {:ok, state}
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

  defp object_key(config, partition_index, seq) do
    padded = seq |> Integer.to_string() |> String.pad_leading(@seq_width, "0")
    "#{partition_prefix(config, partition_index)}#{padded}.wal"
  end

  defp seq_from_key(key) do
    key
    |> Path.basename(".wal")
    |> String.to_integer()
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

    acc = acc ++ keys

    case result do
      %{"IsTruncated" => "true", "NextContinuationToken" => token} ->
        list_keys(req, config, partition_index, token, acc)

      _result ->
        Enum.sort(acc)
    end
  end
end
