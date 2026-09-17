defmodule DurableBuffer.Queue.Store do
  @moduledoc """
  Object storage I/O for the queue: the manifest with compare-and-set, and
  data batches.

  Configuration comes from `config/1`:

    * `:bucket` (required)
    * `:prefix` — where batch objects go, default `"durable_buffer/queue"`
    * `:manifest` — the manifest key, default `"<prefix>/manifest"`
    * `:compression` — `:zstd` (default) or `:none`
    * `:conditional_writes` — `:etag` (default) or `:gcs_generation`
    * `:manifest_gap_ms` — the pause between one node's manifest writes, see
      `DurableBuffer.Queue.Appender`; default 50
    * `:req_options` — merged into the `Req` request, as for
      `DurableBuffer.Backend.S3`

  With `:etag` a manifest update sends `if-match` with the ETag of the read,
  and a create sends `if-none-match: *`. S3 and MinIO support both. With
  `:gcs_generation` they are `x-goog-if-generation-match` with the read's
  `x-goog-generation`, and `0` for a create, for the GCS XML API. A 412 or a
  409 is a conflict: another writer changed the manifest first.
  """

  @compile {:no_warn_undefined, [Req, ReqS3]}

  @conflict_backoff_ms 5

  alias DurableBuffer.Queue.Batch
  alias DurableBuffer.Queue.Manifest

  @type version :: {:etag, String.t()} | {:generation, String.t()} | nil
  @type cache :: {Manifest.t(), version()} | nil
  @type updater ::
          (Manifest.t() -> {:write, Manifest.t(), term()} | {:skip, term()} | {:error, term()})

  @doc """
  Normalizes queue options into a config map.
  """
  @spec config(keyword()) :: map()
  def config(opts) do
    unless DurableBuffer.Backend.S3.available?() do
      raise ArgumentError,
            "the queue backend needs :req and :req_s3, which are not loaded. " <>
              ~s|Add {:req, "~> 0.5"} and {:req_s3, "~> 0.2"} to your own application.|
    end

    prefix = opts |> Keyword.get(:prefix, "durable_buffer/queue") |> String.trim_trailing("/")

    conditional_writes = Keyword.get(opts, :conditional_writes, :etag)

    unless conditional_writes in [:etag, :gcs_generation] do
      raise ArgumentError,
            ":conditional_writes must be :etag or :gcs_generation, got " <>
              inspect(conditional_writes)
    end

    %{
      bucket: Keyword.fetch!(opts, :bucket),
      prefix: prefix,
      manifest: Keyword.get(opts, :manifest, prefix <> "/manifest"),
      compression: Batch.validate!(Keyword.get(opts, :compression, :zstd)),
      conditional_writes: conditional_writes,
      manifest_gap_ms: Keyword.get(opts, :manifest_gap_ms, 50),
      req_options: Keyword.get(opts, :req_options, [])
    }
  end

  @doc """
  Builds the `Req` request for `config`.
  """
  @spec req(map()) :: Req.Request.t()
  def req(config) do
    config.req_options
    |> Req.new()
    |> ReqS3.attach()
  end

  @doc """
  Reads the manifest and the version to compare against on write. A missing
  manifest reads as empty with a `nil` version.
  """
  @spec read_manifest(Req.Request.t(), map()) ::
          {:ok, Manifest.t(), version()} | {:error, term()}
  def read_manifest(req, config) do
    case Req.get(req, url: url(config, config.manifest), decode_body: false) do
      {:ok, %{status: 200, body: body} = response} ->
        with {:ok, manifest} <- Manifest.decode(body),
             {:ok, version} <- version(config, response) do
          {:ok, manifest, version}
        end

      {:ok, %{status: 404}} ->
        {:ok, Manifest.new(), nil}

      {:ok, %{status: status}} ->
        {:error, {:unexpected_status, status}}

      {:error, exception} ->
        {:error, exception}
    end
  end

  @doc """
  Writes the manifest only if it still matches `version`. A `nil` version
  writes only if no manifest exists.
  """
  @spec write_manifest(Req.Request.t(), map(), Manifest.t(), version()) ::
          :ok | :conflict | {:error, term()}
  def write_manifest(req, config, manifest, version) do
    case put_manifest(req, config, manifest, version) do
      {:ok, _version} -> :ok
      other -> other
    end
  end

  defp put_manifest(req, config, manifest, version) do
    body = IO.iodata_to_binary(Manifest.encode(manifest))

    case Req.put(req,
           url: url(config, config.manifest),
           body: body,
           headers: condition_headers(config, version)
         ) do
      {:ok, %{status: status} = response} when status in 200..299 ->
        case version(config, response) do
          {:ok, version} -> {:ok, version}
          {:error, _missing} -> {:ok, nil}
        end

      {:ok, %{status: status}} when status in [404, 409, 412] ->
        :conflict

      {:ok, %{status: status}} ->
        {:error, {:unexpected_status, status}}

      {:error, exception} ->
        {:error, exception}
    end
  end

  @doc """
  Reads the manifest, applies `fun`, and writes the result with
  compare-and-set, retrying on conflict.

  `fun` returns `{:write, manifest, result}` to write, `{:skip, result}` to
  return without writing, or `{:error, reason}` to stop. The success tuple
  also carries how many writes conflicted before one landed.
  """
  @spec update_manifest(Req.Request.t(), map(), updater()) ::
          {:ok, term(), non_neg_integer()} | {:error, term()}
  def update_manifest(req, config, fun) do
    with {:ok, result, conflicts, _cache} <- update_manifest(req, config, fun, nil) do
      {:ok, result, conflicts}
    end
  end

  @doc """
  Like `update_manifest/3`, but starts from `cache`, the `{manifest, version}`
  this writer wrote last, instead of reading.

  A writer that is usually the only one writing saves a GET per update: the
  write succeeds against the cached version, and only a conflict costs a
  read. Returns the manifest and version it wrote as the next cache, or `nil`
  when there is none. Only conflicts after a fresh read are counted.

  Each retry after a conflict first sleeps a random time up to a bound that
  doubles per conflict (5 ms, capped at 500 ms). Without the jitter, writers
  with the same request latency can fall into lockstep, and one of them loses
  every race.
  """
  @spec update_manifest(Req.Request.t(), map(), updater(), cache()) ::
          {:ok, term(), non_neg_integer(), cache()} | {:error, term()}
  def update_manifest(req, config, fun, cache), do: update(req, config, fun, cache, 0)

  defp update(req, config, fun, nil, conflicts) do
    with {:ok, manifest, version} <- read_manifest(req, config) do
      apply_update(req, config, fun, {manifest, version}, conflicts, :fresh)
    end
  end

  defp update(req, config, fun, cache, conflicts) do
    apply_update(req, config, fun, cache, conflicts, :cached)
  end

  defp apply_update(req, config, fun, {manifest, version}, conflicts, source) do
    case fun.(manifest) do
      {:skip, result} ->
        {:ok, result, conflicts, {manifest, version}}

      {:error, reason} ->
        {:error, reason}

      {:write, updated, result} ->
        case put_manifest(req, config, updated, version) do
          {:ok, nil} ->
            {:ok, result, conflicts, nil}

          {:ok, new_version} ->
            {:ok, result, conflicts, {updated, new_version}}

          :conflict ->
            conflicts = if source == :fresh, do: conflicts + 1, else: conflicts
            Process.sleep(:rand.uniform(backoff_ms(conflicts)))
            update(req, config, fun, nil, conflicts)

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Uploads a data batch object.
  """
  @spec put_batch(Req.Request.t(), map(), String.t(), binary()) :: :ok | {:error, term()}
  def put_batch(req, config, location, body) do
    case Req.put(req, url: url(config, location), body: body) do
      {:ok, %{status: status}} when status in 200..299 -> :ok
      {:ok, %{status: status}} -> {:error, {:unexpected_status, status}}
      {:error, exception} -> {:error, exception}
    end
  end

  @doc """
  Downloads a data batch object.
  """
  @spec get_batch(Req.Request.t(), map(), String.t()) :: {:ok, binary()} | {:error, term()}
  def get_batch(req, config, location) do
    case Req.get(req, url: url(config, location), decode_body: false) do
      {:ok, %{status: 200, body: body}} -> {:ok, body}
      {:ok, %{status: 404}} -> {:error, :not_found}
      {:ok, %{status: status}} -> {:error, {:unexpected_status, status}}
      {:error, exception} -> {:error, exception}
    end
  end

  @doc """
  Deletes an object.
  """
  @spec delete(Req.Request.t(), map(), String.t()) :: :ok | {:error, term()}
  def delete(req, config, key) do
    case Req.delete(req, url: url(config, key)) do
      {:ok, %{status: status}} when status in 200..299 or status == 404 -> :ok
      {:ok, %{status: status}} -> {:error, {:unexpected_status, status}}
      {:error, exception} -> {:error, exception}
    end
  end

  @doc """
  Lists the keys of every `.batch` object under the prefix.
  """
  @spec list_batches(Req.Request.t(), map()) :: {:ok, [String.t()]} | {:error, term()}
  def list_batches(req, config) do
    list(req, config, config.prefix <> "/", nil, [])
  end

  @doc """
  The key for a new batch object: `<prefix>/<ULID>.batch`.
  """
  @spec batch_location(map(), String.t()) :: String.t()
  def batch_location(config, ulid), do: "#{config.prefix}/#{ulid}.batch"

  defp list(req, config, prefix, token, acc) do
    params =
      [{"list-type", "2"}, {"prefix", prefix}] ++
        if(token, do: [{"continuation-token", token}], else: [])

    case Req.get(req, url: "s3://#{config.bucket}?#{URI.encode_query(params)}") do
      {:ok, %{status: 200, body: %{"ListBucketResult" => result}}} ->
        keys =
          result
          |> Map.get("Contents", [])
          |> List.wrap()
          |> Enum.map(&Map.fetch!(&1, "Key"))
          |> Enum.filter(&String.ends_with?(&1, ".batch"))

        case result do
          %{"IsTruncated" => "true", "NextContinuationToken" => next} ->
            list(req, config, prefix, next, acc ++ keys)

          _last_page ->
            {:ok, acc ++ keys}
        end

      {:ok, %{status: status}} ->
        {:error, {:unexpected_status, status}}

      {:error, exception} ->
        {:error, exception}
    end
  end

  defp backoff_ms(conflicts),
    do: min(@conflict_backoff_ms * Integer.pow(2, min(conflicts, 6)), 500)

  defp url(config, key), do: "s3://#{config.bucket}/#{key}"

  defp version(%{conditional_writes: :etag}, response) do
    case Req.Response.get_header(response, "etag") do
      [etag | _rest] -> {:ok, {:etag, etag}}
      [] -> {:error, :missing_etag}
    end
  end

  defp version(%{conditional_writes: :gcs_generation}, response) do
    case Req.Response.get_header(response, "x-goog-generation") do
      [generation | _rest] -> {:ok, {:generation, generation}}
      [] -> {:error, :missing_generation}
    end
  end

  defp condition_headers(%{conditional_writes: :etag}, nil), do: [{"if-none-match", "*"}]
  defp condition_headers(_config, {:etag, etag}), do: [{"if-match", etag}]

  defp condition_headers(%{conditional_writes: :gcs_generation}, nil),
    do: [{"x-goog-if-generation-match", "0"}]

  defp condition_headers(_config, {:generation, generation}),
    do: [{"x-goog-if-generation-match", generation}]
end
