defmodule DurableBuffer.Test.FakeS3 do
  @moduledoc """
  In-memory S3 served through `Req.Test`: PUT/GET/DELETE objects plus
  ListObjectsV2 with continuation-token pagination, backed by an Agent so
  each test owns an isolated bucket.

  A PUT stamps the object with the current time. `age/3` rewrites that
  stamp, so a test can make a segment look old without waiting.

  Conditional writes follow S3 and GCS. Every GET and PUT response carries an
  `etag` (the quoted MD5 of the body) and an `x-goog-generation`. A PUT with
  `if-none-match: *` or `x-goog-if-generation-match: 0` fails with 412 when
  the object exists. A PUT with `if-match` or a non-zero
  `x-goog-if-generation-match` fails with 412 when the object changed, and
  with 404 (`if-match`) or 412 (generation) when it is missing. The check and
  the write are one atomic step.
  """

  import Plug.Conn

  def start_store do
    Agent.start_link(fn -> %{} end)
  end

  def objects(store) do
    Agent.get(store, fn objects ->
      Map.new(objects, fn {key, {body, _ms, _generation}} -> {key, body} end)
    end)
  end

  @doc """
  Backdates every object under `prefix` by `ms` milliseconds.
  """
  def age(store, prefix, ms) do
    Agent.update(store, fn objects ->
      Map.new(objects, fn
        {key, {body, stamp, generation}} ->
          if String.starts_with?(key, prefix),
            do: {key, {body, stamp - ms, generation}},
            else: {key, {body, stamp, generation}}
      end)
    end)
  end

  def call(conn, store, opts \\ []) do
    page_size = Keyword.get(opts, :page_size, 1000)
    conn = fetch_query_params(conn)

    case {conn.method, Map.get(conn, :request_path) || "/"} do
      {"PUT", "/" <> key} ->
        {body, conn} = read_full_body(conn, [])
        put(conn, store, key, body)

      {"GET", "/"} ->
        list(conn, store, page_size)

      {"GET", "/" <> key} ->
        case Agent.get(store, &Map.fetch(&1, key)) do
          {:ok, {body, _stamp, generation}} ->
            conn |> version_headers(body, generation) |> send_resp(200, body)

          :error ->
            send_resp(conn, 404, "")
        end

      {"DELETE", "/" <> key} ->
        Agent.update(store, &Map.delete(&1, key))
        send_resp(conn, 204, "")
    end
  end

  defp put(conn, store, key, body) do
    conditions = conditions(conn)
    stamp = System.system_time(:millisecond)
    generation = System.unique_integer([:positive, :monotonic])

    result =
      Agent.get_and_update(store, fn objects ->
        case check(Map.get(objects, key), conditions) do
          :ok -> {:ok, Map.put(objects, key, {body, stamp, generation})}
          status -> {status, objects}
        end
      end)

    case result do
      :ok -> conn |> version_headers(body, generation) |> send_resp(200, "")
      status -> send_resp(conn, status, "")
    end
  end

  defp conditions(conn) do
    %{
      if_match: header(conn, "if-match"),
      if_none_match: header(conn, "if-none-match"),
      generation_match: header(conn, "x-goog-if-generation-match")
    }
  end

  defp header(conn, name) do
    case get_req_header(conn, name) do
      [value | _rest] -> value
      [] -> nil
    end
  end

  defp check(existing, %{if_none_match: "*"}) when existing != nil, do: 412
  defp check(nil, %{if_match: etag}) when etag != nil, do: 404
  defp check(nil, %{generation_match: generation}) when generation not in [nil, "0"], do: 412
  defp check(existing, %{generation_match: "0"}) when existing != nil, do: 412

  defp check({body, _stamp, generation}, conditions) do
    cond do
      conditions.if_match != nil and conditions.if_match != etag(body) ->
        412

      conditions.generation_match not in [nil, "0"] and
          conditions.generation_match != Integer.to_string(generation) ->
        412

      true ->
        :ok
    end
  end

  defp check(nil, _conditions), do: :ok

  defp version_headers(conn, body, generation) do
    conn
    |> put_resp_header("etag", etag(body))
    |> put_resp_header("x-goog-generation", Integer.to_string(generation))
  end

  defp etag(body), do: ~s("#{Base.encode16(:erlang.md5(body), case: :lower)}")

  defp read_full_body(conn, acc) do
    case read_body(conn) do
      {:ok, chunk, conn} -> {IO.iodata_to_binary([acc, chunk]), conn}
      {:more, chunk, conn} -> read_full_body(conn, [acc, chunk])
    end
  end

  defp list(conn, store, page_size) do
    prefix = Map.get(conn.query_params, "prefix", "")
    continuation_token = Map.get(conn.query_params, "continuation-token")

    matching =
      store
      |> Agent.get(& &1)
      |> Enum.filter(fn {key, _object} -> String.starts_with?(key, prefix) end)
      |> Enum.sort_by(fn {key, _object} -> key end)
      |> Enum.drop_while(fn {key, _object} -> continuation_token && key <= continuation_token end)

    {page, rest} = Enum.split(matching, page_size)

    contents =
      Enum.map_join(page, fn {key, {body, stamp, _generation}} ->
        "<Contents><Key>#{key}</Key>" <>
          "<LastModified>#{stamp |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()}</LastModified>" <>
          "<Size>#{byte_size(body)}</Size></Contents>"
      end)

    truncation =
      case {rest, page} do
        {[], _page} ->
          "<IsTruncated>false</IsTruncated>"

        {_rest, page} ->
          "<IsTruncated>true</IsTruncated>" <>
            "<NextContinuationToken>#{page |> List.last() |> elem(0)}</NextContinuationToken>"
      end

    xml =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" <>
        "<ListBucketResult><Name>fake</Name>#{truncation}#{contents}</ListBucketResult>"

    conn
    |> put_resp_content_type("application/xml")
    |> send_resp(200, xml)
  end
end
