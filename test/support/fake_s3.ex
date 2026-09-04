defmodule DurableBuffer.Test.FakeS3 do
  @moduledoc """
  In-memory S3 served through `Req.Test`: PUT/GET/DELETE objects plus
  ListObjectsV2 with continuation-token pagination, backed by an Agent so
  each test owns an isolated bucket.

  A PUT stamps the object with the current time. `age/3` rewrites that
  stamp, so a test can make a segment look old without waiting.
  """

  import Plug.Conn

  def start_store do
    Agent.start_link(fn -> %{} end)
  end

  def objects(store) do
    Agent.get(store, fn objects -> Map.new(objects, fn {key, {body, _ms}} -> {key, body} end) end)
  end

  @doc """
  Backdates every object under `prefix` by `ms` milliseconds.
  """
  def age(store, prefix, ms) do
    Agent.update(store, fn objects ->
      Map.new(objects, fn
        {key, {body, stamp}} ->
          if String.starts_with?(key, prefix),
            do: {key, {body, stamp - ms}},
            else: {key, {body, stamp}}
      end)
    end)
  end

  def call(conn, store, opts \\ []) do
    page_size = Keyword.get(opts, :page_size, 1000)
    conn = fetch_query_params(conn)

    case {conn.method, Map.get(conn, :request_path) || "/"} do
      {"PUT", "/" <> key} ->
        {:ok, body, conn} = read_body(conn)
        stamp = System.system_time(:millisecond)
        Agent.update(store, &Map.put(&1, key, {body, stamp}))
        send_resp(conn, 200, "")

      {"GET", "/"} ->
        list(conn, store, page_size)

      {"GET", "/" <> key} ->
        case Agent.get(store, &Map.fetch(&1, key)) do
          {:ok, {body, _stamp}} -> send_resp(conn, 200, body)
          :error -> send_resp(conn, 404, "")
        end

      {"DELETE", "/" <> key} ->
        Agent.update(store, &Map.delete(&1, key))
        send_resp(conn, 204, "")
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
      Enum.map_join(page, fn {key, {body, stamp}} ->
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
