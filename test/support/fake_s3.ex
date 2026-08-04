defmodule DurableBuffer.Test.FakeS3 do
  @moduledoc """
  In-memory S3 served through `Req.Test`: PUT/GET/DELETE objects plus
  ListObjectsV2 with continuation-token pagination, backed by an Agent so
  each test owns an isolated bucket.
  """

  import Plug.Conn

  def start_store do
    Agent.start_link(fn -> %{} end)
  end

  def objects(store) do
    Agent.get(store, & &1)
  end

  def call(conn, store, opts \\ []) do
    page_size = Keyword.get(opts, :page_size, 1000)
    conn = fetch_query_params(conn)

    case {conn.method, Map.get(conn, :request_path) || "/"} do
      {"PUT", "/" <> key} ->
        {:ok, body, conn} = read_body(conn)
        Agent.update(store, &Map.put(&1, key, body))
        send_resp(conn, 200, "")

      {"GET", "/"} ->
        list(conn, store, page_size)

      {"GET", "/" <> key} ->
        case Agent.get(store, &Map.fetch(&1, key)) do
          {:ok, body} -> send_resp(conn, 200, body)
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
      |> objects()
      |> Map.keys()
      |> Enum.filter(&String.starts_with?(&1, prefix))
      |> Enum.sort()
      |> Enum.drop_while(fn key -> continuation_token && key <= continuation_token end)

    {page, rest} = Enum.split(matching, page_size)

    contents = Enum.map_join(page, fn key -> "<Contents><Key>#{key}</Key></Contents>" end)

    truncation =
      case {rest, page} do
        {[], _page} ->
          "<IsTruncated>false</IsTruncated>"

        {_rest, page} ->
          "<IsTruncated>true</IsTruncated>" <>
            "<NextContinuationToken>#{List.last(page)}</NextContinuationToken>"
      end

    xml =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" <>
        "<ListBucketResult><Name>fake</Name>#{truncation}#{contents}</ListBucketResult>"

    conn
    |> put_resp_content_type("application/xml")
    |> send_resp(200, xml)
  end
end
