defmodule DurableBuffer.Backend.S3Test do
  use ExUnit.Case, async: true

  alias DurableBuffer.Backend.S3
  alias DurableBuffer.Test.FakeS3
  alias DurableBuffer.WAL

  defp start_backend(context_opts \\ []) do
    {:ok, store} = FakeS3.start_store()
    stub_name = :"fake_s3_#{System.unique_integer([:positive])}"
    fake_opts = Keyword.take(context_opts, [:page_size])

    Req.Test.stub(stub_name, fn conn -> FakeS3.call(conn, store, fake_opts) end)

    config =
      S3.init_config(
        bucket: "test-bucket",
        prefix: "buffers/test",
        req_options: [plug: {Req.Test, stub_name}, retry: false]
      )

    {config, store}
  end

  defp encode_batch(payloads) do
    entries = Enum.map(payloads, &elem(WAL.encode(&1), 0))
    {entries, IO.iodata_length(entries)}
  end

  test "each commit writes one immutable segment object" do
    {config, store} = start_backend()
    {:ok, state} = S3.open(config, 0)

    {batch, bytes} = encode_batch(["a", "b"])
    {:ok, state} = S3.commit(state, batch, bytes)
    {batch, bytes} = encode_batch(["c"])
    {:ok, state} = S3.commit(state, batch, bytes)

    assert Map.keys(FakeS3.objects(store)) |> Enum.sort() == [
             "buffers/test/p0/000000000000.wal",
             "buffers/test/p0/000000000001.wal"
           ]

    assert Enum.to_list(S3.stream(config, 0)) == ["a", "b", "c"]
    assert :ok = S3.close(state)
  end

  test "streams an empty partition as an empty list" do
    {config, _store} = start_backend()
    assert Enum.to_list(S3.stream(config, 0)) == []
  end

  test "partitions use distinct key prefixes" do
    {config, _store} = start_backend()
    {:ok, state0} = S3.open(config, 0)
    {:ok, state1} = S3.open(config, 1)

    {batch, bytes} = encode_batch(["p0"])
    {:ok, _state0} = S3.commit(state0, batch, bytes)
    {batch, bytes} = encode_batch(["p1"])
    {:ok, _state1} = S3.commit(state1, batch, bytes)

    assert Enum.to_list(S3.stream(config, 0)) == ["p0"]
    assert Enum.to_list(S3.stream(config, 1)) == ["p1"]
  end

  test "open resumes the sequence after existing segments" do
    {config, store} = start_backend()
    {:ok, state} = S3.open(config, 0)
    {batch, bytes} = encode_batch(["first"])
    {:ok, _state} = S3.commit(state, batch, bytes)

    {:ok, reopened} = S3.open(config, 0)
    assert reopened.seq == 1

    {batch, bytes} = encode_batch(["second"])
    {:ok, _state} = S3.commit(reopened, batch, bytes)

    assert map_size(FakeS3.objects(store)) == 2
    assert Enum.to_list(S3.stream(config, 0)) == ["first", "second"]
  end

  test "listing paginates with continuation tokens" do
    {config, _store} = start_backend(page_size: 2)
    {:ok, state} = S3.open(config, 0)

    state =
      Enum.reduce(1..5, state, fn index, state ->
        {batch, bytes} = encode_batch(["entry-#{index}"])
        {:ok, state} = S3.commit(state, batch, bytes)
        state
      end)

    assert Enum.to_list(S3.stream(config, 0)) == Enum.map(1..5, &"entry-#{&1}")

    {:ok, reopened} = S3.open(config, 0)
    assert reopened.seq == state.seq
  end

  test "truncate deletes only the partition's segments" do
    {config, store} = start_backend()
    {:ok, state0} = S3.open(config, 0)
    {:ok, state1} = S3.open(config, 1)

    {batch, bytes} = encode_batch(["drop"])
    {:ok, state0} = S3.commit(state0, batch, bytes)
    {batch, bytes} = encode_batch(["keep"])
    {:ok, _state1} = S3.commit(state1, batch, bytes)

    {:ok, _state0} = S3.truncate(state0)

    assert Map.keys(FakeS3.objects(store)) == ["buffers/test/p1/000000000000.wal"]
  end

  test "a failed PUT surfaces as a commit error" do
    stub_name = :"fake_s3_#{System.unique_integer([:positive])}"

    Req.Test.stub(stub_name, fn conn ->
      case conn.method do
        "PUT" ->
          Plug.Conn.send_resp(conn, 500, "boom")

        "GET" ->
          conn
          |> Plug.Conn.put_resp_content_type("application/xml")
          |> Plug.Conn.send_resp(
            200,
            "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
          )
      end
    end)

    config =
      S3.init_config(
        bucket: "test-bucket",
        req_options: [plug: {Req.Test, stub_name}, retry: false]
      )

    {:ok, state} = S3.open(config, 0)
    {batch, bytes} = encode_batch(["doomed"])

    assert {:error, {:unexpected_status, 500}, _state} = S3.commit(state, batch, bytes)
  end
end
