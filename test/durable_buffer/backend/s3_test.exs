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
    {:ok, state} = S3.commit(state, batch, bytes, span(S3, state, batch))
    {batch, bytes} = encode_batch(["c"])
    {:ok, state} = S3.commit(state, batch, bytes, span(S3, state, batch))

    assert Map.keys(FakeS3.objects(store)) |> Enum.sort() == [
             "buffers/test/p0/000000000000.wal",
             "buffers/test/p0/000000000002.wal"
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
    {:ok, _state0} = S3.commit(state0, batch, bytes, span(S3, state0, batch))
    {batch, bytes} = encode_batch(["p1"])
    {:ok, _state1} = S3.commit(state1, batch, bytes, span(S3, state1, batch))

    assert Enum.to_list(S3.stream(config, 0)) == ["p0"]
    assert Enum.to_list(S3.stream(config, 1)) == ["p1"]
  end

  test "open resumes offsets after existing segments" do
    {config, store} = start_backend()
    {:ok, state} = S3.open(config, 0)
    {batch, bytes} = encode_batch(["first"])
    {:ok, _state} = S3.commit(state, batch, bytes, span(S3, state, batch))

    {:ok, reopened} = S3.open(config, 0)
    assert S3.offsets(reopened) == %{first: 0, next: 1}

    {batch, bytes} = encode_batch(["second"])
    {:ok, _state} = S3.commit(reopened, batch, bytes, span(S3, reopened, batch))

    assert map_size(FakeS3.objects(store)) == 2
    assert Enum.to_list(S3.stream(config, 0)) == ["first", "second"]
  end

  test "listing paginates with continuation tokens" do
    {config, _store} = start_backend(page_size: 2)
    {:ok, state} = S3.open(config, 0)

    state =
      Enum.reduce(1..5, state, fn index, state ->
        {batch, bytes} = encode_batch(["entry-#{index}"])
        {:ok, state} = S3.commit(state, batch, bytes, span(S3, state, batch))
        state
      end)

    assert Enum.to_list(S3.stream(config, 0)) == Enum.map(1..5, &"entry-#{&1}")

    {:ok, reopened} = S3.open(config, 0)
    assert S3.offsets(reopened) == S3.offsets(state)
    assert S3.offsets(reopened) == %{first: 0, next: 5}
  end

  test "truncate deletes only the partition's segments" do
    {config, store} = start_backend()
    {:ok, state0} = S3.open(config, 0)
    {:ok, state1} = S3.open(config, 1)

    {batch, bytes} = encode_batch(["drop"])
    {:ok, state0} = S3.commit(state0, batch, bytes, span(S3, state0, batch))
    {batch, bytes} = encode_batch(["keep"])
    {:ok, _state1} = S3.commit(state1, batch, bytes, span(S3, state1, batch))

    {:ok, _state0} = S3.truncate(state0, 1)

    assert Map.keys(FakeS3.objects(store)) |> Enum.sort() == [
             "buffers/test/p0/base",
             "buffers/test/p1/000000000000.wal"
           ]
  end

  test "offsets stay monotonic across a truncate" do
    {config, _store} = start_backend()
    {:ok, state} = S3.open(config, 0)

    {batch, bytes} = encode_batch(["one", "two"])
    {:ok, state} = S3.commit(state, batch, bytes, span(S3, state, batch))
    assert S3.offsets(state) == %{first: 0, next: 2}

    {:ok, state} = S3.truncate(state, 2)
    assert S3.offsets(state) == %{first: 2, next: 2}

    {:ok, reopened} = S3.open(config, 0)
    assert S3.offsets(reopened) == %{first: 2, next: 2}

    {batch, bytes} = encode_batch(["three"])
    {:ok, state} = S3.commit(reopened, batch, bytes, span(S3, reopened, batch))
    assert S3.offsets(state) == %{first: 2, next: 3}

    assert Enum.to_list(S3.stream(config, 0, with_offsets: true)) == [{2, "three"}]
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

    assert {:error, {:unexpected_status, 500}, _state} =
             S3.commit(state, batch, bytes, span(S3, state, batch))
  end

  defp span(module, state, batch) do
    {payloads, _valid, _rest} = batch |> IO.iodata_to_binary() |> DurableBuffer.WAL.decode_all()
    {module.offsets(state).next, length(payloads)}
  end

  test "trim drops only segments that lie entirely below the point" do
    {config, store} = start_backend()
    {:ok, state} = S3.open(config, 0)

    state =
      Enum.reduce([["a", "b"], ["c", "d"], ["e", "f"]], state, fn payloads, state ->
        {batch, bytes} = encode_batch(payloads)
        {:ok, state} = S3.commit(state, batch, bytes, span(S3, state, batch))
        state
      end)

    assert S3.offsets(state) == %{first: 0, next: 6}

    {:ok, state} = S3.trim(state, 3)

    assert S3.offsets(state) == %{first: 2, next: 6}
    assert Enum.to_list(S3.stream(config, 0)) == ~w(c d e f)

    assert Map.keys(FakeS3.objects(store)) |> Enum.sort() == [
             "buffers/test/p0/000000000002.wal",
             "buffers/test/p0/000000000004.wal",
             "buffers/test/p0/base"
           ]

    {:ok, reopened} = S3.open(config, 0)
    assert S3.offsets(reopened) == %{first: 2, next: 6}

    assert Enum.to_list(S3.stream(config, 0, from: 4, with_offsets: true)) ==
             [{4, "e"}, {5, "f"}]
  end

  describe "retention_point/2" do
    setup do
      {config, store} = start_backend()
      {:ok, state} = S3.open(config, 0)

      state =
        Enum.reduce([["a", "b"], ["c", "d"], ["e", "f"]], state, fn payloads, state ->
          {batch, bytes} = encode_batch(payloads)
          {:ok, state} = S3.commit(state, batch, bytes, span(S3, state, batch))
          state
        end)

      %{config: config, store: store, state: state}
    end

    test "cuts at the oldest segment written after the cutoff", %{store: store, state: state} do
      FakeS3.age(store, "buffers/test/p0/000000000000.wal", 60_000)
      FakeS3.age(store, "buffers/test/p0/000000000002.wal", 60_000)

      assert {:ok, 4} = S3.retention_point(state, %{ms: 30_000, bytes: nil})
    end

    test "keeps the newest segment when every one is older", %{store: store, state: state} do
      FakeS3.age(store, "buffers/test/p0/", 60_000)

      assert {:ok, 4} = S3.retention_point(state, %{ms: 30_000, bytes: nil})
    end

    test "cuts to leave no more than retention_bytes", %{state: state} do
      {:ok, point} = S3.retention_point(state, %{ms: nil, bytes: 30})

      assert point == 4
    end

    test "reports :none while neither bound is exceeded", %{state: state} do
      assert :none = S3.retention_point(state, %{ms: 60_000, bytes: 1_000_000})
    end

    test "reports the oldest segment's age and the bytes held", %{store: store, state: state} do
      FakeS3.age(store, "buffers/test/p0/000000000000.wal", 60_000)

      assert %{oldest_ms: oldest_ms, bytes: bytes} = S3.retention_status(state)
      assert System.system_time(:millisecond) - oldest_ms >= 60_000
      assert bytes == 6 * (8 + 1)
    end
  end
end
