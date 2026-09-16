defmodule DurableBuffer.TieredBufferTest do
  use ExUnit.Case, async: false

  alias DurableBuffer.Backend.S3
  alias DurableBuffer.Backend.Tiered
  alias DurableBuffer.Test.FakeS3

  @moduletag :tmp_dir

  setup {Req.Test, :set_req_test_to_shared}

  defp start_buffer(tmp_dir, backend_opts, handler \\ &FakeS3.call/2) do
    {:ok, store} = FakeS3.start_store()
    stub_name = :"fake_s3_#{System.unique_integer([:positive])}"
    Req.Test.stub(stub_name, fn conn -> handler.(conn, store) end)
    name = :"tiered_#{System.unique_integer([:positive])}"

    backend_opts =
      Keyword.merge(
        [
          dir: tmp_dir,
          bucket: "test-bucket",
          prefix: "events",
          req_options: [plug: {Req.Test, stub_name}, retry: false]
        ],
        backend_opts
      )

    start_supervised!({DurableBuffer, name: name, backend: {Tiered, backend_opts}, partitions: 1})

    s3_config = S3.init_config(Keyword.take(backend_opts, [:bucket, :prefix, :req_options]))
    %{name: name, store: store, s3_config: s3_config}
  end

  defp eventually(check, attempts \\ 200) do
    cond do
      check.() -> true
      attempts == 0 -> false
      true -> Process.sleep(10) && eventually(check, attempts - 1)
    end
  end

  test "ack: :local appends, reads, and uploads in the background", %{tmp_dir: tmp_dir} do
    %{name: name, s3_config: s3_config} =
      start_buffer(tmp_dir, segment_bytes: 64, segment_ms: 20)

    payloads = for index <- 1..20, do: "entry-#{index}"

    for payload <- payloads do
      assert {:ok, _offset} = DurableBuffer.append(name, "k", payload)
    end

    assert Enum.to_list(DurableBuffer.stream(name, "k")) == payloads
    assert DurableBuffer.offsets(name, "k") == %{first: 0, durable: 20, next: 20}

    assert eventually(fn -> Enum.to_list(S3.stream(s3_config, 0)) == payloads end)
    assert Enum.to_list(DurableBuffer.stream(name, "k", from: 15)) == Enum.drop(payloads, 15)
  end

  test "ack: :remote returns an append only once S3 holds it", %{tmp_dir: tmp_dir} do
    %{name: name, s3_config: s3_config} =
      start_buffer(tmp_dir, ack: :remote)

    tasks =
      for index <- 1..50 do
        Task.async(fn -> DurableBuffer.append(name, "k", "entry-#{index}") end)
      end

    results = Task.await_many(tasks, 5_000)
    assert Enum.all?(results, &match?({:ok, _offset}, &1))

    uploaded = s3_config |> S3.stream(0) |> Enum.count()
    assert uploaded == 50
    assert DurableBuffer.offsets(name, "k").durable == 50
    assert length(Enum.to_list(DurableBuffer.stream(name, "k"))) == 50
  end

  test "ack: :remote readers see only uploaded entries", %{tmp_dir: tmp_dir} do
    refuse_puts = fn conn, store ->
      if conn.method == "PUT",
        do: Plug.Conn.send_resp(conn, 503, "down"),
        else: FakeS3.call(conn, store)
    end

    %{name: name} = start_buffer(tmp_dir, [ack: :remote], refuse_puts)

    :ok = DurableBuffer.append_async(name, "k", "not yet")
    Process.sleep(50)

    assert DurableBuffer.offsets(name, "k") == %{first: 0, durable: 0, next: 1}
    assert Enum.to_list(DurableBuffer.stream(name, "k")) == []
    assert Enum.to_list(DurableBuffer.stream(name, "k", dirty: true)) == ["not yet"]
  end

  test "truncate empties both tiers", %{tmp_dir: tmp_dir} do
    %{name: name, store: store} = start_buffer(tmp_dir, segment_bytes: 1)

    {:ok, _offset} = DurableBuffer.append(name, "k", "one")
    {:ok, _offset} = DurableBuffer.append(name, "k", "two")
    assert eventually(fn -> map_size(FakeS3.objects(store)) >= 2 end)

    :ok = DurableBuffer.truncate(name, "k")

    assert DurableBuffer.offsets(name, "k") == %{first: 2, durable: 2, next: 2}
    assert Enum.to_list(DurableBuffer.stream(name, "k")) == []
    assert Map.keys(FakeS3.objects(store)) == ["events/p0/base"]
  end
end
