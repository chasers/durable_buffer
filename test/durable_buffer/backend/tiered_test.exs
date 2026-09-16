defmodule DurableBuffer.Backend.TieredTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Backend.S3
  alias DurableBuffer.Backend.Tiered
  alias DurableBuffer.Test.FakeS3
  alias DurableBuffer.WAL

  @moduletag :tmp_dir

  defp start_s3(handler) do
    {:ok, store} = FakeS3.start_store()
    stub_name = :"fake_s3_#{System.unique_integer([:positive])}"

    Req.Test.stub(stub_name, fn conn ->
      if handler, do: handler.(conn, store), else: FakeS3.call(conn, store)
    end)

    {store, stub_name}
  end

  defp config(tmp_dir, stub_name, opts) do
    [
      dir: tmp_dir,
      bucket: "test-bucket",
      prefix: "tiered",
      req_options: [plug: {Req.Test, stub_name}, retry: false]
    ]
    |> Keyword.merge(opts)
    |> Tiered.init_config()
  end

  defp setup_backend(tmp_dir, opts \\ []) do
    {store, stub_name} = start_s3(Keyword.get(opts, :handler))
    config = config(tmp_dir, stub_name, Keyword.delete(opts, :handler))
    {:ok, state} = Tiered.open(config, 0)
    %{config: config, state: state, store: store}
  end

  defp commit(state, payloads) do
    entries = Enum.map(payloads, &elem(WAL.encode(&1), 0))
    span = {Tiered.offsets(state).next, length(payloads)}
    Tiered.commit(state, entries, IO.iodata_length(entries), span)
  end

  defp await_uploaded(state, through) do
    if state.uploaded >= through do
      state
    else
      receive do
        {:backend, message} ->
          {_completions, state} = Tiered.handle_message(message, state)
          await_uploaded(state, through)
      after
        2_000 -> flunk("upload through #{through} did not arrive, at #{state.uploaded}")
      end
    end
  end

  defp keys(store), do: store |> FakeS3.objects() |> Map.keys() |> Enum.sort()

  defp local_files(tmp_dir), do: tmp_dir |> Path.join("p0") |> File.ls!() |> Enum.sort()

  test "ack: :local commits to the active segment without an upload", %{tmp_dir: tmp_dir} do
    %{config: config, state: state, store: store} = setup_backend(tmp_dir)

    {:ok, state} = commit(state, ["a", "b"])
    {:ok, state} = commit(state, ["c"])

    assert Tiered.offsets(state) == %{first: 0, next: 3}
    assert Tiered.durable_offset(state) == 3
    assert keys(store) == []
    assert local_files(tmp_dir) == ["000000000000.wal"]
    assert Enum.to_list(Tiered.stream(config, 0)) == ~w(a b c)
    assert :ok = Tiered.close(state)
  end

  test "a full segment seals, uploads under the S3 key layout, and leaves disk",
       %{tmp_dir: tmp_dir} do
    %{config: config, state: state, store: store} = setup_backend(tmp_dir, segment_bytes: 20)

    {:ok, state} = commit(state, ["one", "two"])
    {:ok, state} = commit(state, ["three"])
    state = await_uploaded(state, 2)

    assert "tiered/p0/000000000000.wal" in keys(store)
    assert local_files(tmp_dir) == ["000000000002.wal", "uploaded"]
    assert Tiered.uploaded(config, 0) == 2

    assert Enum.to_list(Tiered.stream(config, 0, with_offsets: true)) ==
             [{0, "one"}, {1, "two"}, {2, "three"}]

    assert Enum.to_list(S3.stream(config.s3, 0)) == ~w(one two)
    assert :ok = Tiered.close(state)
  end

  test "an old active segment seals on its rotate message", %{tmp_dir: tmp_dir} do
    %{state: state, store: store} = setup_backend(tmp_dir, segment_ms: 10)

    {:ok, state} = commit(state, ["late"])
    state = await_uploaded(state, 1)

    assert keys(store) == ["tiered/p0/000000000000.wal"]
    assert state.active == nil
    assert :ok = Tiered.close(state)
  end

  test "ack: :remote settles a commit only after its segment uploads", %{tmp_dir: tmp_dir} do
    %{config: config, state: state, store: store} =
      setup_backend(tmp_dir, ack: :remote)

    assert config.fsync == false

    {:ok, state} = commit(state, ["durable"])

    assert keys(store) == ["tiered/p0/000000000000.wal"]
    assert Tiered.durable_offset(state) == 1
    assert :ok = Tiered.close(state)
  end

  test "ack: :remote keeps a commit pending while the PUT fails", %{tmp_dir: tmp_dir} do
    {:ok, failures} = Agent.start_link(fn -> 2 end)

    handler = fn conn, store ->
      fail? =
        conn.method == "PUT" and Agent.get_and_update(failures, &{&1 > 0, max(&1 - 1, 0)})

      if fail?,
        do: Plug.Conn.send_resp(conn, 500, "boom"),
        else: FakeS3.call(conn, store)
    end

    %{state: state, store: store} =
      setup_backend(tmp_dir, ack: :remote, handler: handler)

    entries = [elem(WAL.encode("retry me"), 0)]

    assert {:pending, state} =
             Tiered.commit_async(state, entries, IO.iodata_length(entries), {0, 1}, :tag)

    assert Tiered.durable_offset(state) == 0

    completions = await_completion(state, :tag)
    assert completions == [{:tag, :ok}]
    assert keys(store) == ["tiered/p0/000000000000.wal"]
  end

  defp await_completion(state, tag) do
    receive do
      {:backend, message} ->
        case Tiered.handle_message(message, state) do
          {[{^tag, _result}] = completions, _state} -> completions
          {[], state} -> await_completion(state, tag)
        end
    after
      2_000 -> flunk("commit #{inspect(tag)} never settled")
    end
  end

  test "open seals and uploads what the last run left on disk", %{tmp_dir: tmp_dir} do
    %{config: config, state: state, store: store} = setup_backend(tmp_dir)

    {:ok, state} = commit(state, ["before", "restart"])
    :ok = Tiered.close(state)

    path = Path.join([tmp_dir, "p0", "000000000000.wal"])
    File.write!(path, <<100::32, 0::32, "torn">>, [:append])

    {:ok, state} = Tiered.open(config, 0)
    assert Tiered.offsets(state) == %{first: 0, next: 2}
    state = await_uploaded(state, 2)

    {:ok, state} = commit(state, ["after"])

    assert keys(store) == ["tiered/p0/000000000000.wal"]
    assert local_files(tmp_dir) == ["000000000002.wal", "uploaded"]
    assert Enum.to_list(Tiered.stream(config, 0)) == ~w(before restart after)
    assert :ok = Tiered.close(state)
  end

  describe "open after a crash" do
    test "adopts S3 as the watermark when the crash fell after the PUT", %{tmp_dir: tmp_dir} do
      %{config: config, state: state, store: store} = setup_backend(tmp_dir, segment_bytes: 1)

      {:ok, state} = commit(state, ["uploaded"])
      state = await_uploaded(state, 1)
      :ok = Tiered.close(state)

      File.rm!(Path.join([tmp_dir, "p0", "uploaded"]))

      File.write!(
        Path.join([tmp_dir, "p0", "000000000000.wal"]),
        store |> FakeS3.objects() |> Map.fetch!("tiered/p0/000000000000.wal")
      )

      {:ok, state} = Tiered.open(config, 0)

      assert state.uploaded == 1
      assert local_files(tmp_dir) == []
      assert Enum.to_list(Tiered.stream(config, 0)) == ["uploaded"]
      assert :ok = Tiered.close(state)
    end

    test "deletes an uploaded segment the crash left on disk", %{tmp_dir: tmp_dir} do
      %{config: config, state: state} =
        setup_backend(tmp_dir, segment_bytes: 1, local_retention: 1_000_000)

      {:ok, state} = commit(state, ["uploaded"])
      state = await_uploaded(state, 1)
      :ok = Tiered.close(state)

      {:ok, state} = Tiered.open(%{config | local_retention: :uploaded}, 0)

      assert local_files(tmp_dir) == ["uploaded"]
      assert Tiered.offsets(state) == %{first: 0, next: 1}
      assert :ok = Tiered.close(state)
    end

    test "removes an empty segment left by a half-done rotation", %{tmp_dir: tmp_dir} do
      %{config: config, state: state} = setup_backend(tmp_dir)

      {:ok, state} = commit(state, ["a", "b"])
      :ok = Tiered.close(state)
      File.write!(Path.join([tmp_dir, "p0", "000000000002.wal"]), "")

      {:ok, state} = Tiered.open(config, 0)
      state = await_uploaded(state, 2)

      assert Tiered.offsets(state) == %{first: 0, next: 2}
      assert local_files(tmp_dir) == ["uploaded"]
      assert :ok = Tiered.close(state)
    end
  end

  test "open resumes offsets from S3 when the local disk is empty", %{tmp_dir: tmp_dir} do
    %{config: config, state: state} = setup_backend(tmp_dir, segment_bytes: 1)

    {:ok, state} = commit(state, ["x", "y"])
    state = await_uploaded(state, 2)
    :ok = Tiered.close(state)

    File.rm_rf!(Path.join(tmp_dir, "p0"))

    {:ok, state} = Tiered.open(config, 0)
    assert Tiered.offsets(state) == %{first: 0, next: 2}
    assert Enum.to_list(Tiered.stream(config, 0)) == ~w(x y)
    assert :ok = Tiered.close(state)
  end

  test "stream reads a local segment from S3 when it is deleted mid-read",
       %{tmp_dir: tmp_dir} do
    %{config: config, state: state} =
      setup_backend(tmp_dir, segment_bytes: 1, local_retention: 1_000_000)

    {:ok, state} = commit(state, ["kept"])
    {:ok, state} = commit(state, ["deleted"])
    state = await_uploaded(state, 2)

    assert local_files(tmp_dir) == ["000000000000.wal", "000000000001.wal", "uploaded"]

    second = Path.join([tmp_dir, "p0", "000000000001.wal"])

    entries =
      config
      |> Tiered.stream(0, with_offsets: true)
      |> Stream.each(fn
        {0, _payload} -> File.rm!(second)
        _entry -> :ok
      end)
      |> Enum.to_list()

    assert entries == [{0, "kept"}, {1, "deleted"}]
    refute File.exists?(second)
    assert :ok = Tiered.close(state)
  end

  test "stream stops at the limit and starts at from", %{tmp_dir: tmp_dir} do
    %{config: config, state: state} = setup_backend(tmp_dir, segment_bytes: 20)

    {:ok, state} = commit(state, ["a", "b"])
    {:ok, state} = commit(state, ["c", "d"])
    {:ok, state} = commit(state, ["e"])
    state = await_uploaded(state, 4)

    assert Enum.to_list(Tiered.stream(config, 0, from: 1, limit: fn -> 4 end)) == ~w(b c d)
    assert Enum.to_list(Tiered.stream(config, 0, from: 4)) == ~w(e)
    assert :ok = Tiered.close(state)
  end

  test "trim never drops a segment that is not uploaded", %{tmp_dir: tmp_dir} do
    %{config: config, state: state, store: store} =
      setup_backend(tmp_dir, segment_bytes: 1, local_retention: 1_000_000)

    {:ok, state} = commit(state, ["a"])
    {:ok, state} = commit(state, ["b"])
    state = await_uploaded(state, 2)
    :ok = Tiered.close(state)

    {:ok, state} = Tiered.open(%{config | segment_bytes: 1_000_000}, 0)
    {:ok, state} = commit(state, ["c"])

    {:ok, state} = Tiered.trim(state, 3)

    assert Tiered.offsets(state) == %{first: 2, next: 3}
    assert "tiered/p0/000000000002.wal" not in keys(store)
    assert Enum.to_list(Tiered.stream(config, 0)) == ~w(c)
    assert :ok = Tiered.close(state)
  end

  test "truncate empties both tiers and keeps offsets monotonic", %{tmp_dir: tmp_dir} do
    %{config: config, state: state, store: store} = setup_backend(tmp_dir, segment_bytes: 1)

    {:ok, state} = commit(state, ["gone"])
    {:ok, state} = commit(state, ["also gone"])
    state = await_uploaded(state, 2)

    {:ok, state} = Tiered.truncate(state, 2)

    assert Tiered.offsets(state) == %{first: 2, next: 2}
    assert keys(store) == ["tiered/p0/base"]
    assert Enum.to_list(Tiered.stream(config, 0)) == []

    {:ok, state} = commit(state, ["fresh"])
    assert Enum.to_list(Tiered.stream(config, 0, with_offsets: true)) == [{2, "fresh"}]
    assert :ok = Tiered.close(state)
  end

  test "a commit is refused while the upload backlog is over max_local_bytes",
       %{tmp_dir: tmp_dir} do
    handler = fn conn, store ->
      if conn.method == "PUT",
        do: Plug.Conn.send_resp(conn, 503, "down"),
        else: FakeS3.call(conn, store)
    end

    %{state: state} =
      setup_backend(tmp_dir, segment_bytes: 1, max_local_bytes: 10, handler: handler)

    {:ok, state} = commit(state, ["first segment"])

    assert {:error, :upload_backlog, state} = commit(state, ["refused"])
    assert Tiered.offsets(state).next == 1
    assert :ok = Tiered.close(state)
  end

  test "init_config rejects an unknown ack mode", %{tmp_dir: tmp_dir} do
    assert_raise ArgumentError, ~r/:ack must be/, fn ->
      Tiered.init_config(dir: tmp_dir, bucket: "b", ack: :quorum)
    end
  end
end
