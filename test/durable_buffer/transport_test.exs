defmodule DurableBuffer.TransportTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Backend.Local
  alias DurableBuffer.Backend.Replica
  alias DurableBuffer.FailingTransport
  alias DurableBuffer.RecordingTransport
  alias DurableBuffer.Replica.Sender
  alias DurableBuffer.Transport
  alias DurableBuffer.WAL

  @moduletag :tmp_dir
  @moduletag capture_log: true

  @resync_chunk_bytes 1024 * 1024

  defp dirs(tmp_dir) do
    {Path.join(tmp_dir, "primary"), Path.join(tmp_dir, "replica")}
  end

  defp entry(payload) do
    payload |> WAL.encode() |> elem(0) |> IO.iodata_to_binary()
  end

  defp encode_batch(payloads) do
    entries = Enum.map(payloads, &elem(WAL.encode(&1), 0))
    {entries, IO.iodata_length(entries)}
  end

  defp span(state, batch) do
    first = Replica.offsets(state).next
    {first, length(batch)}
  end

  defp await_watermark(target) do
    assert_receive {:backend, {:watermark, _node, watermark}}, 5000

    if watermark < target do
      await_watermark(target)
    else
      assert watermark >= target
    end
  end

  describe "the transport: option" do
    test "defaults to distribution", %{tmp_dir: tmp_dir} do
      {primary_dir, _replica_dir} = dirs(tmp_dir)
      assert Replica.init_config(dir: primary_dir).transport == Transport.Distribution
    end

    test "accepts a module implementing the behaviour", %{tmp_dir: tmp_dir} do
      {primary_dir, _replica_dir} = dirs(tmp_dir)

      config = Replica.init_config(dir: primary_dir, transport: RecordingTransport)
      assert config.transport == RecordingTransport
    end

    test "raises for a module that does not implement it", %{tmp_dir: tmp_dir} do
      {primary_dir, _replica_dir} = dirs(tmp_dir)

      assert_raise ArgumentError, ~r/does not implement DurableBuffer.Transport/, fn ->
        Replica.init_config(dir: primary_dir, transport: Enum)
      end
    end

    test "raises for a module that is not loaded", %{tmp_dir: tmp_dir} do
      {primary_dir, _replica_dir} = dirs(tmp_dir)

      assert_raise ArgumentError, ~r/does not implement DurableBuffer.Transport/, fn ->
        Replica.init_config(dir: primary_dir, transport: NoSuchTransport)
      end
    end

    test "raises for a value that is not a module", %{tmp_dir: tmp_dir} do
      {primary_dir, _replica_dir} = dirs(tmp_dir)

      assert_raise ArgumentError, ~r/expects a module/, fn ->
        Replica.init_config(dir: primary_dir, transport: "gen_rpc")
      end
    end
  end

  describe "a transport that fails" do
    setup %{tmp_dir: tmp_dir} do
      {_primary_dir, replica_dir} = dirs(tmp_dir)
      on_exit(fn -> FailingTransport.set(replica_dir, :ok) end)
      :ok
    end

    for mode <- [:error, :raise] do
      @mode mode

      test "a send that returns #{inspect(mode)} never stops the partition", ctx do
        {primary_dir, replica_dir} = dirs(ctx.tmp_dir)
        FailingTransport.set(replica_dir, @mode)

        config =
          Replica.init_config(
            dir: primary_dir,
            replica_dir: replica_dir,
            replicas: [node()],
            ack: 1,
            transport: FailingTransport
          )

        {:ok, state} = Replica.open(config, 0)
        {batch, bytes} = encode_batch(["local-only"])

        assert {:ok, state} = Replica.commit(state, batch, bytes, span(state, batch))
        assert Enum.to_list(Replica.stream(config, 0)) == ["local-only"]

        assert :ok = Replica.close(state)
      end
    end

    test "a channel that cannot open never stops the partition", ctx do
      {primary_dir, replica_dir} = dirs(ctx.tmp_dir)
      FailingTransport.set(replica_dir, :no_channel)

      config =
        Replica.init_config(
          dir: primary_dir,
          replica_dir: replica_dir,
          replicas: [node()],
          ack: 1,
          transport: FailingTransport
        )

      {:ok, state} = Replica.open(config, 0)
      {batch, bytes} = encode_batch(["local-only"])

      assert {:ok, state} = Replica.commit(state, batch, bytes, span(state, batch))
      assert Enum.to_list(Replica.stream(config, 0)) == ["local-only"]

      assert :ok = Replica.close(state)
    end

    test "nil is a legal channel, not a stalled sender", ctx do
      {primary_dir, replica_dir} = dirs(ctx.tmp_dir)
      FailingTransport.set(replica_dir, :nil_channel)

      config =
        Replica.init_config(
          dir: primary_dir,
          replica_dir: replica_dir,
          replicas: [node()],
          ack: 1,
          transport: FailingTransport
        )

      {:ok, state} = Replica.open(config, 0)
      {batch, bytes} = encode_batch(["local-only"])

      assert {:ok, state} = Replica.commit(state, batch, bytes, span(state, batch))
      assert Enum.to_list(Replica.stream(config, 0)) == ["local-only"]

      assert :ok = Replica.close(state)
    end
  end

  describe "the resync window" do
    test "bounds in-flight bytes when the transport does not block", ctx do
      {primary_dir, replica_dir} = dirs(ctx.tmp_dir)
      window = 64 * 1024
      wal_bytes = 8 * 1024 * 1024
      entry = entry(:binary.copy("x", 8192))
      entries = div(wal_bytes, byte_size(entry))

      {:ok, local} = Local.open(Local.init_config(dir: primary_dir), 0)

      local =
        Enum.reduce(1..entries, local, fn _index, local ->
          {:ok, local} = Local.commit(local, entry, byte_size(entry), {0, 1})
          local
        end)

      tail = Local.offset(local)
      assert tail > 4 * @resync_chunk_bytes

      FailingTransport.set(replica_dir, :blackhole)
      on_exit(fn -> FailingTransport.set(replica_dir, :ok) end)

      {:ok, sender} =
        Sender.start_link(
          owner: self(),
          node: node(),
          dir: replica_dir,
          partition_index: 0,
          primary_dir: primary_dir,
          primary_tail: tail,
          epoch: 0,
          rpc_timeout: 60_000,
          max_bytes: window,
          transport: FailingTransport
        )

      on_exit(fn -> if Process.alive?(sender), do: Sender.stop(sender) end)

      Process.sleep(300)

      sent = FailingTransport.sent_bytes(replica_dir)

      assert sent > 0,
             "the sender never started the resync"

      assert sent <= window + 2 * @resync_chunk_bytes,
             "the sender streamed #{sent} bytes of a #{tail}-byte WAL with no ack; " <>
               "the resync window should have capped it near #{window + @resync_chunk_bytes}"

      Local.close(local)
    end
  end

  describe "the data path" do
    test "every live batch goes through the configured transport", %{tmp_dir: tmp_dir} do
      {primary_dir, replica_dir} = dirs(tmp_dir)
      {:ok, _} = RecordingTransport.subscribe(replica_dir)

      config =
        Replica.init_config(
          dir: primary_dir,
          replica_dir: replica_dir,
          replicas: [node()],
          transport: RecordingTransport
        )

      {:ok, state} = Replica.open(config, 0)

      {batch, bytes} = encode_batch(["one"])
      {:ok, state} = Replica.commit(state, batch, bytes, span(state, batch))
      assert_receive {:transport_batch, ^replica_dir, 0, 0, ^bytes}, 5000

      {next, next_bytes} = encode_batch(["two"])
      {:ok, state} = Replica.commit(state, next, next_bytes, span(state, next))
      assert_receive {:transport_batch, ^replica_dir, 0, ^bytes, ^next_bytes}, 5000

      assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) == ["one", "two"]
      assert :ok = Replica.close(state)
    end

    test "a resync goes through the configured transport", %{tmp_dir: tmp_dir} do
      {primary_dir, replica_dir} = dirs(tmp_dir)
      {:ok, _} = RecordingTransport.subscribe(replica_dir)

      {:ok, local} = Local.open(Local.init_config(dir: primary_dir), 0)

      local =
        Enum.reduce(["old-one", "old-two"], local, fn payload, local ->
          binary = entry(payload)
          {:ok, local} = Local.commit(local, binary, byte_size(binary), {0, 1})
          local
        end)

      tail = Local.offset(local)

      {:ok, sender} =
        Sender.start_link(
          owner: self(),
          node: node(),
          dir: replica_dir,
          partition_index: 0,
          primary_dir: primary_dir,
          primary_tail: tail,
          epoch: 0,
          rpc_timeout: 500,
          max_bytes: 64 * 1024 * 1024,
          transport: RecordingTransport
        )

      on_exit(fn -> if Process.alive?(sender), do: Sender.stop(sender) end)

      assert_receive {:transport_batch, ^replica_dir, 0, 0, ^tail}, 5000
      await_watermark({0, tail})

      assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) ==
               ["old-one", "old-two"]

      Local.close(local)
    end
  end
end
