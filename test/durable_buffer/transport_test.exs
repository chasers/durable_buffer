defmodule DurableBuffer.TransportTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Backend.Local
  alias DurableBuffer.Backend.Replica
  alias DurableBuffer.RecordingTransport
  alias DurableBuffer.Replica.Sender
  alias DurableBuffer.Transport
  alias DurableBuffer.WAL

  @moduletag :tmp_dir
  @moduletag capture_log: true

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

      {:ok, _sender} =
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

      assert_receive {:transport_batch, ^replica_dir, 0, 0, ^tail}, 5000

      assert Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0)) ==
               ["old-one", "old-two"]

      Local.close(local)
    end
  end
end
