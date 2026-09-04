defmodule DurableBuffer.Backend.LocalTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Backend.Local
  alias DurableBuffer.WAL

  @moduletag :tmp_dir

  defp open(tmp_dir, partition_index \\ 0) do
    config = Local.init_config(dir: tmp_dir)
    {:ok, state} = Local.open(config, partition_index)
    {config, state}
  end

  defp encode_batch(payloads) do
    entries = Enum.map(payloads, &elem(WAL.encode(&1), 0))
    {entries, IO.iodata_length(entries)}
  end

  test "commit then stream round-trips payloads", %{tmp_dir: tmp_dir} do
    {config, state} = open(tmp_dir)
    {batch, bytes} = encode_batch(["one", "two", "three"])

    assert {:ok, state} = Local.commit(state, batch, bytes, span(Local, state, batch))
    assert Enum.to_list(Local.stream(config, 0)) == ["one", "two", "three"]
    assert :ok = Local.close(state)
  end

  test "streams an empty list for a partition never written", %{tmp_dir: tmp_dir} do
    {config, _state} = open(tmp_dir)
    assert Enum.to_list(Local.stream(config, 7)) == []
  end

  test "partitions write to distinct files", %{tmp_dir: tmp_dir} do
    {config, state0} = open(tmp_dir, 0)
    {_config, state1} = open(tmp_dir, 1)

    {batch0, bytes0} = encode_batch(["p0"])
    {batch1, bytes1} = encode_batch(["p1"])
    {:ok, _} = Local.commit(state0, batch0, bytes0, span(Local, state0, batch0))
    {:ok, _} = Local.commit(state1, batch1, bytes1, span(Local, state1, batch1))

    assert Enum.to_list(Local.stream(config, 0)) == ["p0"]
    assert Enum.to_list(Local.stream(config, 1)) == ["p1"]
  end

  test "open recovers a torn tail and appends after it", %{tmp_dir: tmp_dir} do
    {config, state} = open(tmp_dir)
    {batch, bytes} = encode_batch(["before-crash"])
    {:ok, state} = Local.commit(state, batch, bytes, span(Local, state, batch))
    :ok = Local.close(state)

    path = Path.join(tmp_dir, "p0.wal")
    File.write!(path, <<100::32, 0::32, "torn">>, [:append])

    {_config, state} = open(tmp_dir)
    {batch, bytes} = encode_batch(["after-crash"])
    {:ok, state} = Local.commit(state, batch, bytes, span(Local, state, batch))

    assert Enum.to_list(Local.stream(config, 0)) == ["before-crash", "after-crash"]
    assert :ok = Local.close(state)
  end

  test "truncate discards committed data", %{tmp_dir: tmp_dir} do
    {config, state} = open(tmp_dir)
    {batch, bytes} = encode_batch(["gone"])
    {:ok, state} = Local.commit(state, batch, bytes, span(Local, state, batch))

    assert {:ok, state} = Local.truncate(state, 0)
    assert Enum.to_list(Local.stream(config, 0)) == []

    {batch, bytes} = encode_batch(["fresh"])
    assert {:ok, state} = Local.commit(state, batch, bytes, span(Local, state, batch))
    assert Enum.to_list(Local.stream(config, 0)) == ["fresh"]
    assert :ok = Local.close(state)
  end

  test "fsync defaults to true and can be disabled", %{tmp_dir: tmp_dir} do
    assert Local.init_config(dir: tmp_dir).fsync == true

    config = Local.init_config(dir: tmp_dir, fsync: false)
    {:ok, state} = Local.open(config, 0)
    {batch, bytes} = encode_batch(["unsynced"])

    assert {:ok, state} = Local.commit(state, batch, bytes, span(Local, state, batch))
    assert Enum.to_list(Local.stream(config, 0)) == ["unsynced"]
    assert :ok = Local.close(state)
  end

  test "offset tracks the WAL size across commit, truncate, and reopen", %{tmp_dir: tmp_dir} do
    {_config, state} = open(tmp_dir)
    assert Local.offset(state) == 0

    {batch, bytes} = encode_batch(["one", "two"])
    {:ok, state} = Local.commit(state, batch, bytes, span(Local, state, batch))
    assert Local.offset(state) == bytes

    :ok = Local.close(state)
    {_config, state} = open(tmp_dir)
    assert Local.offset(state) == bytes

    {:ok, state} = Local.truncate(state, 0)
    assert Local.offset(state) == 0
    assert :ok = Local.close(state)
  end

  test "streams entries larger than the read chunk", %{tmp_dir: tmp_dir} do
    {config, state} = open(tmp_dir)
    big = :binary.copy("x", 200_000)
    {batch, bytes} = encode_batch([big, "small"])
    {:ok, state} = Local.commit(state, batch, bytes, span(Local, state, batch))

    assert Enum.to_list(Local.stream(config, 0)) == [big, "small"]
    assert :ok = Local.close(state)
  end

  defp span(module, state, batch) do
    {payloads, _valid, _rest} = batch |> IO.iodata_to_binary() |> DurableBuffer.WAL.decode_all()
    {module.offsets(state).next, length(payloads)}
  end
end
