defmodule DurableBuffer.Backend.LocalTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Backend.Local
  alias DurableBuffer.Meta
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

  describe "a trim interrupted by a crash" do
    setup %{tmp_dir: tmp_dir} do
      {config, state} = open(tmp_dir)

      state =
        Enum.reduce(0..9, state, fn index, state ->
          {batch, bytes} = encode_batch(["e#{index}"])
          {:ok, state} = Local.commit(state, batch, bytes, span(Local, state, batch))
          state
        end)

      :ok = Local.close(state)
      %{config: config, wal: Path.join(tmp_dir, "p0.wal")}
    end

    test "before the rename, keeps every entry", %{tmp_dir: tmp_dir, config: config, wal: wal} do
      untrimmed = File.read!(wal)

      :ok =
        Meta.store_pending!(tmp_dir, 0, %Meta{
          base_offset: 5,
          base_byte_offset: byte_size(untrimmed)
        })

      File.write!(wal <> ".trim", "partially copied")

      {:ok, state} = Local.open(config, 0)

      assert Local.offsets(state) == %{first: 0, next: 10}
      assert Enum.to_list(Local.stream(config, 0)) == Enum.map(0..9, &"e#{&1}")
      refute File.exists?(wal <> ".trim")
      refute File.exists?(Path.join(tmp_dir, "p0.trim"))
      assert :ok = Local.close(state)
    end

    test "after the rename, adopts the new base", %{tmp_dir: tmp_dir, config: config, wal: wal} do
      {:ok, before} = Local.open(config, 0)
      cut = Local.offset(before) - byte_size(Enum.map_join(5..9, &"e#{&1}")) - 5 * 8
      :ok = Local.close(before)

      untrimmed = File.read!(wal)
      :ok = Meta.store_pending!(tmp_dir, 0, %Meta{base_offset: 5, base_byte_offset: cut})
      File.write!(wal, binary_part(untrimmed, cut, byte_size(untrimmed) - cut))

      {:ok, state} = Local.open(config, 0)

      assert Local.offsets(state) == %{first: 5, next: 10}
      assert Enum.to_list(Local.stream(config, 0)) == Enum.map(5..9, &"e#{&1}")
      refute File.exists?(Path.join(tmp_dir, "p0.trim"))
      assert :ok = Local.close(state)
    end

    test "a completed trim leaves no pending record", %{tmp_dir: tmp_dir, config: config} do
      {:ok, state} = Local.open(config, 0)
      {:ok, state} = Local.trim(state, 5)

      assert Local.offsets(state) == %{first: 5, next: 10}
      refute File.exists?(Path.join(tmp_dir, "p0.trim"))
      refute File.exists?(Path.join(tmp_dir, "p0.wal.trim"))
      assert :ok = Local.close(state)
    end

    test "a corrupt pending record keeps every entry", %{tmp_dir: tmp_dir, config: config} do
      File.write!(Path.join(tmp_dir, "p0.trim"), "not-a-record")

      {:ok, state} = Local.open(config, 0)

      assert Local.offsets(state) == %{first: 0, next: 10}
      assert :ok = Local.close(state)
    end
  end

  test "trim_bytes keeps a mirrored partition's offsets sane", %{tmp_dir: tmp_dir} do
    config = Local.init_config(dir: tmp_dir, index: false)
    {:ok, state} = Local.open(config, 0)

    state =
      Enum.reduce(0..4, state, fn index, state ->
        {batch, bytes} = encode_batch(["m#{index}"])
        {:ok, state} = Local.commit(state, batch, bytes, {0, 0})
        state
      end)

    cut = byte_size("m0") + 8
    assert {:ok, state} = Local.trim_bytes(state, cut)

    assert Local.offsets(state) == %{first: 0, next: 4}
    assert :ok = Local.close(state)

    {:ok, reopened} = Local.open(config, 0)
    assert Local.offsets(reopened) == %{first: 0, next: 4}
    assert Enum.to_list(Local.stream(config, 0)) == Enum.map(1..4, &"m#{&1}")
    assert :ok = Local.close(reopened)
  end

  defp span(module, state, batch) do
    {payloads, _valid, _rest} = batch |> IO.iodata_to_binary() |> DurableBuffer.WAL.decode_all()
    {module.offsets(state).next, length(payloads)}
  end
end
