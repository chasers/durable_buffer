defmodule DurableBuffer.WALTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.WAL

  describe "encode/1 and decode_all/1" do
    test "round-trips a sequence of payloads" do
      payloads = ["hello", "", :binary.copy(<<0>>, 1000), "world"]

      binary =
        payloads
        |> Enum.map(fn payload ->
          {entry, size} = WAL.encode(payload)
          assert size == IO.iodata_length(entry)
          entry
        end)
        |> IO.iodata_to_binary()

      assert {^payloads, valid, ""} = WAL.decode_all(binary)
      assert valid == byte_size(binary)
    end

    test "accepts iodata payloads" do
      {entry, _size} = WAL.encode(["ab", ?c, ["d"]])
      assert {["abcd"], _valid, ""} = entry |> IO.iodata_to_binary() |> WAL.decode_all()
    end

    test "stops at a short header" do
      {entry, _size} = WAL.encode("full")
      binary = IO.iodata_to_binary(entry) <> <<1, 2, 3>>

      assert {["full"], valid, <<1, 2, 3>>} = WAL.decode_all(binary)
      assert valid == byte_size(binary) - 3
    end

    test "stops at a torn payload" do
      {entry, _size} = WAL.encode("complete")
      {torn, _size} = WAL.encode("truncated-away")
      torn_binary = binary_part(IO.iodata_to_binary(torn), 0, 12)

      assert {["complete"], _valid, ^torn_binary} =
               WAL.decode_all(IO.iodata_to_binary(entry) <> torn_binary)
    end

    test "stops at a CRC mismatch" do
      {entry, _size} = WAL.encode("payload")
      <<len::32, _crc::32, rest::binary>> = IO.iodata_to_binary(entry)
      corrupted = <<len::32, 0::32, rest::binary>>

      assert {[], 0, ^corrupted} = WAL.decode_all(corrupted)
    end
  end

  describe "recover!/1" do
    @describetag :tmp_dir

    test "returns zeroes for a missing file", %{tmp_dir: tmp_dir} do
      assert WAL.recover!(Path.join(tmp_dir, "missing.wal")) == {0, 0}
    end

    test "truncates a torn tail in place", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "torn.wal")
      {entry, size} = WAL.encode("keep-me")
      File.write!(path, [entry, <<9::32, 0::32, "to">>])

      assert WAL.recover!(path) == {size, 1}
      assert {["keep-me"], ^size, ""} = path |> File.read!() |> WAL.decode_all()
    end

    test "leaves a clean file untouched", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "clean.wal")
      {entry, size} = WAL.encode("data")
      File.write!(path, entry)

      assert WAL.recover!(path) == {size, 1}
      assert File.stat!(path).size == size
    end
  end
end
