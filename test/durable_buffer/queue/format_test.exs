defmodule DurableBuffer.Queue.FormatTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Queue.Batch
  alias DurableBuffer.Queue.Manifest
  alias DurableBuffer.Queue.ULID

  describe "ULID" do
    test "encodes the time in 26 sortable characters" do
      earlier = ULID.generate(1_700_000_000_000)
      later = ULID.generate(1_700_000_000_001)

      assert byte_size(earlier) == 26
      assert earlier =~ ~r/^[0-9A-HJKMNP-TV-Z]{26}$/
      assert earlier < later
      assert ULID.time_ms(earlier) == {:ok, 1_700_000_000_000}
      assert ULID.time_ms(String.downcase(later)) == {:ok, 1_700_000_000_001}
    end

    test "rejects strings that are not ULIDs" do
      assert ULID.time_ms("not-a-ulid") == :error
      assert ULID.time_ms(String.duplicate("U", 26)) == :error
      assert ULID.time_ms("8" <> String.duplicate("0", 25)) == :error
    end
  end

  describe "Manifest" do
    test "matches the RFC 0001 byte layout" do
      metadata = [%{start_index: 0, ingestion_time_ms: -5, payload: "pq"}]
      {manifest, [7]} = Manifest.append(%{Manifest.new() | next_sequence: 7}, [{"a/b", metadata}])
      manifest = Manifest.set_epoch(manifest, 3)

      entry =
        <<7::64-little, 3::16-little, "a/b", 1::32-little, 0::32-little, -5::64-signed-little,
          2::32-little, "pq">>

      expected =
        <<byte_size(entry)::32-little, entry::binary, 1::32-little, 8::64-little, 3::64-little,
          1::16-little>>

      assert IO.iodata_to_binary(Manifest.encode(manifest)) == expected
      assert {:ok, decoded} = Manifest.decode(expected)
      assert decoded == manifest
    end

    test "appends without touching earlier bytes and dequeues through a sequence" do
      {manifest, [0, 1]} =
        Manifest.append(Manifest.new(), [
          {"x/0", []},
          {"x/1", [%{start_index: 0, ingestion_time_ms: 1, payload: ""}]}
        ])

      body_before = manifest.body
      {manifest, [2]} = Manifest.append(manifest, [{"x/2", []}])

      assert binary_part(manifest.body, 0, byte_size(body_before)) == body_before
      assert Enum.map(Manifest.entries(manifest), & &1.sequence) == [0, 1, 2]
      assert manifest.entry_count == 3

      {manifest, removed} = Manifest.dequeue(manifest, 1)
      assert Enum.map(removed, & &1.location) == ["x/0", "x/1"]
      assert Manifest.entries(manifest) == [%{sequence: 2, location: "x/2", metadata: []}]
      assert manifest.entry_count == 1
      assert manifest.next_sequence == 3

      {manifest, [3]} = Manifest.append(manifest, [{"x/3", []}])
      assert Enum.map(Manifest.entries(manifest), & &1.sequence) == [2, 3]
    end

    test "starts uninitialized and skips that epoch on wrap" do
      manifest = Manifest.new()
      assert manifest.epoch == Manifest.uninitialized_epoch()
      assert Manifest.next_epoch(manifest) == 0
      assert Manifest.next_epoch(Manifest.set_epoch(manifest, 4)) == 5

      assert Manifest.next_epoch(Manifest.set_epoch(manifest, Manifest.uninitialized_epoch() - 1)) ==
               0
    end

    test "rejects a truncated or unknown manifest" do
      assert Manifest.decode(<<1, 2, 3>>) == {:error, :truncated_manifest}

      assert Manifest.decode(<<0::160, 2::16-little>>) ==
               {:error, {:unsupported_manifest_version, 2}}
    end
  end

  describe "Batch" do
    test "round-trips records with each codec and a fixed footer" do
      records = ["", "one", :binary.copy("repeat ", 500)]

      for codec <- [:none, :zstd] do
        encoded = Batch.encode(records, codec)
        code = if codec == :zstd, do: 1, else: 0

        assert binary_part(encoded, byte_size(encoded), -7) ==
                 <<code, 3::32-little, 1::16-little>>

        assert Batch.decode(encoded) == {:ok, records}
      end

      assert byte_size(Batch.encode(records, :zstd)) < byte_size(Batch.encode(records, :none))
    end

    test "matches the RFC 0001 record layout when uncompressed" do
      assert Batch.encode(["ab", "c"], :none) ==
               <<2::32-little, "ab", 1::32-little, "c", 0, 2::32-little, 1::16-little>>
    end

    test "rejects unknown codes and wrong counts" do
      assert Batch.decode(<<7, 0::32, 1::16-little>>) == {:error, {:unsupported_compression, 7}}

      assert Batch.decode(<<0, 2::32-little, 1::16-little>>) ==
               {:error, {:record_count_mismatch, 0, 2}}

      assert_raise ArgumentError, fn -> Batch.validate!(:gzip) end
    end
  end
end
