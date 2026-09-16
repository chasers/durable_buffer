defmodule DurableBuffer.CompressionTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Compression

  test "every codec round-trips iodata" do
    data = ["frame one ", :binary.copy("repeated ", 100)]

    for codec <- [:zstd, :gzip, :none] do
      compressed = Compression.compress(data, codec)
      assert is_binary(compressed)
      assert Compression.decompress(compressed, codec) == IO.iodata_to_binary(data)
    end
  end

  @tag :tmp_dir
  test "compress_file matches whole-binary decompression for every codec", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "segment.wal")
    data = for index <- 1..20_000, into: "", do: "line #{index} #{rem(index, 7)}\n"
    File.write!(path, data)

    for codec <- [:zstd, :gzip, :none] do
      assert {:ok, compressed} = Compression.compress_file(path, codec, 4096)
      assert Compression.decompress(compressed, codec) == data
    end

    assert {:error, :enoent} = Compression.compress_file(Path.join(tmp_dir, "missing"), :zstd)
  end

  test "keys carry the codec" do
    for codec <- [:zstd, :gzip, :none] do
      key = "prefix/p3/000000000042" <> Compression.extension(codec)
      assert Compression.parse_key(key) == {:ok, 42, codec}
    end

    assert Compression.parse_key("prefix/p3/base") == :error
    assert Compression.parse_key("prefix/p3/000000000042.wal.lz4") == :error
    assert Compression.parse_key("prefix/p3/abc.wal") == :error
  end

  test "defaults to zstd on this runtime and validates codecs" do
    assert Compression.default() == :zstd
    assert Compression.validate!(:gzip) == :gzip
    assert_raise ArgumentError, fn -> Compression.validate!(:brotli) end
  end
end
