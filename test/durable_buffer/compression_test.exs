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

  test "validates codecs" do
    assert Compression.validate!(:zstd) == :zstd
    assert Compression.validate!(:gzip) == :gzip
    assert_raise ArgumentError, fn -> Compression.validate!(:brotli) end
  end
end
