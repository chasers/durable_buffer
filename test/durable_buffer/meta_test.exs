defmodule DurableBuffer.MetaTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Meta

  @moduletag :tmp_dir

  test "loads zeroes when no meta file exists", %{tmp_dir: tmp_dir} do
    assert Meta.load(tmp_dir, 0) == %Meta{epoch: 0, base_offset: 0, base_byte_offset: 0}
    assert Meta.epoch(tmp_dir, 0) == 0
  end

  test "round-trips every field", %{tmp_dir: tmp_dir} do
    meta = %Meta{epoch: 42, base_offset: 900, base_byte_offset: 12_345}

    assert :ok = Meta.store!(tmp_dir, 3, meta)
    assert Meta.load(tmp_dir, 3) == meta
    assert Meta.epoch(tmp_dir, 3) == 42
  end

  test "metadata is per partition", %{tmp_dir: tmp_dir} do
    :ok = Meta.store!(tmp_dir, 0, %Meta{epoch: 1})
    :ok = Meta.store!(tmp_dir, 1, %Meta{epoch: 2, base_offset: 7})

    assert Meta.epoch(tmp_dir, 0) == 1
    assert Meta.load(tmp_dir, 1) == %Meta{epoch: 2, base_offset: 7, base_byte_offset: 0}
  end

  test "update! merges into what is already stored", %{tmp_dir: tmp_dir} do
    :ok = Meta.store!(tmp_dir, 0, %Meta{epoch: 4, base_offset: 10})

    assert %Meta{epoch: 5, base_offset: 10} =
             Meta.update!(tmp_dir, 0, &%{&1 | epoch: &1.epoch + 1})

    assert Meta.load(tmp_dir, 0) == %Meta{epoch: 5, base_offset: 10, base_byte_offset: 0}
  end

  test "reads the pre-0.4.0 epoch-only record", %{tmp_dir: tmp_dir} do
    body = <<9::64-big>>
    File.write!(Path.join(tmp_dir, "p0.meta"), [body, <<:erlang.crc32(body)::32-big>>])

    assert Meta.load(tmp_dir, 0) == %Meta{epoch: 9, base_offset: 0, base_byte_offset: 0}
  end

  test "a corrupt meta file reads as zeroes", %{tmp_dir: tmp_dir} do
    :ok = Meta.store!(tmp_dir, 0, %Meta{epoch: 7})
    File.write!(Path.join(tmp_dir, "p0.meta"), "garbage-bytes")

    assert Meta.load(tmp_dir, 0) == %Meta{}
  end

  test "a crc mismatch reads as zeroes", %{tmp_dir: tmp_dir} do
    File.write!(Path.join(tmp_dir, "p0.meta"), <<7::64-big, 0::64-big, 0::64-big, 0::32-big>>)

    assert Meta.load(tmp_dir, 0) == %Meta{}
  end
end
