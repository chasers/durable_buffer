defmodule DurableBuffer.EpochTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Epoch

  @moduletag :tmp_dir

  test "loads 0 when no meta file exists", %{tmp_dir: tmp_dir} do
    assert Epoch.load(tmp_dir, 0) == 0
  end

  test "round-trips a stored epoch", %{tmp_dir: tmp_dir} do
    assert :ok = Epoch.store!(tmp_dir, 3, 42)
    assert Epoch.load(tmp_dir, 3) == 42
  end

  test "epochs are per partition", %{tmp_dir: tmp_dir} do
    :ok = Epoch.store!(tmp_dir, 0, 1)
    :ok = Epoch.store!(tmp_dir, 1, 2)

    assert Epoch.load(tmp_dir, 0) == 1
    assert Epoch.load(tmp_dir, 1) == 2
  end

  test "a corrupt meta file reads as 0", %{tmp_dir: tmp_dir} do
    :ok = Epoch.store!(tmp_dir, 0, 7)
    File.write!(Path.join(tmp_dir, "p0.meta"), "garbage-bytes")

    assert Epoch.load(tmp_dir, 0) == 0
  end

  test "a crc mismatch reads as 0", %{tmp_dir: tmp_dir} do
    File.write!(Path.join(tmp_dir, "p0.meta"), <<7::64-big, 0::32-big>>)

    assert Epoch.load(tmp_dir, 0) == 0
  end
end
