defmodule DurableBuffer.Backend.Local.IndexTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Backend.Local.Index

  @moduletag :tmp_dir

  defp build(tmp_dir, records, wal_size \\ 1_000_000) do
    handle = Index.open(tmp_dir, 0, wal_size)

    for {first_offset, byte_pos} <- records,
        do: :ok = Index.append(handle, first_offset, byte_pos)

    :ok = Index.close(handle)
    handle
  end

  test "returns nil when there is no index", %{tmp_dir: tmp_dir} do
    assert Index.seek(tmp_dir, 0, 5) == nil
  end

  test "returns nil for an empty index", %{tmp_dir: tmp_dir} do
    build(tmp_dir, [])
    assert Index.seek(tmp_dir, 0, 5) == nil
  end

  test "finds the floor record", %{tmp_dir: tmp_dir} do
    build(tmp_dir, [{0, 0}, {3, 90}, {6, 180}, {9, 270}])

    assert Index.seek(tmp_dir, 0, 0) == {0, 0}
    assert Index.seek(tmp_dir, 0, 2) == {0, 0}
    assert Index.seek(tmp_dir, 0, 3) == {90, 3}
    assert Index.seek(tmp_dir, 0, 5) == {90, 3}
    assert Index.seek(tmp_dir, 0, 9) == {270, 9}
    assert Index.seek(tmp_dir, 0, 99) == {270, 9}
  end

  test "returns nil when every record is past the wanted offset", %{tmp_dir: tmp_dir} do
    build(tmp_dir, [{10, 0}, {20, 90}])
    assert Index.seek(tmp_dir, 0, 4) == nil
  end

  test "drops records the WAL does not back", %{tmp_dir: tmp_dir} do
    build(tmp_dir, [{0, 0}, {3, 90}, {6, 180}, {9, 270}])

    handle = Index.open(tmp_dir, 0, 200)
    :ok = Index.close(handle)

    assert Index.seek(tmp_dir, 0, 99) == {180, 6}
    assert File.stat!(Path.join(tmp_dir, "p0.idx")).size == 3 * 20
  end

  test "drops a torn trailing record", %{tmp_dir: tmp_dir} do
    build(tmp_dir, [{0, 0}, {3, 90}])
    path = Path.join(tmp_dir, "p0.idx")
    File.write!(path, [File.read!(path), <<1, 2, 3>>], [:binary])

    handle = Index.open(tmp_dir, 0, 1_000_000)
    :ok = Index.close(handle)

    assert Index.seek(tmp_dir, 0, 99) == {90, 3}
  end

  test "reset empties the index", %{tmp_dir: tmp_dir} do
    handle = Index.open(tmp_dir, 0, 0)
    :ok = Index.append(handle, 0, 0)
    handle = Index.reset(handle)
    :ok = Index.close(handle)

    assert Index.seek(tmp_dir, 0, 0) == nil
  end
end
