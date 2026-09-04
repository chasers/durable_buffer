defmodule DurableBuffer.Backend.Local.IndexTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Backend.Local.Index

  @moduletag :tmp_dir

  @record_size 28

  defp build(tmp_dir, records, wal_size \\ 1_000_000, base_offset \\ 0) do
    handle = Index.open(tmp_dir, 0, 0)

    for {first_offset, byte_pos, commit_ms} <- records,
        do: :ok = Index.append(handle, first_offset, byte_pos, commit_ms)

    :ok = Index.close(handle)
    :ok = Index.close(Index.open(tmp_dir, 0, wal_size, base_offset))
  end

  defp dated(records, start_ms \\ 1_000) do
    for {{first_offset, byte_pos}, step} <- Enum.with_index(records),
        do: {first_offset, byte_pos, start_ms + step * 100}
  end

  test "returns nil when there is no index", %{tmp_dir: tmp_dir} do
    assert Index.seek(tmp_dir, 0, 5) == nil
  end

  test "returns nil for an empty index over an empty WAL", %{tmp_dir: tmp_dir} do
    build(tmp_dir, [], 0)
    assert Index.seek(tmp_dir, 0, 5) == nil
  end

  test "finds the floor record", %{tmp_dir: tmp_dir} do
    build(tmp_dir, dated([{0, 0}, {3, 90}, {6, 180}, {9, 270}]))

    assert Index.seek(tmp_dir, 0, 0) == {0, 0}
    assert Index.seek(tmp_dir, 0, 2) == {0, 0}
    assert Index.seek(tmp_dir, 0, 3) == {90, 3}
    assert Index.seek(tmp_dir, 0, 5) == {90, 3}
    assert Index.seek(tmp_dir, 0, 9) == {270, 9}
    assert Index.seek(tmp_dir, 0, 99) == {270, 9}
  end

  test "returns nil when every record is past the wanted offset", %{tmp_dir: tmp_dir} do
    build(tmp_dir, [{10, 0, 1_000}, {20, 90, 1_100}], 1_000_000, 10)
    assert Index.seek(tmp_dir, 0, 4) == nil
  end

  test "drops records the WAL does not back", %{tmp_dir: tmp_dir} do
    build(tmp_dir, dated([{0, 0}, {3, 90}, {6, 180}, {9, 270}]), 200)

    assert Index.seek(tmp_dir, 0, 99) == {180, 6}
    assert File.stat!(Path.join(tmp_dir, "p0.idx")).size == 3 * @record_size
  end

  test "drops a torn trailing record", %{tmp_dir: tmp_dir} do
    build(tmp_dir, dated([{0, 0}, {3, 90}]))
    path = Path.join(tmp_dir, "p0.idx")
    File.write!(path, [File.read!(path), <<1, 2, 3>>], [:binary])

    handle = Index.open(tmp_dir, 0, 1_000_000)
    :ok = Index.close(handle)

    assert Index.seek(tmp_dir, 0, 99) == {90, 3}
  end

  test "reset empties the index", %{tmp_dir: tmp_dir} do
    handle = Index.open(tmp_dir, 0, 0)
    :ok = Index.append(handle, 0, 0, 1_000)
    handle = Index.reset(handle)
    :ok = Index.close(handle)

    assert Index.seek(tmp_dir, 0, 0) == nil
  end

  test "discards an index written before the record carried a timestamp", %{tmp_dir: tmp_dir} do
    legacy =
      for {first_offset, byte_pos} <- [{0, 0}, {3, 90}] do
        body = <<first_offset::64-big, byte_pos::64-big>>
        [body, <<:erlang.crc32(body)::32-big>>]
      end

    path = Path.join(tmp_dir, "p0.idx")
    File.write!(path, legacy)

    handle = Index.open(tmp_dir, 0, 0)
    :ok = Index.close(handle)

    assert Index.seek(tmp_dir, 0, 99) == nil
  end

  describe "seek_time/3" do
    test "finds the oldest batch at or after the cutoff", %{tmp_dir: tmp_dir} do
      build(tmp_dir, [{0, 0, 1_000}, {3, 90, 2_000}, {6, 180, 3_000}])

      assert Index.seek_time(tmp_dir, 0, 500) == {:ok, 0}
      assert Index.seek_time(tmp_dir, 0, 1_000) == {:ok, 0}
      assert Index.seek_time(tmp_dir, 0, 1_001) == {:ok, 3}
      assert Index.seek_time(tmp_dir, 0, 2_000) == {:ok, 3}
      assert Index.seek_time(tmp_dir, 0, 2_500) == {:ok, 6}
    end

    test "keeps the last batch when every record is older", %{tmp_dir: tmp_dir} do
      build(tmp_dir, [{0, 0, 1_000}, {3, 90, 2_000}, {6, 180, 3_000}])

      assert Index.seek_time(tmp_dir, 0, 9_000) == {:ok, 6}
    end

    test "is unknown without an index", %{tmp_dir: tmp_dir} do
      assert Index.seek_time(tmp_dir, 0, 1_000) == :unknown
    end
  end

  describe "seek_byte/3" do
    test "finds the oldest batch at or after the byte position", %{tmp_dir: tmp_dir} do
      build(tmp_dir, dated([{0, 0}, {3, 90}, {6, 180}]))

      assert Index.seek_byte(tmp_dir, 0, 0) == {:ok, 0}
      assert Index.seek_byte(tmp_dir, 0, 1) == {:ok, 3}
      assert Index.seek_byte(tmp_dir, 0, 90) == {:ok, 3}
      assert Index.seek_byte(tmp_dir, 0, 91) == {:ok, 6}
    end

    test "keeps the last batch when every record is below it", %{tmp_dir: tmp_dir} do
      build(tmp_dir, dated([{0, 0}, {3, 90}, {6, 180}]))

      assert Index.seek_byte(tmp_dir, 0, 100_000) == {:ok, 6}
    end
  end

  test "oldest reports the head's commit time", %{tmp_dir: tmp_dir} do
    assert Index.oldest(tmp_dir, 0) == :unknown

    build(tmp_dir, [{0, 0, 1_234}, {3, 90, 5_678}])
    assert Index.oldest(tmp_dir, 0) == {:ok, 1_234}
  end

  test "dates an undated head as now when the partition opens", %{tmp_dir: tmp_dir} do
    before = System.system_time(:millisecond)
    handle = Index.open(tmp_dir, 0, 500)
    :ok = Index.close(handle)

    assert {:ok, commit_ms} = Index.oldest(tmp_dir, 0)
    assert commit_ms >= before
    assert Index.seek(tmp_dir, 0, 0) == {0, 0}
  end

  test "does not date a head the index already covers", %{tmp_dir: tmp_dir} do
    build(tmp_dir, [{0, 0, 1_234}], 500)

    assert Index.oldest(tmp_dir, 0) == {:ok, 1_234}
    assert File.stat!(Path.join(tmp_dir, "p0.idx")).size == @record_size
  end

  test "a trim carries the dropped batch's time to the new head", %{tmp_dir: tmp_dir} do
    handle = Index.open(tmp_dir, 0, 0)
    :ok = Index.append(handle, 0, 0, 1_000)
    :ok = Index.append(handle, 5, 150, 2_000)
    :ok = Index.append(handle, 10, 300, 3_000)

    handle = Index.trim(handle, 7, 210)
    :ok = Index.close(handle)

    assert Index.oldest(tmp_dir, 0) == {:ok, 2_000}
    assert Index.seek(tmp_dir, 0, 8) == {210, 7}
    assert Index.seek(tmp_dir, 0, 10) == {300, 10}
  end

  test "a trim landing on a batch boundary keeps that record", %{tmp_dir: tmp_dir} do
    handle = Index.open(tmp_dir, 0, 0)
    :ok = Index.append(handle, 0, 0, 1_000)
    :ok = Index.append(handle, 5, 150, 2_000)

    handle = Index.trim(handle, 5, 150)
    :ok = Index.close(handle)

    assert Index.oldest(tmp_dir, 0) == {:ok, 2_000}
    assert File.stat!(Path.join(tmp_dir, "p0.idx")).size == @record_size
  end
end
