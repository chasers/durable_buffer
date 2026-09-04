defmodule DurableBuffer.Partition.CommitterTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.ManualAsyncBackend
  alias DurableBuffer.Partition.Committer
  alias DurableBuffer.WAL

  defp start_committer do
    config = ManualAsyncBackend.init_config(owner: self())
    {:ok, committer} = Committer.start_link(ManualAsyncBackend, config, 0)
    committer
  end

  defp submit(committer) do
    {entry, bytes} = WAL.encode("payload")
    :ok = Committer.commit(committer, [{:entries, nil, [entry], 1, :offset}], bytes)
    assert_receive {:submitted, tag, ^committer}
    tag
  end

  test "raises the dwell hint when completions are slow and lowers it when fast" do
    committer = start_committer()

    for _round <- 1..6 do
      tag = submit(committer)
      Process.sleep(15)
      send(committer, {:backend, {:complete, tag}})
      assert_receive :commit_done
    end

    assert_receive {:dwell_hint, 2}, 1000

    for _round <- 1..20 do
      tag = submit(committer)
      send(committer, {:backend, {:complete, tag}})
      assert_receive :commit_done
    end

    assert_receive {:dwell_hint, 0}, 1000
  end

  test "sends the hint only when the recommendation changes" do
    committer = start_committer()

    for _round <- 1..3 do
      tag = submit(committer)
      send(committer, {:backend, {:complete, tag}})
      assert_receive :commit_done
    end

    refute_received {:dwell_hint, _hint}
  end
end
