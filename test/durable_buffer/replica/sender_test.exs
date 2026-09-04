defmodule DurableBuffer.Replica.SenderTest do
  use ExUnit.Case, async: true

  alias DurableBuffer.Backend.Local
  alias DurableBuffer.Replica.Sender
  alias DurableBuffer.WAL

  @moduletag :tmp_dir
  @moduletag capture_log: true

  defp dirs(tmp_dir) do
    {Path.join(tmp_dir, "primary"), Path.join(tmp_dir, "replica")}
  end

  defp entry(payload) do
    payload |> WAL.encode() |> elem(0) |> IO.iodata_to_binary()
  end

  defp open_primary(primary_dir) do
    {:ok, local} = Local.open(Local.init_config(dir: primary_dir), 0)
    local
  end

  defp start_sender(tmp_dir, opts) do
    {primary_dir, replica_dir} = dirs(tmp_dir)

    {:ok, sender} =
      Sender.start_link(
        Keyword.merge(
          [
            owner: self(),
            node: node(),
            dir: replica_dir,
            partition_index: 0,
            primary_dir: primary_dir,
            primary_tail: 0,
            epoch: 0,
            rpc_timeout: 500,
            max_bytes: 64 * 1024 * 1024
          ],
          opts
        )
      )

    sender
  end

  defp commit_both(local, sender, binary) do
    offset = Local.offset(local)
    {:ok, local} = Local.commit(local, binary, byte_size(binary), {offset, 1})
    :ok = Sender.commit(sender, 0, offset, binary)
    local
  end

  defp await_watermark(target) do
    assert_receive {:backend, {:watermark, _node, watermark}}, 5000

    if watermark < target do
      await_watermark(target)
    else
      assert watermark == target
    end
  end

  defp replica_entries(tmp_dir) do
    {_primary_dir, replica_dir} = dirs(tmp_dir)
    Enum.to_list(Local.stream(Local.init_config(dir: replica_dir), 0))
  end

  test "pipelines batches and forwards watermark acks", %{tmp_dir: tmp_dir} do
    {primary_dir, _replica_dir} = dirs(tmp_dir)
    sender = start_sender(tmp_dir, [])
    local = open_primary(primary_dir)

    local = commit_both(local, sender, entry("one"))
    local = commit_both(local, sender, entry("two"))

    await_watermark({0, Local.offset(local)})
    assert replica_entries(tmp_dir) == ["one", "two"]
    Local.close(local)
  end

  test "re-sends unacked batches after the writer dies", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)
    sender = start_sender(tmp_dir, [])
    local = open_primary(primary_dir)

    local = commit_both(local, sender, entry("survives"))
    await_watermark({0, Local.offset(local)})

    writer = DurableBuffer.Replica.writer_pid(replica_dir, 0, true)
    :ok = GenServer.stop(writer)

    local = commit_both(local, sender, entry("after-crash"))

    await_watermark({0, Local.offset(local)})
    assert replica_entries(tmp_dir) == ["survives", "after-crash"]
    Local.close(local)
  end

  test "resyncs a fresh replica from the primary WAL on attach", %{tmp_dir: tmp_dir} do
    {primary_dir, _replica_dir} = dirs(tmp_dir)
    local = open_primary(primary_dir)

    {:ok, local} =
      Local.commit(
        local,
        entry("old-one"),
        byte_size(entry("old-one")),
        {Local.offsets(local).next, 1}
      )

    {:ok, local} =
      Local.commit(
        local,
        entry("old-two"),
        byte_size(entry("old-two")),
        {Local.offsets(local).next, 1}
      )

    tail = Local.offset(local)

    _sender = start_sender(tmp_dir, primary_tail: tail)

    await_watermark({0, tail})
    assert replica_entries(tmp_dir) == ["old-one", "old-two"]
    Local.close(local)
  end

  test "resyncs after the replica loses its WAL", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)
    sender = start_sender(tmp_dir, [])
    local = open_primary(primary_dir)

    local = commit_both(local, sender, entry("lost"))
    await_watermark({0, Local.offset(local)})

    writer = DurableBuffer.Replica.writer_pid(replica_dir, 0, true)
    :ok = GenServer.stop(writer)
    File.rm!(Path.join(replica_dir, "p0.wal"))

    local = commit_both(local, sender, entry("after-wipe"))

    await_watermark({0, Local.offset(local)})
    assert replica_entries(tmp_dir) == ["lost", "after-wipe"]

    assert File.read!(Path.join(primary_dir, "p0.wal")) ==
             File.read!(Path.join(replica_dir, "p0.wal"))

    Local.close(local)
  end

  test "drops its queue and reattaches when it overflows toward an unreachable node", %{
    tmp_dir: tmp_dir
  } do
    b1 = entry("queued")
    b2 = entry("overflows")
    sender = start_sender(tmp_dir, node: :unreachable@nohost, max_bytes: byte_size(b1))

    :ok = Sender.commit(sender, 0, 0, b1)
    :ok = Sender.commit(sender, 0, byte_size(b1), b2)

    refute_receive {:backend, {:watermark, _node, _watermark}}, 200
  end

  test "reset adopts the new epoch and clears the queue", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)
    sender = start_sender(tmp_dir, [])
    local = open_primary(primary_dir)

    local = commit_both(local, sender, entry("pre-truncate"))
    await_watermark({0, Local.offset(local)})

    {:ok, local} = Local.truncate(local, 0)
    :ok = Sender.reset(sender, 1)
    :ok = DurableBuffer.Replica.truncate(replica_dir, 0, 1, 0)

    fresh = entry("fresh")
    {:ok, local} = Local.commit(local, fresh, byte_size(fresh), {Local.offsets(local).next, 1})
    :ok = Sender.commit(sender, 1, 0, fresh)

    await_watermark({1, byte_size(fresh)})
    assert replica_entries(tmp_dir) == ["fresh"]
    Local.close(local)
  end

  test "drops an ack that belongs to an earlier attach", %{tmp_dir: tmp_dir} do
    {primary_dir, _replica_dir} = dirs(tmp_dir)
    local = open_primary(primary_dir)
    sender = start_sender(tmp_dir, [])

    commit_both(local, sender, entry("replicated"))
    assert_receive {:backend, {:watermark, _node, {0, _offset}}}, 1000

    send(sender, {:replica_ack, make_ref(), {0, 999_999}})

    refute_receive {:backend, {:watermark, _node, {0, 999_999}}}, 200

    Sender.stop(sender)
    Local.close(local)
  end

  test "adopts the replica tail on attach instead of truncating it", %{tmp_dir: tmp_dir} do
    {primary_dir, replica_dir} = dirs(tmp_dir)
    local = open_primary(primary_dir)
    sender = start_sender(tmp_dir, [])

    kept = entry("on-both")
    ahead = entry("already-durable-on-the-replica")

    local = commit_both(local, sender, kept)
    await_watermark({0, byte_size(kept)})
    flush_watermarks()

    {:ok, _watermark} = DurableBuffer.Replica.commit(replica_dir, 0, 0, byte_size(kept), ahead)
    assert replica_entries(tmp_dir) == ["on-both", "already-durable-on-the-replica"]

    GenServer.stop(DurableBuffer.Replica.writer_pid(replica_dir, 0, true))
    :ok = Sender.commit(sender, 0, byte_size(kept), ahead)

    assert_receive {:backend, {:watermark, _node, watermark}}, 5000
    assert watermark == {0, byte_size(kept) + byte_size(ahead)}
    assert replica_entries(tmp_dir) == ["on-both", "already-durable-on-the-replica"]

    Sender.stop(sender)
    Local.close(local)
  end

  defp flush_watermarks do
    receive do
      {:backend, {:watermark, _node, _watermark}} -> flush_watermarks()
    after
      50 -> :ok
    end
  end

  test "reset re-attaches at once and truncates a replica on the old epoch",
       %{tmp_dir: tmp_dir} do
    {primary_dir, _replica_dir} = dirs(tmp_dir)
    local = open_primary(primary_dir)
    sender = start_sender(tmp_dir, [])

    binary = entry("pre-truncate")
    local = commit_both(local, sender, binary)
    await_watermark({0, byte_size(binary)})
    assert replica_entries(tmp_dir) == ["pre-truncate"]

    :ok = Sender.reset(sender, 1)

    assert_receive {:backend, {:adopted, _node, 1}}, 2000
    assert replica_entries(tmp_dir) == []

    Sender.stop(sender)
    Local.close(local)
  end
end
