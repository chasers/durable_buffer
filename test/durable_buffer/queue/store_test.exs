defmodule DurableBuffer.Queue.StoreTest do
  use ExUnit.Case, async: false

  import DurableBuffer.Test.QueueCase

  alias DurableBuffer.Queue.Appender
  alias DurableBuffer.Queue.Manifest
  alias DurableBuffer.Queue.Store

  setup {Req.Test, :set_req_test_to_shared}

  for mode <- [:etag, :gcs_generation] do
    test "compare-and-set rejects a stale write with #{mode}" do
      {config, _store} = queue_config(conditional_writes: unquote(mode))
      req = Store.req(config)

      assert {:ok, empty, nil} = Store.read_manifest(req, config)
      {first, _sequences} = Manifest.append(empty, [{"a", []}])
      assert :ok = Store.write_manifest(req, config, first, nil)
      assert :conflict = Store.write_manifest(req, config, first, nil)

      {:ok, read, version} = Store.read_manifest(req, config)
      assert Enum.map(Manifest.entries(read), & &1.location) == ["a"]

      {second, _sequences} = Manifest.append(read, [{"b", []}])
      assert :ok = Store.write_manifest(req, config, second, version)

      {stale, _sequences} = Manifest.append(read, [{"c", []}])
      assert :conflict = Store.write_manifest(req, config, stale, version)

      {:ok, read, _version} = Store.read_manifest(req, config)
      assert Enum.map(Manifest.entries(read), & &1.location) == ["a", "b"]
    end
  end

  test "update_manifest retries a conflict and counts it" do
    {config, _store} = queue_config()
    req = Store.req(config)
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    assert {:ok, [1], 1} =
             Store.update_manifest(req, config, fn manifest ->
               if Agent.get_and_update(attempts, &{&1, &1 + 1}) == 0 do
                 {racer, _sequences} = Manifest.append(manifest, [{"racer", []}])
                 :ok = Store.write_manifest(req, config, racer, nil)
               end

               {manifest, sequences} = Manifest.append(manifest, [{"mine", []}])
               {:write, manifest, sequences}
             end)

    {:ok, read, _version} = Store.read_manifest(req, config)
    assert Enum.map(Manifest.entries(read), & &1.location) == ["racer", "mine"]
  end

  test "batches round-trip and list by suffix" do
    {config, _store} = queue_config()
    req = Store.req(config)

    assert :ok = Store.put_batch(req, config, config.prefix <> "/one.batch", "body")
    assert :ok = Store.put_batch(req, config, config.prefix <> "/manifest-ish.txt", "x")
    assert {:ok, "body"} = Store.get_batch(req, config, config.prefix <> "/one.batch")
    assert {:error, :not_found} = Store.get_batch(req, config, config.prefix <> "/gone.batch")
    assert {:ok, [key]} = Store.list_batches(req, config)
    assert key == config.prefix <> "/one.batch"
    assert :ok = Store.delete(req, config, key)
    assert {:ok, []} = Store.list_batches(req, config)
  end

  test "config validates its options" do
    assert_raise ArgumentError, ~r/conditional_writes/, fn ->
      Store.config(bucket: "b", conditional_writes: :lease)
    end

    assert_raise ArgumentError, ~r/:zstd or :none/, fn ->
      Store.config(bucket: "b", compression: :gzip)
    end
  end

  describe "Appender" do
    test "group-commits concurrent enqueues into few manifest writes" do
      {config, _store} = queue_config(handler: slow_puts(20))
      appender = Appender.ensure_started(config)
      assert Appender.ensure_started(config) == appender

      results =
        1..100
        |> Enum.map(fn index ->
          Task.async(fn -> Appender.append(appender, [{"loc-#{index}", []}]) end)
        end)
        |> Task.await_many(10_000)

      sequences = results |> Enum.flat_map(fn {:ok, sequences} -> sequences end) |> Enum.sort()
      assert sequences == Enum.to_list(0..99)

      stats = Appender.stats(appender)
      assert stats.entries == 100
      assert stats.writes < 50

      {:ok, manifest, _version} = Store.read_manifest(Store.req(config), config)
      assert manifest.entry_count == 100
    end

    test "a lone appender writes from its cache and reads only after a conflict" do
      {:ok, gets} = Agent.start_link(fn -> 0 end)

      {config, _store} =
        queue_config(
          handler: fn conn, store ->
            if conn.method == "GET", do: Agent.update(gets, &(&1 + 1))
            DurableBuffer.Test.FakeS3.call(conn, store)
          end
        )

      appender = Appender.ensure_started(config)

      for index <- 1..5 do
        assert {:ok, [sequence]} = Appender.append(appender, [{"loc-#{index}", []}])
        assert sequence == index - 1
      end

      assert Agent.get(gets, & &1) == 1

      req = Store.req(config)
      {:ok, manifest, version} = Store.read_manifest(req, config)
      {manifest, _sequences} = Manifest.append(manifest, [{"other-node", []}])
      :ok = Store.write_manifest(req, config, manifest, version)
      Agent.update(gets, fn _count -> 0 end)

      assert {:ok, [6]} = Appender.append(appender, [{"after-conflict", []}])
      assert Agent.get(gets, & &1) == 1

      {:ok, manifest, _version} = Store.read_manifest(req, config)

      assert manifest |> Manifest.entries() |> Enum.map(& &1.location) |> List.last() ==
               "after-conflict"
    end

    test "two appenders on one manifest lose no entries" do
      {config, _store} = queue_config(handler: slow_puts(5))
      first = start_supervised!({DurableBuffer.Test.AppenderProbe, config}, id: :first)
      second = start_supervised!({DurableBuffer.Test.AppenderProbe, config}, id: :second)

      results =
        for index <- 1..60 do
          appender = if rem(index, 2) == 0, do: first, else: second
          Task.async(fn -> Appender.append(appender, [{"loc-#{index}", []}]) end)
        end
        |> Task.await_many(10_000)

      assert Enum.all?(results, &match?({:ok, [_sequence]}, &1))

      {:ok, manifest, _version} = Store.read_manifest(Store.req(config), config)
      entries = Manifest.entries(manifest)
      assert Enum.map(entries, & &1.sequence) == Enum.to_list(0..59)

      assert entries |> Enum.map(& &1.location) |> Enum.sort() ==
               Enum.sort(for i <- 1..60, do: "loc-#{i}")

      assert Appender.stats(first).conflicts + Appender.stats(second).conflicts > 0
    end

    test "a failed write fails every request in it" do
      {config, _store} =
        queue_config(
          handler: fn conn, store ->
            if conn.method == "PUT",
              do: Plug.Conn.send_resp(conn, 503, "down"),
              else: DurableBuffer.Test.FakeS3.call(conn, store)
          end
        )

      appender = Appender.ensure_started(config)
      assert {:error, {:unexpected_status, 503}} = Appender.append(appender, [{"x", []}])
    end
  end

  defp slow_puts(ms) do
    fn conn, store ->
      if conn.method == "PUT", do: Process.sleep(ms)
      DurableBuffer.Test.FakeS3.call(conn, store)
    end
  end
end
