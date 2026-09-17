defmodule DurableBuffer.QueueTest do
  use ExUnit.Case, async: false

  import DurableBuffer.Test.QueueCase

  alias DurableBuffer.Backend.Queue
  alias DurableBuffer.Queue.Batch
  alias DurableBuffer.Queue.Consumer
  alias DurableBuffer.Queue.ConsumerServer
  alias DurableBuffer.Queue.GC
  alias DurableBuffer.Queue.Manifest
  alias DurableBuffer.Queue.Store
  alias DurableBuffer.Queue.ULID
  alias DurableBuffer.Test.FakeS3

  setup {Req.Test, :set_req_test_to_shared}

  defp start_buffer(config, opts \\ []) do
    name = :"queue_#{System.unique_integer([:positive])}"

    start_supervised!(
      {DurableBuffer,
       Keyword.merge([name: name, backend: {Queue, config_opts(config)}, partitions: 2], opts)}
    )

    name
  end

  defp config_opts(config) do
    [
      bucket: config.bucket,
      prefix: config.prefix,
      manifest: config.manifest,
      compression: config.compression,
      conditional_writes: config.conditional_writes,
      req_options: config.req_options
    ]
  end

  defp manifest(config) do
    {:ok, manifest, _version} = Store.read_manifest(Store.req(config), config)
    manifest
  end

  defp produce(config, batches) do
    req = Store.req(config)

    items =
      for records <- batches do
        location = Store.batch_location(config, ULID.generate())
        :ok = Store.put_batch(req, config, location, Batch.encode(records, :zstd))
        {location, []}
      end

    {:ok, _sequences, _conflicts} =
      Store.update_manifest(req, config, fn manifest ->
        {manifest, sequences} = Manifest.append(manifest, items)
        {:write, manifest, sequences}
      end)

    Enum.map(items, &elem(&1, 0))
  end

  describe "Backend.Queue" do
    test "an append returns once its batch is in the manifest" do
      {config, store} = queue_config()
      name = start_buffer(config)

      assert {:ok, _offset} = DurableBuffer.append(name, "k", "hello")

      [entry] = Manifest.entries(manifest(config))
      assert entry.location =~ ~r"^#{config.prefix}/[0-9A-Z]{26}\.batch$"
      [metadata] = entry.metadata
      assert Queue.source(metadata) == {DurableBuffer.partition_index(name, "k"), 0}
      assert {:ok, ["hello"]} = Batch.decode(FakeS3.objects(store)[entry.location])
      assert Enum.to_list(DurableBuffer.stream(name, "k")) == ["hello"]
    end

    test "a partition's batches enter the manifest in offset order" do
      {config, _store} =
        queue_config(
          handler: fn conn, store ->
            if conn.method == "PUT" and String.ends_with?(conn.request_path, ".batch"),
              do: Process.sleep(:rand.uniform(15))

            FakeS3.call(conn, store)
          end
        )

      name = start_buffer(config, partitions: 1, max_batch_entries: 10)

      for index <- 1..200 do
        :ok = DurableBuffer.append_async(name, "k", "entry-#{index}")
      end

      :ok = DurableBuffer.sync(name, "k")

      firsts =
        config
        |> manifest()
        |> Manifest.entries()
        |> Enum.map(fn %{metadata: [metadata]} -> elem(Queue.source(metadata), 1) end)

      assert firsts == Enum.sort(firsts)
      assert length(firsts) >= 20
      assert Enum.to_list(DurableBuffer.stream(name, "k")) == for(i <- 1..200, do: "entry-#{i}")
    end

    test "a failed batch PUT fails the append and leaves the manifest alone" do
      {config, _store} =
        queue_config(
          handler: fn conn, store ->
            if conn.method == "PUT" and String.ends_with?(conn.request_path, ".batch"),
              do: Plug.Conn.send_resp(conn, 500, "boom"),
              else: FakeS3.call(conn, store)
          end
        )

      name = start_buffer(config)

      assert {:error, {:unexpected_status, 500}} = DurableBuffer.append(name, "k", "lost")
      assert manifest(config).entry_count == 0
    end
  end

  describe "Consumer" do
    test "reads in order, acks in order, and dequeues every ack_interval" do
      {config, _store} = queue_config()
      produce(config, [["a"], ["b", "c"], ["d"]])

      {:ok, consumer} = Consumer.open(config, ack_interval: 2)
      {:ok, first, consumer} = Consumer.next_batch(consumer)
      assert first.entries == ["a"]
      assert {:error, {:out_of_order_ack, 0, 1}} = Consumer.ack(consumer, 1)
      assert {:error, {:not_handed_out, 1}} = Consumer.ack(%{consumer | last_acked: 0}, 1)

      {:ok, consumer} = Consumer.ack(consumer, 0)
      assert manifest(config).entry_count == 3

      {:ok, second, consumer} = Consumer.next_batch(consumer)
      assert second.entries == ["b", "c"]
      {:ok, consumer} = Consumer.ack(consumer, 1)
      assert Enum.map(Manifest.entries(manifest(config)), & &1.sequence) == [2]

      {:ok, third, consumer} = Consumer.next_batch(consumer)
      {:ok, consumer} = Consumer.ack(consumer, third.sequence)
      assert {:ok, nil, consumer} = Consumer.next_batch(consumer)
      {:ok, _consumer} = Consumer.flush(consumer)
      assert manifest(config).entry_count == 0
    end

    test "a new consumer fences the old one" do
      {config, _store} = queue_config()
      produce(config, [["a"], ["b"]])

      {:ok, old} = Consumer.open(config)
      {:ok, _batch, old} = Consumer.next_batch(old)
      {:ok, old} = Consumer.ack(old, 0)

      {:ok, new} = Consumer.open(config)
      assert new.epoch == old.epoch + 1

      assert {:error, :fenced} = Consumer.next_batch(old)
      assert {:error, :fenced} = Consumer.flush(old)

      {:ok, batch, _new} = Consumer.next_batch(new)
      assert batch.entries == ["a"]
    end

    test "resumes after last_acked" do
      {config, _store} = queue_config()
      produce(config, [["a"], ["b"], ["c"]])

      {:ok, consumer} = Consumer.open(config, last_acked: 1)
      {:ok, batch, consumer} = Consumer.next_batch(consumer)
      assert {batch.sequence, batch.entries} == {2, ["c"]}
      assert {:ok, _consumer} = Consumer.ack(consumer, 2)
    end

    test "read-ahead hands out descriptors and ack_through dequeues in one write" do
      {config, _store} = queue_config()
      produce(config, for(i <- 0..4, do: ["r#{i}"]))

      {:ok, consumer} = Consumer.open(config)
      {:ok, descriptors, consumer} = Consumer.next_descriptors(consumer, 3)
      assert Enum.map(descriptors, & &1.sequence) == [0, 1, 2]

      fetched =
        descriptors
        |> Enum.map(fn descriptor -> Task.async(fn -> Consumer.fetch(config, descriptor) end) end)
        |> Task.await_many()

      assert Enum.map(fetched, fn {:ok, batch} -> batch.entries end) == [["r0"], ["r1"], ["r2"]]
      assert {:error, {:not_handed_out, 3}} = Consumer.ack_through(consumer, 3)

      {:ok, consumer} = Consumer.ack_through(consumer, 2)
      assert Enum.map(Manifest.entries(manifest(config)), & &1.sequence) == [3, 4]
      assert {:error, {:non_monotonic_ack, 2, 1}} = Consumer.ack_through(consumer, 1)

      {:ok, rest, _consumer} = Consumer.next_descriptors(consumer, 10)
      assert Enum.map(rest, & &1.sequence) == [3, 4]
    end

    test "a fetch of a missing batch is an error and does not move the cursor" do
      {config, store} = queue_config()
      [location] = produce(config, [["gone"]])
      Agent.update(store, &Map.delete(&1, location))

      {:ok, consumer} = Consumer.open(config)
      assert {:error, :not_found} = Consumer.next_batch(consumer)
      assert consumer.handed_out == nil
    end
  end

  describe "GC" do
    test "deletes only unreferenced batches older than the oldest entry and the grace period" do
      {config, store} = queue_config()
      req = Store.req(config)
      now = System.system_time(:millisecond)

      put = fn time_ms ->
        location = Store.batch_location(config, ULID.generate(time_ms))
        :ok = Store.put_batch(req, config, location, Batch.encode(["x"], :none))
        location
      end

      old_orphan = put.(now - 3_600_000)
      referenced_location = put.(now - 1_800_000)
      newer_than_oldest = put.(now - 900_000)
      young_orphan = put.(now - 1_000)

      {:ok, _result, _conflicts} =
        Store.update_manifest(req, config, fn manifest ->
          {manifest, sequences} = Manifest.append(manifest, [{referenced_location, []}])
          {:write, manifest, sequences}
        end)

      assert {:ok, [^old_orphan]} = GC.run(config, grace_ms: 60_000)

      keys = store |> FakeS3.objects() |> Map.keys()
      assert referenced_location in keys
      assert newer_than_oldest in keys
      assert young_orphan in keys

      {:ok, consumer} = Consumer.open(config)
      {:ok, descriptors, consumer} = Consumer.next_descriptors(consumer, 1)
      {:ok, _consumer} = Consumer.ack_through(consumer, hd(descriptors).sequence)

      assert {:ok, deleted} = GC.run(config, grace_ms: 60_000)
      assert Enum.sort(deleted) == Enum.sort([referenced_location, newer_than_oldest])
    end
  end

  describe "ConsumerServer" do
    test "delivers produced appends in order, retries, and dead-letters" do
      {config, _store} = queue_config()
      name = start_buffer(config, partitions: 1)
      parent = self()

      for payload <- ["ok-1", "flaky", "poison", "ok-2"] do
        {:ok, _offset} = DurableBuffer.append(name, "k", payload)
      end

      {:ok, attempts} = Agent.start_link(fn -> %{} end)

      handler = fn batch ->
        [payload] = batch.entries

        count =
          Agent.get_and_update(
            attempts,
            &{Map.get(&1, payload, 0) + 1, Map.update(&1, payload, 1, fn n -> n + 1 end)}
          )

        send(parent, {:handled, payload, count})

        cond do
          payload == "poison" -> {:error, :bad_payload}
          payload == "flaky" and count == 1 -> raise "transient"
          true -> :ok
        end
      end

      start_supervised!(
        {ConsumerServer,
         queue: config,
         handler: handler,
         dead_letter: fn batch, reason -> send(parent, {:dead, batch.entries, reason}) end,
         max_attempts: 3,
         backoff_ms: 5,
         poll_ms: 20,
         ack_interval: 1,
         gc_interval_ms: :infinity}
      )

      assert_receive {:handled, "ok-1", 1}, 2_000
      assert_receive {:handled, "flaky", 1}, 2_000
      assert_receive {:handled, "flaky", 2}, 2_000
      assert_receive {:handled, "poison", 3}, 2_000
      assert_receive {:dead, ["poison"], :bad_payload}, 2_000
      assert_receive {:handled, "ok-2", 1}, 2_000
      refute_receive {:handled, "ok-1", 2}, 100

      assert eventually(fn -> manifest(config).entry_count == 0 end)
    end

    test "stops with shutdown fenced when another consumer takes over" do
      {config, _store} = queue_config()
      Process.flag(:trap_exit, true)

      {:ok, server} =
        ConsumerServer.start_link(
          queue: config,
          handler: fn _batch -> :ok end,
          poll_ms: 10,
          gc_interval_ms: :infinity
        )

      {:ok, _takeover} = Consumer.open(config)
      assert_receive {:EXIT, ^server, {:shutdown, :fenced}}, 2_000
    end
  end

  defp eventually(check, attempts \\ 200) do
    cond do
      check.() -> true
      attempts == 0 -> false
      true -> Process.sleep(10) && eventually(check, attempts - 1)
    end
  end
end
