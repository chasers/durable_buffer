defmodule DurableBuffer.Backend.Queue do
  @moduledoc """
  Experimental stateless backend in the style of Open Data Buffer: each group
  commit becomes a data batch object plus an entry in a shared queue manifest.

  A commit settles once both steps succeed:

    1. The batch is written to `<prefix>/<ULID>.batch` in the Open Data Buffer
       batch format (see `DurableBuffer.Queue.Batch`), compressed with zstd by
       default.
    2. Its location is appended to the manifest by the node's
       `DurableBuffer.Queue.Appender`, which group-commits manifest writes
       with compare-and-set.

  Nothing is kept on local disk, so any node can stop at any time without
  losing an acked append. A `DurableBuffer.Queue.Consumer` or
  `DurableBuffer.Queue.ConsumerServer` reads the manifest, and no bucket is
  listed on either path.

  PUTs overlap: up to `:max_inflight_commits` per partition run at once. Their
  manifest entries are released in submission order, so a partition's batches
  enter the manifest in offset order. Each entry carries one metadata item
  whose payload is `partition u32 LE | first_offset u64 LE`, readable with
  `source/1`.

  This is a queue, not a log, so some buffer operations do not apply:

    * Offsets returned by `append` count from zero in each run. The durable
      identity of an entry is its manifest sequence and its index in the batch.
    * `DurableBuffer.stream/3` reads the manifest without fencing and fetches
      the partition's batches. It is for debugging and tests.
    * `truncate` does nothing. The consumer decides what leaves the queue.
    * There is no retention: consumer acks and `DurableBuffer.Queue.GC` remove
      data.

  Options are those of `DurableBuffer.Queue.Store.config/1`.
  """

  @behaviour DurableBuffer.Backend

  alias DurableBuffer.Queue.Appender
  alias DurableBuffer.Queue.Batch
  alias DurableBuffer.Queue.Manifest
  alias DurableBuffer.Queue.Store
  alias DurableBuffer.Queue.ULID
  alias DurableBuffer.WAL

  @impl DurableBuffer.Backend
  def init_config(opts), do: Store.config(opts)

  @impl DurableBuffer.Backend
  def open(config, partition_index) do
    {:ok,
     %{
       config: config,
       partition_index: partition_index,
       req: Store.req(config),
       appender: Appender.ensure_started(config),
       pending: []
     }}
  end

  @doc """
  Decodes the `{partition, first_offset}` a batch's metadata item carries.
  """
  @spec source(Manifest.metadata()) :: {non_neg_integer(), non_neg_integer()} | :unknown
  def source(%{payload: <<partition::32-little, first_offset::64-little>>}),
    do: {partition, first_offset}

  def source(_metadata), do: :unknown

  @impl DurableBuffer.Backend
  def commit(state, batch, byte_size, span) do
    tag = make_ref()
    {:pending, state} = commit_async(state, batch, byte_size, span, tag)
    await_commit(state, tag)
  end

  defp await_commit(state, tag) do
    receive do
      {:backend, message} ->
        {completions, state} = handle_message(message, state)

        case List.keyfind(completions, tag, 0) do
          {^tag, :ok} -> {:ok, state}
          {^tag, {:error, reason}} -> {:error, reason, state}
          nil -> await_commit(state, tag)
        end
    end
  end

  @impl DurableBuffer.Backend
  def commit_async(state, batch, _byte_size, {first_offset, _count}, tag) do
    config = state.config
    owner = self()
    location = Store.batch_location(config, ULID.generate())
    %{req: req} = state

    metadata = [
      %{
        start_index: 0,
        ingestion_time_ms: System.system_time(:millisecond),
        payload: <<state.partition_index::32-little, first_offset::64-little>>
      }
    ]

    spawn(fn ->
      result =
        try do
          {records, _valid, _rest} = batch |> IO.iodata_to_binary() |> WAL.decode_all()
          Store.put_batch(req, config, location, Batch.encode(records, config.compression))
        rescue
          exception -> {:error, exception}
        end

      send(owner, {:backend, {:put, tag, result}})
    end)

    entry = %{tag: tag, location: location, metadata: metadata, status: :putting}
    {:pending, %{state | pending: state.pending ++ [entry]}}
  end

  @impl DurableBuffer.Backend
  def handle_message({:put, tag, result}, state) do
    status = if result == :ok, do: :put, else: {:failed, result}
    pending = Enum.map(state.pending, &if(&1.tag == tag, do: %{&1 | status: status}, else: &1))
    release(%{state | pending: pending})
  end

  def handle_message({:enqueued, ref, result}, state) do
    {settled, pending} = Enum.split_with(state.pending, &(&1.status == {:enqueuing, ref}))
    reply = if match?({:ok, _sequences}, result), do: :ok, else: result
    {Enum.map(settled, &{&1.tag, reply}), %{state | pending: pending}}
  end

  def handle_message(_message, state), do: {[], state}

  defp release(state) do
    {failed, pending} = Enum.split_with(state.pending, &match?({:failed, _result}, &1.status))
    completions = Enum.map(failed, fn %{tag: tag, status: {:failed, result}} -> {tag, result} end)

    ready =
      pending
      |> Enum.drop_while(&match?({:enqueuing, _ref}, &1.status))
      |> Enum.take_while(&(&1.status == :put))

    case ready do
      [] ->
        {completions, %{state | pending: pending}}

      ready ->
        ref = make_ref()
        tags = MapSet.new(ready, & &1.tag)
        items = Enum.map(ready, &{&1.location, &1.metadata})
        :ok = Appender.enqueue(state.appender, self(), ref, items)

        pending =
          Enum.map(
            pending,
            &if(MapSet.member?(tags, &1.tag), do: %{&1 | status: {:enqueuing, ref}}, else: &1)
          )

        {completions, %{state | pending: pending}}
    end
  end

  @doc """
  Streams the partition's payloads from the batches the manifest references,
  oldest first. Reads without fencing, so it never disturbs a consumer.
  """
  @impl DurableBuffer.Backend
  def stream(config, partition_index) do
    Stream.flat_map([:build], fn :build ->
      req = Store.req(config)
      {:ok, manifest, _version} = Store.read_manifest(req, config)

      manifest
      |> Manifest.entries()
      |> Stream.filter(fn entry ->
        Enum.any?(entry.metadata, &match?({^partition_index, _first}, source(&1)))
      end)
      |> Stream.flat_map(fn entry ->
        {:ok, body} = Store.get_batch(req, config, entry.location)
        {:ok, records} = Batch.decode(body)
        records
      end)
    end)
  end

  @impl DurableBuffer.Backend
  def truncate(state, _next), do: {:ok, state}

  @impl DurableBuffer.Backend
  def close(_state), do: :ok
end
