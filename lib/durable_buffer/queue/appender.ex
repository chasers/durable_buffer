defmodule DurableBuffer.Queue.Appender do
  @moduledoc """
  Appends entries to one queue manifest, group-committing the writes.

  There is one appender per manifest per node, shared by every partition that
  produces into it. Requests that arrive while a manifest write is in flight
  queue up, and the next write takes all of them: one GET, one append of every
  queued entry, one compare-and-set PUT. So the manifest write rate stays near
  `1 / (GET + PUT latency)` per node, whatever the partition count or load.

  The appender keeps the manifest and version from its last write and writes
  the next update against them without a GET. While it is the only writer,
  an append costs one PUT. When another node wrote in between, the PUT
  conflicts and the update reads the manifest again.

  After each write the appender waits `:manifest_gap_ms` before it starts the
  next one; requests keep queuing meanwhile. The gap gives other nodes a
  window: without it the last winner, writing from its cache in one request,
  can take every write while a node that lost needs a GET and a PUT.

  A conflict means another node wrote first. The write reads the manifest
  again and retries. Each request gets its sequences back, in order, as
  `{:backend, {:enqueued, ref, {:ok, sequences}}}` sent to the process that
  asked, or `{:error, reason}` if the write failed.

  The write runs in a monitored process, so a crash in it fails the waiting
  requests instead of the appender.
  """

  use GenServer

  alias DurableBuffer.Queue.Manifest
  alias DurableBuffer.Queue.Store

  @doc """
  Returns the appender for `config`'s manifest, starting it when needed.
  """
  @spec ensure_started(map()) :: pid()
  def ensure_started(config) do
    key = {:queue_appender, config.bucket, config.manifest}

    case Registry.lookup(DurableBuffer.Registry, key) do
      [{pid, _value}] ->
        pid

      [] ->
        name = {:via, Registry, {DurableBuffer.Registry, key}}

        case DynamicSupervisor.start_child(
               DurableBuffer.Queue.AppenderSupervisor,
               %{
                 id: __MODULE__,
                 start: {GenServer, :start_link, [__MODULE__, config, [name: name]]},
                 restart: :temporary
               }
             ) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
  end

  @doc """
  Asks for `items` (`{location, metadata}` pairs) to be appended. The reply
  arrives later as `{:backend, {:enqueued, ref, result}}` sent to `reply_to`.
  """
  @spec enqueue(pid(), pid(), reference(), [{String.t(), [Manifest.metadata()]}]) :: :ok
  def enqueue(appender, reply_to, ref, items) do
    GenServer.cast(appender, {:enqueue, reply_to, ref, items})
  end

  @doc """
  Appends `items` and waits for the result.
  """
  @spec append(pid(), [{String.t(), [Manifest.metadata()]}], timeout()) ::
          {:ok, [non_neg_integer()]} | {:error, term()}
  def append(appender, items, timeout \\ 30_000) do
    ref = make_ref()
    :ok = enqueue(appender, self(), ref, items)

    receive do
      {:backend, {:enqueued, ^ref, result}} -> result
    after
      timeout -> {:error, :timeout}
    end
  end

  @doc """
  Counts of manifest writes, conflicts, and entries appended so far.
  """
  @spec stats(pid()) :: %{
          writes: non_neg_integer(),
          conflicts: non_neg_integer(),
          entries: non_neg_integer()
        }
  def stats(appender), do: GenServer.call(appender, :stats)

  @impl GenServer
  def init(config) do
    {:ok,
     %{
       config: config,
       req: Store.req(config),
       queued: [],
       writing: nil,
       resting: false,
       cache: nil,
       writes: 0,
       conflicts: 0,
       entries: 0
     }}
  end

  @impl GenServer
  def handle_cast({:enqueue, reply_to, ref, items}, state) do
    {:noreply, maybe_write(%{state | queued: [{reply_to, ref, items} | state.queued]})}
  end

  @impl GenServer
  def handle_call(:stats, _from, state) do
    {:reply, Map.take(state, [:writes, :conflicts, :entries]), state}
  end

  @impl GenServer
  def handle_info(
        {:DOWN, monitor, :process, _pid, reason},
        %{writing: {monitor, requests}} = state
      ) do
    state = %{state | writing: nil}

    state =
      case reason do
        {:written, {:ok, sequences, conflicts, cache}} ->
          reply_sequences(requests, sequences)

          %{
            state
            | cache: cache,
              writes: state.writes + 1,
              conflicts: state.conflicts + conflicts,
              entries: state.entries + length(sequences)
          }

        {:written, {:error, error}} ->
          reply_all(requests, {:error, error})
          %{state | cache: nil}

        crash ->
          reply_all(requests, {:error, {:manifest_write_crashed, crash}})
          %{state | cache: nil}
      end

    {:noreply, rest(state)}
  end

  def handle_info(:rested, state), do: {:noreply, maybe_write(%{state | resting: false})}

  def handle_info(_message, state), do: {:noreply, state}

  defp rest(%{config: %{manifest_gap_ms: 0}} = state), do: maybe_write(state)

  defp rest(state) do
    Process.send_after(self(), :rested, state.config.manifest_gap_ms)
    %{state | resting: true}
  end

  defp maybe_write(%{writing: nil, resting: false, queued: [_first | _rest]} = state) do
    requests = Enum.reverse(state.queued)
    items = Enum.flat_map(requests, fn {_reply_to, _ref, items} -> items end)
    %{req: req, config: config, cache: cache} = state

    {_pid, monitor} =
      spawn_monitor(fn ->
        result =
          Store.update_manifest(
            req,
            config,
            fn manifest ->
              {manifest, sequences} = Manifest.append(manifest, items)
              {:write, manifest, sequences}
            end,
            cache
          )

        exit({:written, result})
      end)

    %{state | queued: [], writing: {monitor, requests}}
  end

  defp maybe_write(state), do: state

  defp reply_sequences(requests, sequences) do
    Enum.reduce(requests, sequences, fn {reply_to, ref, items}, remaining ->
      {mine, rest} = Enum.split(remaining, length(items))
      send(reply_to, {:backend, {:enqueued, ref, {:ok, mine}}})
      rest
    end)
  end

  defp reply_all(requests, result) do
    Enum.each(requests, fn {reply_to, ref, _items} ->
      send(reply_to, {:backend, {:enqueued, ref, result}})
    end)
  end
end
