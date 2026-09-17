defmodule DurableBuffer.Queue.ConsumerServer do
  @moduledoc """
  A supervised consumer loop over a queue manifest, with retries,
  dead-lettering, and garbage collection.

      {DurableBuffer.Queue.ConsumerServer,
       queue: [bucket: "logs", prefix: "events/queue"],
       handler: &MyApp.Ingest.handle_batch/1,
       dead_letter: &MyApp.Ingest.dead_letter/2}

  For each batch, in manifest order:

    1. Call `handler.(batch)`. `:ok` acks the batch.
    2. An `{:error, reason}` return or a raise retries the same batch after a
       backoff, up to `:max_attempts` calls in total.
    3. After the last attempt, call `dead_letter.(batch, reason)`, then ack.
       The default dead letter logs the batch's location and drops it.

  With no batch waiting, the loop flushes its acks and polls again after
  `:poll_ms`. A storage error backs off and tries again; a failed ack is
  retried for the same batch, so no batch is skipped. Once another
  consumer opens the manifest the server stops with `{:shutdown, :fenced}`,
  so a supervisor restarts it only if this node should still own the queue.

  Options:

    * `:queue` (required) — options for `DurableBuffer.Queue.Store.config/1`,
      or a config map
    * `:handler` (required) — `(batch -> :ok | {:error, term()})`
    * `:dead_letter` — `(batch, reason -> any())`
    * `:max_attempts` — default 5
    * `:backoff_ms` — first retry delay, doubled per attempt, default 100
    * `:poll_ms` — default 1000
    * `:last_acked`, `:ack_interval` — as for `DurableBuffer.Queue.Consumer.open/2`
    * `:gc_interval_ms` — default 5 minutes, or `:infinity`
    * `:gc_grace_ms` — default 10 minutes
    * `:name` — GenServer name
  """

  use GenServer

  require Logger

  alias DurableBuffer.Queue.Consumer
  alias DurableBuffer.Queue.GC
  alias DurableBuffer.Queue.Store

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @impl GenServer
  def init(opts) do
    config =
      case Keyword.fetch!(opts, :queue) do
        %{} = config -> config
        queue_opts -> Store.config(queue_opts)
      end

    case Consumer.open(config, Keyword.take(opts, [:last_acked, :ack_interval])) do
      {:ok, consumer} ->
        state = %{
          consumer: consumer,
          config: config,
          handler: Keyword.fetch!(opts, :handler),
          dead_letter: Keyword.get(opts, :dead_letter, &log_dead_letter/2),
          max_attempts: Keyword.get(opts, :max_attempts, 5),
          backoff_ms: Keyword.get(opts, :backoff_ms, 100),
          poll_ms: Keyword.get(opts, :poll_ms, 1_000),
          gc_interval_ms: Keyword.get(opts, :gc_interval_ms, 300_000),
          gc_grace_ms: Keyword.get(opts, :gc_grace_ms, 600_000),
          fenced?: false
        }

        send(self(), :poll)
        schedule_gc(state)
        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_info(:poll, state) do
    case Consumer.next_batch(state.consumer) do
      {:ok, nil, consumer} ->
        state = %{state | consumer: consumer}

        case Consumer.flush(consumer) do
          {:ok, consumer} ->
            Process.send_after(self(), :poll, state.poll_ms)
            {:noreply, %{state | consumer: consumer}}

          {:error, reason} ->
            storage_error(state, reason)
        end

      {:ok, batch, consumer} ->
        attempt(%{state | consumer: consumer}, batch, 1)

      {:error, reason} ->
        storage_error(state, reason)
    end
  end

  def handle_info({:retry, batch, attempt}, state), do: attempt(state, batch, attempt)

  def handle_info({:ack, batch}, state), do: ack(state, batch)

  def handle_info(:gc, state) do
    config = state.config
    grace_ms = state.gc_grace_ms

    spawn(fn ->
      case GC.run(config, grace_ms: grace_ms) do
        {:ok, _deleted} -> :ok
        {:error, reason} -> Logger.warning("durable_buffer: queue GC failed: #{inspect(reason)}")
      end
    end)

    schedule_gc(state)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, %{fenced?: true}), do: :ok

  def terminate(_reason, state) do
    _flushed = Consumer.flush(state.consumer)
    :ok
  end

  defp attempt(state, batch, attempt) do
    case call_handler(state.handler, batch) do
      :ok ->
        ack(state, batch)

      {:error, _reason} when attempt < state.max_attempts ->
        delay = state.backoff_ms * Integer.pow(2, attempt - 1)
        Process.send_after(self(), {:retry, batch, attempt + 1}, delay)
        {:noreply, state}

      {:error, reason} ->
        state.dead_letter.(batch, reason)
        ack(state, batch)
    end
  end

  defp ack(state, batch) do
    case Consumer.ack(state.consumer, batch.sequence) do
      {:ok, consumer} ->
        send(self(), :poll)
        {:noreply, %{state | consumer: consumer}}

      {:error, :fenced} ->
        storage_error(state, :fenced)

      {:error, reason} ->
        Logger.warning("durable_buffer: queue ack failed, retrying: #{inspect(reason)}")
        Process.send_after(self(), {:ack, batch}, state.poll_ms)
        {:noreply, state}
    end
  end

  defp call_handler(handler, batch) do
    case handler.(batch) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_handler_result, other}}
    end
  rescue
    exception -> {:error, exception}
  end

  defp storage_error(state, :fenced), do: {:stop, {:shutdown, :fenced}, %{state | fenced?: true}}

  defp storage_error(state, reason) do
    Logger.warning("durable_buffer: queue consumer error, retrying: #{inspect(reason)}")
    Process.send_after(self(), :poll, state.poll_ms)
    {:noreply, state}
  end

  defp schedule_gc(%{gc_interval_ms: :infinity}), do: :ok
  defp schedule_gc(state), do: Process.send_after(self(), :gc, state.gc_interval_ms)

  defp log_dead_letter(batch, reason) do
    Logger.error(
      "durable_buffer: dropping queue batch #{batch.sequence} at #{batch.location} " <>
        "after retries: #{inspect(reason)}"
    )
  end
end
