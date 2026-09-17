defmodule DurableBuffer.Queue.GC do
  @moduledoc """
  Deletes batch objects that the manifest no longer references, as Open Data
  Buffer's garbage collector does.

  One cycle reads the manifest without fencing, lists `<prefix>/*.batch`, and
  deletes a batch only when all of these hold:

    * the manifest does not reference it;
    * its ULID time is older than the oldest manifest entry's ULID time, so a
      batch that is written but not yet enqueued survives;
    * its ULID time is older than `:grace_ms` (default 10 minutes), which
      covers a producer that lags between its PUT and its manifest append, and
      a consumer still fetching a batch it just acked.

  This is the only place the queue lists the bucket. Run it every few minutes,
  not per batch. A failed delete is retried on the next cycle.
  """

  require Logger

  alias DurableBuffer.Queue.Manifest
  alias DurableBuffer.Queue.Store
  alias DurableBuffer.Queue.ULID

  @doc """
  Runs one cycle. Returns the keys it deleted.

  Options: `:grace_ms` and `:now_ms` (the current time, for tests).
  """
  @spec run(map(), keyword()) :: {:ok, [String.t()]} | {:error, term()}
  def run(config, opts \\ []) do
    grace_ms = Keyword.get(opts, :grace_ms, 600_000)
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
    req = Store.req(config)

    with {:ok, manifest, _version} <- Store.read_manifest(req, config),
         {:ok, keys} <- Store.list_batches(req, config) do
      entries = Manifest.entries(manifest)
      referenced = MapSet.new(entries, & &1.location)
      oldest_ms = oldest_entry_ms(entries)

      deleted =
        for key <- keys,
            not MapSet.member?(referenced, key),
            {:ok, time_ms} <- [key_time(key)],
            oldest_ms == nil or time_ms < oldest_ms,
            time_ms < now_ms - grace_ms,
            delete(req, config, key) == :ok,
            do: key

      {:ok, deleted}
    end
  end

  defp oldest_entry_ms([]), do: nil

  defp oldest_entry_ms([oldest | _rest]) do
    case key_time(oldest.location) do
      {:ok, time_ms} -> time_ms
      :error -> 0
    end
  end

  defp key_time(key), do: key |> Path.basename(".batch") |> ULID.time_ms()

  defp delete(req, config, key) do
    case Store.delete(req, config, key) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("durable_buffer: queue GC could not delete #{key}: #{inspect(reason)}")
        :error
    end
  end
end
