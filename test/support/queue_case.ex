defmodule DurableBuffer.Test.QueueCase do
  @moduledoc """
  Helpers for queue tests: a `FakeS3` bucket behind `Req.Test` and a queue
  config pointing at it.
  """

  alias DurableBuffer.Queue.Store
  alias DurableBuffer.Test.FakeS3

  @doc """
  Starts a fake bucket and returns `{config, store}`. `:handler` wraps
  `FakeS3.call/2` to inject faults or latency. Other options go to
  `DurableBuffer.Queue.Store.config/1`.
  """
  def queue_config(opts \\ []) do
    {:ok, store} = FakeS3.start_store()
    stub_name = :"queue_s3_#{System.unique_integer([:positive])}"
    handler = Keyword.get(opts, :handler, &FakeS3.call/2)
    Req.Test.stub(stub_name, fn conn -> handler.(conn, store) end)

    config =
      opts
      |> Keyword.drop([:handler])
      |> Keyword.put_new(:bucket, "test-bucket")
      |> Keyword.put_new(:prefix, "queue-#{System.unique_integer([:positive])}")
      |> Keyword.put(:req_options, plug: {Req.Test, stub_name}, retry: false)
      |> Store.config()

    {config, store}
  end
end
