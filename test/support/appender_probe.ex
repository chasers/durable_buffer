defmodule DurableBuffer.Test.AppenderProbe do
  @moduledoc """
  Starts an unregistered `DurableBuffer.Queue.Appender`, so a test can run two
  appenders against one manifest the way two nodes would.
  """

  def child_spec(config) do
    %{id: __MODULE__, start: {GenServer, :start_link, [DurableBuffer.Queue.Appender, config]}}
  end
end
