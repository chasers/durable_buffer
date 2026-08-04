defmodule DurableBuffer.Backend do
  @moduledoc """
  Behaviour for durability backends.

  A backend instance is owned by a single `DurableBuffer.Partition` writer.
  `commit/3` receives an already-framed batch of WAL entries and must not
  return `{:ok, state}` until the batch is durable per the backend's
  guarantee (fsync, replica acks, S3 PUT).
  """

  @type config :: map()
  @type state :: term()

  @callback init_config(keyword()) :: config()
  @callback open(config(), partition_index :: non_neg_integer()) :: {:ok, state()}
  @callback commit(state(), batch :: iodata(), byte_size :: non_neg_integer()) ::
              {:ok, state()} | {:error, term(), state()}
  @callback stream(config(), partition_index :: non_neg_integer()) :: Enumerable.t()
  @callback truncate(state()) :: {:ok, state()}
  @callback close(state()) :: :ok

  @doc """
  Normalizes a `{module, opts}` backend spec into `{module, config}`.
  """
  @spec normalize({module(), keyword()} | module()) :: {module(), config()}
  def normalize({module, opts}) when is_atom(module) and is_list(opts) do
    {module, module.init_config(opts)}
  end

  def normalize(module) when is_atom(module) do
    normalize({module, []})
  end
end
