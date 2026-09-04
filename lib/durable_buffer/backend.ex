defmodule DurableBuffer.Backend do
  @moduledoc """
  Behaviour for durability backends.

  A backend instance is owned by a single `DurableBuffer.Partition` writer.
  `commit/4` receives an already-framed batch of WAL entries, the span of
  logical offsets it occupies, and must not return `{:ok, state}` until the
  batch is durable per the backend's guarantee (fsync, replica acks, S3
  PUT).

  A backend may additionally implement the asynchronous commit contract —
  `commit_async/5` and `handle_message/2` — to let commits overlap:
  `commit_async/5` submits a batch and returns either `{:done, result,
  state}` (settled immediately) or `{:pending, state}` (settled later). A
  pending commit settles when a `{:backend, message}` message arrives at the
  owning process and `handle_message/2` returns its tag in the completions
  list. Any timers or acknowledgement traffic the backend needs must be
  addressed to `self()` at `commit_async/5` time, wrapped as `{:backend,
  message}`. `DurableBuffer.Partition.Committer` uses this contract when
  available to pipeline commits, replying to callers strictly in submission
  order.
  """

  @type config :: map()
  @type state :: term()
  @type tag :: term()
  @type result :: :ok | {:error, term()}

  @typedoc """
  Where a batch lands in the logical log: the offset of its first entry and
  how many entries it carries. Backends that name or index data by offset
  need it; the rest ignore it.
  """
  @type span :: {first_offset :: non_neg_integer(), count :: non_neg_integer()}

  @typedoc """
  Returns the byte offset through which the partition's data currently meets
  the backend's durability guarantee. Re-evaluated as a reader advances, so
  a stream sees data that becomes durable while it runs.
  """
  @type limit_fun :: (-> non_neg_integer())

  @typedoc """
  A retention policy: keep at most `:ms` of age, at most `:bytes` on disk,
  or both. A `nil` bound is not applied. At least one is always set, because
  `DurableBuffer.trim/2` refuses a buffer that declares neither.
  """
  @type policy :: %{ms: pos_integer() | nil, bytes: pos_integer() | nil}

  @typedoc """
  What retention has to work with for one partition: the commit time of the
  oldest retained batch, and the bytes it retains. `:oldest_ms` is `nil`
  when the partition is empty or cannot date its head.
  """
  @type retention_status :: %{oldest_ms: integer() | nil, bytes: non_neg_integer()}

  @typedoc """
  Options accepted by `c:stream/3`.

    * `:limit` — a `t:limit_fun/0` bounding the read at the durable offset,
      or `nil` for an ungated read.
    * `:from` — first logical entry offset to yield, inclusive.
    * `:with_offsets` — yield `{offset, payload}` instead of `payload`.
  """
  @type stream_opts :: [
          limit: limit_fun() | nil,
          from: non_neg_integer(),
          with_offsets: boolean()
        ]

  @callback init_config(keyword()) :: config()
  @callback open(config(), partition_index :: non_neg_integer()) :: {:ok, state()}
  @callback commit(state(), batch :: iodata(), byte_size :: non_neg_integer(), span()) ::
              {:ok, state()} | {:error, term(), state()}
  @callback commit_async(
              state(),
              batch :: iodata(),
              byte_size :: non_neg_integer(),
              span(),
              tag()
            ) :: {:done, result(), state()} | {:pending, state()}
  @callback handle_message(message :: term(), state()) :: {[{tag(), result()}], state()}
  @callback stream(config(), partition_index :: non_neg_integer()) :: Enumerable.t()
  @callback stream(config(), partition_index :: non_neg_integer(), opts :: keyword()) ::
              Enumerable.t()
  @callback durable_offset(state()) :: non_neg_integer()
  @callback offsets(state()) :: %{first: non_neg_integer(), next: non_neg_integer()}
  @callback trim(state(), upto :: non_neg_integer()) ::
              {:ok, state()} | {:error, term(), state()}
  @callback retention_point(state(), policy()) :: {:ok, non_neg_integer()} | :none
  @callback retention_status(state()) :: retention_status()
  @callback truncate(state(), next_offset :: non_neg_integer()) :: {:ok, state()}
  @callback close(state()) :: :ok

  @optional_callbacks commit_async: 5,
                      handle_message: 2,
                      stream: 3,
                      durable_offset: 1,
                      offsets: 1,
                      trim: 2,
                      retention_point: 2,
                      retention_status: 1

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

  @doc """
  Whether `module` can apply a retention policy and report on it.
  """
  @spec applies_retention?(module()) :: boolean()
  def applies_retention?(module) do
    function_exported?(module, :retention_point, 2) and
      function_exported?(module, :retention_status, 1)
  end

  @doc """
  Whether `module` assigns and recovers logical entry offsets.
  """
  @spec tracks_offsets?(module()) :: boolean()
  def tracks_offsets?(module) do
    function_exported?(module, :offsets, 1) and function_exported?(module, :stream, 3)
  end

  @doc """
  Whether `module` can gate reads at its durable offset.

  A backend that cannot is read ungated. `DurableBuffer.Backend.S3` is the
  case that matters: an object exists only once its PUT succeeded, so its
  reads are already exact and there is nothing to gate.
  """
  @spec gates_reads?(module()) :: boolean()
  def gates_reads?(module) do
    function_exported?(module, :stream, 3) and
      function_exported?(module, :durable_offset, 1)
  end

  @doc """
  Whether `module` implements the asynchronous commit contract.
  """
  @spec async?(module()) :: boolean()
  def async?(module) do
    function_exported?(module, :commit_async, 5) and
      function_exported?(module, :handle_message, 2)
  end
end
