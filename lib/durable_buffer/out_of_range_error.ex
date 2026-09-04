defmodule DurableBuffer.OutOfRangeError do
  @moduledoc """
  Raised by `DurableBuffer.stream/3` when `:from` names an offset the
  partition no longer retains.

  Starting the read at the retained base instead would hand the consumer a
  contiguous-looking stream with a hole in it, and nothing else protects a
  consumer that falls behind its buffer's retention window. Raising makes
  the gap the caller's decision: resync in full, or restart from `:first`.

  `stream/3` reads the partition's bounds before it builds the stream, so
  this is raised at the call rather than part-way through iterating.

      try do
        DurableBuffer.stream(:events, user_id, from: cursor) |> Enum.each(&handle/1)
      rescue
        error in DurableBuffer.OutOfRangeError ->
          resync_from(error.first)
      end
  """

  defexception [:name, :partition_index, :requested, :first]

  @type t :: %__MODULE__{
          name: atom(),
          partition_index: non_neg_integer(),
          requested: non_neg_integer(),
          first: non_neg_integer()
        }

  @impl Exception
  def message(error) do
    "#{inspect(error.name)} partition #{error.partition_index} no longer retains " <>
      "offset #{error.requested}. The oldest retained offset is #{error.first}. " <>
      "Read from there, or resync in full."
  end
end
