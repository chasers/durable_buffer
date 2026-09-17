defmodule DurableBuffer.Queue.ULID do
  @moduledoc """
  ULIDs for data batch object names.

  A ULID is 48 bits of Unix time in milliseconds followed by 80 random bits,
  written as 26 characters of Crockford base32. Names sort by creation time,
  and `DurableBuffer.Queue.GC` reads the time back out of them.
  """

  import Bitwise

  @alphabet ~c"0123456789ABCDEFGHJKMNPQRSTVWXYZ"
  @decode @alphabet |> Enum.with_index() |> Map.new()

  @doc """
  Generates a ULID for `time_ms`, the current time by default.
  """
  @spec generate(non_neg_integer()) :: String.t()
  def generate(time_ms \\ System.system_time(:millisecond)) do
    <<random::80>> = :crypto.strong_rand_bytes(10)
    encode(time_ms <<< 80 ||| random)
  end

  @doc """
  Returns the millisecond timestamp of `ulid`, or `:error` when it is not a
  valid ULID.
  """
  @spec time_ms(String.t()) :: {:ok, non_neg_integer()} | :error
  def time_ms(ulid) when byte_size(ulid) == 26 do
    ulid
    |> String.upcase()
    |> String.to_charlist()
    |> Enum.reduce_while(0, fn char, acc ->
      case Map.fetch(@decode, char) do
        {:ok, value} -> {:cont, acc <<< 5 ||| value}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      :error -> :error
      value when value >>> 128 == 0 -> {:ok, value >>> 80}
      _overflow -> :error
    end
  end

  def time_ms(_other), do: :error

  defp encode(value) do
    for shift <- 125..0//-5, into: "" do
      <<Enum.at(@alphabet, value >>> shift &&& 31)>>
    end
  end
end
