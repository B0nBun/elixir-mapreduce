defmodule Mapreduce.Callbacks do
  @spec map(String.t(), File.io_device()) :: [{String.t(), String.t()}]
  def map(_filename, stream) do
    stream
    |> Enum.flat_map(&String.split(&1))
    |> Enum.map(fn w -> {w, "1"} end)
  end

  @spec reduce(String.t(), [String.t()]) :: String.t()
  def reduce(_key, values) do
    values
    |> length()
    |> Integer.to_string()
  end
end
