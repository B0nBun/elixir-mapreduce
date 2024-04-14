defmodule Mapreduce.Callbacks do
  @spec map(String.t(), File.io_device()) :: [{String.t(), String.t()}]
  def map(filename, stream) do
    stream
    |> Enum.flat_map(&String.split(&1))
    |> Enum.uniq()
    |> Enum.map(&{&1, filename})
  end

  @spec reduce(String.t(), [String.t()]) :: String.t()
  def reduce(_key, values) do
    joined =
      values
      |> Enum.sort()
      |> Enum.join(", ")
    "#{length(values)} #{joined}"
  end
end
