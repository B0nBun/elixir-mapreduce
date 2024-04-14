defmodule Mapreduce.Callbacks do
  @spec map(String.t(), File.io_device()) :: [{String.t(), String.t()}]
  def map(filename, stream) do
    pattern = ~r/[0-9]+/
    stream
    |> Enum.map(&String.trim_trailing(&1))
    |> Enum.with_index(1)
    |> Enum.filter(fn {line, _} -> String.match?(line, pattern) end)
    |> Enum.map(fn {line, idx} -> {"#{filename}:#{idx}", line} end)
  end

  @spec reduce(String.t(), [String.t()]) :: String.t()
  def reduce(_key, values) do
    if length(values) == 0 do
      ""
    else
      hd(values)
    end
  end
end
