defmodule Mapreduce.Application do
  use Application

  @spec map(String.t(), File.io_device()) :: list({String.t(), String.t()})
  defp map(_filename, stream) do
    stream
    |> Enum.flat_map(&String.split(&1))
    |> Enum.map(fn w -> {w, "1"} end)
  end

  @spec reduce(String.t(), list(String.t())) :: String.t()
  defp reduce(_key, values) do
    values
    |> length()
    |> Integer.to_string()
  end

  @impl true
  def start(_type, _args) do
    Mapreduce.under_supervisor(
      Path.wildcard("texts/*.txt"),
      &map(&1, &2),
      &reduce(&1, &2),
      workers_num: 5,
      name: Mapreduce.Application.Supervisor
    )
  end
end
