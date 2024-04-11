defmodule Mapreduce.Application do
  use Application

  @spec map(String.t(), File.Stream.t()) :: list({String.t(), String.t()})
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
    files = Path.wildcard("texts/*.txt")

    coordinator_child = %{
      id: Coordinator,
      start: {Coordinator, :start_link, [{:global, :coordinator}, files]},
      restart: :transient
    }

    map_lambda = &map(&1, &2)
    reduce_lambda = &reduce(&1, &2)

    worker_children =
      Enum.map(
        0..1,
        &%{
          id: {Worker, &1},
          start: {Worker, :start_link, [{:global, :coordinator}, map_lambda, reduce_lambda]},
          restart: :transient
        }
      )

    children = [coordinator_child | worker_children]
    Supervisor.start_link(children, strategy: :one_for_one, name: Mapreduce.Supervisor)
  end
end
