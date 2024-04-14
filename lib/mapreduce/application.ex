defmodule Mapreduce.Application do
  use Application

  @usage_message "TODO: Usage..."

  @spec map(String.t(), File.io_device()) :: [{String.t(), String.t()}]
  defp map(_filename, stream) do
    stream
    |> Enum.flat_map(&String.split(&1))
    |> Enum.map(fn w -> {w, "1"} end)
  end

  @spec reduce(String.t(), [String.t()]) :: String.t()
  defp reduce(_key, values) do
    values
    |> length()
    |> Integer.to_string()
  end

  @impl true
  def start(_type, _args) do
    {type, coordinator_name} = get_params()
    node = :"#{coordinator_name}"
    if type == :worker do
      case Node.connect(node) do
        true -> IO.puts "Connected to node '#{coordinator_name}'"
        res -> raise "Couldn't connect to node '#{coordinator_name}', got #{res}"
      end
    end

    case type do
      :worker -> Worker.start_link(node, {:global, :coordinator}, &map(&1, &2), &reduce(&1, &2))
      :coordinator -> Coordinator.start_link({:global, :coordinator}, Path.wildcard("texts/*.txt"))
    end
  end

  def get_params() do
    {:ok, type} = case System.get_env("MR_TYPE") do
      "worker" -> {:ok, :worker}
      "coordinator" -> {:ok, :coordinator}
      _ -> {:error, @usage_message}
    end

    {:ok, coordinator_name} = case {type, System.get_env("MR_COORD_NAME")} do
      {:worker, ""} -> {:error, @usage_message}
      {:worker, nil} -> {:error, @usage_message}
      {_, name} -> {:ok, name}
    end

    {type, coordinator_name}
  end
end
