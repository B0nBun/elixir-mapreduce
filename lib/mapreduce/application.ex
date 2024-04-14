# Needed so that warnings about 'undefined module' don't show up
defmodule Mapreduce.Callbacks do end

defmodule Mapreduce.Application do
  use Application

  @usage_message "TODO: Usage..."

  @impl true
  def start(_type, _args) do
    {type, coordinator_name, app_filename} = get_params()
    node = :"#{coordinator_name}"
    if type == :worker do
      case Node.connect(node) do
        true -> IO.puts "Connected to node '#{coordinator_name}'"
        res -> raise "Couldn't connect to node '#{coordinator_name}', got #{res}"
      end
    end

    if type == :worker do
      Code.eval_file(app_filename)
    end

    map = &Mapreduce.Callbacks.map(&1, &2)
    reduce = &Mapreduce.Callbacks.reduce(&1, &2)

    case type do
      :worker -> Worker.start_link(node, {:global, :coordinator}, map, reduce)
      :coordinator -> Coordinator.start_link({:global, :coordinator}, Path.wildcard("texts/*"))
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

    {:ok, app_filename} = case {type, System.get_env("MR_APP_FILE")} do
      {:worker, ""} -> {:error, @usage_message}
      {:worker, nil} -> {:error, @usage_message}
      {_, name} -> {:ok, name}
    end

    {type, coordinator_name, app_filename}
  end
end
