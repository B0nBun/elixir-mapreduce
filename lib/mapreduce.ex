defmodule Mapreduce do
  def sequential(files, map, reduce) do
    outfile = "mr-out-0"
    File.touch!(outfile)
    {:ok, file} = File.open(outfile, [:write])

    files
    |> Enum.flat_map(fn file -> map.(file, File.stream!(file)) end)
    |> Enum.reduce(%{}, fn {k, v}, map -> Map.put(map, k, [v | Map.get(map, k, [])]) end)
    |> Enum.map(fn {k, values} -> {k, reduce.(k, values)} end)
    |> Worker.write_result_to_file(file)

    File.close(file)
    :ok
  end

  # TODO: Implement using nodes of processes under supervisor
  def under_supervisor(files, map, reduce, options \\ []) do
    defaults = [workers_num: 5, name: Mapreduce.Supervisor]
    options = Keyword.merge(defaults, options)

    coordinator_child = %{
      id: Coordinator,
      start: {Coordinator, :start_link, [{:global, :coordinator}, files]},
      restart: :transient
    }

    worker_children =
      Enum.map(
        1..Keyword.get(options, :workers_num),
        &%{
          id: {Worker, &1},
          start: {Worker, :start_link, [Node.self(), {:global, :coordinator}, map, reduce]},
          restart: :transient
        }
      )

    children = [coordinator_child | worker_children]
    Supervisor.start_link(children, strategy: :one_for_one, name: Keyword.get(options, :name))
  end
end
