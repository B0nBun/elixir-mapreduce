defmodule Mapreduce.Application do
  use Application

  @impl true
  def start(_type, _args) do
    files = [
      "non-existant-1.txt",
      "non-existant-2.txt",
      "non-existant-3.txt",
      "non-existant-4.txt"
    ]

    coordinator_child = %{
      id: Coordinator,
      start: {Coordinator, :start_link, [{:global, :coordinator}, files]},
      restart: :transient
    }

    worker_children =
      Enum.map(
        0..4,
        &%{
          id: {Worker, &1},
          start: {Worker, :start_link, [{:global, :coordinator}]},
          restart: :transient
        }
      )

    children = [coordinator_child | worker_children]
    Supervisor.start_link(children, strategy: :one_for_one, name: Mapreduce.Supervisor)
  end

  @impl true
  def stop(_) do
    IO.puts("Stopped?")
  end
end
