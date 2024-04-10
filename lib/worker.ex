defmodule Worker do
  require Logger

  def map(_filename, contents) do
    words = String.split(contents, " ", trim: true)
    Enum.map(words, fn w -> {w, "1"} end)
  end

  def reduce(_key, values) do
    Integer.to_string(length(values))
  end

  def start_link(coordinator_server) do
    Logger.info("Worker: Spawned link")
    pid = spawn_link(fn -> loop(coordinator_server) end)
    {:ok, pid}
  end

  defp handle_map(server, task_id, _filename) do
    Logger.info("Worker: Handling map task (id=#{task_id})")
    :timer.sleep(1000 + :rand.uniform(1000))
    Logger.info("Worker: Completed map task (id=#{task_id})")
    Coordinator.complete_task(server, task_id)
  end

  defp handle_reduce(server, task_id, _reduce_idx) do
    Logger.info("Worker: Handling reduce task (id=#{task_id})")
    :timer.sleep(1000 + :rand.uniform(1000))
    Logger.info("Worker: Completed reduce task (id=#{task_id})")
    Coordinator.complete_task(server, task_id)
  end

  def loop(server) do
    Logger.info("Worker: probing coordinator for tasks")
    reply = Coordinator.get_task(server)

    case reply do
      {:map, task_id, filename} -> handle_map(server, task_id, filename)
      {:reduce, task_id, reduce_idx} -> handle_reduce(server, task_id, reduce_idx)
      {:no_task} -> :timer.sleep(1000)
    end

    if Coordinator.done?(server) do
      Logger.info("Worker: Recieved done?=true, exiting")
    else
      loop(server)
    end
  end
end
