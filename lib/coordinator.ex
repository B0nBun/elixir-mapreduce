defmodule MapTask do
  defstruct status: :idle, id: 0, filename: ""
end

defmodule ReduceTask do
  defstruct status: :idle, id: 0, reduce_idx: 0
end

defmodule Coordinator do
  require Logger
  use GenServer

  @worker_timeout 5000

  defstruct tasks: [], reduce_num: 0, next_id: 1

  def start_link(name, files) do
    GenServer.start_link(__MODULE__, files, name: name)
  end

  @impl true
  def init(files) do
    coordinator = %Coordinator{}
    coordinator = with_map_tasks(coordinator, files)
    coordinator = %{coordinator | reduce_num: length(files)}
    Logger.info("Coordinator: Initialized")
    {:ok, coordinator}
  end

  @impl true
  def handle_call(:get_task, _from, coordinator) do
    idle_map = Enum.find(coordinator.tasks, &(is_map_task(&1) and &1.status == :idle))
    idle_reduce = Enum.find(coordinator.tasks, &(is_reduce_task(&1) and &1.status == :idle))

    reply =
      cond do
        idle_map != nil -> {:map, idle_map.id, idle_map.filename}
        idle_reduce != nil -> {:reduce, idle_reduce.id, idle_reduce.reduce_idx}
        true -> {:no_task}
      end

    next_state =
      cond do
        idle_map != nil ->
          %{
            coordinator
            | tasks: set_task_status(coordinator.tasks, idle_map.id, :progress)
          }

        idle_reduce != nil ->
          %{
            coordinator
            | tasks: set_task_status(coordinator.tasks, idle_reduce.id, :progress)
          }

        true ->
          coordinator
      end

    case reply do
      {:map, task_id, _} ->
        Process.send_after(self(), {:check_task_completed, task_id}, @worker_timeout)

      {:reduce, task_id, _} ->
        Process.send_after(self(), {:check_task_completed, task_id}, @worker_timeout)

      _ ->
        nil
    end

    Logger.info("Coordinator: Giving task to worker. Replying #{inspect(reply)}")
    {:reply, reply, next_state}
  end

  @impl true
  def handle_call({:complete_task, task_id}, _from, coordinator) do
    Logger.info("Coordinator: Completed task (id=#{task_id})")
    completed_task = Enum.find(coordinator.tasks, &(task_id == &1.id))

    coordinator = %{
      coordinator
      | tasks: set_task_status(coordinator.tasks, task_id, :complete)
    }

    all_maps_complete =
      Enum.filter(coordinator.tasks, &is_map_task(&1))
      |> Enum.all?(&(&1.status == :complete))

    coordinator =
      if is_map_task(completed_task) && all_maps_complete do
        Logger.info("Coordinator: All map tasks completed, added reduce tasks")
        with_reduce_tasks(coordinator)
      else
        coordinator
      end

    {:reply, :ok, coordinator}
  end

  @impl true
  def handle_call(:done?, _from, coordinator) do
    done? = Enum.all?(coordinator.tasks, &(&1.status == :complete))

    if done? do
      Logger.info("Coordinator: Work is done, scheduling a stop signal")
      Process.send_after(self(), :stop, @worker_timeout)
    end

    {:reply, done?, coordinator}
  end

  @impl true
  def handle_info({:check_task_completed, task_id}, coordinator) do
    to_check = Enum.find(coordinator.tasks, &(&1.id == task_id))

    coordinator =
      if to_check.status != :complete do
        Logger.info(
          "Coordinator: Worker didn't complete task (id=#{task_id}). Setting status to idle"
        )

        %{
          coordinator
          | tasks: set_task_status(coordinator.tasks, task_id, :idle)
        }
      else
        coordinator
      end

    {:noreply, coordinator}
  end

  @impl true
  def handle_info(:stop, coordinator) do
    Logger.info("Coordinator: Recieved handle_info(:stop), stopping")
    {:stop, :shutdown, coordinator}
  end

  def get_task(server) do
    GenServer.call(server, :get_task)
  end

  def complete_task(server, task_id) do
    GenServer.call(server, {:complete_task, task_id})
  end

  def done?(server) do
    GenServer.call(server, :done?)
  end

  defp with_map_tasks(coordinator, files) do
    map_tasks =
      Enum.map(Enum.with_index(files), fn {file, idx} ->
        %MapTask{
          status: :idle,
          id: coordinator.next_id + idx,
          filename: file
        }
      end)

    %{
      coordinator
      | tasks: coordinator.tasks ++ map_tasks,
        next_id: coordinator.next_id + length(map_tasks)
    }
  end

  defp with_reduce_tasks(coordinator) do
    reduce_tasks =
      Enum.map(1..coordinator.reduce_num, fn num ->
        %ReduceTask{
          status: :idle,
          id: coordinator.next_id + num - 1,
          reduce_idx: num
        }
      end)

    %{
      coordinator
      | tasks: coordinator.tasks ++ reduce_tasks,
        next_id: coordinator.next_id + coordinator.reduce_num
    }
  end

  defp is_map_task(%MapTask{}), do: true
  defp is_map_task(_), do: false

  defp is_reduce_task(%ReduceTask{}), do: true
  defp is_reduce_task(_), do: false

  defp set_task_status(tasks, task_id, new_status) do
    Enum.map(tasks, fn task ->
      case task do
        %MapTask{id: ^task_id} -> %MapTask{task | status: new_status}
        %ReduceTask{id: ^task_id} -> %ReduceTask{task | status: new_status}
        _ -> task
      end
    end)
  end
end
