defmodule MapTask do
  defstruct status: :idle, id: 0, filename: ""

  @type status :: Coordinator.task_status()
  @type t :: %MapTask{
          status: Coordinator.task_status(),
          id: Coordinator.task_id(),
          filename: String.t()
        }
end

defmodule ReduceTask do
  defstruct status: :idle, id: 0, reduce_idx: 0

  @type status :: Coordinator.task_status()
  @type t :: %ReduceTask{
          status: Coordinator.task_status(),
          id: Coordinator.task_id(),
          reduce_idx: integer()
        }
end

defmodule Coordinator do
  require Logger
  use GenServer

  @worker_timeout 5000

  defstruct tasks: [], reduce_num: 0, next_id: 1

  @type task_id() :: integer()
  @type task_status() :: :idle | :progress | :complete
  @type task() :: ReduceTask.t() | MapTask.t()
  @type t() :: %Coordinator{tasks: list(task()), reduce_num: integer(), next_id: task_id()}

  @spec start_link(GenServer.name(), list(String.t())) :: GenServer.on_start()
  def start_link(name, files) do
    GenServer.start_link(__MODULE__, files, name: name)
  end

  @impl true
  @spec init(String.t()) :: {:ok, %Coordinator{}}
  def init(files) do
    coordinator = %Coordinator{}
    coordinator = with_map_tasks(coordinator, files)
    coordinator = %{coordinator | reduce_num: length(files)}
    Logger.info("Coordinator: Initialized")
    {:ok, coordinator}
  end

  @type get_task_reply() ::
          {:map, task_id(), String.t(), integer()}
          | {:reduce, task_id(), integer()}
          | {:no_task}

  @impl true
  @spec handle_call(:get_task, any(), Coordinator.t()) ::
          {:reply, get_task_reply(), Coordinator.t()}
  def handle_call(:get_task, _from, coordinator) do
    idle_map = Enum.find(coordinator.tasks, &(is_map_task(&1) and &1.status == :idle))
    idle_reduce = Enum.find(coordinator.tasks, &(is_reduce_task(&1) and &1.status == :idle))

    reply =
      cond do
        idle_map != nil -> {:map, idle_map.id, idle_map.filename, coordinator.reduce_num}
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
  @spec handle_call({:get_task, task_id()}, any(), Coordinator.t()) ::
          {:reply, :ok, Coordinator.t()}
  def handle_call({:complete_task, task_id}, _from, coordinator) do
    Logger.info("Coordinator: Completed task (id=#{task_id})")
    completed_task = Enum.find(coordinator.tasks, &(task_id == &1.id))

    coordinator = %{
      coordinator
      | tasks: set_task_status(coordinator.tasks, task_id, :complete)
    }

    all_maps_complete =
      coordinator.tasks
      |> Enum.filter(&is_map_task(&1))
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
  @spec handle_call(:done?, any(), Coordinator.t()) :: {:reply, boolean(), Coordinator.t()}
  def handle_call(:done?, _from, coordinator) do
    done? = Enum.all?(coordinator.tasks, &(&1.status == :complete))

    if done? do
      Logger.info("Coordinator: Work is done, scheduling a stop signal")
      Process.send_after(self(), :stop, @worker_timeout)
    end

    {:reply, done?, coordinator}
  end

  @impl true
  @spec handle_call({:file_open, String.t(), list(File.mode())}, any(), Coordinator.t()) :: {:reply, File.io_device(), Coordinator.t()}
  def handle_call({:file_open, filename, modes}, _from, coordinator) do
    file = File.open!(filename, modes)
    {:reply, file, coordinator}
  end

  @impl true
  @spec handle_call({:file_close, File.io_device()}, any(), Coordinator.t()) :: {:reply, :ok, Coordinator.t()}
  def handle_call({:file_close, file}, _from, coordinator) do
    File.close(file)
    {:reply, :ok, coordinator}
  end

  @impl true
  @spec handle_call({:map_results_filenames, integer()}, any(), Coordinator.t()) :: {:reply, list(String.t()), Coordinator.t()}
  def handle_call({:map_results_filenames, reduce_idx}, _from, coordinator) do
    {:reply, Path.wildcard("mr-map-inter-#{reduce_idx}-*"), coordinator}
  end

  @impl true
  @spec handle_info({:check_task_completed, task_id()}, Coordinator.t()) ::
          {:noreply, Coordinator.t()}
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
  @spec handle_info(:stop, Coordinator.t()) :: {:stop, :shutdown, Coordinator.t()}
  def handle_info(:stop, coordinator) do
    Logger.info("Coordinator: Recieved handle_info(:stop), stopping")
    {:stop, :shutdown, coordinator}
  end

  @spec get_task(GenServer.name()) :: get_task_reply()
  def get_task(server) do
    GenServer.call(server, :get_task)
  end

  @spec complete_task(GenServer.name(), task_id()) :: :ok
  def complete_task(server, task_id) do
    GenServer.call(server, {:complete_task, task_id})
  end

  @spec done?(GenServer.name()) :: boolean()
  def done?(server) do
    GenServer.call(server, :done?)
  end

  @spec file_open(GenServer.name(), String.t(), list(File.mode())) :: File.io_device()
  def file_open(server, filename, modes) do
    GenServer.call(server, {:file_open, filename, modes})
  end

  @spec file_close(GenServer.name(), File.io_device()) :: :ok
  def file_close(server, file) do
    GenServer.call(server, {:file_close, file})
  end

  @spec filenames_with_map_results(GenServer.name(), integer()) :: String.t()
  def filenames_with_map_results(server, reduce_idx) do
    GenServer.call(server, {:map_results_filenames, reduce_idx})
  end

  @spec filename_for_map_result(integer(), Coordinator.task_id()) :: String.t()
  def filename_for_map_result(reduce_idx, task_id) do
    "mr-map-inter-#{reduce_idx}-#{task_id}"
  end

  @spec file_for_output(integer()) :: String.t()
  def file_for_output(reduce_idx) do
    "mr-out-#{reduce_idx}"
  end

  @spec with_map_tasks(Coordinator.t(), list(String.t())) :: Coordinator.t()
  defp with_map_tasks(coordinator, files) do
    map_tasks =
      files
      |> Enum.with_index(coordinator.next_id)
      |> Enum.map(fn {file, id} ->
        %MapTask{
          status: :idle,
          id: id,
          filename: file
        }
      end)

    %{
      coordinator
      | tasks: coordinator.tasks ++ map_tasks,
        next_id: coordinator.next_id + length(map_tasks)
    }
  end

  @spec with_reduce_tasks(Coordinator.t()) :: Coordinator.t()
  defp with_reduce_tasks(coordinator) do
    reduce_tasks =
      1..coordinator.reduce_num
      |> Enum.map(fn num ->
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

  @spec is_map_task(task()) :: boolean
  defp is_map_task(%MapTask{}), do: true
  defp is_map_task(_), do: false

  @spec is_reduce_task(task()) :: boolean
  defp is_reduce_task(%ReduceTask{}), do: true
  defp is_reduce_task(_), do: false

  @spec set_task_status(list(task()), task_id(), task_status()) :: list(task())
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
