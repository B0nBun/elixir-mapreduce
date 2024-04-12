# TODO: Remove intermediate files
defmodule Worker do
  require Logger

  @type mapf :: (String.t(), File.io_device() -> list({String.t(), String.t()}))
  @type reducef :: (String.t(), list(String.t()) -> String.t())

  @spec start_link(GenServer.name(), mapf(), reducef()) :: {:ok, pid()}
  def start_link(coordinator_server, map, reduce) do
    Logger.info("Worker: Spawned link")
    pid = spawn_link(fn -> loop(coordinator_server, map, reduce) end)
    {:ok, pid}
  end

  @spec loop(GenServer.name(), mapf(), reducef()) :: :ok
  def loop(server, map, reduce) do
    Logger.info("Worker: probing coordinator for tasks")
    reply = Coordinator.get_task(server)

    case reply do
      {:map, task_id, filename, reduce_num} ->
        handle_map(server, map, task_id, filename, reduce_num)

      {:reduce, task_id, reduce_idx} ->
        handle_reduce(server, reduce, task_id, reduce_idx)

      {:no_task} ->
        :timer.sleep(1000)
    end

    if Coordinator.done?(server) do
      Logger.info("Worker: Recieved done?=true, exiting")
      :ok
    else
      loop(server, map, reduce)
    end
  end

  @spec handle_map(GenServer.name(), mapf(), Coordinator.task_id(), String.t(), integer()) :: :ok
  defp handle_map(server, map, task_id, filename, reduce_num) do
    Logger.info("Worker: Handling map task (id=#{task_id})")
    file = File.open!(filename)
    map_pairs = map.(filename, file)
    buckets = split_into_buckets(map_pairs, reduce_num)

    buckets
    |> Enum.each(fn {reduce_idx, pairs} ->
      outfile = file_for_map_result(reduce_idx, task_id)
      write_pairs_to_file(pairs, outfile)
    end)

    Logger.info("Worker: Completed map task (id=#{task_id})")
    Coordinator.complete_task(server, task_id)
  end

  @spec handle_reduce(GenServer.name(), reducef(), Coordinator.task_id(), integer()) :: :ok
  defp handle_reduce(server, reduce, task_id, reduce_idx) do
    Logger.info("Worker: Handling reduce task (id=#{task_id})")
    files = files_with_map_results(reduce_idx)

    # TODO: This can be optimized by sorting by key
    pairs =
      files
      |> Enum.map(&read_pairs_from_file!(&1))
      |> flatten_pairs()

    outfile = "mr-out-#{reduce_idx}"
    File.touch!(outfile)
    {:ok, file} = File.open(outfile, [:write])

    pairs
    |> Enum.map(fn {key, values} -> {key, reduce.(key, values)}end)
    |> write_result_to_file(file)

    File.close(file)

    Logger.info("Worker: Completed reduce task (id=#{task_id})")
    Coordinator.complete_task(server, task_id)
  end

  @spec write_result_to_file(list({String.t(), String.t()}), File.io_device()) :: :ok
  def write_result_to_file(pairs, file) do
    Enum.each(pairs, fn {key, value} ->
      IO.binwrite(file, "#{key} #{value}\n")
    end)
    :ok
  end

  @spec split_into_buckets(list({String.t(), String.t()}), integer()) :: %{
          integer() => list({String.t(), String.t()})
        }
  defp split_into_buckets(pairs, reduce_num) do
    buckets =
      List.duplicate([], reduce_num)
      |> Enum.with_index(1)
      |> Enum.map(fn {v, k} -> {k, v} end)
      |> Map.new()

    buckets =
      Enum.reduce(pairs, buckets, fn {key, val}, buckets ->
        reduce_idx = rem(bucket_hash(key), reduce_num) + 1
        Map.put(buckets, reduce_idx, [{key, val} | buckets[reduce_idx]])
      end)

    buckets
  end

  @spec bucket_hash(String.t()) :: integer()
  defp bucket_hash(key) do
    bits = :crypto.hash(:md5, key)
    <<number::32, _::binary>> = bits
    number
  end

  @spec file_for_map_result(integer(), Coordinator.task_id()) :: String.t()
  defp file_for_map_result(reduce_idx, task_id) do
    "mr-map-inter-#{reduce_idx}-#{task_id}"
  end

  @spec files_with_map_results(Coordinator.task_id()) :: String.t()
  defp files_with_map_results(reduce_idx) do
    Path.wildcard("mr-map-inter-#{reduce_idx}-*")
  end

  @spec write_pairs_to_file(list({String.t(), String.t()}), String.t()) ::
          :ok | {:error, File.posix()}
  defp write_pairs_to_file(pairs, file) do
    binary = :erlang.term_to_binary(pairs)
    File.write(file, binary)
  end

  @spec read_pairs_from_file!(String.t()) :: list({String.t(), String.t()})
  defp read_pairs_from_file!(file) do
    File.read!(file) |> :erlang.binary_to_term()
  end

  @spec flatten_pairs(list(list({String.t(), String.t()}))) ::
          list({String.t(), list(String.t())})
  defp flatten_pairs(lists_of_pairs) do
    lists_of_pairs
    |> Enum.flat_map(& &1)
    |> Enum.reduce(%{}, fn {k, v}, map ->
      current = map[k] || []
      Map.put(map, k, [v | current])
    end)
    |> Enum.map(& &1)
  end
end
