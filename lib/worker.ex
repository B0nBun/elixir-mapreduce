defmodule Worker do
  require Logger

  @type mapf :: (String.t(), File.io_device() -> [{String.t(), String.t()}])
  @type reducef :: (String.t(), [String.t()] -> String.t())

  @spec start_link(Node.t(), GenServer.name(), mapf(), reducef()) :: {:ok, pid()}
  def start_link(node, coordinator_server, map, reduce) do
    Logger.info("Worker: Spawned link")
    pid = spawn_link(fn -> loop(node, coordinator_server, map, reduce) end)
    {:ok, pid}
  end

  @spec loop(Node.t(), GenServer.name(), mapf(), reducef()) :: :ok
  def loop(node, server, map, reduce) do
    Logger.info("Worker: probing coordinator for tasks")
    reply = :rpc.call(node, Coordinator, :get_task, [server])

    case reply do
      {:map, task_id, filename, reduce_num} ->
        handle_map(node, server, map, task_id, filename, reduce_num)

      {:reduce, task_id, reduce_idx} ->
        handle_reduce(node, server, reduce, task_id, reduce_idx)

      {:no_task} ->
        :timer.sleep(1000)
    end

    if :rpc.call(node, Coordinator, :done?, [server]) do
      Logger.info("Worker: Recieved done?=true, exiting")
      :ok
    else
      loop(node, server, map, reduce)
    end
  end

  @spec handle_map(Node.t(), GenServer.name(), mapf(), Coordinator.task_id(), String.t(), integer()) :: :ok
  defp handle_map(node, server, map, task_id, filename, reduce_num) do
    Logger.info("Worker: Handling map task (id=#{task_id})")
    file = :rpc.call(node, Coordinator, :file_open, [server, filename, [:read]])
    map_pairs = map.(filename, IO.stream(file, :line))
    :rpc.call(node, Coordinator, :file_close, [server, file])
    buckets = split_into_buckets(map_pairs, reduce_num)

    buckets
    |> Enum.each(fn {reduce_idx, pairs} ->
      filename = Coordinator.filename_for_map_result(reduce_idx, task_id)
      file = :rpc.call(node, Coordinator, :file_open, [server, filename, [:write]])
      write_pairs(pairs, file)
      :rpc.call(node, Coordinator, :file_close, [server, file])
    end)

    Logger.info("Worker: Completed map task (id=#{task_id})")
    :rpc.call(node, Coordinator, :complete_task, [server, task_id])
  end

  @spec handle_reduce(Node.t(), GenServer.name(), reducef(), Coordinator.task_id(), integer()) :: :ok
  defp handle_reduce(node, server, reduce, task_id, reduce_idx) do
    Logger.info("Worker: Handling reduce task (id=#{task_id})")
    filenames = :rpc.call(node, Coordinator, :filenames_with_map_results, [server, reduce_idx])

    # TODO: This can be optimized by sorting by key
    pairs =
      filenames
      |> Enum.map(fn filename ->
        file = :rpc.call(node, Coordinator, :file_open, [server, filename, [:read]])
        pairs = read_pairs(file)
        :rpc.call(node, Coordinator, :file_close, [server, file])
        pairs
      end)
      |> flatten_pairs()

    outfile = Coordinator.file_for_output(reduce_idx)
    file = :rpc.call(node, Coordinator, :file_open, [server, outfile, [:write]])

    pairs
    |> Enum.map(fn {key, values} -> {key, reduce.(key, values)}end)
    |> write_result_to_file(file)

    :rpc.call(node, Coordinator, :file_close, [server, file])

    Logger.info("Worker: Completed reduce task (id=#{task_id})")
    :rpc.call(node, Coordinator, :complete_task, [server, task_id])
  end

  @spec write_result_to_file([{String.t(), String.t()}], File.io_device()) :: :ok
  def write_result_to_file(pairs, file) do
    Enum.each(pairs, fn {key, value} ->
      IO.binwrite(file, "#{key} #{value}\n")
    end)
    :ok
  end

  @spec split_into_buckets([{String.t(), String.t()}], integer()) :: %{
          integer() => [{String.t(), String.t()}]
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

  @spec write_pairs([{String.t(), String.t()}], File.io_device()) :: :ok
  def write_pairs(pairs, file) do
    binary = :erlang.term_to_binary(pairs)
    IO.binwrite(file, binary)
  end

  @spec read_pairs(File.io_device()) :: [{String.t(), String.t()}]
  def read_pairs(file) do
    IO.binread(file, :all) |> :erlang.binary_to_term()
  end

  @spec flatten_pairs([[{String.t(), String.t()}]]) ::
          [{String.t(), [String.t()]}]
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
