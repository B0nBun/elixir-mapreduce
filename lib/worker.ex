# TODO: Actually do the work

defmodule Worker do
  require Logger

  def start_link(coordinator_server, map, reduce) do
    Logger.info("Worker: Spawned link")
    pid = spawn_link(fn -> loop(coordinator_server, map, reduce) end)
    {:ok, pid}
  end

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
    else
      loop(server, map, reduce)
    end
  end

  defp handle_map(server, map, task_id, filename, reduce_num) do
    Logger.info("Worker: Handling map task (id=#{task_id})")
    stream = File.stream!(filename)
    map_pairs = map.(filename, stream)
    buckets = split_into_buckets(map_pairs, reduce_num)

    buckets
    |> Enum.each(fn {reduce_idx, pairs} ->
      outfile = file_for_map_result(reduce_idx, task_id)
      write_pairs_to_file(pairs, outfile)
    end)

    Logger.info("Worker: Completed map task (id=#{task_id})")
    Coordinator.complete_task(server, task_id)
  end

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

    Enum.each(pairs, fn {key, values} ->
      output = reduce.(key, values)
      IO.binwrite(file, "#{key} #{output}\n")
    end)

    File.close(file)

    Logger.info("Worker: Completed reduce task (id=#{task_id})")
    Coordinator.complete_task(server, task_id)
  end

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

  defp bucket_hash(key) do
    bits = :crypto.hash(:md5, key)
    <<number::32, _::binary>> = bits
    number
  end

  defp file_for_map_result(reduce_idx, task_id) do
    "mr-map-inter-#{reduce_idx}-#{task_id}"
  end

  defp files_with_map_results(reduce_idx) do
    Path.wildcard("mr-map-inter-#{reduce_idx}-*")
  end

  defp write_pairs_to_file(pairs, file) do
    binary = :erlang.term_to_binary(pairs)
    File.write(file, binary)
  end

  defp read_pairs_from_file!(file) do
    # TODO: Maybe can read with File.stream
    File.read!(file) |> :erlang.binary_to_term()
  end

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
