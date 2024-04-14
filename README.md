> As description suggests, this is a very unfinished implementation of MapReduce, written in Elixir purely for educational purposes.

The idea to implement this came from the [MIT course on distributed systems](https://pdos.csail.mit.edu/6.824/)

### Basic idea of MapReduce

MapReduce is a system in which multiple machines (one coordinator and several workers) concurrently process large amounts of data using two "callback" functions: Map and Reduce.

Examples of such "apps" or "callbacks" can be found in `/mrapps` directory.

```elixir
# Word-counter example
defmodule Mapreduce.Callbacks do
  # Map is called for each specified file and returns a list of key-value pairs
  @spec map(String.t(), File.io_device()) :: [{String.t(), String.t()}]
  def map(_filename, stream) do
    stream
    |> Enum.flat_map(&String.split(&1))
    |> Enum.map(fn w -> {w, "1"} end)
  end

  # Reduce is called for each key in the "mapped" result with
  # key and values which were produced by all of the map functions
  @spec reduce(String.t(), [String.t()]) :: String.t()
  def reduce(_key, values) do
    values
    |> length()
    |> Integer.to_string()
  end
end
```

As a result, mapreduce produces an unordered list of key-value pairs. This implementation outputs them as `mr-out-*` files.

### Usage of `run-mr.sh`

`run-mr.sh` is a script which runs the coordinator and all of the worker nodes locally *(The script requires `GNU parallel` to work)*.

```sh
# Run a MapReduce word-counting example
FILES_GLOB='texts/*' MR_APP_FILE=mrapps/word_count.exs WORKER_N=3 bash run-mr.sh
# A lot of output and logs
# ...
# Application mapreduce exited: shutdown
# (Ctrl+C to exit the process)

sort -k2 -n mr-out* | tail
# to 15696
# and 23052
# the 29610
```

- `MR_APP_FILE` - Specifies the mrapp (map and reduce functions) that should be used
- `WORKER_N` - number of workers

### Runing with elixir

Elixir/Mix doesn't provide a good way of dealing with cli arguments, so all parameters are passed through environment variables

```sh
# Running a coordinator
MR_FILES_GLOB='texts/*' MR_TYPE=coordinator elixir --name coordinator@0.0.0.0 --cookie cookie --no-halt -S mix

# Running a worker
MR_APP_FILE=mrapps/word_count.exs MR_TYPE=worker MR_COORD_NAME=coordinator@0.0.0.0 elixir --name worker1@0.0.0.0 --cookie cookie --no-halt -S mix
```

In theory, those processes all can be ran on different machines and communicate through a magic of erlang by themselves.
