defmodule MapreduceTest do
  use ExUnit.Case
  doctest Mapreduce

  test "greets the world" do
    assert Mapreduce.hello() == :world
  end
end
