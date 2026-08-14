defmodule Temporal.ApplicationTest do
  use ExUnit.Case, async: true

  test "supervises connections independently" do
    assert Process.alive?(Process.whereis(Temporal.ConnectionSupervisor))
  end
end
