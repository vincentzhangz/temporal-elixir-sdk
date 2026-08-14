defmodule Temporal.CancellationTest do
  use ExUnit.Case, async: true

  defmodule BlockingTransport do
    @behaviour Temporal.RPC.Transport

    def unary(test_pid, _method, _request, _options) do
      send(test_pid, {:transport_started, self()})
      receive do: (:never -> {:ok, ""})
    end
  end

  test "deadline cancels the in-flight transport process" do
    {:ok, connection} =
      Temporal.Connection.open(transport: BlockingTransport, transport_state: self())

    assert {:error, %Temporal.RPC.Error{status: :deadline_exceeded}} =
             Temporal.Connection.unary(connection, "/blocked", "", timeout: 10)

    assert_receive {:transport_started, transport_pid}
    monitor = Process.monitor(transport_pid)
    assert_receive {:DOWN, ^monitor, :process, ^transport_pid, _reason}
    Temporal.Connection.close(connection)
  end

  test "caller death cancels the in-flight transport process" do
    parent = self()

    {:ok, connection} =
      Temporal.Connection.open(transport: BlockingTransport, transport_state: parent)

    caller =
      spawn(fn -> Temporal.Connection.unary(connection, "/blocked", "", timeout: :infinity) end)

    assert_receive {:transport_started, transport_pid}
    monitor = Process.monitor(transport_pid)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^transport_pid, _reason}
    Temporal.Connection.close(connection)
  end
end
