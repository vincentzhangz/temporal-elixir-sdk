defmodule Temporal.RPCTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Workflowservice.V1.GetSystemInfoResponse
  alias Temporal.Api.Workflowservice.V1.GetSystemInfoResponse.Capabilities

  defmodule WireTransport do
    @behaviour Temporal.RPC.Transport

    @impl true
    def unary(test_pid, method, request, options) do
      send(test_pid, {:unary, method, request, options})

      response =
        GetSystemInfoResponse.encode(%GetSystemInfoResponse{
          server_version: "1.63.3",
          capabilities: %Capabilities{signal_and_query_header: true}
        })

      {:ok, response}
    end
  end

  test "system_info sends official protobuf bytes through the configured transport" do
    assert {:ok, connection} =
             Temporal.Connection.open(transport: WireTransport, transport_state: self())

    on_exit(fn ->
      DynamicSupervisor.terminate_child(Temporal.ConnectionSupervisor, connection)
    end)

    assert {:ok, %GetSystemInfoResponse{} = response} =
             Temporal.RPC.system_info(connection, timeout: 1_000)

    assert response.server_version == "1.63.3"
    assert response.capabilities.signal_and_query_header

    assert_receive {:unary, "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
                    <<>>, [timeout: 1_000]}
  end
end
