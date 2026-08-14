defmodule Temporal.ClientTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Workflowservice.V1.{
    StartWorkflowExecutionRequest,
    StartWorkflowExecutionResponse
  }

  defmodule ClientTransport do
    @behaviour Temporal.RPC.Transport

    def unary(test_pid, method, bytes, options) do
      send(test_pid, {method, StartWorkflowExecutionRequest.decode(bytes), options})

      {:ok,
       StartWorkflowExecutionResponse.encode(%StartWorkflowExecutionResponse{run_id: "run-1"})}
    end
  end

  test "starts a named workflow with an honest JSON payload" do
    {:ok, connection} =
      Temporal.Connection.open(
        transport: ClientTransport,
        transport_state: self(),
        namespace: "payments",
        identity: "client-test"
      )

    on_exit(fn -> Temporal.Connection.close(connection) end)

    assert {:ok, handle} =
             Temporal.Client.start_workflow(connection, "Greeting", "Temporal",
               id: "workflow-1",
               task_queue: "elixir"
             )

    assert handle.run_id == "run-1"

    assert_receive {"/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
                    request, _options}

    assert request.namespace == "payments"
    assert request.identity == "client-test"
    assert request.workflow_id == "workflow-1"
    assert request.workflow_type.name == "Greeting"
    assert {:ok, "Temporal"} = Temporal.Payload.decode(request.input)
  end
end
