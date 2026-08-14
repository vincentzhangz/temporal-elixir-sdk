defmodule Temporal.ClientSignalTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{Header, Payload}

  alias Temporal.Api.Workflowservice.V1.{
    SignalWithStartWorkflowExecutionRequest,
    SignalWithStartWorkflowExecutionResponse,
    SignalWorkflowExecutionRequest,
    SignalWorkflowExecutionResponse
  }

  defmodule Transport do
    @behaviour Temporal.RPC.Transport

    def unary(test_pid, method, bytes, options) do
      {request_module, response} =
        case method do
          "/temporal.api.workflowservice.v1.WorkflowService/SignalWorkflowExecution" ->
            {SignalWorkflowExecutionRequest, %SignalWorkflowExecutionResponse{}}

          "/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution" ->
            {SignalWithStartWorkflowExecutionRequest,
             %SignalWithStartWorkflowExecutionResponse{run_id: "started-run"}}
        end

      send(test_pid, {method, request_module.decode(bytes), options})
      {:ok, response.__struct__.encode(response)}
    end
  end

  setup do
    {:ok, connection} =
      Temporal.Connection.open(
        transport: Transport,
        transport_state: self(),
        namespace: "payments",
        identity: "signal-client"
      )

    on_exit(fn -> Temporal.Connection.close(connection) end)
    %{connection: connection}
  end

  test "signals a handle with stable targeting headers and request ID", %{connection: connection} do
    handle = %Temporal.Client.Handle{
      connection: connection,
      namespace: "payments",
      workflow_id: "account-1",
      run_id: "run-1"
    }

    header = %Header{fields: %{"trace" => %Payload{data: "abc"}}}

    assert :ok =
             Temporal.Client.signal_workflow(handle, "deposit", 42,
               request_id: "signal-1",
               header: header,
               timeout: 1_000
             )

    assert_receive {"/temporal.api.workflowservice.v1.WorkflowService/SignalWorkflowExecution",
                    request, options}

    assert request.namespace == "payments"
    assert request.identity == "signal-client"
    assert request.workflow_execution.workflow_id == "account-1"
    assert request.workflow_execution.run_id == "run-1"
    assert request.signal_name == "deposit"
    assert request.request_id == "signal-1"
    assert request.header == header
    assert {:ok, 42} = Temporal.Payload.decode(request.input)
    assert options[:timeout] == 1_000
  end

  test "signals the latest run by workflow ID", %{connection: connection} do
    assert :ok =
             Temporal.Client.signal_workflow(connection, "account-1", "deposit", 7,
               request_id: "signal-2"
             )

    assert_receive {"/temporal.api.workflowservice.v1.WorkflowService/SignalWorkflowExecution",
                    request, _options}

    assert request.workflow_execution == %Temporal.Api.Common.V1.WorkflowExecution{
             workflow_id: "account-1",
             run_id: ""
           }
  end

  test "atomically signals with start and returns a run-bound handle", %{connection: connection} do
    assert {:ok, handle} =
             Temporal.Client.signal_with_start(
               connection,
               "AccountWorkflow",
               %{balance: 0},
               "deposit",
               42,
               id: "account-1",
               task_queue: "accounts",
               request_id: "sws-1",
               workflow_id_conflict_policy: :WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
             )

    assert handle.workflow_id == "account-1"
    assert handle.run_id == "started-run"

    assert_receive {"/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution",
                    request, _options}

    assert request.request_id == "sws-1"

    assert request.workflow_id_conflict_policy ==
             :WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING

    assert {:ok, %{"balance" => 0}} = Temporal.Payload.decode(request.input)
    assert {:ok, 42} = Temporal.Payload.decode(request.signal_input)
  end

  test "returns structured validation errors without making an RPC", %{connection: connection} do
    assert {:error, {:invalid_option, :signal_name, ""}} =
             Temporal.Client.signal_workflow(connection, "account-1", "", nil)

    refute_receive {_method, _request, _options}
  end
end
