defmodule Temporal.ClientOperationsTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

  alias Temporal.Api.Workflowservice.V1.{
    DescribeWorkflowExecutionRequest,
    DescribeWorkflowExecutionResponse,
    ListWorkflowExecutionsRequest,
    ListWorkflowExecutionsResponse,
    TerminateWorkflowExecutionRequest,
    TerminateWorkflowExecutionResponse
  }

  alias Temporal.Api.Workflow.V1.WorkflowExecutionInfo

  defmodule Transport do
    @behaviour Temporal.RPC.Transport

    def unary(test_pid, method, bytes, options) do
      {request_module, response} =
        case method do
          "/temporal.api.workflowservice.v1.WorkflowService/TerminateWorkflowExecution" ->
            {TerminateWorkflowExecutionRequest, %TerminateWorkflowExecutionResponse{}}

          "/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkflowExecution" ->
            {DescribeWorkflowExecutionRequest,
             %DescribeWorkflowExecutionResponse{
               workflow_execution_info: %WorkflowExecutionInfo{
                 execution: %WorkflowExecution{
                   workflow_id: "account-1",
                   run_id: "run-1"
                 },
                 type: %WorkflowType{name: "Greeting"},
                 status: :WORKFLOW_EXECUTION_STATUS_RUNNING
               }
             }}

          "/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions" ->
            {ListWorkflowExecutionsRequest,
             %ListWorkflowExecutionsResponse{
               executions: [
                 %WorkflowExecutionInfo{
                   execution: %WorkflowExecution{
                     workflow_id: "account-1",
                     run_id: "run-1"
                   },
                   type: %WorkflowType{name: "Greeting"}
                 }
               ],
               next_page_token: "page-2"
             }}
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
        identity: "ops-client"
      )

    on_exit(fn -> Temporal.Connection.close(connection) end)

    %{
      connection: connection,
      handle: %Temporal.Client.Handle{
        connection: connection,
        namespace: "payments",
        workflow_id: "account-1",
        run_id: "run-1"
      }
    }
  end

  test "terminates a handle with the configured reason and identity", %{handle: handle} do
    assert :ok = Temporal.Client.terminate_workflow(handle, reason: "audit", details: "x")

    assert_receive {"/temporal.api.workflowservice.v1.WorkflowService/TerminateWorkflowExecution",
                    request, _options}

    assert request.namespace == "payments"
    assert request.identity == "ops-client"
    assert request.workflow_execution.workflow_id == "account-1"
    assert request.workflow_execution.run_id == "run-1"
    assert request.reason == "audit"
    assert {:ok, "x"} = Temporal.Payload.decode(request.details)
    assert request.first_execution_run_id == "run-1"
  end

  test "terminates by ID with default reason", %{connection: connection} do
    assert :ok = Temporal.Client.terminate_workflow(connection, "account-2")

    assert_receive {"/temporal.api.workflowservice.v1.WorkflowService/TerminateWorkflowExecution",
                    request, _options}

    assert request.workflow_execution.workflow_id == "account-2"
    assert request.workflow_execution.run_id == ""
    assert request.reason == "terminated by client"
  end

  test "describes a handle and exposes the execution info", %{handle: handle} do
    assert {:ok, %DescribeWorkflowExecutionResponse{} = response} =
             Temporal.Client.describe_workflow(handle)

    assert response.workflow_execution_info.execution.workflow_id == "account-1"
    assert response.workflow_execution_info.type.name == "Greeting"
    assert response.workflow_execution_info.status == :WORKFLOW_EXECUTION_STATUS_RUNNING
  end

  test "lists workflows with a query and page token", %{connection: connection} do
    assert {:ok, %{executions: [execution], next_page_token: "page-2"}} =
             Temporal.Client.list_workflows(connection, "WorkflowType='Greeting'", page_size: 10)

    assert execution.execution.workflow_id == "account-1"
    assert execution.type.name == "Greeting"
  end
end
