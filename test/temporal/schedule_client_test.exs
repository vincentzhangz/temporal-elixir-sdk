defmodule Temporal.ScheduleClientTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Workflowservice.V1.{
    CreateScheduleRequest,
    CreateScheduleResponse,
    DeleteScheduleRequest,
    DeleteScheduleResponse,
    DescribeScheduleRequest,
    DescribeScheduleResponse,
    ExecuteMultiOperationRequest,
    ExecuteMultiOperationResponse,
    ListSchedulesRequest,
    ListSchedulesResponse
  }

  defmodule Transport do
    @behaviour Temporal.RPC.Transport

    alias Temporal.Api.Workflowservice.V1.{
      CreateScheduleRequest,
      CreateScheduleResponse,
      DeleteScheduleRequest,
      DeleteScheduleResponse,
      DescribeScheduleRequest,
      DescribeScheduleResponse,
      ExecuteMultiOperationRequest,
      ExecuteMultiOperationResponse,
      ListSchedulesRequest,
      ListSchedulesResponse,
      StartWorkflowExecutionResponse,
      UpdateWorkflowExecutionResponse
    }

    def unary(test_pid, method, bytes, _options) do
      case method do
        "/temporal.api.workflowservice.v1.WorkflowService/CreateSchedule" ->
          send(test_pid, {:create, CreateScheduleRequest.decode(bytes)})

          {:ok, CreateScheduleResponse.encode(%CreateScheduleResponse{})}

        "/temporal.api.workflowservice.v1.WorkflowService/DescribeSchedule" ->
          send(test_pid, {:describe, DescribeScheduleRequest.decode(bytes)})

          {:ok, DescribeScheduleResponse.encode(%DescribeScheduleResponse{})}

        "/temporal.api.workflowservice.v1.WorkflowService/DeleteSchedule" ->
          send(test_pid, {:delete, DeleteScheduleRequest.decode(bytes)})

          {:ok, DeleteScheduleResponse.encode(%DeleteScheduleResponse{})}

        "/temporal.api.workflowservice.v1.WorkflowService/ListSchedules" ->
          send(test_pid, {:list, ListSchedulesRequest.decode(bytes)})

          {:ok, ListSchedulesResponse.encode(%ListSchedulesResponse{})}

        "/temporal.api.workflowservice.v1.WorkflowService/ExecuteMultiOperation" ->
          send(test_pid, {:multi, ExecuteMultiOperationRequest.decode(bytes)})

          {:ok,
           ExecuteMultiOperationResponse.encode(%ExecuteMultiOperationResponse{
             responses: [
               %ExecuteMultiOperationResponse.Response{
                 response:
                   {:start_workflow,
                    %StartWorkflowExecutionResponse{
                      run_id: "run-1"
                    }}
               },
               %ExecuteMultiOperationResponse.Response{
                 response:
                   {:update_workflow,
                    %UpdateWorkflowExecutionResponse{
                      outcome: %Temporal.Api.Update.V1.Outcome{
                        value: {:success, Temporal.Payload.encode(42)}
                      }
                    }}
               }
             ]
           })}
      end
    end
  end

  setup do
    {:ok, connection} =
      Temporal.Connection.open(
        transport: Transport,
        transport_state: self(),
        namespace: "schedules",
        identity: "schedule-client"
      )

    on_exit(fn -> Temporal.Connection.close(connection) end)
    %{connection: connection}
  end

  test "create_schedule builds a cron-schedule request", %{connection: connection} do
    assert {:ok, _} =
             Temporal.ScheduleClient.create_schedule(
               connection,
               "sched-1",
               "Greeting",
               "Temporal",
               task_queue: "elixir",
               cron: "0 * * * *"
             )

    assert_receive {:create, %CreateScheduleRequest{schedule_id: "sched-1", schedule: schedule}}
    assert schedule.spec.cron_string == ["0 * * * *"]
    assert {:start_workflow, %{workflow_type: %{name: "Greeting"}}} = schedule.action.action
    assert {:ok, "Temporal"} = Temporal.Payload.decode(elem(schedule.action.action, 1).input)
  end

  test "describe/delete/list schedules hit the right RPCs", %{connection: connection} do
    assert {:ok, _} = Temporal.ScheduleClient.describe_schedule(connection, "sched-1")
    assert_receive {:describe, %DescribeScheduleRequest{schedule_id: "sched-1"}}

    assert {:ok, _} = Temporal.ScheduleClient.delete_schedule(connection, "sched-1")
    assert_receive {:delete, %DeleteScheduleRequest{schedule_id: "sched-1"}}

    assert {:ok, %{schedules: [], next_page_token: ""}} =
             Temporal.ScheduleClient.list_schedules(connection)

    assert_receive {:list, %ListSchedulesRequest{namespace: "schedules"}}
  end

  test "execute_multi_operation sends a start + update batch", %{connection: connection} do
    assert {:ok, _} =
             Temporal.Client.execute_multi_operation(connection, "Greeting", "Temporal",
               id: "wf-1",
               task_queue: "elixir",
               update: {"add", 1}
             )

    assert_receive {:multi, %ExecuteMultiOperationRequest{} = request}

    assert length(request.operations) == 2

    assert {:start_workflow, %{workflow_id: "wf-1"}} = List.first(request.operations).operation
    assert {:update_workflow, update} = List.last(request.operations).operation
    assert update.request.input.name == "add"
  end

  test "update_with_start returns the update outcome and a run-bound handle", %{
    connection: connection
  } do
    assert {:ok, 42, %Temporal.Client.Handle{run_id: "run-1"} = handle} =
             Temporal.Client.update_with_start(connection, "Greeting", "Temporal", "add", 1,
               id: "wf-1",
               task_queue: "elixir"
             )

    assert handle.workflow_id == "wf-1"

    assert_receive {:multi, %ExecuteMultiOperationRequest{} = request}
    assert length(request.operations) == 2

    assert {:start_workflow, %{workflow_id: "wf-1"}} = List.first(request.operations).operation
    assert {:update_workflow, update} = List.last(request.operations).operation
    assert update.request.input.name == "add"
  end
end
