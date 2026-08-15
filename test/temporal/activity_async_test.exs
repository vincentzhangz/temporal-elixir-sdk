defmodule Temporal.ActivityAsyncTest do
  use ExUnit.Case, async: true

  alias Temporal.Activity.Async
  alias Temporal.Activity.Worker, as: ActivityWorker
  alias Temporal.Api.Common.V1.{ActivityType, WorkflowExecution}

  alias Temporal.Api.Workflowservice.V1.{
    PollActivityTaskQueueResponse,
    RespondActivityTaskCompletedRequest,
    RespondActivityTaskCompletedResponse
  }

  defmodule FakeTransport do
    @behaviour Temporal.RPC.Transport

    alias Temporal.Api.Common.V1.{ActivityType, WorkflowExecution}

    alias Temporal.Api.Workflowservice.V1.{
      PollActivityTaskQueueResponse,
      RespondActivityTaskCompletedResponse
    }

    @impl true
    def unary(test_pid, method, request, _options) do
      send(test_pid, {:rpc, method, request})
      {:ok, encode_response(method, request)}
    end

    defp encode_response(
           "/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue",
           _req
         ) do
      PollActivityTaskQueueResponse.encode(%PollActivityTaskQueueResponse{
        task_token: "token-1",
        activity_id: "activity-1",
        workflow_execution: %WorkflowExecution{workflow_id: "wf", run_id: "run"},
        activity_type: %ActivityType{name: "Example"},
        attempt: 1,
        input: Temporal.Payload.encode("input")
      })
    end

    defp encode_response(
           "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
           _req
         ) do
      RespondActivityTaskCompletedResponse.encode(%RespondActivityTaskCompletedResponse{})
    end

    defp encode_response(_method, _req), do: <<>>
  end

  defmodule EmptyPollTransport do
    @behaviour Temporal.RPC.Transport

    alias Temporal.Api.Workflowservice.V1.{
      PollActivityTaskQueueResponse,
      RespondActivityTaskCompletedResponse
    }

    @impl true
    def unary(test_pid, method, request, _options) do
      send(test_pid, {:rpc, method, request})

      case method do
        "/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue" ->
          {:ok, PollActivityTaskQueueResponse.encode(%PollActivityTaskQueueResponse{})}

        "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted" ->
          {:ok,
           RespondActivityTaskCompletedResponse.encode(%RespondActivityTaskCompletedResponse{})}

        _other ->
          {:ok, <<>>}
      end
    end
  end

  test "async completion responds on the worker's behalf" do
    {:ok, connection} =
      Temporal.Connection.open(
        target: "localhost:7233",
        namespace: "ns",
        transport: FakeTransport,
        transport_state: self()
      )

    on_exit(fn -> Temporal.Connection.close(connection) end)

    {:ok, worker} =
      ActivityWorker.start_link(
        connection: connection,
        task_queue: "activities",
        activities: %{
          "Example" => fn _arg ->
            Process.sleep(500)
            :ran_synchronously
          end
        }
      )

    on_exit(fn -> if Process.alive?(worker), do: ActivityWorker.stop(worker) end)

    # Poll delivers a task; the worker registers its token.
    assert_receive {:rpc,
                    "/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue", _}

    # The activity is still running (sleeps 500ms); complete it async by token.
    assert :ok = Async.complete_async(worker, "token-1", %{"done" => true})

    assert_receive {:rpc,
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
                    request}

    decoded =
      RespondActivityTaskCompletedRequest.decode(request)

    assert {:ok, %{"done" => true}} = Temporal.Payload.decode(decoded.result)
  end

  test "submit_eager executes an eager Activity task without polling" do
    {:ok, connection} =
      Temporal.Connection.open(
        target: "localhost:7233",
        namespace: "ns",
        transport: EmptyPollTransport,
        transport_state: self()
      )

    on_exit(fn -> Temporal.Connection.close(connection) end)

    {:ok, worker} =
      ActivityWorker.start_link(
        connection: connection,
        task_queue: "activities",
        activities: %{"Example" => fn _arg -> :eager_done end}
      )

    on_exit(fn -> if Process.alive?(worker), do: ActivityWorker.stop(worker) end)

    eager_task = %PollActivityTaskQueueResponse{
      task_token: "eager-token",
      activity_id: "eager-1",
      workflow_execution: %WorkflowExecution{workflow_id: "wf", run_id: "run"},
      activity_type: %ActivityType{name: "Example"},
      attempt: 1,
      input: Temporal.Payload.encode("input")
    }

    # No poll RPC should occur for the eager task.
    assert :ok = ActivityWorker.submit_eager(worker, eager_task)

    assert_receive {:rpc,
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
                    request}

    assert RespondActivityTaskCompletedRequest.decode(request).task_token ==
             "eager-token"
  end
end
