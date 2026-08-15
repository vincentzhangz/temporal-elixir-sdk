defmodule Temporal.QueryTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Query.V1.WorkflowQuery

  alias Temporal.Api.Workflowservice.V1.{
    PollWorkflowTaskQueueResponse,
    RespondWorkflowTaskCompletedRequest
  }

  alias Temporal.Worker.Runtime
  alias Temporal.Workflow.TaskKernel.Query

  defp greeting_task(queries) do
    %PollWorkflowTaskQueueResponse{
      task_token: "token",
      workflow_execution: %WorkflowExecution{workflow_id: "workflow-id", run_id: "run-id"},
      workflow_type: %WorkflowType{name: "Greeting"},
      started_event_id: 3,
      queries: queries,
      history: %History{
        events: [
          %HistoryEvent{
            event_id: 1,
            event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
            attributes:
              {:workflow_execution_started_event_attributes,
               %WorkflowExecutionStartedEventAttributes{
                 workflow_id: "workflow-id",
                 workflow_type: %WorkflowType{name: "Greeting"},
                 input: Temporal.Payload.encode("Temporal")
               }}
          },
          %HistoryEvent{
            event_id: 2,
            event_type: :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
            attributes:
              {:workflow_task_scheduled_event_attributes,
               %WorkflowTaskScheduledEventAttributes{attempt: 1}}
          },
          %HistoryEvent{
            event_id: 3,
            event_type: :EVENT_TYPE_WORKFLOW_TASK_STARTED,
            attributes:
              {:workflow_task_started_event_attributes,
               %WorkflowTaskStartedEventAttributes{scheduled_event_id: 2}}
          }
        ]
      }
    }
  end

  defp greeting_with_handlers(name) do
    Temporal.Workflow.set_query_handler("get_name", fn _args -> name end)
    "hello #{name}"
  end

  test "answers queries attached to a Workflow Task via query_results" do
    queries = %{
      "query-id-1" => %WorkflowQuery{
        query_type: "get_name",
        query_args: Temporal.Payload.encode(nil)
      }
    }

    task = greeting_task(queries)

    assert {:ok, %RespondWorkflowTaskCompletedRequest{query_results: results}} =
             Runtime.complete(task, %{"Greeting" => &greeting_with_handlers/1}, "worker")

    assert %{result_type: :QUERY_RESULT_TYPE_ANSWERED, answer: answer} = results["query-id-1"]
    assert {:ok, "Temporal"} = Temporal.Payload.decode(answer)
  end

  test "reports query handler failures with QUERY_RESULT_TYPE_FAILED" do
    queries = %{
      "query-id-1" => %WorkflowQuery{
        query_type: "explode",
        query_args: Temporal.Payload.encode(nil)
      }
    }

    task = greeting_task(queries)

    handlers = fn name ->
      Temporal.Workflow.set_query_handler("explode", fn _args -> raise "boom" end)
      "hello #{name}"
    end

    assert {:ok, %RespondWorkflowTaskCompletedRequest{query_results: results}} =
             Runtime.complete(task, %{"Greeting" => handlers}, "worker")

    assert %{result_type: :QUERY_RESULT_TYPE_FAILED, error_message: message} =
             results["query-id-1"]

    assert message =~ "boom"
  end

  test "returns failed results for unregistered query handlers" do
    queries = %{
      "query-id-1" => %WorkflowQuery{
        query_type: "unknown",
        query_args: Temporal.Payload.encode(nil)
      }
    }

    task = greeting_task(queries)

    assert {:ok, %RespondWorkflowTaskCompletedRequest{query_results: results}} =
             Runtime.complete(task, %{"Greeting" => &greeting_with_handlers/1}, "worker")

    assert %{result_type: :QUERY_RESULT_TYPE_FAILED, error_message: message} =
             results["query-id-1"]

    assert message =~ "unknown"
  end

  test "runs queries against the captured workflow context" do
    result = Query.run(%{}, %{query_handlers: %{}})
    assert result == %{}
  end
end
