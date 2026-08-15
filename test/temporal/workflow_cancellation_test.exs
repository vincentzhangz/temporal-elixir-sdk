defmodule Temporal.WorkflowCancellationTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Command.V1.Command
  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    WorkflowExecutionCancelRequestedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse
  alias Temporal.Worker.Runtime
  alias Temporal.Workflow.{HistoryCursor, Replay}

  @workflow_id "cancel-workflow"
  @run_id "6bb2e5fd-7305-4c5c-9f43-b5470f53d573"

  test "fail_workflow/1 emits a FailWorkflowExecution command" do
    task = live_task("task-token")

    assert {:ok, completion, _cursor} =
             Runtime.prepare(
               task,
               %{"Greeting" => &failing_workflow/1},
               "worker",
               nil
             )

    assert [%Command{command_type: :COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION} = command] =
             completion.commands

    assert {:fail_workflow_execution_command_attributes,
            %{failure: %Temporal.Api.Failure.V1.Failure{message: "boom"}}} =
             command.attributes
  end

  test "replays a workflow that emits a FailWorkflowExecution close event" do
    history = %History{
      events: [
        started_event(),
        event(2, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(3, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(2)),
        event(4, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(2, 3)),
        failed_event(5, 4)
      ]
    }

    assert {:ok, %HistoryCursor{status: :failed, next_event_id: 6}} =
             Replay.replay(history, &failing_workflow/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  test "cancel_workflow/0 emits a CancelWorkflowExecution command" do
    task = live_task("task-token")

    assert {:ok, completion, _cursor} =
             Runtime.prepare(
               task,
               %{"Greeting" => &canceling_workflow/1},
               "worker",
               nil
             )

    assert [%Command{command_type: :COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION}] =
             completion.commands
  end

  test "replays a workflow that emits a CancelWorkflowExecution close event" do
    history = %History{
      events: [
        started_event(),
        event(2, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(3, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(2)),
        event(4, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(2, 3)),
        canceled_event(5, 4)
      ]
    }

    assert {:ok, %HistoryCursor{status: :canceled, next_event_id: 6}} =
             Replay.replay(history, &canceling_workflow/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  test "a cancel-requested event lets workflow code observe and cancel" do
    history = %History{
      events: [
        started_event(),
        event(2, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(3, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(2)),
        event(4, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(2, 3)),
        event(
          5,
          :EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
          {:workflow_execution_cancel_requested_event_attributes,
           %WorkflowExecutionCancelRequestedEventAttributes{identity: "client"}}
        ),
        event(6, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(7, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(6)),
        event(8, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(6, 7)),
        canceled_event(9, 8)
      ]
    }

    assert {:ok, %HistoryCursor{status: :canceled, next_event_id: 10}} =
             Replay.replay(history, &cleanup_cancel_workflow/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  defp failing_workflow(_input) do
    Temporal.Workflow.fail_workflow(Temporal.ApplicationError.exception(message: "boom"))
  end

  defp canceling_workflow(_input) do
    Temporal.Workflow.cancel_workflow()
  end

  defp cleanup_cancel_workflow(input) do
    if input == "cancel-me" do
      Temporal.Workflow.cancel_workflow()
    else
      :ok
    end
  end

  defp live_task(token) do
    %PollWorkflowTaskQueueResponse{
      task_token: token,
      workflow_execution: %WorkflowExecution{workflow_id: @workflow_id, run_id: @run_id},
      workflow_type: %WorkflowType{name: "Greeting"},
      started_event_id: 3,
      history: %History{
        events: [
          started_event(),
          event(2, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
          event(3, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(2))
        ]
      }
    }
  end

  defp started_event do
    event(
      1,
      :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
      {:workflow_execution_started_event_attributes,
       %WorkflowExecutionStartedEventAttributes{
         workflow_id: @workflow_id,
         workflow_type: %WorkflowType{name: "Greeting"},
         input: Temporal.Payload.encode("cancel-me")
       }}
    )
  end

  defp failed_event(id, completed_id) do
    event(
      id,
      :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
      {:workflow_execution_failed_event_attributes,
       %Temporal.Api.History.V1.WorkflowExecutionFailedEventAttributes{
         workflow_task_completed_event_id: completed_id,
         failure:
           Temporal.Failure.to_proto(
             Temporal.ApplicationError.exception(message: "boom"),
             []
           )
       }}
    )
  end

  defp canceled_event(id, completed_id) do
    event(
      id,
      :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
      {:workflow_execution_canceled_event_attributes,
       %Temporal.Api.History.V1.WorkflowExecutionCanceledEventAttributes{
         workflow_task_completed_event_id: completed_id
       }}
    )
  end

  defp scheduled_attributes do
    {:workflow_task_scheduled_event_attributes, %WorkflowTaskScheduledEventAttributes{attempt: 1}}
  end

  defp started_attributes(scheduled_id) do
    {:workflow_task_started_event_attributes,
     %WorkflowTaskStartedEventAttributes{scheduled_event_id: scheduled_id}}
  end

  defp completed_attributes(scheduled_id, started_id) do
    {:workflow_task_completed_event_attributes,
     %WorkflowTaskCompletedEventAttributes{
       scheduled_event_id: scheduled_id,
       started_event_id: started_id
     }}
  end

  defp event(id, type, attributes) do
    %HistoryEvent{event_id: id, event_type: type, attributes: attributes}
  end
end
