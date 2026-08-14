defmodule Temporal.ContinueAsNewTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    WorkflowExecutionContinuedAsNewEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse
  alias Temporal.Worker.Runtime
  alias Temporal.Workflow.Replay

  test "emits ContinueAsNew as the sole command and stops the current run" do
    assert {:ok, completion, _cursor} =
             Runtime.prepare(
               live_task(),
               %{"Counter" => &counter/1},
               "worker",
               nil
             )

    assert [command] = completion.commands
    assert command.command_type == :COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION

    {:continue_as_new_workflow_execution_command_attributes, attributes} = command.attributes
    assert attributes.workflow_type.name == "Counter"
    assert attributes.task_queue.name == "counter-queue"
    assert {:ok, %{"remaining" => 1}} = Temporal.Payload.decode(attributes.input)
  end

  test "replays and semantically matches WorkflowExecutionContinuedAsNew" do
    history =
      %History{
        events:
          live_task().history.events ++
            [
              event(
                4,
                :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
                {:workflow_task_completed_event_attributes,
                 %WorkflowTaskCompletedEventAttributes{
                   scheduled_event_id: 2,
                   started_event_id: 3
                 }}
              ),
              event(
                5,
                :EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
                {:workflow_execution_continued_as_new_event_attributes,
                 %WorkflowExecutionContinuedAsNewEventAttributes{
                   new_execution_run_id: "next-run",
                   workflow_type: %WorkflowType{name: "Counter"},
                   task_queue: %Temporal.Api.Taskqueue.V1.TaskQueue{name: "counter-queue"},
                   input: Temporal.Payload.encode(%{"remaining" => 1}),
                   workflow_task_completed_event_id: 4
                 }}
              )
            ]
      }

    assert {:ok, cursor} =
             Replay.replay(history, &counter/1,
               workflow_id: "counter-workflow",
               run_id: "first-run"
             )

    assert cursor.status == :continued_as_new
    assert cursor.new_execution_run_id == "next-run"
  end

  test "rejects unsupported Continue-As-New option combinations" do
    workflow = fn state ->
      Temporal.Workflow.continue_as_new(state, workflow_execution_timeout: 10)
    end

    assert {:error, {:workflow_failed, %ArgumentError{message: message}, _stacktrace}} =
             Runtime.prepare(live_task(), %{"Counter" => workflow}, "worker", nil)

    assert message =~ "unsupported continue-as-new option"
  end

  defp counter(%{"remaining" => remaining} = state) do
    Temporal.Workflow.continue_as_new(%{state | "remaining" => remaining - 1})
  end

  defp live_task do
    %PollWorkflowTaskQueueResponse{
      task_token: "token",
      workflow_execution: %WorkflowExecution{
        workflow_id: "counter-workflow",
        run_id: "first-run"
      },
      workflow_type: %WorkflowType{name: "Counter"},
      started_event_id: 3,
      history: %History{
        events: [
          event(
            1,
            :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
            {:workflow_execution_started_event_attributes,
             %WorkflowExecutionStartedEventAttributes{
               workflow_id: "counter-workflow",
               workflow_type: %WorkflowType{name: "Counter"},
               task_queue: %Temporal.Api.Taskqueue.V1.TaskQueue{name: "counter-queue"},
               input: Temporal.Payload.encode(%{"remaining" => 2})
             }}
          ),
          event(
            2,
            :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
            {:workflow_task_scheduled_event_attributes,
             %WorkflowTaskScheduledEventAttributes{attempt: 1}}
          ),
          event(
            3,
            :EVENT_TYPE_WORKFLOW_TASK_STARTED,
            {:workflow_task_started_event_attributes,
             %WorkflowTaskStartedEventAttributes{scheduled_event_id: 2}}
          )
        ]
      }
    }
  end

  defp event(id, type, attributes) do
    %HistoryEvent{event_id: id, event_type: type, attributes: attributes}
  end
end
