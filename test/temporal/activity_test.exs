defmodule Temporal.ActivityTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{ActivityType, WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    ActivityTaskCanceledEventAttributes,
    ActivityTaskCompletedEventAttributes,
    ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    ActivityTaskStartedEventAttributes,
    History,
    HistoryEvent,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Workflowservice.V1.{
    PollActivityTaskQueueResponse,
    PollWorkflowTaskQueueResponse
  }

  alias Temporal.Activity.Runtime, as: ActivityRuntime
  alias Temporal.Worker.Runtime, as: WorkflowRuntime
  alias Temporal.Workflow.Replay

  @workflow_id "activity-workflow"
  @run_id "activity-run"

  test "workflow API emits a validated ScheduleActivityTask command" do
    assert {:ok, completion, _cursor} =
             WorkflowRuntime.prepare(
               workflow_task(),
               %{"ActivityGreeting" => &activity_workflow/1},
               "worker",
               nil
             )

    assert [command] = completion.commands
    assert command.command_type == :COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK

    {:schedule_activity_task_command_attributes, attributes} = command.attributes
    assert attributes.activity_id == "activity-1"
    assert attributes.activity_type.name == "ComposeGreeting"
    assert attributes.task_queue.name == "activity-queue"
    assert attributes.start_to_close_timeout.seconds == 10
    assert attributes.schedule_to_close_timeout.seconds == 30
    assert {:ok, "Temporal"} = Temporal.Payload.decode(attributes.input)
  end

  test "activity executor completes one-argument activities and fences the token" do
    task = %PollActivityTaskQueueResponse{
      task_token: "activity-token",
      workflow_execution: %WorkflowExecution{workflow_id: @workflow_id, run_id: @run_id},
      activity_id: "activity-1",
      activity_type: %ActivityType{name: "ComposeGreeting"},
      attempt: 1,
      input: Temporal.Payload.encode("Temporal")
    }

    assert {:ok, completion, fence} =
             ActivityRuntime.prepare(
               task,
               %{"ComposeGreeting" => fn name -> "hello #{name}" end},
               "activity-worker",
               nil
             )

    assert completion.task_token == "activity-token"
    assert {:ok, "hello Temporal"} = Temporal.Payload.decode(completion.result)

    assert {:error, {:stale_activity_task, %{activity_id: "activity-1"}}} =
             ActivityRuntime.prepare(
               task,
               %{"ComposeGreeting" => fn name -> "hello #{name}" end},
               "activity-worker",
               fence
             )
  end

  test "activity executor maps exceptions to Temporal failures" do
    task = %PollActivityTaskQueueResponse{
      task_token: "activity-token",
      workflow_execution: %WorkflowExecution{workflow_id: @workflow_id, run_id: @run_id},
      activity_id: "activity-1",
      activity_type: %ActivityType{name: "Failing"},
      attempt: 1,
      input: Temporal.Payload.encode("Temporal")
    }

    assert {:error_response, failure, _fence} =
             ActivityRuntime.prepare(
               task,
               %{"Failing" => fn _ -> raise "boom" end},
               "activity-worker",
               nil
             )

    assert failure.task_token == "activity-token"
    assert failure.failure.message =~ "boom"
  end

  test "replays schedule, Activity completion, and final workflow completion" do
    assert {:ok, cursor} =
             Replay.replay(activity_history(), &activity_workflow/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert cursor.status == :completed
    assert cursor.activity_scheduled_event_id == 5
    assert cursor.activity_started_event_id == 6
    assert cursor.activity_completed_event_id == 7
    assert cursor.next_event_id == 12
  end

  test "detects changed Activity command attributes during replay" do
    changed = fn name ->
      Temporal.Workflow.execute_activity("DifferentActivity", name,
        task_queue: "activity-queue",
        start_to_close_timeout: 10,
        schedule_to_close_timeout: 30
      )
    end

    assert {:error,
            {:nondeterminism,
             %{
               event_id: 5,
               command_type: :COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
               field: :activity_type
             }}} =
             Replay.replay(activity_history(), changed,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  test "maps Activity cancellation history to typed Workflow errors" do
    events =
      activity_history().events
      |> Enum.take(6)
      |> Kernel.++([
        event(
          7,
          :EVENT_TYPE_ACTIVITY_TASK_CANCELED,
          {:activity_task_canceled_event_attributes,
           %ActivityTaskCanceledEventAttributes{
             scheduled_event_id: 5,
             started_event_id: 6,
             latest_cancel_requested_event_id: 0
           }}
        )
      ])

    assert {:error,
            {:workflow_failed,
             %Temporal.ActivityError{cause: %Temporal.CanceledError{acknowledged: true}},
             _stacktrace}} =
             Replay.replay(%History{events: events}, &activity_workflow/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  test "replays multiple sequential Activities with deterministic schedule IDs" do
    workflow = fn name ->
      first =
        Temporal.Workflow.execute_activity("ComposeGreeting", name,
          task_queue: "activity-queue",
          start_to_close_timeout: 10,
          schedule_to_close_timeout: 30
        )

      Temporal.Workflow.execute_activity("Uppercase", first,
        task_queue: "activity-queue",
        start_to_close_timeout: 10,
        schedule_to_close_timeout: 30
      )
    end

    history = sequential_activity_history()

    assert {:ok, cursor} =
             Replay.replay(history, workflow, workflow_id: @workflow_id, run_id: @run_id)

    assert cursor.status == :completed

    assert cursor.activity_outcomes == %{
             1 => {:ok, "hello Temporal"},
             2 => {:ok, "HELLO TEMPORAL"}
           }
  end

  test "delivers terminal Activity failure to Workflow code as typed errors" do
    workflow = fn name ->
      try do
        activity_workflow(name)
      rescue
        error in Temporal.ActivityError ->
          application = error.cause
          "#{application.type}:#{application.message}:#{error.retry_state}"
      end
    end

    events =
      activity_history().events
      |> Enum.take(6)
      |> Kernel.++([
        event(
          7,
          :EVENT_TYPE_ACTIVITY_TASK_FAILED,
          {:activity_task_failed_event_attributes,
           %ActivityTaskFailedEventAttributes{
             scheduled_event_id: 5,
             started_event_id: 6,
             retry_state: :RETRY_STATE_NON_RETRYABLE_FAILURE,
             failure: %Temporal.Api.Failure.V1.Failure{
               message: "declined",
               failure_info:
                 {:application_failure_info,
                  %Temporal.Api.Failure.V1.ApplicationFailureInfo{
                    type: "PaymentDeclined",
                    non_retryable: true
                  }}
             }
           }}
        ),
        event(
          8,
          :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
          {:workflow_task_scheduled_event_attributes,
           %WorkflowTaskScheduledEventAttributes{attempt: 1}}
        ),
        event(
          9,
          :EVENT_TYPE_WORKFLOW_TASK_STARTED,
          {:workflow_task_started_event_attributes,
           %WorkflowTaskStartedEventAttributes{scheduled_event_id: 8}}
        ),
        event(
          10,
          :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
          {:workflow_task_completed_event_attributes,
           %WorkflowTaskCompletedEventAttributes{scheduled_event_id: 8, started_event_id: 9}}
        ),
        event(
          11,
          :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
          {:workflow_execution_completed_event_attributes,
           %WorkflowExecutionCompletedEventAttributes{
             workflow_task_completed_event_id: 10,
             result:
               Temporal.Payload.encode(
                 "PaymentDeclined:declined:RETRY_STATE_NON_RETRYABLE_FAILURE"
               )
           }}
        )
      ])

    assert {:ok, %{status: :completed}} =
             Replay.replay(%History{events: events}, workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  defp activity_workflow(name) do
    Temporal.Workflow.execute_activity("ComposeGreeting", name,
      task_queue: "activity-queue",
      start_to_close_timeout: 10,
      schedule_to_close_timeout: 30
    )
  end

  defp workflow_task do
    %PollWorkflowTaskQueueResponse{
      task_token: "workflow-token",
      workflow_execution: %WorkflowExecution{workflow_id: @workflow_id, run_id: @run_id},
      workflow_type: %WorkflowType{name: "ActivityGreeting"},
      started_event_id: 3,
      history: %History{events: Enum.take(activity_history().events, 3)}
    }
  end

  defp activity_history do
    %History{
      events: [
        event(
          1,
          :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
          {:workflow_execution_started_event_attributes,
           %WorkflowExecutionStartedEventAttributes{
             workflow_id: @workflow_id,
             workflow_type: %WorkflowType{name: "ActivityGreeting"},
             input: Temporal.Payload.encode("Temporal")
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
        ),
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
          :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
          {:activity_task_scheduled_event_attributes,
           %ActivityTaskScheduledEventAttributes{
             activity_id: "activity-1",
             activity_type: %ActivityType{name: "ComposeGreeting"},
             task_queue: %Temporal.Api.Taskqueue.V1.TaskQueue{name: "activity-queue"},
             input: Temporal.Payload.encode("Temporal"),
             workflow_task_completed_event_id: 4,
             start_to_close_timeout: %Google.Protobuf.Duration{seconds: 10},
             schedule_to_close_timeout: %Google.Protobuf.Duration{seconds: 30}
           }}
        ),
        event(
          6,
          :EVENT_TYPE_ACTIVITY_TASK_STARTED,
          {:activity_task_started_event_attributes,
           %ActivityTaskStartedEventAttributes{scheduled_event_id: 5, attempt: 1}}
        ),
        event(
          7,
          :EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
          {:activity_task_completed_event_attributes,
           %ActivityTaskCompletedEventAttributes{
             scheduled_event_id: 5,
             started_event_id: 6,
             result: Temporal.Payload.encode("hello Temporal")
           }}
        ),
        event(
          8,
          :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
          {:workflow_task_scheduled_event_attributes,
           %WorkflowTaskScheduledEventAttributes{attempt: 1}}
        ),
        event(
          9,
          :EVENT_TYPE_WORKFLOW_TASK_STARTED,
          {:workflow_task_started_event_attributes,
           %WorkflowTaskStartedEventAttributes{scheduled_event_id: 8}}
        ),
        event(
          10,
          :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
          {:workflow_task_completed_event_attributes,
           %WorkflowTaskCompletedEventAttributes{
             scheduled_event_id: 8,
             started_event_id: 9
           }}
        ),
        event(
          11,
          :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
          {:workflow_execution_completed_event_attributes,
           %WorkflowExecutionCompletedEventAttributes{
             workflow_task_completed_event_id: 10,
             result: Temporal.Payload.encode("hello Temporal")
           }}
        )
      ]
    }
  end

  defp sequential_activity_history do
    first = Enum.take(activity_history().events, 7)

    %History{
      events:
        first ++
          [
            event(
              8,
              :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
              {:workflow_task_scheduled_event_attributes,
               %WorkflowTaskScheduledEventAttributes{attempt: 1}}
            ),
            event(
              9,
              :EVENT_TYPE_WORKFLOW_TASK_STARTED,
              {:workflow_task_started_event_attributes,
               %WorkflowTaskStartedEventAttributes{scheduled_event_id: 8}}
            ),
            event(
              10,
              :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
              {:workflow_task_completed_event_attributes,
               %WorkflowTaskCompletedEventAttributes{scheduled_event_id: 8, started_event_id: 9}}
            ),
            event(
              11,
              :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
              {:activity_task_scheduled_event_attributes,
               %ActivityTaskScheduledEventAttributes{
                 activity_id: "activity-2",
                 activity_type: %ActivityType{name: "Uppercase"},
                 task_queue: %Temporal.Api.Taskqueue.V1.TaskQueue{name: "activity-queue"},
                 input: Temporal.Payload.encode("hello Temporal"),
                 workflow_task_completed_event_id: 10,
                 start_to_close_timeout: %Google.Protobuf.Duration{seconds: 10},
                 schedule_to_close_timeout: %Google.Protobuf.Duration{seconds: 30}
               }}
            ),
            event(
              12,
              :EVENT_TYPE_ACTIVITY_TASK_STARTED,
              {:activity_task_started_event_attributes,
               %ActivityTaskStartedEventAttributes{scheduled_event_id: 11, attempt: 1}}
            ),
            event(
              13,
              :EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
              {:activity_task_completed_event_attributes,
               %ActivityTaskCompletedEventAttributes{
                 scheduled_event_id: 11,
                 started_event_id: 12,
                 result: Temporal.Payload.encode("HELLO TEMPORAL")
               }}
            ),
            event(
              14,
              :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
              {:workflow_task_scheduled_event_attributes,
               %WorkflowTaskScheduledEventAttributes{attempt: 1}}
            ),
            event(
              15,
              :EVENT_TYPE_WORKFLOW_TASK_STARTED,
              {:workflow_task_started_event_attributes,
               %WorkflowTaskStartedEventAttributes{scheduled_event_id: 14}}
            ),
            event(
              16,
              :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
              {:workflow_task_completed_event_attributes,
               %WorkflowTaskCompletedEventAttributes{
                 scheduled_event_id: 14,
                 started_event_id: 15
               }}
            ),
            event(
              17,
              :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
              {:workflow_execution_completed_event_attributes,
               %WorkflowExecutionCompletedEventAttributes{
                 workflow_task_completed_event_id: 16,
                 result: Temporal.Payload.encode("HELLO TEMPORAL")
               }}
            )
          ]
    }
  end

  defp event(id, type, attributes) do
    %HistoryEvent{event_id: id, event_type: type, attributes: attributes}
  end
end
