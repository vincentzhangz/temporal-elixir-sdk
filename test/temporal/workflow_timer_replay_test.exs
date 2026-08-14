defmodule Temporal.WorkflowTimerReplayTest do
  use ExUnit.Case, async: true

  alias Google.Protobuf.Duration
  alias Temporal.Api.Common.V1.{ActivityType, WorkflowType}

  alias Temporal.Api.History.V1.{
    ActivityTaskCompletedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    ActivityTaskStartedEventAttributes,
    History,
    HistoryEvent,
    TimerCanceledEventAttributes,
    TimerFiredEventAttributes,
    TimerStartedEventAttributes,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionContinuedAsNewEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Taskqueue.V1.TaskQueue
  alias Temporal.Workflow.Replay

  @workflow_id "timer-workflow"
  @run_id "timer-run"

  test "replays the official StartTimer and TimerFired event sequence" do
    workflow = fn _input ->
      :ok = Temporal.Workflow.sleep(1_000)
      "awake"
    end

    assert {:ok, cursor} =
             Replay.replay(timer_history(), workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert cursor.status == :completed
    assert cursor.timer_outcomes == %{1 => :fired}
  end

  test "batches multiple timers and wakes them in recorded fire order" do
    workflow = fn _input ->
      slow = Temporal.Workflow.new_timer(100)
      fast = Temporal.Workflow.new_timer(50)
      :ok = Temporal.Workflow.await(fast)
      :ok = Temporal.Workflow.await(slow)
      "both fired"
    end

    assert {:ok, cursor} =
             Replay.replay(multiple_timer_history(), workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert cursor.timer_outcomes == %{1 => :fired, 2 => :fired}
    assert cursor.status == :completed
  end

  test "replays a timer beside an Activity without changing command order" do
    workflow = fn _input ->
      timer = Temporal.Workflow.new_timer(100)

      result =
        Temporal.Workflow.execute_activity("Echo", "value",
          task_queue: "timers",
          start_to_close_timeout: 10
        )

      :ok = Temporal.Workflow.await(timer)
      result
    end

    assert {:ok, cursor} =
             Replay.replay(timer_activity_history(), workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert cursor.timer_outcomes == %{1 => :fired}
    assert cursor.activity_outcomes == %{1 => {:ok, "echoed"}}
  end

  test "replays cancel-after-start and resolves the future with CanceledError" do
    workflow = fn _input ->
      timer = Temporal.Workflow.new_timer(100)

      Temporal.Workflow.execute_activity("Echo", "value",
        task_queue: "timers",
        start_to_close_timeout: 10
      )

      :ok = Temporal.Workflow.cancel_timer(timer)

      try do
        Temporal.Workflow.await(timer)
      rescue
        Temporal.CanceledError -> "timer canceled"
      end
    end

    assert {:ok, cursor} =
             Replay.replay(canceled_timer_history(), workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert cursor.timer_outcomes == %{1 => :canceled}
  end

  test "TimerFired wins the cancel race and removes the stale cancel command" do
    workflow = fn _input ->
      timer = Temporal.Workflow.new_timer(100)

      Temporal.Workflow.execute_activity("Echo", "value",
        task_queue: "timers",
        start_to_close_timeout: 10
      )

      :ok = Temporal.Workflow.cancel_timer(timer)

      try do
        Temporal.Workflow.await(timer)
      rescue
        Temporal.CanceledError -> "timer canceled"
      end
    end

    assert {:ok, cursor} =
             Replay.replay(fired_cancel_race_history(), workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert cursor.timer_outcomes == %{1 => :fired}
    assert cursor.command.command_type == :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION
  end

  test "timer resolution can deterministically continue as new and abandons old-run timers" do
    workflow = fn _input ->
      :ok = Temporal.Workflow.sleep(1_000)
      Temporal.Workflow.continue_as_new("next")
    end

    assert {:ok, cursor} =
             Replay.replay(timer_continue_history(), workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert cursor.status == :continued_as_new
    assert cursor.new_execution_run_id == "next-run"
    assert cursor.timer_outcomes == %{1 => :fired}
  end

  test "Continue-As-New abandons an unstarted timer without emitting StartTimer" do
    workflow = fn _input ->
      _abandoned = Temporal.Workflow.new_timer(60_000)
      Temporal.Workflow.continue_as_new("next")
    end

    assert {:ok, cursor} =
             Replay.replay(abandoned_timer_continue_history(), workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert cursor.status == :continued_as_new
    assert cursor.timer_outcomes == %{}
  end

  defp timer_history do
    %History{
      events: [
        event(
          1,
          :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
          {:workflow_execution_started_event_attributes,
           %WorkflowExecutionStartedEventAttributes{
             workflow_id: @workflow_id,
             workflow_type: %WorkflowType{name: "TimerWorkflow"},
             input: Temporal.Payload.encode(nil)
           }}
        ),
        event(
          2,
          :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
          {:workflow_task_scheduled_event_attributes, %WorkflowTaskScheduledEventAttributes{}}
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
           %WorkflowTaskCompletedEventAttributes{scheduled_event_id: 2, started_event_id: 3}}
        ),
        event(
          5,
          :EVENT_TYPE_TIMER_STARTED,
          {:timer_started_event_attributes,
           %TimerStartedEventAttributes{
             timer_id: "timer-1",
             start_to_fire_timeout: %Duration{seconds: 1},
             workflow_task_completed_event_id: 4
           }}
        ),
        event(
          6,
          :EVENT_TYPE_TIMER_FIRED,
          {:timer_fired_event_attributes,
           %TimerFiredEventAttributes{timer_id: "timer-1", started_event_id: 5}}
        ),
        event(
          7,
          :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
          {:workflow_task_scheduled_event_attributes, %WorkflowTaskScheduledEventAttributes{}}
        ),
        event(
          8,
          :EVENT_TYPE_WORKFLOW_TASK_STARTED,
          {:workflow_task_started_event_attributes,
           %WorkflowTaskStartedEventAttributes{scheduled_event_id: 7}}
        ),
        event(
          9,
          :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
          {:workflow_task_completed_event_attributes,
           %WorkflowTaskCompletedEventAttributes{scheduled_event_id: 7, started_event_id: 8}}
        ),
        event(
          10,
          :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
          {:workflow_execution_completed_event_attributes,
           %WorkflowExecutionCompletedEventAttributes{
             workflow_task_completed_event_id: 9,
             result: Temporal.Payload.encode("awake")
           }}
        )
      ]
    }
  end

  defp multiple_timer_history do
    base = Enum.take(timer_history().events, 4)

    %History{
      events:
        base ++
          [
            timer_started(5, "timer-1", %Duration{nanos: 100_000_000}, 4),
            timer_started(6, "timer-2", %Duration{nanos: 50_000_000}, 4),
            timer_fired(7, "timer-2", 6),
            timer_fired(8, "timer-1", 5),
            workflow_task_scheduled(9),
            workflow_task_started(10, 9),
            workflow_task_completed(11, 9, 10),
            event(
              12,
              :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
              {:workflow_execution_completed_event_attributes,
               %WorkflowExecutionCompletedEventAttributes{
                 workflow_task_completed_event_id: 11,
                 result: Temporal.Payload.encode("both fired")
               }}
            )
          ]
    }
  end

  defp timer_activity_history do
    base = Enum.take(timer_history().events, 4)

    %History{
      events:
        base ++
          [
            timer_started(5, "timer-1", %Duration{nanos: 100_000_000}, 4),
            event(
              6,
              :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
              {:activity_task_scheduled_event_attributes,
               %ActivityTaskScheduledEventAttributes{
                 activity_id: "activity-1",
                 activity_type: %ActivityType{name: "Echo"},
                 task_queue: %TaskQueue{name: "timers"},
                 input: Temporal.Payload.encode("value"),
                 start_to_close_timeout: %Duration{seconds: 10},
                 workflow_task_completed_event_id: 4
               }}
            ),
            event(
              7,
              :EVENT_TYPE_ACTIVITY_TASK_STARTED,
              {:activity_task_started_event_attributes,
               %ActivityTaskStartedEventAttributes{scheduled_event_id: 6}}
            ),
            event(
              8,
              :EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
              {:activity_task_completed_event_attributes,
               %ActivityTaskCompletedEventAttributes{
                 scheduled_event_id: 6,
                 started_event_id: 7,
                 result: Temporal.Payload.encode("echoed")
               }}
            ),
            workflow_task_scheduled(9),
            workflow_task_started(10, 9),
            workflow_task_completed(11, 9, 10),
            timer_fired(12, "timer-1", 5),
            workflow_task_scheduled(13),
            workflow_task_started(14, 13),
            workflow_task_completed(15, 13, 14),
            event(
              16,
              :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
              {:workflow_execution_completed_event_attributes,
               %WorkflowExecutionCompletedEventAttributes{
                 workflow_task_completed_event_id: 15,
                 result: Temporal.Payload.encode("echoed")
               }}
            )
          ]
    }
  end

  defp canceled_timer_history do
    %History{events: events} = timer_activity_history()

    initial = Enum.take(events, 11)

    %History{
      events:
        initial ++
          [
            event(
              12,
              :EVENT_TYPE_TIMER_CANCELED,
              {:timer_canceled_event_attributes,
               %TimerCanceledEventAttributes{
                 timer_id: "timer-1",
                 started_event_id: 5,
                 workflow_task_completed_event_id: 11
               }}
            ),
            event(
              13,
              :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
              {:workflow_execution_completed_event_attributes,
               %WorkflowExecutionCompletedEventAttributes{
                 workflow_task_completed_event_id: 11,
                 result: Temporal.Payload.encode("timer canceled")
               }}
            )
          ]
    }
  end

  defp fired_cancel_race_history do
    %History{events: events} = canceled_timer_history()

    %History{
      events:
        Enum.map(events, fn
          %HistoryEvent{event_id: 12} ->
            timer_fired(12, "timer-1", 5)

          %HistoryEvent{
            event_id: 13,
            attributes: {:workflow_execution_completed_event_attributes, attrs}
          } =
              event ->
            %{
              event
              | attributes:
                  {:workflow_execution_completed_event_attributes,
                   %{attrs | result: Temporal.Payload.encode(:ok)}}
            }

          event ->
            event
        end)
    }
  end

  defp timer_continue_history do
    %History{events: events} = timer_history()
    initial = Enum.take(events, 9)

    %History{
      events:
        initial ++
          [
            event(
              10,
              :EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
              {:workflow_execution_continued_as_new_event_attributes,
               %WorkflowExecutionContinuedAsNewEventAttributes{
                 workflow_task_completed_event_id: 9,
                 new_execution_run_id: "next-run",
                 workflow_type: %WorkflowType{name: "TimerWorkflow"},
                 input: Temporal.Payload.encode("next")
               }}
            )
          ]
    }
  end

  defp abandoned_timer_continue_history do
    base = Enum.take(timer_history().events, 4)

    %History{
      events:
        base ++
          [
            event(
              5,
              :EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
              {:workflow_execution_continued_as_new_event_attributes,
               %WorkflowExecutionContinuedAsNewEventAttributes{
                 workflow_task_completed_event_id: 4,
                 new_execution_run_id: "next-run",
                 workflow_type: %WorkflowType{name: "TimerWorkflow"},
                 input: Temporal.Payload.encode("next")
               }}
            )
          ]
    }
  end

  defp timer_started(id, timer_id, timeout, completed_id) do
    event(
      id,
      :EVENT_TYPE_TIMER_STARTED,
      {:timer_started_event_attributes,
       %TimerStartedEventAttributes{
         timer_id: timer_id,
         start_to_fire_timeout: timeout,
         workflow_task_completed_event_id: completed_id
       }}
    )
  end

  defp timer_fired(id, timer_id, started_id) do
    event(
      id,
      :EVENT_TYPE_TIMER_FIRED,
      {:timer_fired_event_attributes,
       %TimerFiredEventAttributes{timer_id: timer_id, started_event_id: started_id}}
    )
  end

  defp workflow_task_scheduled(id) do
    event(
      id,
      :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
      {:workflow_task_scheduled_event_attributes, %WorkflowTaskScheduledEventAttributes{}}
    )
  end

  defp workflow_task_started(id, scheduled_id) do
    event(
      id,
      :EVENT_TYPE_WORKFLOW_TASK_STARTED,
      {:workflow_task_started_event_attributes,
       %WorkflowTaskStartedEventAttributes{scheduled_event_id: scheduled_id}}
    )
  end

  defp workflow_task_completed(id, scheduled_id, started_id) do
    event(
      id,
      :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
      {:workflow_task_completed_event_attributes,
       %WorkflowTaskCompletedEventAttributes{
         scheduled_event_id: scheduled_id,
         started_event_id: started_id
       }}
    )
  end

  defp event(id, type, attributes),
    do: %HistoryEvent{event_id: id, event_type: type, attributes: attributes}
end
