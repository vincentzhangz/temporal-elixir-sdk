defmodule Temporal.WorkflowSignalReplayTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.WorkflowType

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionSignaledEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Workflow.Replay

  @workflow_id "signal-workflow"
  @run_id "signal-run"

  test "replays buffered signals through a named handler in strict FIFO order" do
    workflow = fn _input ->
      :ok =
        Temporal.Workflow.set_signal_handler(
          "append",
          fn value, context, state ->
            entry = [context.event_id, value]
            {:ok, Map.update(state, :seen, [entry], &(&1 ++ [entry]))}
          end
        )

      state = Temporal.Workflow.await_signal_state(&(length(Map.get(&1, :seen, [])) == 2))
      state.seen
    end

    assert {:ok, cursor} =
             Replay.replay(signal_history(), workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert cursor.status == :completed
    assert Enum.map(cursor.signal_events, & &1.event_id) == [5, 7]
  end

  test "dynamic handlers receive names and headers and duplicate request IDs run once" do
    workflow = fn _input ->
      :ok =
        Temporal.Workflow.set_dynamic_signal_handler(fn value, context, state ->
          entry = [context.signal_name, context.headers.fields["trace"].data, value]
          {:ok, Map.update(state, :seen, [entry], &(&1 ++ [entry]))}
        end)

      state = Temporal.Workflow.await_signal_state(&(length(Map.get(&1, :seen, [])) == 1))
      state.seen
    end

    history =
      signal_history([
        signal(5, "unknown", 1, "same-request"),
        workflow_task_scheduled(6),
        signal(7, "unknown", 2, "same-request"),
        workflow_task_started(8, 6),
        workflow_task_completed(9, 6, 8),
        completed(10, 9, [["unknown", "trace-5", 1]])
      ])

    assert {:ok, cursor} =
             Replay.replay(history, workflow, workflow_id: @workflow_id, run_id: @run_id)

    assert cursor.status == :completed
  end

  defp signal_history(events \\ nil) do
    events =
      events ||
        [
          signal(5, "append", "first", "request-1"),
          workflow_task_scheduled(6),
          signal(7, "append", "second", "request-2"),
          workflow_task_started(8, 6),
          workflow_task_completed(9, 6, 8),
          completed(10, 9, [[5, "first"], [7, "second"]])
        ]

    %History{events: initial_events() ++ events}
  end

  defp initial_events do
    [
      event(
        1,
        :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
        {:workflow_execution_started_event_attributes,
         %WorkflowExecutionStartedEventAttributes{
           workflow_id: @workflow_id,
           workflow_type: %WorkflowType{name: "SignalWorkflow"},
           input: Temporal.Payload.encode(nil)
         }}
      ),
      workflow_task_scheduled(2),
      workflow_task_started(3, 2),
      workflow_task_completed(4, 2, 3)
    ]
  end

  defp signal(id, name, value, request_id) do
    event(
      id,
      :EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
      {:workflow_execution_signaled_event_attributes,
       %WorkflowExecutionSignaledEventAttributes{
         signal_name: name,
         input: Temporal.Payload.encode(value),
         identity: "client",
         request_id: request_id,
         header: %Temporal.Api.Common.V1.Header{
           fields: %{"trace" => %Temporal.Api.Common.V1.Payload{data: "trace-#{id}"}}
         }
       }}
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

  defp completed(id, workflow_task_completed_id, result) do
    event(
      id,
      :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
      {:workflow_execution_completed_event_attributes,
       %WorkflowExecutionCompletedEventAttributes{
         workflow_task_completed_event_id: workflow_task_completed_id,
         result: Temporal.Payload.encode(result)
       }}
    )
  end

  defp event(id, type, attributes),
    do: %HistoryEvent{event_id: id, event_type: type, attributes: attributes}
end
