defmodule Temporal.WorkflowReplayTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskFailedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes,
    WorkflowTaskTimedOutEventAttributes
  }

  alias Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse
  alias Temporal.Worker.Runtime
  alias Temporal.Workflow.{HistoryCursor, Replay}

  @workflow_id "hello-workflow"
  @run_id "6bb2e5fd-7305-4c5c-9f43-b5470f53d573"

  test "replays a completed simple workflow and exposes identity/event cursors" do
    assert {:ok, cursor} =
             Replay.replay(completed_history(), &greeting/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert %HistoryCursor{
             workflow_id: @workflow_id,
             run_id: @run_id,
             next_event_id: 6,
             last_event_id: 5,
             workflow_task_scheduled_event_id: 2,
             workflow_task_started_event_id: 3,
             workflow_task_completed_event_id: 4,
             status: :completed
           } = cursor
  end

  test "replays the official-server-generated hello history JSON fixture" do
    fixture =
      Path.expand("../fixtures/live_hello_history.json", __DIR__)
      |> File.read!()

    assert {:ok,
            %HistoryCursor{
              workflow_id: "temporal-elixir-1218",
              run_id: "019ffc0a-d8b7-7ec5-b9c6-7ef1e3f01fee",
              status: :completed,
              next_event_id: 6
            }} =
             Replay.replay(fixture, &greeting/1,
               workflow_id: "temporal-elixir-1218",
               run_id: "019ffc0a-d8b7-7ec5-b9c6-7ef1e3f01fee"
             )
  end

  test "reports deterministic nondeterminism when workflow output changes" do
    assert {:error,
            {:nondeterminism,
             %{
               event_id: 5,
               expected_event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
               message: message
             }}} =
             Replay.replay(completed_history(), fn name -> "goodbye #{name}" end,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    assert message =~ "CompleteWorkflowExecution"
    assert message =~ "result"
  end

  test "rejects gapped and duplicate histories with actionable event diagnostics" do
    [first, _second, third, fourth, fifth] = completed_history().events
    gapped = %History{events: [first, third, fourth, fifth]}

    assert {:error,
            {:invalid_history,
             %{event_id: 3, expected_event_id: 2, message: "history event ID gap" <> _}}} =
             Replay.replay(gapped, &greeting/1, workflow_id: @workflow_id, run_id: @run_id)

    [first, second | rest] = completed_history().events
    duplicate = %History{events: [first, second, second | rest]}

    assert {:error,
            {:invalid_history,
             %{event_id: 2, expected_event_id: 3, message: "duplicate history event ID" <> _}}} =
             Replay.replay(duplicate, &greeting/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  test "validates workflow-task event ordering and correlation attributes" do
    history =
      update_event(completed_history(), 3, fn event ->
        %{event | attributes: started_attributes(99)}
      end)

    assert {:error,
            {:invalid_history,
             %{
               event_id: 3,
               field: :scheduled_event_id,
               expected: 2,
               actual: 99
             }}} =
             Replay.replay(history, &greeting/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )

    history =
      update_event(completed_history(), 2, fn event ->
        %{event | event_type: :EVENT_TYPE_WORKFLOW_TASK_STARTED}
      end)

    assert {:error,
            {:nondeterminism,
             %{
               event_id: 2,
               expected_event_type: :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
               actual_event_type: :EVENT_TYPE_WORKFLOW_TASK_STARTED
             }}} =
             Replay.replay(history, &greeting/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  test "recognizes failed and timed-out workflow tasks before a successful retry" do
    for closing_event <- [:failed, :timed_out] do
      assert {:ok,
              %HistoryCursor{
                status: :completed,
                workflow_task_scheduled_event_id: 5,
                workflow_task_started_event_id: 6,
                workflow_task_completed_event_id: 7,
                next_event_id: 9
              }} =
               Replay.replay(retried_history(closing_event), &greeting/1,
                 workflow_id: @workflow_id,
                 run_id: @run_id
               )
    end
  end

  test "preserves explicit unsupported errors" do
    unsupported =
      %History{
        events:
          Enum.take(completed_history().events, 3) ++
            [
              event(
                4,
                :EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
                {:workflow_execution_terminated_event_attributes, nil}
              )
            ]
      }

    assert {:error,
            {:unsupported_history_event,
             %{
               event_id: 4,
               event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
               feature: :workflow_termination
             }}} =
             Replay.replay(unsupported, &greeting/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  test "live execution is fenced by run, started event, and task token" do
    task = live_task("task-token")

    assert {:ok, completion, cursor} =
             Runtime.prepare(task, %{"Greeting" => &greeting/1}, "worker", nil)

    assert completion.task_token == "task-token"
    assert cursor.run_id == @run_id
    assert cursor.workflow_task_started_event_id == 3

    assert {:error,
            {:stale_workflow_task,
             %{run_id: @run_id, started_event_id: 3, reason: :task_already_completed}}} =
             Runtime.prepare(task, %{"Greeting" => &greeting/1}, "worker", cursor)

    assert {:error, {:task_token_mismatch, %{started_event_id: 3}}} =
             Runtime.prepare(
               live_task("different-token"),
               %{"Greeting" => &greeting/1},
               "worker",
               cursor
             )
  end

  test "rejects cross-run cursor reuse and malformed missing attributes" do
    cursor = HistoryCursor.new(workflow_id: @workflow_id, run_id: "another-run")

    assert {:error,
            {:workflow_identity_mismatch,
             %{field: :run_id, expected: "another-run", actual: @run_id}}} =
             Runtime.prepare(live_task("token"), %{"Greeting" => &greeting/1}, "worker", cursor)

    malformed =
      update_event(completed_history(), 2, fn event ->
        %{event | attributes: nil}
      end)

    assert {:error,
            {:invalid_history,
             %{event_id: 2, message: "missing WorkflowTaskScheduled attributes"}}} =
             Replay.replay(malformed, &greeting/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  defp greeting(name), do: "hello #{name}"

  defp completed_history do
    %History{
      events: [
        started_event(),
        event(2, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(3, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(2)),
        event(4, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(2, 3)),
        terminal_event(5, 4)
      ]
    }
  end

  defp retried_history(closing_event) do
    close =
      case closing_event do
        :failed ->
          {:workflow_task_failed_event_attributes,
           %WorkflowTaskFailedEventAttributes{scheduled_event_id: 2, started_event_id: 3}}

        :timed_out ->
          {:workflow_task_timed_out_event_attributes,
           %WorkflowTaskTimedOutEventAttributes{scheduled_event_id: 2, started_event_id: 3}}
      end

    close_type =
      case closing_event do
        :failed -> :EVENT_TYPE_WORKFLOW_TASK_FAILED
        :timed_out -> :EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
      end

    %History{
      events: [
        started_event(),
        event(2, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(3, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(2)),
        event(4, close_type, close),
        event(5, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(6, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(5)),
        event(7, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(5, 6)),
        terminal_event(8, 7)
      ]
    }
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
         input: Temporal.Payload.encode("Temporal")
       }}
    )
  end

  defp terminal_event(id, completed_id) do
    event(
      id,
      :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
      {:workflow_execution_completed_event_attributes,
       %WorkflowExecutionCompletedEventAttributes{
         workflow_task_completed_event_id: completed_id,
         result: Temporal.Payload.encode("hello Temporal")
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

  defp update_event(%History{} = history, event_id, update) do
    %{
      history
      | events:
          Enum.map(history.events, fn event ->
            if event.event_id == event_id, do: update.(event), else: event
          end)
    }
  end
end
