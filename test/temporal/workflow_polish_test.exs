defmodule Temporal.WorkflowPolishTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    MarkerRecordedEventAttributes,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse
  alias Temporal.Worker.Runtime
  alias Temporal.Workflow.{HistoryCursor, Replay}

  @workflow_id "polish-workflow"
  @run_id "6bb2e5fd-7305-4c5c-9f43-b5470f53d573"

  test "get_version/3 records a Version marker and replays deterministically" do
    workflow = fn _input ->
      v = Temporal.Workflow.get_version("fooChange", Temporal.Workflow.get_version_default(), 0)
      "version=#{v}"
    end

    task = live_task("token")

    assert {:ok, completion, _state} =
             Runtime.prepare(task, %{"Greeting" => workflow}, "worker", nil)

    marker = Enum.find(completion.commands, &(&1.command_type == :COMMAND_TYPE_RECORD_MARKER))
    assert {:record_marker_command_attributes, %{marker_name: "Version"}} = marker.attributes
  end

  test "execute_local_activity/3 runs inline and records a LocalActivity marker" do
    parent = self()

    workflow = fn _input ->
      result =
        Temporal.Workflow.execute_local_activity("Local", "arg",
          activity: fn arg ->
            send(parent, {:local_ran, arg})
            "local-#{arg}"
          end
        )

      "got=#{result}"
    end

    task = live_task("token")

    assert {:ok, completion, _state} =
             Runtime.prepare(task, %{"Greeting" => workflow}, "worker", nil)

    assert_receive {:local_ran, "arg"}

    marker = Enum.find(completion.commands, &(&1.command_type == :COMMAND_TYPE_RECORD_MARKER))

    assert {:record_marker_command_attributes, %{marker_name: "LocalActivity"}} =
             marker.attributes
  end

  test "side_effect/1 records its result and returns it" do
    parent = self()

    workflow = fn _input ->
      result =
        Temporal.Workflow.side_effect(fn ->
          send(parent, :ran)
          41 + 1
        end)

      "side=#{result}"
    end

    task = live_task("token")

    assert {:ok, completion, _state} =
             Runtime.prepare(task, %{"Greeting" => workflow}, "worker", nil)

    assert_receive :ran

    marker = Enum.find(completion.commands, &(&1.command_type == :COMMAND_TYPE_RECORD_MARKER))
    assert {:record_marker_command_attributes, %{marker_name: "SideEffect"}} = marker.attributes
  end

  test "mutable_side_effect/3 records a MutableSideEffect marker on first run" do
    workflow = fn _input ->
      result =
        Temporal.Workflow.mutable_side_effect(
          "config-key",
          fn -> %{"version" => 1} end,
          &(&1 == &2)
        )

      "value=#{result["version"]}"
    end

    task = live_task("token")

    assert {:ok, completion, _state} =
             Runtime.prepare(task, %{"Greeting" => workflow}, "worker", nil)

    marker = Enum.find(completion.commands, &(&1.command_type == :COMMAND_TYPE_RECORD_MARKER))

    assert {:record_marker_command_attributes, %{marker_name: "MutableSideEffect"}} =
             marker.attributes
  end

  test "mutable_side_effect/3 returns the recorded value on replay" do
    # A history where the workflow ran once (recorded the marker) then a second
    # task re-invokes it; replay must return the recorded value, not re-run.
    history = %History{
      events: [
        started_event(),
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
        },
        %HistoryEvent{
          event_id: 4,
          event_type: :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
          attributes:
            {:workflow_task_completed_event_attributes,
             %WorkflowTaskCompletedEventAttributes{scheduled_event_id: 2, started_event_id: 3}}
        },
        %HistoryEvent{
          event_id: 5,
          event_type: :EVENT_TYPE_MARKER_RECORDED,
          attributes:
            {:marker_recorded_event_attributes,
             %MarkerRecordedEventAttributes{
               marker_name: "MutableSideEffect",
               workflow_task_completed_event_id: 4,
               details: %{
                 "id" => Temporal.Payload.encode("config-key"),
                 "data" => Temporal.Payload.encode(%{"version" => 1})
               }
             }}
        },
        %HistoryEvent{
          event_id: 6,
          event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
          attributes:
            {:workflow_execution_completed_event_attributes,
             %WorkflowExecutionCompletedEventAttributes{
               workflow_task_completed_event_id: 4,
               result: Temporal.Payload.encode("value=1")
             }}
        }
      ]
    }

    workflow = fn _input ->
      result =
        Temporal.Workflow.mutable_side_effect(
          "config-key",
          fn -> %{"version" => 1} end,
          &(&1 == &2)
        )

      "value=#{result["version"]}"
    end

    assert {:ok, %HistoryCursor{status: :completed}} =
             Replay.replay(history, workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  test "deterministic_keys/1 sorts map keys" do
    assert Temporal.Workflow.deterministic_keys(%{"b" => 1, "a" => 2, "c" => 3}) == [
             "a",
             "b",
             "c"
           ]
  end

  test "replays a history containing a Version marker event" do
    history = %History{
      events: [
        started_event(),
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
        },
        %HistoryEvent{
          event_id: 4,
          event_type: :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
          attributes:
            {:workflow_task_completed_event_attributes,
             %WorkflowTaskCompletedEventAttributes{scheduled_event_id: 2, started_event_id: 3}}
        },
        %HistoryEvent{
          event_id: 5,
          event_type: :EVENT_TYPE_MARKER_RECORDED,
          attributes:
            {:marker_recorded_event_attributes,
             %MarkerRecordedEventAttributes{
               marker_name: "Version",
               workflow_task_completed_event_id: 4,
               details: %{
                 "fooChange" => Temporal.Payload.encode(0),
                 "changeId" => Temporal.Payload.encode("fooChange")
               }
             }}
        },
        %HistoryEvent{
          event_id: 6,
          event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
          attributes:
            {:workflow_execution_completed_event_attributes,
             %WorkflowExecutionCompletedEventAttributes{
               workflow_task_completed_event_id: 4,
               result: Temporal.Payload.encode("version=0")
             }}
        }
      ]
    }

    workflow = fn _input ->
      v = Temporal.Workflow.get_version("fooChange", Temporal.Workflow.get_version_default(), 0)
      "version=#{v}"
    end

    assert {:ok, %HistoryCursor{status: :completed}} =
             Replay.replay(history, workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
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

  defp started_event do
    %HistoryEvent{
      event_id: 1,
      event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
      attributes:
        {:workflow_execution_started_event_attributes,
         %WorkflowExecutionStartedEventAttributes{
           workflow_id: @workflow_id,
           workflow_type: %WorkflowType{name: "Greeting"},
           input: Temporal.Payload.encode("Temporal")
         }}
    }
  end
end
