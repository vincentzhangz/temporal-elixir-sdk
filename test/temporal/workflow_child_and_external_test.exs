defmodule Temporal.WorkflowChildAndExternalTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Command.V1.Command
  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    ChildWorkflowExecutionCompletedEventAttributes,
    ChildWorkflowExecutionStartedEventAttributes,
    History,
    HistoryEvent,
    StartChildWorkflowExecutionInitiatedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse
  alias Temporal.Worker.Runtime
  alias Temporal.Workflow.{HistoryCursor, Replay}

  @workflow_id "parent-workflow"
  @run_id "6bb2e5fd-7305-4c5c-9f43-b5470f53d573"

  test "execute_child_workflow/3 emits a StartChildWorkflowExecution command" do
    task = live_task("task-token")

    assert {:ok, completion, _cursor} =
             Runtime.prepare(
               task,
               %{"Greeting" => &parent_workflow/1},
               "worker",
               nil
             )

    assert [%Command{command_type: :COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION} = command] =
             completion.commands

    assert {:start_child_workflow_execution_command_attributes, attributes} = command.attributes
    assert attributes.workflow_id == "child-1"
    assert attributes.workflow_type.name == "ChildGreeting"
    assert attributes.task_queue.name == "child-queue"
  end

  test "replays a parent workflow that waits on a completed child workflow" do
    history = %History{
      events: [
        started_event(),
        event(2, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(3, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(2)),
        event(4, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(2, 3)),
        event(
          5,
          :EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
          {:start_child_workflow_execution_initiated_event_attributes,
           %StartChildWorkflowExecutionInitiatedEventAttributes{
             workflow_id: "child-1",
             workflow_type: %WorkflowType{name: "ChildGreeting"},
             task_queue: %Temporal.Api.Taskqueue.V1.TaskQueue{name: "child-queue"},
             input: Temporal.Payload.encode("Temporal"),
             workflow_task_completed_event_id: 4
           }}
        ),
        event(
          6,
          :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
          {:child_workflow_execution_started_event_attributes,
           %ChildWorkflowExecutionStartedEventAttributes{
             initiated_event_id: 5,
             workflow_execution: %WorkflowExecution{
               workflow_id: "child-1",
               run_id: "child-run-1"
             },
             workflow_type: %WorkflowType{name: "ChildGreeting"}
           }}
        ),
        event(
          7,
          :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
          {:child_workflow_execution_completed_event_attributes,
           %ChildWorkflowExecutionCompletedEventAttributes{
             result: Temporal.Payload.encode("hello from child"),
             initiated_event_id: 5,
             started_event_id: 6,
             workflow_execution: %WorkflowExecution{
               workflow_id: "child-1",
               run_id: "child-run-1"
             },
             workflow_type: %WorkflowType{name: "ChildGreeting"}
           }}
        ),
        event(8, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(9, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(8)),
        event(10, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(8, 9)),
        event(
          11,
          :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
          {:workflow_execution_completed_event_attributes,
           %Temporal.Api.History.V1.WorkflowExecutionCompletedEventAttributes{
             workflow_task_completed_event_id: 10,
             result: Temporal.Payload.encode("hello from child")
           }}
        )
      ]
    }

    assert {:ok, %HistoryCursor{status: :completed, next_event_id: 12}} =
             Replay.replay(history, &parent_workflow/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  test "signal_external_workflow/5 emits a SignalExternalWorkflowExecution command" do
    task = live_task("task-token")

    assert {:ok, completion, _cursor} =
             Runtime.prepare(
               task,
               %{"Greeting" => &external_signal_workflow/1},
               "worker",
               nil
             )

    assert [%Command{command_type: :COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION} = command] =
             completion.commands

    assert {:signal_external_workflow_execution_command_attributes, attributes} =
             command.attributes

    assert attributes.execution.workflow_id == "target-workflow"
    assert attributes.signal_name == "deposit"
    assert {:ok, 100} = Temporal.Payload.decode(attributes.input)
  end

  test "replays an external signal that is recorded as signaled" do
    history = %History{
      events: [
        started_event(),
        event(2, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(3, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(2)),
        event(4, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(2, 3)),
        event(
          5,
          :EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
          {:signal_external_workflow_execution_initiated_event_attributes,
           %Temporal.Api.History.V1.SignalExternalWorkflowExecutionInitiatedEventAttributes{
             workflow_task_completed_event_id: 4,
             workflow_execution: %WorkflowExecution{
               workflow_id: "target-workflow",
               run_id: ""
             },
             signal_name: "deposit",
             input: Temporal.Payload.encode(100)
           }}
        ),
        event(
          6,
          :EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
          {:external_workflow_execution_signaled_event_attributes,
           %Temporal.Api.History.V1.ExternalWorkflowExecutionSignaledEventAttributes{
             initiated_event_id: 5,
             workflow_execution: %WorkflowExecution{
               workflow_id: "target-workflow",
               run_id: ""
             }
           }}
        ),
        event(7, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(8, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(7)),
        event(9, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(7, 8)),
        event(
          10,
          :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
          {:workflow_execution_completed_event_attributes,
           %Temporal.Api.History.V1.WorkflowExecutionCompletedEventAttributes{
             workflow_task_completed_event_id: 9,
             result: Temporal.Payload.encode(:ok)
           }}
        )
      ]
    }

    assert {:ok, %HistoryCursor{status: :completed, next_event_id: 11}} =
             Replay.replay(history, &external_signal_workflow/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  defp parent_workflow(_input) do
    Temporal.Workflow.execute_child_workflow("ChildGreeting", "Temporal",
      task_queue: "child-queue",
      workflow_id: "child-1"
    )
  end

  test "cancel_scope/1 emits RequestCancelExternalWorkflowExecution for scoped children" do
    workflow = fn _input ->
      scope = Temporal.Workflow.new_cancellation_scope()

      child =
        Temporal.Workflow.execute_child_workflow("ChildGreeting", "Temporal",
          task_queue: "child-queue",
          workflow_id: "child-1",
          cancellation_scope: scope
        )

      Temporal.Workflow.cancel_scope(scope)
      child
    end

    # History: the child is initiated and started; the workflow resumes, calls
    # cancel_scope (emitting the external-cancel command), and completes.
    history = %History{
      events: [
        started_event(),
        event(2, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(3, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(2)),
        event(4, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(2, 3)),
        event(
          5,
          :EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
          {:start_child_workflow_execution_initiated_event_attributes,
           %StartChildWorkflowExecutionInitiatedEventAttributes{
             workflow_id: "child-1",
             workflow_type: %WorkflowType{name: "ChildGreeting"},
             task_queue: %Temporal.Api.Taskqueue.V1.TaskQueue{name: "child-queue"},
             input: Temporal.Payload.encode("Temporal"),
             workflow_task_completed_event_id: 4
           }}
        ),
        event(
          6,
          :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
          {:child_workflow_execution_started_event_attributes,
           %ChildWorkflowExecutionStartedEventAttributes{
             initiated_event_id: 5,
             workflow_execution: %WorkflowExecution{workflow_id: "child-1", run_id: "child-run"},
             workflow_type: %WorkflowType{name: "ChildGreeting"}
           }}
        ),
        event(
          7,
          :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
          {:child_workflow_execution_completed_event_attributes,
           %ChildWorkflowExecutionCompletedEventAttributes{
             result: Temporal.Payload.encode("done"),
             initiated_event_id: 5,
             started_event_id: 6,
             workflow_execution: %WorkflowExecution{workflow_id: "child-1", run_id: "child-run"},
             workflow_type: %WorkflowType{name: "ChildGreeting"}
           }}
        ),
        event(8, :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduled_attributes()),
        event(9, :EVENT_TYPE_WORKFLOW_TASK_STARTED, started_attributes(8)),
        event(10, :EVENT_TYPE_WORKFLOW_TASK_COMPLETED, completed_attributes(8, 9)),
        event(
          11,
          :EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
          {:request_cancel_external_workflow_execution_initiated_event_attributes,
           %Temporal.Api.History.V1.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
             workflow_task_completed_event_id: 10,
             workflow_execution: %WorkflowExecution{workflow_id: "child-1", run_id: ""},
             child_workflow_only: true
           }}
        ),
        event(
          12,
          :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
          {:workflow_execution_completed_event_attributes,
           %Temporal.Api.History.V1.WorkflowExecutionCompletedEventAttributes{
             workflow_task_completed_event_id: 10,
             result: Temporal.Payload.encode("done")
           }}
        )
      ]
    }

    assert {:ok, %HistoryCursor{status: :completed}} =
             Replay.replay(history, workflow,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  test "execute_nexus_operation/4 emits a ScheduleNexusOperation command and replays completion" do
    workflow = fn _input ->
      client = Temporal.Workflow.new_nexus_client("my-endpoint", "my-service")
      Temporal.Workflow.execute_nexus_operation(client, "op", %{"x" => 1})
    end

    task = live_task("token")

    assert {:ok, completion, _state} =
             Runtime.prepare(task, %{"Greeting" => workflow}, "worker", nil)

    assert [%Command{command_type: :COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION} = command] =
             completion.commands

    assert {:schedule_nexus_operation_command_attributes,
            %{endpoint: "my-endpoint", service: "my-service", operation: "op"}} =
             command.attributes
  end

  defp external_signal_workflow(_input) do
    Temporal.Workflow.signal_external_workflow("target-workflow", "", "deposit", 100)
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
