defmodule Temporal.WorkflowRuntimeTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{Payload, Payloads, WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Workflowservice.V1.{
    PollWorkflowTaskQueueResponse,
    RespondWorkflowTaskFailedRequest
  }

  alias Temporal.Worker.Runtime

  test "dispatches a registered one-argument workflow and owns the task token" do
    input = Temporal.Payload.encode("Temporal")

    task = %PollWorkflowTaskQueueResponse{
      task_token: "opaque-token",
      workflow_execution: %WorkflowExecution{workflow_id: "workflow-id", run_id: "run-id"},
      workflow_type: %WorkflowType{name: "Greeting"},
      started_event_id: 3,
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
                 input: input
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

    assert {:ok, completion} =
             Runtime.complete(task, %{"Greeting" => fn name -> "hello #{name}" end}, "worker")

    assert completion.task_token == "opaque-token"
    assert [command] = completion.commands
    assert command.command_type == :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION

    {:complete_workflow_execution_command_attributes, attributes} = command.attributes
    assert {:ok, "hello Temporal"} = Temporal.Payload.decode(attributes.result)
  end

  test "returns explicit unsupported errors for histories beyond the single-run subset" do
    task = %PollWorkflowTaskQueueResponse{
      task_token: "token",
      workflow_execution: %WorkflowExecution{workflow_id: "workflow-id", run_id: "run-id"},
      workflow_type: %WorkflowType{name: "Greeting"},
      started_event_id: 3,
      history: %History{
        events: [
          %HistoryEvent{
            event_id: 1,
            event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
            attributes:
              {:workflow_execution_started_event_attributes,
               %WorkflowExecutionStartedEventAttributes{
                 workflow_id: "workflow-id",
                 workflow_type: %WorkflowType{name: "Greeting"}
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
          },
          %HistoryEvent{
            event_id: 4,
            event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
            attributes: {:workflow_execution_terminated_event_attributes, nil}
          }
        ]
      }
    }

    assert {:error,
            {:unsupported_history_event,
             %{
               event_id: 4,
               event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
               feature: :workflow_termination
             }}} =
             Runtime.complete(task, %{"Greeting" => fn -> :ok end}, "worker")
  end

  test "turns nondeterminism into a typed Workflow Task failure response" do
    task = %PollWorkflowTaskQueueResponse{
      task_token: "failure-token",
      workflow_execution: %WorkflowExecution{workflow_id: "workflow-id", run_id: "run-id"},
      workflow_type: %WorkflowType{name: "Greeting"},
      started_event_id: 3,
      history: %History{
        events: [
          %HistoryEvent{
            event_id: 1,
            event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
            attributes:
              {:workflow_execution_started_event_attributes,
               %WorkflowExecutionStartedEventAttributes{
                 workflow_id: "workflow-id",
                 workflow_type: %WorkflowType{name: "Greeting"}
               }}
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

    assert {:failed, %RespondWorkflowTaskFailedRequest{} = failure, {:invalid_history, _}} =
             Runtime.prepare_response(task, %{"Greeting" => fn -> :ok end}, "worker", nil,
               namespace: "default"
             )

    assert failure.task_token == "failure-token"
    assert failure.namespace == "default"
    assert failure.identity == "worker"
    assert failure.cause == :WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR
    assert failure.failure.message =~ "history event ID gap"
  end

  test "payload codec supports only the honest JSON/plain subset" do
    assert %Payloads{payloads: [%Payload{metadata: metadata}]} =
             payloads = Temporal.Payload.encode(42)

    assert metadata["encoding"] == "json/plain"
    assert {:ok, 42} = Temporal.Payload.decode(payloads)

    assert {:error, {:unsupported_payload_encoding, "binary/custom"}} =
             Temporal.Payload.decode(%Payloads{
               payloads: [%Payload{metadata: %{"encoding" => "binary/custom"}, data: "x"}]
             })
  end
end
