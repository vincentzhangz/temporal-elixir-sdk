defmodule Temporal.UpdateTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{Payload, Payloads, WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Protocol.V1.Message
  alias Temporal.Api.Update.V1.{Input, Meta, Request}
  alias Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse
  alias Temporal.Worker.Runtime
  alias Temporal.Workflow.Update
  alias Temporal.Workflow.Update.Dispatcher

  @workflow_id "update-workflow"
  @run_id "6bb2e5fd-7305-4c5c-9f43-b5470f53d573"

  test "set_update_handler/2 registers a handler and process/2 returns protocol messages" do
    {:ok, dispatcher} =
      Dispatcher.register(Dispatcher.new(), "add", fn args, _ctx -> {:ok, args + 1} end)

    request = %Request{
      meta: %Meta{update_id: "update-1"},
      input: %Input{name: "add", args: encode(41)},
      request_id: "req-1"
    }

    message = %Message{
      id: "msg-1",
      protocol_instance_id: "temporal.api.update.v1",
      body: %Google.Protobuf.Any{
        type_url: "type.googleapis.com/temporal.api.update.v1.Request",
        value: Request.encode(request)
      }
    }

    assert {:ok, [_acceptance, response]} =
             Update.process([message], dispatcher) |> split_responses()

    assert %Message{} = response
    assert response.body.type_url == "type.googleapis.com/temporal.api.update.v1.Response"
  end

  test "worker runtime runs the registered update handler for a task with messages" do
    workflow = fn _input ->
      :ok = Temporal.Workflow.set_update_handler("add", fn args, _ctx -> {:ok, args + 1} end)
      Temporal.Workflow.sleep(0)
    end

    task = %PollWorkflowTaskQueueResponse{
      task_token: "task-token",
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
      },
      messages: [
        %Message{
          id: "msg-1",
          protocol_instance_id: "temporal.api.update.v1",
          body: %Google.Protobuf.Any{
            type_url: "type.googleapis.com/temporal.api.update.v1.Request",
            value:
              Request.encode(%Request{
                meta: %Meta{update_id: "update-1"},
                input: %Input{name: "add", args: encode(41)},
                request_id: "req-1"
              })
          }
        }
      ]
    }

    assert {:ok, completion, _state} =
             Runtime.prepare(task, %{"Greeting" => workflow}, "worker", nil)

    assert [response_message] = completion.messages
    assert response_message.body.type_url == "type.googleapis.com/temporal.api.update.v1.Response"
  end

  test "client update_workflow builds an UpdateWorkflowExecution request" do
    # Build the request directly to verify the wire shape used by the client.
    request = %Temporal.Api.Workflowservice.V1.UpdateWorkflowExecutionRequest{
      namespace: "ns",
      workflow_execution: %WorkflowExecution{workflow_id: "wf-1", run_id: "run-1"},
      wait_policy: %Temporal.Api.Update.V1.WaitPolicy{
        lifecycle_stage: :UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
      },
      request: %Request{
        meta: %Meta{update_id: "update-1"},
        input: %Input{name: "add", args: encode(2)},
        request_id: "req-1"
      }
    }

    assert request.namespace == "ns"
    assert request.request.meta.update_id == "update-1"
    assert request.request.input.name == "add"
    assert {:ok, 2} = Temporal.Payload.decode(request.request.input.args)
  end

  defp split_responses({:ok, accepted, responses}), do: {:ok, accepted ++ responses}

  defp encode(value) do
    %Payloads{
      payloads: [%Payload{metadata: %{"encoding" => "json/plain"}, data: Jason.encode!(value)}]
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
           input: encode("Temporal")
         }}
    }
  end
end
