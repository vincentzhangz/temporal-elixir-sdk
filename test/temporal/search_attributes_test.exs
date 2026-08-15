defmodule Temporal.SearchAttributesAndConverterTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{
    Payload,
    Payloads,
    SearchAttributes,
    WorkflowExecution,
    WorkflowType
  }

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    UpsertWorkflowSearchAttributesEventAttributes,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse
  alias Temporal.Worker.Runtime
  alias Temporal.Workflow.{HistoryCursor, Replay}

  @workflow_id "attrs-workflow"
  @run_id "6bb2e5fd-7305-4c5c-9f43-b5470f53d573"

  test "upsert_search_attributes/1 and upsert_memo/1 emit the right commands" do
    workflow = fn _input ->
      :ok =
        Temporal.Workflow.upsert_search_attributes(%{
          "CustomIntField" => 1,
          "CustomBoolField" => true
        })

      :ok = Temporal.Workflow.upsert_memo(%{"Key1" => "value1"})
      Temporal.Workflow.sleep(0)
    end

    task = live_task("token")

    assert {:ok, completion, _state} =
             Runtime.prepare(task, %{"Greeting" => workflow}, "worker", nil)

    commands = completion.commands

    upsert =
      Enum.find(commands, &(&1.command_type == :COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES))

    assert {:upsert_workflow_search_attributes_command_attributes, %{search_attributes: sa}} =
             upsert.attributes

    assert {:ok, 1} =
             Temporal.Payload.decode(%Payloads{
               payloads: [Map.fetch!(sa.indexed_fields, "CustomIntField")]
             })

    modify = Enum.find(commands, &(&1.command_type == :COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES))

    assert {:modify_workflow_properties_command_attributes, %{upserted_memo: memo}} =
             modify.attributes

    assert {:ok, "value1"} =
             Temporal.Payload.decode(%Payloads{payloads: [Map.fetch!(memo.fields, "Key1")]})
  end

  test "the default Converter round-trips json/plain payloads" do
    payloads = Temporal.Converter.encode(%{"a" => 1, "b" => [1, 2]})
    assert {:ok, %{"a" => 1, "b" => [1, 2]}} = Temporal.Converter.decode(payloads)
    assert {:ok, nil} = Temporal.Converter.decode(nil)
  end

  defmodule ReverseConverter do
    @behaviour Temporal.Converter

    @impl true
    def encode(value), do: Temporal.Converter.encode(value)

    @impl true
    def decode(payloads) do
      case Temporal.Converter.decode(payloads) do
        {:ok, value} -> {:ok, value}
        error -> error
      end
    end
  end

  test "a custom converter module can be dispatched through the behaviour" do
    payloads = Temporal.Converter.encode("custom", ReverseConverter)
    assert {:ok, "custom"} = Temporal.Converter.decode(payloads, ReverseConverter)
  end

  test "the Base64 codec round-trips payloads through the codec chain" do
    payloads = Temporal.Payload.encode(%{"secret" => "value"}, [Temporal.Codec.Base64])

    # The encoded data is base64-wrapped.
    [%Payload{data: data}] = payloads.payloads
    assert is_binary(data)

    assert {:ok, %{"secret" => "value"}} =
             Temporal.Payload.decode(payloads, [Temporal.Codec.Base64])
  end

  test "decode with codecs handles nil payloads" do
    assert {:ok, nil} = Temporal.Payload.decode(nil, [Temporal.Codec.Base64])
  end

  test "replays a history containing an upserted search attribute event" do
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
             %WorkflowTaskCompletedEventAttributes{
               scheduled_event_id: 2,
               started_event_id: 3
             }}
        },
        %HistoryEvent{
          event_id: 5,
          event_type: :EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
          attributes:
            {:upsert_workflow_search_attributes_event_attributes,
             %UpsertWorkflowSearchAttributesEventAttributes{
               workflow_task_completed_event_id: 4,
               search_attributes: %SearchAttributes{
                 indexed_fields: %{
                   "CustomIntField" => %Payload{
                     metadata: %{"encoding" => "json/plain"},
                     data: "1"
                   }
                 }
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
               result: Temporal.Payload.encode(:ok)
             }}
        }
      ]
    }

    assert {:ok, %HistoryCursor{status: :completed}} =
             Replay.replay(history, &upsert_workflow/1,
               workflow_id: @workflow_id,
               run_id: @run_id
             )
  end

  defp upsert_workflow(_input) do
    :ok = Temporal.Workflow.upsert_search_attributes(%{"CustomIntField" => 1})
    :ok
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
           input: %Payloads{
             payloads: [%Payload{metadata: %{"encoding" => "json/plain"}, data: "null"}]
           }
         }}
    }
  end
end
