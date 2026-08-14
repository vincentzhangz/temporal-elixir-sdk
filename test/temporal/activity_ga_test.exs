defmodule Temporal.ActivityGATest do
  use ExUnit.Case, async: true

  alias Temporal.Activity.Runtime
  alias Temporal.Api.Common.V1.{ActivityType, RetryPolicy, WorkflowExecution}
  alias Temporal.Api.Workflowservice.V1.PollActivityTaskQueueResponse

  test "exposes Activity info and heartbeat details while executing" do
    parent = self()

    activity = fn _argument ->
      info = Temporal.Activity.info()
      send(parent, {:info, info})
      assert {:ok, "checkpoint"} = Temporal.Activity.heartbeat_details()
      :done
    end

    assert {:ok, _completion, _fence} =
             Runtime.prepare(
               task(heartbeat_details: Temporal.Payload.encode("checkpoint")),
               %{"Example" => activity},
               "worker",
               nil,
               namespace: "payments",
               task_queue: "activities"
             )

    assert_receive {:info,
                    %Temporal.Activity.Info{
                      namespace: "payments",
                      workflow_id: "workflow",
                      run_id: "run",
                      activity_id: "activity-1",
                      activity_type: "Example",
                      task_queue: "activities",
                      attempt: 2,
                      heartbeat_timeout: %Google.Protobuf.Duration{seconds: 5}
                    }}
  end

  test "encodes heartbeat details and propagates cancellation from the server" do
    parent = self()

    heartbeat = fn request ->
      send(parent, {:heartbeat, request})
      {:ok, %{cancel_requested: true}}
    end

    activity = fn _argument ->
      Temporal.Activity.heartbeat(%{"offset" => 42})
      flunk("heartbeat cancellation must interrupt the Activity")
    end

    assert {:canceled, canceled, _fence} =
             Runtime.prepare(
               task(),
               %{"Example" => activity},
               "worker",
               nil,
               namespace: "default",
               task_queue: "activities",
               heartbeat: heartbeat
             )

    assert_receive {:heartbeat, request}
    assert request.task_token == "token"
    assert {:ok, %{"offset" => 42}} = Temporal.Payload.decode(request.details)
    assert canceled.task_token == "token"
    assert {:ok, %{"offset" => 42}} = Temporal.Payload.decode(canceled.details)
  end

  test "maps typed ApplicationError fields to an official Failure" do
    activity = fn _argument ->
      raise Temporal.ApplicationError,
        message: "card declined",
        type: "PaymentDeclined",
        details: %{"code" => "do_not_honor"},
        non_retryable: true,
        next_retry_delay: 7
    end

    assert {:error_response, response, _fence} =
             Runtime.prepare(task(), %{"Example" => activity}, "worker", nil)

    assert response.failure.message == "card declined"

    assert {:application_failure_info, info} = response.failure.failure_info
    assert info.type == "PaymentDeclined"
    assert info.non_retryable
    assert info.next_retry_delay.seconds == 7
    assert {:ok, %{"code" => "do_not_honor"}} = Temporal.Payload.decode(info.details)
  end

  test "validates heartbeat timeout and retry policy options" do
    Temporal.Workflow.put_context(%{
      activity_outcomes: %{},
      activity_index: 0,
      task_queue: "workflow-queue"
    })

    on_exit(&Temporal.Workflow.clear_context/0)

    assert_raise ArgumentError, ~r/heartbeat_timeout/, fn ->
      Temporal.Workflow.execute_activity("Example", nil,
        task_queue: "activities",
        start_to_close_timeout: 10,
        heartbeat_timeout: 0
      )
    end

    assert_raise ArgumentError, ~r/maximum_attempts/, fn ->
      Temporal.Workflow.execute_activity("Example", nil,
        task_queue: "activities",
        start_to_close_timeout: 10,
        retry_policy: [maximum_attempts: -1]
      )
    end
  end

  test "uses deterministic IDs for sequential Activities" do
    Temporal.Workflow.put_context(%{
      activity_outcomes: %{1 => {:ok, "first-result"}},
      activity_index: 0,
      task_queue: "workflow-queue"
    })

    on_exit(&Temporal.Workflow.clear_context/0)

    command =
      catch_throw(
        (
          assert "first-result" ==
                   Temporal.Workflow.execute_activity("First", nil,
                     task_queue: "activities",
                     start_to_close_timeout: 10
                   )

          Temporal.Workflow.execute_activity("Second", nil,
            task_queue: "activities",
            start_to_close_timeout: 10
          )
        )
      )

    assert {:temporal_workflow_blocked, command} = command
    {:schedule_activity_task_command_attributes, attributes} = command.attributes
    assert attributes.activity_id == "activity-2"
  end

  test "emits request-cancel command for a scheduled Activity" do
    Temporal.Workflow.put_context(%{
      activity_outcomes: %{},
      activity_index: 0,
      task_queue: "workflow-queue"
    })

    on_exit(&Temporal.Workflow.clear_context/0)

    assert {:temporal_workflow_blocked, command} =
             catch_throw(Temporal.Workflow.request_cancel_activity(42))

    assert command.command_type == :COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK
    {:request_cancel_activity_task_command_attributes, attributes} = command.attributes
    assert attributes.scheduled_event_id == 42
  end

  defp task(overrides \\ []) do
    defaults = [
      task_token: "token",
      workflow_namespace: "default",
      workflow_execution: %WorkflowExecution{workflow_id: "workflow", run_id: "run"},
      activity_id: "activity-1",
      activity_type: %ActivityType{name: "Example"},
      attempt: 2,
      input: Temporal.Payload.encode("input"),
      heartbeat_details: nil,
      heartbeat_timeout: %Google.Protobuf.Duration{seconds: 5},
      start_to_close_timeout: %Google.Protobuf.Duration{seconds: 30},
      schedule_to_close_timeout: %Google.Protobuf.Duration{seconds: 60},
      retry_policy: %RetryPolicy{maximum_attempts: 3}
    ]

    struct!(PollActivityTaskQueueResponse, Keyword.merge(defaults, overrides))
  end
end
