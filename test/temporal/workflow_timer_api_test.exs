defmodule Temporal.WorkflowTimerApiTest do
  use ExUnit.Case, async: true

  alias Google.Protobuf.Duration
  alias Temporal.Api.Command.V1.{Command, StartTimerCommandAttributes}
  alias Temporal.Workflow.{CancellationScope, Future, TimerOptions}

  setup do
    on_exit(&Temporal.Workflow.clear_context/0)

    Temporal.Workflow.put_context(%{
      workflow_type: "TimerWorkflow",
      task_queue: "timers",
      operation_index: 0,
      operations: %{},
      pending_commands: []
    })

    :ok
  end

  test "new_timer creates a deterministic awaitable and sleep blocks with StartTimer" do
    assert %Future{id: "timer-1", sequence: 1, type: :timer} =
             Temporal.Workflow.new_timer(12)

    assert {:temporal_workflow_blocked, [first, second]} =
             catch_throw(Temporal.Workflow.sleep(1_001))

    assert %Command{
             command_type: :COMMAND_TYPE_START_TIMER,
             attributes:
               {:start_timer_command_attributes,
                %StartTimerCommandAttributes{timer_id: "timer-1"}}
           } = first

    assert %Command{
             command_type: :COMMAND_TYPE_START_TIMER,
             attributes:
               {:start_timer_command_attributes,
                %StartTimerCommandAttributes{
                  timer_id: "timer-2",
                  start_to_fire_timeout: %Duration{seconds: 1, nanos: 1_000_000}
                }}
           } = second
  end

  test "zero duration resolves immediately without a command" do
    assert :ok = Temporal.Workflow.sleep(0)
    assert %Future{resolution: {:ok, :fired}} = Temporal.Workflow.new_timer(%Duration{})
  end

  test "negative and invalid protobuf durations fail deterministically without a command" do
    assert_raise ArgumentError, "timer duration must not be negative", fn ->
      Temporal.Workflow.sleep(-1)
    end

    assert_raise ArgumentError, "invalid protobuf Duration for timer", fn ->
      Temporal.Workflow.new_timer(%Duration{seconds: 1, nanos: 1_000_000_000})
    end

    assert_raise ArgumentError, "invalid protobuf Duration for timer", fn ->
      Temporal.Workflow.new_timer(%Duration{seconds: 315_576_000_001})
    end

    assert Temporal.Workflow.context().pending_commands == []
  end

  test "positive sub-millisecond durations round up to one millisecond" do
    assert %Future{id: "timer-1"} =
             Temporal.Workflow.new_timer(%Duration{nanos: 1}, %TimerOptions{summary: "tick"})

    assert {:temporal_workflow_blocked,
            [
              %Command{
                command_type: :COMMAND_TYPE_START_TIMER,
                user_metadata: user_metadata,
                attributes:
                  {:start_timer_command_attributes,
                   %StartTimerCommandAttributes{
                     timer_id: "timer-1",
                     start_to_fire_timeout: %Duration{nanos: 1_000_000}
                   }}
              }
            ]} =
             catch_throw(
               Temporal.Workflow.await(%Future{id: "timer-1", sequence: 1, type: :timer})
             )

    assert Jason.decode!(user_metadata.summary.data) == "tick"
  end

  test "resolved and canceled futures are deterministic" do
    Temporal.Workflow.put_context(%{
      workflow_type: "TimerWorkflow",
      task_queue: "timers",
      operation_index: 0,
      operations: %{
        1 => %{type: :timer, id: "timer-1", status: :fired},
        2 => %{type: :timer, id: "timer-2", status: :canceled}
      },
      pending_commands: []
    })

    assert :ok = Temporal.Workflow.await(%Future{id: "timer-1", sequence: 1, type: :timer})

    assert_raise Temporal.CanceledError, "Workflow timer canceled", fn ->
      Temporal.Workflow.await(%Future{id: "timer-2", sequence: 2, type: :timer})
    end
  end

  test "cancel before send removes StartTimer and is idempotent" do
    future = Temporal.Workflow.new_timer(100)
    assert :ok = Temporal.Workflow.cancel_timer(future)
    assert :ok = Temporal.Workflow.cancel_timer(future)
    assert Temporal.Workflow.context().pending_commands == []

    assert_raise Temporal.CanceledError, fn -> Temporal.Workflow.await(future) end
  end

  test "now returns logical history time rather than wall-clock time" do
    Temporal.Workflow.put_context(
      Map.put(
        Temporal.Workflow.context(),
        :logical_time,
        %Google.Protobuf.Timestamp{seconds: 1_700_000_000, nanos: 123_000_000}
      )
    )

    assert Temporal.Workflow.now() == ~U[2023-11-14 22:13:20.123000Z]
  end

  test "cancellation scopes cancel all attached unsent timers without commands" do
    assert %CancellationScope{id: "scope-1"} = scope = Temporal.Workflow.new_cancellation_scope()
    first = Temporal.Workflow.new_timer(10, cancellation_scope: scope)
    second = Temporal.Workflow.new_timer(20, cancellation_scope: scope)

    assert :ok = Temporal.Workflow.cancel_scope(scope)
    assert Temporal.Workflow.context().pending_commands == []
    assert_raise Temporal.CanceledError, fn -> Temporal.Workflow.await(first) end
    assert_raise Temporal.CanceledError, fn -> Temporal.Workflow.await(second) end
  end

  test "property: millisecond inputs round-trip through protobuf duration exactly" do
    for milliseconds <- [1, 2, 999, 1_000, 1_001, 59_999, 60_000, 86_400_001] do
      Temporal.Workflow.put_context(%{
        workflow_type: "TimerWorkflow",
        task_queue: "timers",
        operation_index: 0,
        operations: %{},
        pending_commands: []
      })

      Temporal.Workflow.new_timer(milliseconds)

      assert [
               %Command{
                 attributes:
                   {:start_timer_command_attributes,
                    %StartTimerCommandAttributes{start_to_fire_timeout: duration}}
               }
             ] = Temporal.Workflow.context().pending_commands

      assert duration.seconds * 1_000 + div(duration.nanos, 1_000_000) == milliseconds
    end
  end
end
