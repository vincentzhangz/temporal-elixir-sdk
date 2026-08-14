defmodule Temporal.WorkflowTimerMachineTest do
  use ExUnit.Case, async: true

  alias Google.Protobuf.Duration

  alias Temporal.Api.Command.V1.{
    CancelTimerCommandAttributes,
    Command,
    StartTimerCommandAttributes
  }

  alias Temporal.Api.History.V1.{
    HistoryEvent,
    TimerCanceledEventAttributes,
    TimerFiredEventAttributes,
    TimerStartedEventAttributes
  }

  alias Temporal.Workflow.Machines.Timer

  @timeout %Duration{seconds: 12, nanos: 345}

  test "creates a deterministic StartTimer command from the timer sequence" do
    assert {:ok, machine} = Timer.new(7, @timeout)
    assert machine.timer_id == "timer-7"
    assert machine.sequence == 7

    assert {:ok, started, [command], nil} = Timer.start(machine, 41)
    assert started.state == :start_command_created

    assert %Command{
             command_type: :COMMAND_TYPE_START_TIMER,
             attributes:
               {:start_timer_command_attributes,
                %StartTimerCommandAttributes{
                  timer_id: "timer-7",
                  start_to_fire_timeout: @timeout
                }}
           } = command
  end

  test "correlates TimerStarted and resolves TimerFired in replay and live modes" do
    for mode <- [:replay, :live] do
      machine = started_command_machine(1, 4)

      assert {:ok, started, nil} =
               Timer.apply_event(machine, timer_started(5, "timer-1", @timeout, 4), mode)

      assert started.started_event_id == 5
      assert started.last_event_id == 5

      assert {:ok, fired, :fired} =
               Timer.apply_event(started, timer_fired(9, "timer-1", 5), mode)

      assert fired.state == :fired
      assert fired.last_event_id == 9
    end
  end

  test "creates CancelTimer and resolves its correlated TimerCanceled event" do
    started = started_machine(2, 4, 5)

    assert {:ok, canceling, [command], nil} = Timer.cancel(started, 10)

    assert %Command{
             command_type: :COMMAND_TYPE_CANCEL_TIMER,
             attributes:
               {:cancel_timer_command_attributes,
                %CancelTimerCommandAttributes{timer_id: "timer-2"}}
           } = command

    assert {:ok, canceled, :canceled} =
             Timer.apply_event(canceling, timer_canceled(11, "timer-2", 5, 10), :replay)

    assert canceled.state == :canceled
  end

  test "cancellation before TimerStarted is local and emits no server command" do
    assert {:ok, initialized} = Timer.new(3, @timeout)
    assert {:ok, canceled, [], :canceled} = Timer.cancel(initialized)
    assert canceled.state == :canceled

    start_created = started_command_machine(4, 8)
    assert {:ok, canceled, [], :canceled} = Timer.cancel(start_created)
    assert canceled.state == :canceled
  end

  test "zero duration resolves locally and terminal timers ignore later cancellation" do
    assert {:ok, timer} = Timer.new(11, %Duration{})
    assert {:ok, fired, [], :fired} = Timer.start(timer, 4)
    assert {:ok, ^fired, [], :fired} = Timer.cancel(fired)
    assert {:ok, ^fired, [], :fired} = Timer.cancel(fired, 8)

    assert {:ok, canceled} = Timer.new(12, @timeout)
    assert {:ok, canceled, [], :canceled} = Timer.cancel(canceled)
    assert {:ok, ^canceled, [], :canceled} = Timer.cancel(canceled)
  end

  test "TimerFired wins a permitted fire-vs-cancel race" do
    started = started_machine(5, 4, 5)
    assert {:ok, canceling, [_cancel], nil} = Timer.cancel(started, 10)

    assert {:ok, fired, :fired} =
             Timer.apply_event(canceling, timer_fired(11, "timer-5", 5), :live)

    assert fired.state == :fired
  end

  test "rejects timer command and event semantic mismatches as nondeterminism" do
    machine = started_command_machine(6, 4)

    for {field, event} <- [
          {:timer_id, timer_started(5, "timer-wrong", @timeout, 4)},
          {:start_to_fire_timeout, timer_started(5, "timer-6", %Duration{seconds: 99}, 4)},
          {:workflow_task_completed_event_id, timer_started(5, "timer-6", @timeout, 99)}
        ] do
      assert {:error,
              {:nondeterminism, %{field: ^field, mode: :replay, event_id: 5, timer_id: "timer-6"}}} =
               Timer.apply_event(machine, event, :replay)
    end
  end

  test "rejects mismatched fired and canceled correlations as nondeterminism" do
    started = started_machine(7, 4, 5)

    assert {:error,
            {:nondeterminism, %{field: :started_event_id, expected: 5, actual: 99, event_id: 9}}} =
             Timer.apply_event(started, timer_fired(9, "timer-7", 99), :replay)

    assert {:ok, canceling, [_cancel], nil} = Timer.cancel(started, 10)

    assert {:error,
            {:nondeterminism,
             %{field: :workflow_task_completed_event_id, expected: 10, actual: 88}}} =
             Timer.apply_event(canceling, timer_canceled(11, "timer-7", 5, 88), :replay)
  end

  test "rejects duplicate and out-of-order timer events" do
    started = started_machine(8, 4, 5)

    assert {:error, {:nondeterminism, %{reason: :duplicate_event, event_id: 5}}} =
             Timer.apply_event(started, timer_started(5, "timer-8", @timeout, 4), :replay)

    assert {:error,
            {:nondeterminism, %{reason: :out_of_order_event, event_id: 4, last_event_id: 5}}} =
             Timer.apply_event(started, timer_fired(4, "timer-8", 5), :replay)
  end

  test "rejects nonpositive and causally impossible timer event IDs" do
    machine = started_command_machine(8, 4)

    assert {:error, {:nondeterminism, %{reason: :invalid_event_id, event_id: 0}}} =
             Timer.apply_event(machine, timer_started(0, "timer-8", @timeout, 4), :replay)

    assert {:error,
            {:nondeterminism,
             %{
               reason: :out_of_order_correlation,
               event_id: 4,
               correlated_event_id: 4
             }}} =
             Timer.apply_event(machine, timer_started(4, "timer-8", @timeout, 4), :replay)
  end

  test "rejects events in illegal states and malformed timer events" do
    assert {:ok, machine} = Timer.new(9, @timeout)

    assert {:error,
            {:nondeterminism,
             %{
               reason: :illegal_transition,
               state: :initialized,
               event_type: :EVENT_TYPE_TIMER_FIRED
             }}} =
             Timer.apply_event(machine, timer_fired(5, "timer-9", 4), :live)

    malformed = %HistoryEvent{event_id: 5, event_type: :EVENT_TYPE_TIMER_STARTED}

    assert {:error,
            {:nondeterminism, %{reason: :malformed_event, event_type: :EVENT_TYPE_TIMER_STARTED}}} =
             Timer.apply_event(started_command_machine(9, 4), malformed, :replay)
  end

  test "validates deterministic sequence, duration, modes, and command transitions" do
    assert {:error, {:invalid_sequence, 0}} = Timer.new(0, @timeout)

    assert {:error, {:invalid_timeout, %Duration{seconds: -1}}} =
             Timer.new(1, %Duration{seconds: -1})

    assert {:error, {:invalid_timeout, %Duration{seconds: 315_576_000_001}}} =
             Timer.new(1, %Duration{seconds: 315_576_000_001})

    assert {:ok, machine} = Timer.new(10, @timeout, timer_id: "deadline")
    assert machine.timer_id == "deadline"
    assert {:error, {:invalid_timer_id, ""}} = Timer.new(10, @timeout, timer_id: "")

    assert {:error, {:illegal_transition, :initialized, :cancel_after_start}} =
             Timer.cancel(machine, 12)

    assert {:error, {:invalid_mode, :offline}} =
             Timer.apply_event(machine, timer_fired(5, "deadline", 4), :offline)
  end

  defp started_command_machine(sequence, workflow_task_completed_event_id) do
    {:ok, machine} = Timer.new(sequence, @timeout)
    {:ok, machine, [_command], nil} = Timer.start(machine, workflow_task_completed_event_id)
    machine
  end

  defp started_machine(sequence, workflow_task_completed_event_id, started_event_id) do
    machine = started_command_machine(sequence, workflow_task_completed_event_id)

    {:ok, machine, nil} =
      Timer.apply_event(
        machine,
        timer_started(
          started_event_id,
          "timer-#{sequence}",
          @timeout,
          workflow_task_completed_event_id
        ),
        :replay
      )

    machine
  end

  defp timer_started(event_id, timer_id, timeout, workflow_task_completed_event_id) do
    %HistoryEvent{
      event_id: event_id,
      event_type: :EVENT_TYPE_TIMER_STARTED,
      attributes:
        {:timer_started_event_attributes,
         %TimerStartedEventAttributes{
           timer_id: timer_id,
           start_to_fire_timeout: timeout,
           workflow_task_completed_event_id: workflow_task_completed_event_id
         }}
    }
  end

  defp timer_fired(event_id, timer_id, started_event_id) do
    %HistoryEvent{
      event_id: event_id,
      event_type: :EVENT_TYPE_TIMER_FIRED,
      attributes:
        {:timer_fired_event_attributes,
         %TimerFiredEventAttributes{
           timer_id: timer_id,
           started_event_id: started_event_id
         }}
    }
  end

  defp timer_canceled(
         event_id,
         timer_id,
         started_event_id,
         workflow_task_completed_event_id
       ) do
    %HistoryEvent{
      event_id: event_id,
      event_type: :EVENT_TYPE_TIMER_CANCELED,
      attributes:
        {:timer_canceled_event_attributes,
         %TimerCanceledEventAttributes{
           timer_id: timer_id,
           started_event_id: started_event_id,
           workflow_task_completed_event_id: workflow_task_completed_event_id
         }}
    }
  end
end
