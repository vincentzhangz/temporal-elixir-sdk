defmodule Temporal.WorkflowKernelV2Test do
  use ExUnit.Case, async: true

  alias Temporal.Api.Command.V1.Command
  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Workflowservice.V1.{
    GetWorkflowExecutionHistoryResponse,
    PollWorkflowTaskQueueResponse
  }

  alias Temporal.Workflow.TaskKernel.{
    Activation,
    HistoryPaginator,
    MachineRegistry,
    Reducer,
    State
  }

  test "activation preserves typed jobs and deterministic insertion order" do
    activation =
      Activation.new(3, "token")
      |> Activation.add_job(:workflow, :run)
      |> Activation.add_job(:signal, %{name: "wake"})
      |> Activation.add_job(:activity, %{id: "activity-1"})

    assert Enum.map(activation.jobs, & &1.type) == [:workflow, :signal, :activity]
    assert Enum.map(activation.jobs, & &1.sequence) == [1, 2, 3]
  end

  test "typed registry dispatches an event to exactly one machine" do
    registry =
      MachineRegistry.new()
      |> MachineRegistry.register_type(:timer, fn machine, event, mode ->
        {:ok, Map.put(machine, :event, {event, mode}), :fired}
      end)
      |> elem(1)
      |> MachineRegistry.register(:timer, "timer-1", %{state: :started})
      |> elem(1)

    assert {:ok, next, :fired} =
             MachineRegistry.dispatch(registry, :timer, "timer-1", :timer_fired, :replay)

    assert {:ok, %{event: {:timer_fired, :replay}}} =
             MachineRegistry.fetch(next, :timer, "timer-1")
  end

  test "one reducer owns live history cursor and ordered command batch" do
    state = State.new(namespace: "default", workflow_id: "workflow-id", run_id: "run-id")

    assert {:ok, next} =
             Reducer.reduce_task(
               state,
               task(),
               %{"Greeting" => fn name -> "hello #{name}" end}
             )

    assert next.last_event_id == 3
    assert next.next_event_id == 4
    assert next.task_token == "token"

    assert [%Command{command_type: :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION}] =
             State.commands(next)

    assert Enum.map(next.activation.jobs, & &1.type) == [:workflow, :terminal]

    assert {:ok, %{command_type: :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION}} =
             MachineRegistry.fetch(next.machines, :terminal, "command-1")
  end

  test "reducer fences stale tokens without mutating persisted run state" do
    state = State.new(namespace: "default", workflow_id: "workflow-id", run_id: "run-id")
    assert {:ok, owned} = Reducer.reduce_task(state, task(), %{"Greeting" => fn -> :ok end})

    stale = %{task() | task_token: "different-token"}

    assert {:error, {:task_token_mismatch, %{started_event_id: 3}}} =
             Reducer.reduce_task(owned, stale, %{"Greeting" => fn -> :ok end})

    assert owned.task_token == "token"
    assert owned.last_event_id == 3
  end

  test "offline and live entry points reduce identical terminal history equally" do
    history = completed_history()
    workflow = fn name -> "hello #{name}" end

    offline =
      State.new(
        namespace: "default",
        workflow_id: "workflow-id",
        run_id: "run-id",
        mode: :offline
      )

    live = State.new(namespace: "default", workflow_id: "workflow-id", run_id: "run-id")
    terminal_task = %{task() | history: history}

    assert {:ok, offline_result} = Reducer.reduce_history(offline, history, workflow)

    assert {:ok, live_result} =
             Reducer.reduce_task(live, terminal_task, %{"Greeting" => workflow})

    assert Map.from_struct(offline_result.cursor) |> Map.delete(:task_token) ==
             Map.from_struct(live_result.cursor) |> Map.delete(:task_token)

    assert State.commands(offline_result) == State.commands(live_result)
    assert offline_result.machines == live_result.machines
  end

  test "all page boundaries preserve reducer output" do
    events = completed_history().events
    workflow = fn name -> "hello #{name}" end

    for boundary <- 0..length(events) do
      {first, second} = Enum.split(events, boundary)

      task = %{
        task()
        | history: %History{events: first},
          next_page_token: if(second == [], do: "", else: "remaining")
      }

      fetch = fn "remaining" ->
        {:ok,
         %GetWorkflowExecutionHistoryResponse{
           history: %History{events: second},
           next_page_token: ""
         }}
      end

      assert {:ok, assembled} = HistoryPaginator.assemble(task, fetch)

      state = State.new(namespace: "default", workflow_id: "workflow-id", run_id: "run-id")

      assert {:ok, result} =
               Reducer.reduce_task(state, assembled, %{"Greeting" => workflow})

      assert result.last_event_id == 5
      assert result.cursor.status == :completed
    end
  end

  defp task do
    %PollWorkflowTaskQueueResponse{
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
                 workflow_type: %WorkflowType{name: "Greeting"},
                 input: Temporal.Payload.encode("Temporal")
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
  end

  defp completed_history do
    %History{
      events:
        task().history.events ++
          [
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
              event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
              attributes:
                {:workflow_execution_completed_event_attributes,
                 %WorkflowExecutionCompletedEventAttributes{
                   workflow_task_completed_event_id: 4,
                   result: Temporal.Payload.encode("hello Temporal")
                 }}
            }
          ]
    }
  end
end
