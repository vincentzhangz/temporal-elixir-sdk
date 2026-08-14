defmodule Temporal.WorkflowTaskKernelTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Command.V1.Command
  alias Temporal.Api.History.V1.{History, HistoryEvent}

  alias Temporal.Api.Workflowservice.V1.{
    GetWorkflowExecutionHistoryResponse,
    PollWorkflowTaskQueueResponse
  }

  alias Temporal.Workflow.TaskKernel.{
    CommandBuffer,
    Completion,
    HistoryPaginator,
    MachineRegistry,
    State
  }

  defp command(type), do: %Command{command_type: type}

  test "run state is keyed by namespace and run id" do
    state = State.new(namespace: "default", workflow_id: "workflow", run_id: "run")

    assert State.key(state) == {"default", "run"}
    assert state.workflow_id == "workflow"
    assert state.mode == :live
    assert state.next_event_id == 1
  end

  test "command sequence allocation and buffering preserve emission order" do
    buffer =
      CommandBuffer.new()
      |> CommandBuffer.enqueue(command(:COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK))
      |> CommandBuffer.enqueue(command(:COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION))

    assert [{1, first}, {2, second}] = CommandBuffer.entries(buffer)
    assert first.command_type == :COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK
    assert second.command_type == :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION
    assert CommandBuffer.commands(buffer) == [first, second]
  end

  test "Workflow Task completion accepts zero or multiple ordered commands" do
    commands = [
      command(:COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK),
      command(:COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION)
    ]

    assert Completion.request("token", "worker", []).commands == []
    assert Completion.request("token", "worker", commands).commands == commands
  end

  test "semantic matching consumes only the matching command at the head" do
    buffer =
      CommandBuffer.new()
      |> CommandBuffer.enqueue(command(:COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK))
      |> CommandBuffer.enqueue(command(:COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION))

    assert {:ok, 1, next} =
             CommandBuffer.match_event(buffer, :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)

    assert [{2, _command}] = CommandBuffer.entries(next)

    assert {:error, {:nondeterminism, diagnostic}} =
             CommandBuffer.match_event(next, :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)

    assert diagnostic.command_sequence == 2
    assert diagnostic.expected_event_type == :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
  end

  test "semantic matching recognizes timer start and cancel events" do
    buffer =
      CommandBuffer.new()
      |> CommandBuffer.enqueue(command(:COMMAND_TYPE_START_TIMER))
      |> CommandBuffer.enqueue(command(:COMMAND_TYPE_CANCEL_TIMER))

    assert {:ok, 1, next} = CommandBuffer.match_event(buffer, :EVENT_TYPE_TIMER_STARTED)
    assert {:ok, 2, empty} = CommandBuffer.match_event(next, :EVENT_TYPE_TIMER_CANCELED)
    assert CommandBuffer.entries(empty) == []
  end

  test "machine registry rejects duplicate command IDs and resolves by type and ID" do
    assert {:ok, registry} =
             MachineRegistry.new()
             |> MachineRegistry.register(:activity, "activity-1", %{status: :created})

    assert MachineRegistry.fetch(registry, :activity, "activity-1") ==
             {:ok, %{status: :created}}

    assert {:error, {:duplicate_command_id, %{type: :activity, id: "activity-1"}}} =
             MachineRegistry.register(registry, :activity, "activity-1", %{})
  end

  test "task generations fence duplicate tokens and stale started event IDs" do
    state =
      State.new(namespace: "default", workflow_id: "workflow", run_id: "run")
      |> State.begin_task(3, "token-1")
      |> elem(1)

    assert state.generation == 1

    assert {:error, {:stale_workflow_task, %{reason: :task_already_owned}}} =
             State.begin_task(state, 3, "token-1")

    assert {:error, {:stale_workflow_task, %{reason: :older_started_event}}} =
             State.begin_task(state, 2, "token-2")

    assert {:ok, next} = State.begin_task(state, 7, "token-2")
    assert next.generation == 2
  end

  test "command sequence allocation remains monotonic across Workflow Tasks" do
    state =
      State.new(namespace: "default", workflow_id: "workflow", run_id: "run")
      |> State.record_commands([command(:COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK)])

    assert [{1, _command}] = CommandBuffer.entries(state.command_buffer)
    assert {:ok, state} = State.begin_task(state, 3, "token")

    state = State.record_commands(state, [command(:COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION)])
    assert [{2, _command}] = CommandBuffer.entries(state.command_buffer)
  end

  test "zero-command tasks are valid completions" do
    state = State.new(namespace: "default", workflow_id: "workflow", run_id: "run")
    assert State.commands(state) == []
  end

  test "history application queues activation jobs without running user code" do
    state =
      State.new(namespace: "default", workflow_id: "workflow", run_id: "run")
      |> State.queue_history_job({:resolve_activity, "activity-1"})
      |> State.queue_workflow_job(:run_workflow)

    assert {{:value, {:resolve_activity, "activity-1"}}, _queue} =
             :queue.out(state.history_jobs)

    assert {{:value, :run_workflow}, _queue} = :queue.out(state.workflow_jobs)
  end

  test "paginated poll history is assembled in server order" do
    task = %PollWorkflowTaskQueueResponse{
      history: %History{events: [%HistoryEvent{event_id: 1}]},
      next_page_token: "page-2"
    }

    fetch_page = fn
      "page-2" ->
        {:ok,
         %GetWorkflowExecutionHistoryResponse{
           history: %History{events: [%HistoryEvent{event_id: 2}]},
           next_page_token: "page-3"
         }}

      "page-3" ->
        {:ok,
         %GetWorkflowExecutionHistoryResponse{
           history: %History{events: [%HistoryEvent{event_id: 3}]},
           next_page_token: ""
         }}
    end

    assert {:ok, assembled} = HistoryPaginator.assemble(task, fetch_page)
    assert Enum.map(assembled.history.events, & &1.event_id) == [1, 2, 3]
    assert assembled.next_page_token == ""
  end
end
