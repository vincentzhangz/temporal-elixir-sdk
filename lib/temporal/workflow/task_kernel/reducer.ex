defmodule Temporal.Workflow.TaskKernel.Reducer do
  @moduledoc false

  alias Temporal.Api.History.V1.History
  alias Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse
  alias Temporal.Workflow.CommandBatch
  alias Temporal.Workflow.HistoryCursor

  alias Temporal.Workflow.TaskKernel.{
    Activation,
    CommandBuffer,
    EventReducer,
    MachineRegistry,
    State
  }

  @spec reduce_task(State.t(), PollWorkflowTaskQueueResponse.t(), map()) ::
          {:ok, State.t()} | {:error, term()}
  def reduce_task(
        %State{} = state,
        %PollWorkflowTaskQueueResponse{} = task,
        workflows
      )
      when is_map(workflows) do
    with :ok <- validate_task(task),
         :ok <- validate_identity(state, task),
         :ok <- fence(state, task),
         {:ok, workflow} <- fetch_workflow(workflows, task.workflow_type),
         cursor <- HistoryCursor.new(workflow_id: state.workflow_id, run_id: state.run_id),
         {:ok, replayed, replayed_machines} <-
           EventReducer.reduce(task.history, workflow, cursor, state.machines, state.mode),
         :ok <-
           same(:started_event_id, replayed.workflow_task_started_event_id, task.started_event_id),
         :ok <- same(:workflow_type, replayed.workflow_type, task.workflow_type.name),
         {:ok, owned} <- State.begin_task(state, task.started_event_id, task.task_token),
         recorded <-
           owned
           |> Map.put(:machines, replayed_machines)
           |> State.record_commands(emitted_commands(replayed.command)),
         {:ok, machines, activation} <- activate(recorded, replayed) do
      {:ok,
       recorded
       |> Map.put(:activation, activation)
       |> Map.put(:machines, machines)
       |> Map.put(:cursor, %{replayed | task_token: task.task_token})
       |> Map.put(:next_event_id, replayed.next_event_id)
       |> Map.put(:last_event_id, replayed.last_event_id)}
    end
  end

  @spec reduce_history(State.t(), History.t(), function()) :: {:ok, State.t()} | {:error, term()}
  def reduce_history(%State{} = state, %History{} = history, workflow) do
    cursor = HistoryCursor.new(workflow_id: state.workflow_id, run_id: state.run_id)

    with {:ok, replayed, replayed_machines} <-
           EventReducer.reduce(history, workflow, cursor, state.machines, state.mode),
         owned <-
           %{
             state
             | workflow_task_started_event_id: replayed.workflow_task_started_event_id,
               task_token: ""
           },
         recorded <-
           owned
           |> Map.put(:machines, replayed_machines)
           |> State.record_commands(emitted_commands(replayed.command)),
         {:ok, machines, activation} <- activate(recorded, replayed) do
      {:ok,
       recorded
       |> Map.put(:activation, activation)
       |> Map.put(:machines, machines)
       |> Map.put(:cursor, replayed)
       |> Map.put(:next_event_id, replayed.next_event_id)
       |> Map.put(:last_event_id, replayed.last_event_id)}
    end
  end

  defp validate_task(%{task_token: ""}), do: {:error, :empty_task}
  defp validate_task(%{history: nil}), do: {:error, :missing_history}

  defp validate_task(%{next_page_token: token}) when token != "",
    do: {:error, {:invalid_history_pagination, :unassembled_history}}

  defp validate_task(%{query: query}) when not is_nil(query),
    do: {:error, {:unsupported_feature, :queries}}

  defp validate_task(%{queries: queries}) when map_size(queries) > 0,
    do: {:error, {:unsupported_feature, :queries}}

  defp validate_task(%{messages: [_ | _]}),
    do: {:error, {:unsupported_feature, :updates}}

  defp validate_task(%{history: %{events: []}}), do: {:error, :missing_history}
  defp validate_task(%{history: %{events: _events}}), do: :ok
  defp validate_task(_task), do: {:error, :missing_history}

  defp validate_identity(
         %State{workflow_id: workflow_id, run_id: run_id},
         %{workflow_execution: %{workflow_id: workflow_id, run_id: run_id}}
       ),
       do: :ok

  defp validate_identity(%State{} = state, %{workflow_execution: execution}) do
    {:error,
     {:workflow_identity_mismatch,
      %{
        expected: {state.workflow_id, state.run_id},
        actual: {execution && execution.workflow_id, execution && execution.run_id}
      }}}
  end

  defp fence(%State{workflow_task_started_event_id: nil}, _task), do: :ok

  defp fence(
         %State{
           run_id: run_id,
           workflow_task_started_event_id: started_event_id,
           task_token: task_token
         },
         %{started_event_id: started_event_id, task_token: task_token}
       ) do
    {:error,
     {:stale_workflow_task,
      %{
        run_id: run_id,
        started_event_id: started_event_id,
        reason: :task_already_completed
      }}}
  end

  defp fence(
         %State{workflow_task_started_event_id: started_event_id},
         %{started_event_id: started_event_id}
       ),
       do: {:error, {:task_token_mismatch, %{started_event_id: started_event_id}}}

  defp fence(%State{workflow_task_started_event_id: current}, %{started_event_id: incoming})
       when incoming < current,
       do:
         {:error,
          {:stale_workflow_task,
           %{
             started_event_id: incoming,
             current_started_event_id: current,
             reason: :older_started_event
           }}}

  defp fence(_state, _task), do: :ok

  defp fetch_workflow(workflows, %{name: name}) do
    case Map.fetch(workflows, name) do
      {:ok, workflow} -> {:ok, workflow}
      :error -> {:error, {:workflow_not_registered, name}}
    end
  end

  defp fetch_workflow(_workflows, _type), do: {:error, :missing_workflow_type}

  defp same(_field, value, value), do: :ok

  defp same(field, expected, actual),
    do:
      {:error,
       {:workflow_task_identity_mismatch, %{field: field, expected: expected, actual: actual}}}

  defp activate(state, replayed) do
    activation =
      Activation.new(state.workflow_task_started_event_id, state.task_token)
      |> Activation.add_job(:workflow, %{input: replayed.input})

    Enum.reduce_while(
      CommandBuffer.entries(state.command_buffer),
      {:ok, state.machines, activation},
      fn {sequence, command}, {:ok, machines, jobs} ->
        {type, id} = machine_identity(command, sequence)

        case register_command_machine(machines, type, id, command, sequence) do
          {:ok, next_machines} ->
            {:cont, {:ok, next_machines, Activation.add_job(jobs, type, %{id: id})}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end
    )
  end

  defp register_command_machine(machines, type, id, command, sequence) do
    machine = %{
      command_type: command.command_type,
      command: command,
      sequence: sequence,
      state: :command_created
    }

    case MachineRegistry.fetch(machines, type, id) do
      {:ok, existing} ->
        key = {type, id}
        {:ok, put_in(machines, [:machines, key], Map.merge(machine, existing))}

      :error ->
        MachineRegistry.register(machines, type, id, machine)
    end
  end

  defp machine_identity(
         %{attributes: {:schedule_activity_task_command_attributes, %{activity_id: id}}},
         _sequence
       ),
       do: {:activity, id}

  defp machine_identity(
         %{attributes: {:start_timer_command_attributes, %{timer_id: id}}},
         _sequence
       ),
       do: {:timer, id}

  defp machine_identity(
         %{command_type: :COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION},
         sequence
       ),
       do: {:continue_as_new, "command-#{sequence}"}

  defp machine_identity(
         %{command_type: command_type},
         sequence
       )
       when command_type in [
              :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
              :COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
              :COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION
            ],
       do: {:terminal, "command-#{sequence}"}

  defp machine_identity(_command, sequence), do: {:workflow, "command-#{sequence}"}

  defp emitted_commands(%CommandBatch{commands: commands}), do: commands
  defp emitted_commands(nil), do: []
  defp emitted_commands(command), do: [command]
end
