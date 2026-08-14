defmodule Temporal.Workflow.TaskKernel.State do
  @moduledoc """
  Durable pure-BEAM state for one Workflow Run.

  History jobs and user-code jobs are kept separately so applying server
  history never implicitly schedules Workflow code.
  """

  alias Temporal.Workflow.Machines.{SignalInbox, Timer}
  alias Temporal.Workflow.TaskKernel.{CommandBuffer, MachineRegistry}

  @enforce_keys [:namespace, :workflow_id, :run_id]
  defstruct [
    :namespace,
    :workflow_id,
    :run_id,
    :task_token,
    :workflow_task_started_event_id,
    :activation,
    :cursor,
    mode: :live,
    generation: 0,
    next_event_id: 1,
    last_event_id: 0,
    command_buffer: %CommandBuffer{next_sequence: 1, entries: []},
    machines: nil,
    history_jobs: :queue.new(),
    workflow_jobs: :queue.new()
  ]

  @type t :: %__MODULE__{}

  @spec new(keyword()) :: t()
  def new(options) do
    %__MODULE__{
      namespace: Keyword.fetch!(options, :namespace),
      workflow_id: Keyword.fetch!(options, :workflow_id),
      run_id: Keyword.fetch!(options, :run_id),
      mode: Keyword.get(options, :mode, :live),
      machines: new_machine_registry()
    }
  end

  @spec key(t()) :: {String.t(), String.t()}
  def key(%__MODULE__{namespace: namespace, run_id: run_id}), do: {namespace, run_id}

  @spec begin_task(t(), pos_integer(), binary()) :: {:ok, t()} | {:error, term()}
  def begin_task(
        %__MODULE__{
          workflow_task_started_event_id: started_event_id,
          task_token: task_token
        },
        started_event_id,
        task_token
      )
      when not is_nil(started_event_id) do
    {:error,
     {:stale_workflow_task, %{started_event_id: started_event_id, reason: :task_already_owned}}}
  end

  def begin_task(
        %__MODULE__{workflow_task_started_event_id: current},
        started_event_id,
        _task_token
      )
      when is_integer(current) and started_event_id < current do
    {:error,
     {:stale_workflow_task,
      %{
        started_event_id: started_event_id,
        current_started_event_id: current,
        reason: :older_started_event
      }}}
  end

  def begin_task(%__MODULE__{} = state, started_event_id, task_token)
      when is_integer(started_event_id) and started_event_id > 0 and is_binary(task_token) and
             task_token != "" do
    {:ok,
     %{
       state
       | generation: state.generation + 1,
         workflow_task_started_event_id: started_event_id,
         task_token: task_token,
         command_buffer: %{state.command_buffer | entries: []}
     }}
  end

  @spec record_commands(t(), [struct()]) :: t()
  def record_commands(%__MODULE__{} = state, commands) when is_list(commands) do
    buffer = Enum.reduce(commands, state.command_buffer, &CommandBuffer.enqueue(&2, &1))
    %{state | command_buffer: buffer}
  end

  @spec queue_history_job(t(), term()) :: t()
  def queue_history_job(%__MODULE__{} = state, job),
    do: %{state | history_jobs: :queue.in(job, state.history_jobs)}

  @spec queue_workflow_job(t(), term()) :: t()
  def queue_workflow_job(%__MODULE__{} = state, job),
    do: %{state | workflow_jobs: :queue.in(job, state.workflow_jobs)}

  @spec commands(t()) :: [struct()]
  def commands(%__MODULE__{command_buffer: buffer}), do: CommandBuffer.commands(buffer)

  defp new_machine_registry do
    MachineRegistry.new()
    |> register_type!(:workflow, &dispatch_command_machine/3)
    |> register_type!(:activity, &dispatch_command_machine/3)
    |> register_type!(:workflow_task, &dispatch_command_machine/3)
    |> register_type!(:terminal, &dispatch_command_machine/3)
    |> register_type!(:continue_as_new, &dispatch_command_machine/3)
    |> register_type!(:timer, &Timer.apply_event/3)
    |> register_type!(:signal, &dispatch_signal/3)
  end

  defp register_type!(registry, type, dispatcher) do
    {:ok, next} = MachineRegistry.register_type(registry, type, dispatcher)
    next
  end

  defp dispatch_command_machine(
         machine,
         %{
           event_id: event_id,
           event_type: :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
         } = event,
         _mode
       ) do
    next =
      Map.merge(machine, %{
        last_event: event,
        scheduled_event_id: event_id,
        state: :scheduled
      })

    {:ok, next, :scheduled}
  end

  defp dispatch_command_machine(
         machine,
         %{event_id: event_id, event_type: :EVENT_TYPE_ACTIVITY_TASK_STARTED} = event,
         _mode
       ) do
    next =
      Map.merge(machine, %{
        last_event: event,
        started_event_id: event_id,
        state: :started
      })

    {:ok, next, :started}
  end

  defp dispatch_command_machine(machine, %{event_type: event_type} = event, _mode)
       when event_type in [
              :EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
              :EVENT_TYPE_ACTIVITY_TASK_FAILED,
              :EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
              :EVENT_TYPE_ACTIVITY_TASK_CANCELED,
              :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
              :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
              :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
              :EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
            ] do
    {:ok, Map.merge(machine, %{last_event: event, state: :resolved}), :resolved}
  end

  defp dispatch_command_machine(machine, event, _mode) do
    {:ok, Map.put(machine, :last_event, event), event}
  end

  defp dispatch_signal(%SignalInbox{} = inbox, {signal, destination}, _mode) do
    case SignalInbox.accept(inbox, signal, destination) do
      {:ok, next, resolution} -> {:ok, next, resolution}
      {:error, reason} -> {:error, reason}
    end
  end

  defp dispatch_signal(_inbox, _event, _mode), do: {:error, :invalid_signal_event}
end
