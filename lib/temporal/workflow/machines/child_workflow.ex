defmodule Temporal.Workflow.Machines.ChildWorkflow do
  @moduledoc false

  alias Temporal.Api.History.V1.{
    ChildWorkflowExecutionCanceledEventAttributes,
    ChildWorkflowExecutionCompletedEventAttributes,
    ChildWorkflowExecutionFailedEventAttributes,
    ChildWorkflowExecutionStartedEventAttributes,
    ChildWorkflowExecutionTimedOutEventAttributes,
    StartChildWorkflowExecutionInitiatedEventAttributes
  }

  @type t :: %__MODULE__{
          state: atom(),
          command: struct(),
          initiated_event_id: pos_integer() | nil,
          started_event_id: pos_integer() | nil,
          sequence: pos_integer(),
          id: String.t()
        }

  defstruct [
    :state,
    :command,
    :initiated_event_id,
    :started_event_id,
    :sequence,
    :id,
    :outcome
  ]

  @spec new(term(), struct(), pos_integer()) :: t()
  def new(id, command, sequence) do
    %__MODULE__{state: :command_created, command: command, sequence: sequence, id: id}
  end

  @spec apply_event(t(), map(), :live | :replay) :: {:ok, t(), atom()} | {:error, term()}
  def apply_event(
        %__MODULE__{state: :command_created} = machine,
        %{
          event_id: event_id,
          event_type: :EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
          attributes:
            {:start_child_workflow_execution_initiated_event_attributes,
             %StartChildWorkflowExecutionInitiatedEventAttributes{} = attributes}
        },
        _mode
      ) do
    with :ok <- correlate_initiated(attributes, machine.command) do
      {:ok, %{machine | state: :initiated, initiated_event_id: event_id}, :initiated}
    end
  end

  def apply_event(
        %__MODULE__{state: :initiated, initiated_event_id: initiated_event_id} = machine,
        %{
          event_id: event_id,
          event_type: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
          attributes:
            {:child_workflow_execution_started_event_attributes,
             %ChildWorkflowExecutionStartedEventAttributes{} = attributes}
        },
        _mode
      ) do
    if attributes.initiated_event_id == initiated_event_id do
      {:ok, %{machine | state: :started, started_event_id: event_id}, :started}
    else
      {:error,
       {:nondeterminism,
        %{
          field: :initiated_event_id,
          expected: initiated_event_id,
          actual: attributes.initiated_event_id
        }}}
    end
  end

  def apply_event(
        %__MODULE__{state: state, initiated_event_id: initiated_event_id} = machine,
        %{
          event_type: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
          attributes:
            {:child_workflow_execution_completed_event_attributes,
             %ChildWorkflowExecutionCompletedEventAttributes{} = attributes}
        },
        _mode
      )
      when state in [:initiated, :started] do
    with :ok <- correlate_resolution(initiated_event_id, attributes.initiated_event_id),
         {:ok, result} <- Temporal.Payload.decode(attributes.result) do
      {:ok, %{machine | state: :completed, outcome: {:ok, result}}, :completed}
    end
  end

  def apply_event(
        %__MODULE__{state: state, initiated_event_id: initiated_event_id} = machine,
        %{
          event_type: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
          attributes:
            {:child_workflow_execution_failed_event_attributes,
             %ChildWorkflowExecutionFailedEventAttributes{} = attributes}
        },
        _mode
      )
      when state in [:initiated, :started] do
    with :ok <- correlate_resolution(initiated_event_id, attributes.initiated_event_id) do
      error =
        Temporal.ActivityError.exception(
          message: "Child workflow failed",
          cause: Temporal.Failure.from_proto(attributes.failure),
          scheduled_event_id: attributes.initiated_event_id,
          started_event_id: attributes.started_event_id,
          retry_state: attributes.retry_state
        )

      {:ok, %{machine | state: :failed, outcome: {:error, error}}, :failed}
    end
  end

  def apply_event(
        %__MODULE__{state: state, initiated_event_id: initiated_event_id} = machine,
        %{
          event_type: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
          attributes:
            {:child_workflow_execution_canceled_event_attributes,
             %ChildWorkflowExecutionCanceledEventAttributes{} = attributes}
        },
        _mode
      )
      when state in [:initiated, :started] do
    with :ok <- correlate_resolution(initiated_event_id, attributes.initiated_event_id) do
      {:ok, %{machine | state: :canceled, outcome: {:error, :child_workflow_canceled}}, :canceled}
    end
  end

  def apply_event(
        %__MODULE__{state: state, initiated_event_id: initiated_event_id} = machine,
        %{
          event_type: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
          attributes:
            {:child_workflow_execution_timed_out_event_attributes,
             %ChildWorkflowExecutionTimedOutEventAttributes{} = attributes}
        },
        _mode
      )
      when state in [:initiated, :started] do
    with :ok <- correlate_resolution(initiated_event_id, attributes.initiated_event_id) do
      error =
        Temporal.TimeoutError.exception(
          message: "Child workflow timed out",
          retry_state: attributes.retry_state
        )

      {:ok, %{machine | state: :timed_out, outcome: {:error, error}}, :timed_out}
    end
  end

  def apply_event(machine, event, _mode) do
    {:error,
     {:nondeterminism,
      %{machine: __MODULE__, state: machine.state, actual_event_type: event.event_type}}}
  end

  defp correlate_initiated(
         %{
           workflow_task_completed_event_id: expected,
           workflow_id: workflow_id,
           workflow_type: %{name: workflow_type},
           task_queue: %{name: task_queue}
         },
         %{
           attributes:
             {:start_child_workflow_execution_command_attributes,
              %{
                workflow_id: workflow_id,
                workflow_type: %{name: workflow_type},
                task_queue: %{name: task_queue}
              }}
         }
       )
       when is_integer(expected) and expected > 0,
       do: :ok

  defp correlate_initiated(_attributes, _command), do: :ok

  defp correlate_resolution(expected, expected), do: :ok

  defp correlate_resolution(expected, actual),
    do:
      {:error,
       {:nondeterminism, %{field: :initiated_event_id, expected: expected, actual: actual}}}
end
