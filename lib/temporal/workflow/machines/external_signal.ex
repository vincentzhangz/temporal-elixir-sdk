defmodule Temporal.Workflow.Machines.ExternalSignal do
  @moduledoc false

  alias Temporal.Api.History.V1.{
    ExternalWorkflowExecutionSignaledEventAttributes,
    SignalExternalWorkflowExecutionFailedEventAttributes,
    SignalExternalWorkflowExecutionInitiatedEventAttributes
  }

  @type t :: %__MODULE__{
          state: atom(),
          command: struct(),
          initiated_event_id: pos_integer() | nil,
          sequence: pos_integer(),
          id: String.t(),
          failed_cause: atom() | nil
        }

  defstruct [:state, :command, :initiated_event_id, :sequence, :id, :failed_cause]

  @spec new(term(), struct(), pos_integer()) :: t()
  def new(id, command, sequence) do
    %__MODULE__{state: :command_created, command: command, sequence: sequence, id: id}
  end

  @spec apply_event(t(), map(), :live | :replay) :: {:ok, t(), atom()} | {:error, term()}
  def apply_event(
        %__MODULE__{state: :command_created} = machine,
        %{
          event_id: event_id,
          event_type: :EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
          attributes:
            {:signal_external_workflow_execution_initiated_event_attributes,
             %SignalExternalWorkflowExecutionInitiatedEventAttributes{} = attributes}
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
          event_type: :EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
          attributes:
            {:external_workflow_execution_signaled_event_attributes,
             %ExternalWorkflowExecutionSignaledEventAttributes{} = attributes}
        },
        _mode
      ) do
    if attributes.initiated_event_id == initiated_event_id do
      {:ok, %{machine | state: :signaled}, :signaled}
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
        %__MODULE__{state: :initiated, initiated_event_id: initiated_event_id} = machine,
        %{
          event_type: :EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
          attributes:
            {:signal_external_workflow_execution_failed_event_attributes,
             %SignalExternalWorkflowExecutionFailedEventAttributes{} = attributes}
        },
        _mode
      ) do
    if attributes.initiated_event_id == initiated_event_id do
      {:ok, %{machine | state: :failed, failed_cause: attributes.cause}, :failed}
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

  def apply_event(machine, event, _mode) do
    {:error,
     {:nondeterminism,
      %{machine: __MODULE__, state: machine.state, actual_event_type: event.event_type}}}
  end

  defp correlate_initiated(
         %{workflow_task_completed_event_id: expected, workflow_execution: execution},
         %{
           attributes:
             {:signal_external_workflow_execution_command_attributes,
              %{execution: command_execution}}
         }
       )
       when is_integer(expected) and expected > 0,
       do: same_execution(execution, command_execution)

  defp correlate_initiated(_attributes, _command), do: :ok

  defp same_execution(%{workflow_id: wid, run_id: rid}, %{workflow_id: wid, run_id: rid}), do: :ok

  defp same_execution(expected, actual),
    do:
      {:error,
       {:nondeterminism, %{field: :workflow_execution, expected: expected, actual: actual}}}
end
