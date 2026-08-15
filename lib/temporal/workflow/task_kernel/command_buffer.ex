defmodule Temporal.Workflow.TaskKernel.CommandBuffer do
  @moduledoc """
  Ordered deterministic command stream for one Workflow Run.

  Sequence numbers are SDK-local and monotonic. Recorded history consumes the
  head command only, preserving the same ordering rule used by Temporal cores.
  """

  alias Temporal.Api.Command.V1.Command

  @enforce_keys [:next_sequence, :entries]
  defstruct next_sequence: 1, entries: []

  @type t :: %__MODULE__{
          next_sequence: pos_integer(),
          entries: [{pos_integer(), struct()}]
        }

  @spec new() :: t()
  def new, do: %__MODULE__{next_sequence: 1, entries: []}

  @spec enqueue(t(), struct()) :: t()
  def enqueue(%__MODULE__{} = buffer, %Command{} = command) do
    entry = {buffer.next_sequence, command}

    %{
      buffer
      | next_sequence: buffer.next_sequence + 1,
        entries: buffer.entries ++ [entry]
    }
  end

  @spec entries(t()) :: [{pos_integer(), struct()}]
  def entries(%__MODULE__{entries: entries}), do: entries

  @spec commands(t()) :: [struct()]
  def commands(%__MODULE__{entries: entries}), do: Enum.map(entries, &elem(&1, 1))

  @spec match_event(t(), atom()) :: {:ok, pos_integer(), t()} | {:error, term()}
  def match_event(%__MODULE__{entries: []}, event_type) do
    {:error,
     {:nondeterminism,
      %{
        actual_event_type: event_type,
        message: "recorded command event has no corresponding emitted command"
      }}}
  end

  def match_event(
        %__MODULE__{entries: [{sequence, command} | rest]} = buffer,
        event_type
      ) do
    expected = expected_event_type(command.command_type)

    if expected == event_type do
      {:ok, sequence, %{buffer | entries: rest}}
    else
      {:error,
       {:nondeterminism,
        %{
          command_sequence: sequence,
          command_type: command.command_type,
          expected_event_type: expected,
          actual_event_type: event_type,
          message: "recorded event does not match the next emitted command"
        }}}
    end
  end

  defp expected_event_type(:COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK),
    do: :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED

  defp expected_event_type(:COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK),
    do: :EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED

  defp expected_event_type(:COMMAND_TYPE_START_TIMER), do: :EVENT_TYPE_TIMER_STARTED
  defp expected_event_type(:COMMAND_TYPE_CANCEL_TIMER), do: :EVENT_TYPE_TIMER_CANCELED

  defp expected_event_type(:COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION),
    do: :EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED

  defp expected_event_type(:COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION),
    do: :EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED

  defp expected_event_type(:COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION),
    do: :EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED

  defp expected_event_type(:COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION),
    do: :EVENT_TYPE_NEXUS_OPERATION_SCHEDULED

  defp expected_event_type(:COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES),
    do: :EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES

  defp expected_event_type(:COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES),
    do: :EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED

  defp expected_event_type(:COMMAND_TYPE_RECORD_MARKER),
    do: :EVENT_TYPE_MARKER_RECORDED

  defp expected_event_type(:COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION),
    do: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED

  defp expected_event_type(:COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION),
    do: :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED

  defp expected_event_type(:COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION),
    do: :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED

  defp expected_event_type(:COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION),
    do: :EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW

  defp expected_event_type(_command_type), do: :EVENT_TYPE_UNSPECIFIED
end
