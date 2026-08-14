defmodule Temporal.Workflow.Machines.Timer do
  @moduledoc """
  Pure command/event state machine for one Temporal durable timer.

  The caller owns the collection of commands returned by `start/2` and
  `cancel/1`. Canceling before `TimerStarted` is observed is local: the caller
  must omit the previously returned `StartTimer` command from its pending
  command batch.
  """

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

  @type state ::
          :initialized
          | :start_command_created
          | :started
          | :cancel_command_created
          | :fired
          | :canceled
  @type mode :: :live | :replay
  @type resolution :: :fired | :canceled | nil
  @type nondeterminism :: {:nondeterminism, map()}

  @enforce_keys [:timer_id, :sequence, :start_to_fire_timeout]
  defstruct [
    :timer_id,
    :sequence,
    :start_to_fire_timeout,
    :start_workflow_task_completed_event_id,
    :started_event_id,
    :cancel_workflow_task_completed_event_id,
    :last_event_id,
    state: :initialized
  ]

  @type t :: %__MODULE__{
          timer_id: String.t(),
          sequence: pos_integer(),
          start_to_fire_timeout: Duration.t(),
          start_workflow_task_completed_event_id: pos_integer() | nil,
          started_event_id: pos_integer() | nil,
          cancel_workflow_task_completed_event_id: pos_integer() | nil,
          last_event_id: pos_integer() | nil,
          state: state()
        }

  @spec new(integer(), Duration.t(), keyword()) ::
          {:ok, t()}
          | {:error,
             {:invalid_sequence, term()}
             | {:invalid_timeout, term()}
             | {:invalid_timer_id, term()}}
  def new(sequence, timeout, options \\ [])

  def new(sequence, %Duration{} = timeout, options) when is_integer(sequence) and sequence > 0 do
    timer_id = Keyword.get(options, :timer_id, "timer-#{sequence}")

    cond do
      not valid_duration?(timeout) ->
        {:error, {:invalid_timeout, timeout}}

      not (is_binary(timer_id) and timer_id != "") ->
        {:error, {:invalid_timer_id, timer_id}}

      true ->
        {:ok,
         %__MODULE__{
           timer_id: timer_id,
           sequence: sequence,
           start_to_fire_timeout: timeout
         }}
    end
  end

  def new(sequence, %Duration{} = _timeout, _options),
    do: {:error, {:invalid_sequence, sequence}}

  def new(_sequence, timeout, _options), do: {:error, {:invalid_timeout, timeout}}

  @spec start(t(), pos_integer()) ::
          {:ok, t(), [struct()], resolution()}
          | {:error, {:invalid_event_id, term()} | {:illegal_transition, state(), :start}}
  def start(
        %__MODULE__{
          state: :initialized,
          start_to_fire_timeout: %Duration{seconds: 0, nanos: 0}
        } = timer,
        workflow_task_completed_event_id
      )
      when is_integer(workflow_task_completed_event_id) and workflow_task_completed_event_id > 0 do
    {:ok,
     %{
       timer
       | state: :fired,
         start_workflow_task_completed_event_id: workflow_task_completed_event_id
     }, [], :fired}
  end

  def start(%__MODULE__{state: :initialized} = timer, workflow_task_completed_event_id)
      when is_integer(workflow_task_completed_event_id) and workflow_task_completed_event_id > 0 do
    command = %Command{
      command_type: :COMMAND_TYPE_START_TIMER,
      attributes:
        {:start_timer_command_attributes,
         %StartTimerCommandAttributes{
           timer_id: timer.timer_id,
           start_to_fire_timeout: timer.start_to_fire_timeout
         }}
    }

    next = %{
      timer
      | state: :start_command_created,
        start_workflow_task_completed_event_id: workflow_task_completed_event_id
    }

    {:ok, next, [command], nil}
  end

  def start(%__MODULE__{state: :initialized}, event_id),
    do: {:error, {:invalid_event_id, event_id}}

  def start(%__MODULE__{state: state}, _event_id),
    do: {:error, {:illegal_transition, state, :start}}

  @doc """
  Cancels a timer whose start command has not been recorded by the server.
  """
  @spec cancel(t()) ::
          {:ok, t(), [], :canceled}
          | {:error, {:illegal_transition, state(), :cancel_before_start}}
  def cancel(%__MODULE__{state: state} = timer)
      when state in [:initialized, :start_command_created] do
    {:ok, %{timer | state: :canceled}, [], :canceled}
  end

  def cancel(%__MODULE__{state: state} = timer) when state in [:fired, :canceled],
    do: {:ok, timer, [], state}

  def cancel(%__MODULE__{state: state}),
    do: {:error, {:illegal_transition, state, :cancel_before_start}}

  @doc """
  Creates a server `CancelTimer` command after `TimerStarted` was recorded.
  """
  @spec cancel(t(), pos_integer()) ::
          {:ok, t(), [struct()], nil}
          | {:error,
             {:invalid_event_id, term()} | {:illegal_transition, state(), :cancel_after_start}}
  def cancel(
        %__MODULE__{state: :started} = timer,
        workflow_task_completed_event_id
      )
      when is_integer(workflow_task_completed_event_id) and workflow_task_completed_event_id > 0 do
    command = %Command{
      command_type: :COMMAND_TYPE_CANCEL_TIMER,
      attributes:
        {:cancel_timer_command_attributes,
         %CancelTimerCommandAttributes{timer_id: timer.timer_id}}
    }

    next = %{
      timer
      | state: :cancel_command_created,
        cancel_workflow_task_completed_event_id: workflow_task_completed_event_id
    }

    {:ok, next, [command], nil}
  end

  def cancel(%__MODULE__{state: :started}, event_id),
    do: {:error, {:invalid_event_id, event_id}}

  def cancel(%__MODULE__{state: state} = timer, _event_id) when state in [:fired, :canceled],
    do: {:ok, timer, [], state}

  def cancel(%__MODULE__{state: state}, _event_id),
    do: {:error, {:illegal_transition, state, :cancel_after_start}}

  @spec apply_event(t(), struct(), mode()) ::
          {:ok, t(), resolution()}
          | {:error, nondeterminism() | {:invalid_mode, term()}}
  def apply_event(%__MODULE__{} = timer, %HistoryEvent{} = event, mode)
      when mode in [:live, :replay] do
    with :ok <- validate_event_order(timer, event, mode) do
      transition(timer, event, mode)
    end
  end

  def apply_event(%__MODULE__{}, %HistoryEvent{}, mode), do: {:error, {:invalid_mode, mode}}

  defp transition(
         %__MODULE__{state: :start_command_created} = timer,
         %HistoryEvent{
           event_id: event_id,
           event_type: :EVENT_TYPE_TIMER_STARTED,
           attributes:
             {:timer_started_event_attributes, %TimerStartedEventAttributes{} = attributes}
         },
         mode
       ) do
    with :ok <- match_field(timer, event_id, mode, :timer_id, timer.timer_id, attributes.timer_id),
         :ok <-
           match_field(
             timer,
             event_id,
             mode,
             :start_to_fire_timeout,
             timer.start_to_fire_timeout,
             attributes.start_to_fire_timeout
           ),
         :ok <-
           match_field(
             timer,
             event_id,
             mode,
             :workflow_task_completed_event_id,
             timer.start_workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ) do
      {:ok, %{timer | state: :started, started_event_id: event_id, last_event_id: event_id}, nil}
    end
  end

  defp transition(
         %__MODULE__{state: state} = timer,
         %HistoryEvent{
           event_id: event_id,
           event_type: :EVENT_TYPE_TIMER_FIRED,
           attributes: {:timer_fired_event_attributes, %TimerFiredEventAttributes{} = attributes}
         },
         mode
       )
       when state in [:started, :cancel_command_created] do
    with :ok <- match_field(timer, event_id, mode, :timer_id, timer.timer_id, attributes.timer_id),
         :ok <-
           match_field(
             timer,
             event_id,
             mode,
             :started_event_id,
             timer.started_event_id,
             attributes.started_event_id
           ) do
      {:ok, %{timer | state: :fired, last_event_id: event_id}, :fired}
    end
  end

  defp transition(
         %__MODULE__{state: :cancel_command_created} = timer,
         %HistoryEvent{
           event_id: event_id,
           event_type: :EVENT_TYPE_TIMER_CANCELED,
           attributes:
             {:timer_canceled_event_attributes, %TimerCanceledEventAttributes{} = attributes}
         },
         mode
       ) do
    with :ok <- match_field(timer, event_id, mode, :timer_id, timer.timer_id, attributes.timer_id),
         :ok <-
           match_field(
             timer,
             event_id,
             mode,
             :started_event_id,
             timer.started_event_id,
             attributes.started_event_id
           ),
         :ok <-
           match_field(
             timer,
             event_id,
             mode,
             :workflow_task_completed_event_id,
             timer.cancel_workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ) do
      {:ok, %{timer | state: :canceled, last_event_id: event_id}, :canceled}
    end
  end

  defp transition(
         %__MODULE__{} = timer,
         %HistoryEvent{event_id: event_id, event_type: event_type, attributes: attributes},
         mode
       ) do
    reason =
      if timer_event?(event_type) and malformed_attributes?(event_type, attributes) do
        :malformed_event
      else
        :illegal_transition
      end

    {:error,
     {:nondeterminism,
      %{
        reason: reason,
        mode: mode,
        event_id: event_id,
        event_type: event_type,
        state: timer.state,
        timer_id: timer.timer_id
      }}}
  end

  defp validate_event_order(
         %__MODULE__{} = timer,
         %HistoryEvent{event_id: event_id},
         mode
       )
       when event_id <= 0 do
    nondeterminism(timer, mode, event_id, %{reason: :invalid_event_id})
  end

  defp validate_event_order(
         %__MODULE__{
           state: :start_command_created,
           start_workflow_task_completed_event_id: correlated_event_id
         } = timer,
         %HistoryEvent{event_id: event_id, event_type: :EVENT_TYPE_TIMER_STARTED},
         mode
       )
       when event_id <= correlated_event_id do
    nondeterminism(timer, mode, event_id, %{
      reason: :out_of_order_correlation,
      correlated_event_id: correlated_event_id
    })
  end

  defp validate_event_order(
         %__MODULE__{
           state: :cancel_command_created,
           cancel_workflow_task_completed_event_id: correlated_event_id
         } = timer,
         %HistoryEvent{event_id: event_id, event_type: :EVENT_TYPE_TIMER_CANCELED},
         mode
       )
       when event_id <= correlated_event_id do
    nondeterminism(timer, mode, event_id, %{
      reason: :out_of_order_correlation,
      correlated_event_id: correlated_event_id
    })
  end

  defp validate_event_order(%__MODULE__{last_event_id: nil}, _event, _mode), do: :ok

  defp validate_event_order(
         %__MODULE__{last_event_id: event_id} = timer,
         %HistoryEvent{event_id: event_id},
         mode
       ) do
    nondeterminism(timer, mode, event_id, %{
      reason: :duplicate_event,
      last_event_id: event_id
    })
  end

  defp validate_event_order(
         %__MODULE__{last_event_id: last_event_id} = timer,
         %HistoryEvent{event_id: event_id},
         mode
       )
       when event_id < last_event_id do
    nondeterminism(timer, mode, event_id, %{
      reason: :out_of_order_event,
      last_event_id: last_event_id
    })
  end

  defp validate_event_order(_timer, _event, _mode), do: :ok

  defp match_field(_timer, _event_id, _mode, _field, value, value), do: :ok

  defp match_field(timer, event_id, mode, field, expected, actual) do
    nondeterminism(timer, mode, event_id, %{
      reason: :semantic_mismatch,
      field: field,
      expected: expected,
      actual: actual
    })
  end

  defp nondeterminism(timer, mode, event_id, details) do
    {:error,
     {:nondeterminism,
      Map.merge(
        %{
          mode: mode,
          event_id: event_id,
          state: timer.state,
          timer_id: timer.timer_id
        },
        details
      )}}
  end

  defp timer_event?(event_type),
    do:
      event_type in [
        :EVENT_TYPE_TIMER_STARTED,
        :EVENT_TYPE_TIMER_FIRED,
        :EVENT_TYPE_TIMER_CANCELED
      ]

  defp malformed_attributes?(
         :EVENT_TYPE_TIMER_STARTED,
         {:timer_started_event_attributes, %TimerStartedEventAttributes{}}
       ),
       do: false

  defp malformed_attributes?(
         :EVENT_TYPE_TIMER_FIRED,
         {:timer_fired_event_attributes, %TimerFiredEventAttributes{}}
       ),
       do: false

  defp malformed_attributes?(
         :EVENT_TYPE_TIMER_CANCELED,
         {:timer_canceled_event_attributes, %TimerCanceledEventAttributes{}}
       ),
       do: false

  defp malformed_attributes?(_event_type, _attributes), do: true

  defp valid_duration?(%Duration{seconds: seconds, nanos: nanos}) do
    is_integer(seconds) and seconds >= 0 and seconds <= 315_576_000_000 and
      is_integer(nanos) and nanos >= 0 and
      nanos <= 999_999_999
  end
end
