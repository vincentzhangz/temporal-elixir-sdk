defmodule Temporal.Workflow do
  @moduledoc """
  Deterministic APIs available while Workflow code is executing.

  Sequential Activity calls are replayed in deterministic call order.
  """

  alias Temporal.Api.Command.V1.{
    CancelTimerCommandAttributes,
    Command,
    ContinueAsNewWorkflowExecutionCommandAttributes,
    RequestCancelActivityTaskCommandAttributes,
    ScheduleActivityTaskCommandAttributes,
    StartTimerCommandAttributes
  }

  alias Temporal.Api.Common.V1.{ActivityType, WorkflowType}
  alias Temporal.Api.Sdk.V1.UserMetadata
  alias Temporal.Api.Taskqueue.V1.TaskQueue
  alias Temporal.Workflow.{CancellationScope, Future, TimerOptions}
  alias Temporal.Workflow.Signal.Dispatcher

  @context_key {__MODULE__, :context}

  @doc """
  Installs or replaces a deterministic named signal handler.

  The handler receives `(input, context, state)` and returns
  `{:ok, new_state}` or `{:error, reason}`. Signals recorded before
  registration are delivered immediately in history order.
  """
  @spec set_signal_handler(String.t(), Dispatcher.handler()) :: :ok
  def set_signal_handler(signal_name, handler) do
    context = workflow_context!("set_signal_handler/2")

    case Dispatcher.register(context.signal_dispatcher, signal_name, handler) do
      {:ok, dispatcher, _promoted} -> drive_signal_handlers(context, dispatcher)
      {:error, reason} -> throw({:temporal_signal_failed, reason})
    end
  end

  @doc "Installs or replaces the fallback handler for otherwise unknown signals."
  @spec set_dynamic_signal_handler(Dispatcher.handler()) :: :ok
  def set_dynamic_signal_handler(handler) do
    context = workflow_context!("set_dynamic_signal_handler/1")

    case Dispatcher.register_dynamic(context.signal_dispatcher, handler) do
      {:ok, dispatcher, _promoted} -> drive_signal_handlers(context, dispatcher)
    end
  end

  @doc "Removes a named signal handler for subsequently delivered signals."
  @spec remove_signal_handler(String.t()) :: :ok
  def remove_signal_handler(signal_name) do
    context = workflow_context!("remove_signal_handler/1")
    {:ok, dispatcher, _removed} = Dispatcher.remove(context.signal_dispatcher, signal_name)
    put_context(%{context | signal_dispatcher: dispatcher})
    :ok
  end

  @doc "Removes the dynamic signal fallback."
  @spec remove_dynamic_signal_handler() :: :ok
  def remove_dynamic_signal_handler do
    context = workflow_context!("remove_dynamic_signal_handler/0")
    {:ok, dispatcher, _removed} = Dispatcher.remove_dynamic(context.signal_dispatcher)
    put_context(%{context | signal_dispatcher: dispatcher})
    :ok
  end

  @doc "Returns the state produced by deterministic signal handlers."
  @spec signal_state() :: term()
  def signal_state, do: workflow_context!("signal_state/0").signal_dispatcher.workflow_state

  @doc """
  Waits until a predicate over signal-handler state becomes true.

  This is the BEAM-idiomatic deterministic receive primitive: it yields the
  Workflow Task without using a process mailbox and is re-evaluated from
  history when the next signal arrives.
  """
  @spec await_signal_state((term() -> boolean())) :: term()
  def await_signal_state(predicate) when is_function(predicate, 1) do
    context = workflow_context!("await_signal_state/1")
    state = context.signal_dispatcher.workflow_state

    if predicate.(state) do
      state
    else
      throw({:temporal_workflow_blocked, Map.get(context, :pending_commands, [])})
    end
  end

  def await_signal_state(_predicate),
    do: raise(ArgumentError, "await_signal_state/1 expects a one-argument predicate")

  @doc "Returns whether no signal is buffered, scheduled, or running."
  @spec all_signal_handlers_finished?() :: boolean()
  def all_signal_handlers_finished? do
    workflow_context!("all_signal_handlers_finished?/0").signal_dispatcher
    |> Dispatcher.completable?()
  end

  @doc "Runs ready handlers and waits until all signal work is finished."
  @spec wait_for_all_signal_handlers() :: :ok
  def wait_for_all_signal_handlers do
    context = workflow_context!("wait_for_all_signal_handlers/0")
    :ok = drive_signal_handlers(context, context.signal_dispatcher)
    context = workflow_context!("wait_for_all_signal_handlers/0")

    if Dispatcher.completable?(context.signal_dispatcher) do
      :ok
    else
      throw({:temporal_workflow_blocked, Map.get(context, :pending_commands, [])})
    end
  end

  @doc """
  Returns deterministic logical Workflow time from the current history event.

  This value advances only as recorded history is reduced and must be used in
  Workflow code instead of wall-clock functions.
  """
  @spec now() :: DateTime.t()
  def now do
    case workflow_context!("now/0") do
      %{logical_time: %Google.Protobuf.Timestamp{seconds: seconds, nanos: nanos}} ->
        {:ok, datetime} = DateTime.from_unix(seconds * 1_000_000_000 + nanos, :nanosecond)
        datetime

      _context ->
        raise ArgumentError, "logical Workflow time is unavailable before history activation"
    end
  end

  @doc """
  Suspends Workflow execution for a durable, history-backed duration.

  Integer durations are milliseconds. A valid `Google.Protobuf.Duration` may
  also be supplied. Positive durations below Temporal's one-millisecond timer
  precision are rounded up. Zero resolves locally; negative durations raise
  `ArgumentError` before any command is emitted.
  """
  @spec sleep(non_neg_integer() | Google.Protobuf.Duration.t(), TimerOptions.t() | keyword()) ::
          :ok
  def sleep(duration, options \\ []) do
    duration
    |> new_timer(options)
    |> await()
  end

  @doc """
  Creates a deterministic durable timer future without awaiting it.

  Timer IDs and sequence slots follow Workflow API call order (`timer-1`,
  `timer-2`, ...). Creating more than one future before awaiting batches their
  `StartTimer` commands in that same order.
  """
  @spec new_timer(non_neg_integer() | Google.Protobuf.Duration.t(), TimerOptions.t() | keyword()) ::
          Future.t()
  def new_timer(duration, options \\ []) do
    context = workflow_context!("new_timer/2")
    timer_options = timer_options!(options)
    timeout = timer_duration!(duration)
    sequence = Map.get(context, :operation_index, 0) + 1
    timer_id = "timer-#{sequence}"

    case Map.get(Map.get(context, :operations, %{}), sequence) do
      %{type: :timer, id: ^timer_id, status: status} = operation ->
        operation =
          Map.put(
            operation,
            :cancellation_scope_id,
            timer_options.cancellation_scope && timer_options.cancellation_scope.id
          )

        put_context(
          context
          |> Map.put(:operation_index, sequence)
          |> Map.update!(:operations, &Map.put(&1, sequence, operation))
        )

        timer_future(timer_id, sequence, status)

      nil when timeout.seconds == 0 and timeout.nanos == 0 ->
        put_context(Map.put(context, :operation_index, sequence))
        %Future{id: timer_id, sequence: sequence, type: :timer, resolution: {:ok, :fired}}

      nil ->
        command = start_timer_command(timer_id, timeout, timer_options.summary)

        operation = %{
          type: :timer,
          id: timer_id,
          status: :start_command_created,
          timeout: timeout,
          summary: timer_options.summary,
          cancellation_scope_id:
            timer_options.cancellation_scope && timer_options.cancellation_scope.id
        }

        next =
          context
          |> Map.put(:operation_index, sequence)
          |> Map.update(:operations, %{sequence => operation}, &Map.put(&1, sequence, operation))
          |> Map.update(:pending_commands, [command], &(&1 ++ [command]))

        put_context(next)
        %Future{id: timer_id, sequence: sequence, type: :timer}

      operation ->
        raise ArgumentError,
              "deterministic operation #{sequence} changed from #{inspect(operation.type)} to timer"
    end
  end

  @doc "Awaits a Workflow future, yielding only after its history-backed resolution."
  @spec await(Future.t()) :: :ok | no_return()
  def await(%Future{type: :timer} = future) do
    context = workflow_context!("await/1")

    status =
      case Map.get(Map.get(context, :operations, %{}), future.sequence) do
        %{type: :timer, id: id, status: status} when id == future.id -> status
        nil -> future_status(future.resolution)
        _other -> raise ArgumentError, "future does not belong to this deterministic invocation"
      end

    case status do
      :fired ->
        :ok

      :canceled ->
        raise Temporal.CanceledError, message: "Workflow timer canceled", acknowledged: true

      :cancel_command_created ->
        raise Temporal.CanceledError, message: "Workflow timer canceled", acknowledged: false

      status when status in [:start_command_created, :started] ->
        throw({:temporal_workflow_blocked, Map.get(context, :pending_commands, [])})
    end
  end

  def await(_future), do: raise(ArgumentError, "await/1 expects a Workflow future")

  @doc """
  Cancels a durable timer future.

  A timer canceled before its start batch is sent is resolved locally and emits
  no command. A recorded timer queues one `CancelTimer` command. Repeated
  cancellation of a terminal or already-canceling timer is idempotent.
  """
  @spec cancel_timer(Future.t()) :: :ok
  def cancel_timer(%Future{type: :timer, id: timer_id, sequence: sequence}) do
    context = workflow_context!("cancel_timer/1")
    operations = Map.get(context, :operations, %{})

    case Map.get(operations, sequence) do
      %{type: :timer, id: ^timer_id, status: :start_command_created} = operation ->
        pending =
          Enum.reject(
            Map.get(context, :pending_commands, []),
            &start_timer_command?(&1, timer_id)
          )

        next_operation = %{operation | status: :canceled}

        put_context(%{
          context
          | operations: Map.put(operations, sequence, next_operation),
            pending_commands: pending
        })

      %{type: :timer, id: ^timer_id, status: :started} = operation ->
        command = cancel_timer_command(timer_id)
        next_operation = %{operation | status: :cancel_command_created}

        put_context(%{
          context
          | operations: Map.put(operations, sequence, next_operation),
            pending_commands: Map.get(context, :pending_commands, []) ++ [command]
        })

      %{type: :timer, id: ^timer_id, status: status}
      when status in [:cancel_command_created, :canceled, :fired] ->
        :ok

      _other ->
        raise ArgumentError, "timer future does not belong to this deterministic invocation"
    end

    :ok
  end

  def cancel_timer(_future), do: raise(ArgumentError, "cancel_timer/1 expects a timer future")

  @doc "Creates a deterministic cancellation scope for Workflow operations."
  @spec new_cancellation_scope() :: CancellationScope.t()
  def new_cancellation_scope do
    context = workflow_context!("new_cancellation_scope/0")
    sequence = Map.get(context, :cancellation_scope_index, 0) + 1
    put_context(Map.put(context, :cancellation_scope_index, sequence))
    %CancellationScope{id: "scope-#{sequence}", sequence: sequence}
  end

  @doc "Cancels every nonterminal timer attached to a cancellation scope."
  @spec cancel_scope(CancellationScope.t()) :: :ok
  def cancel_scope(%CancellationScope{id: scope_id}) do
    context = workflow_context!("cancel_scope/1")
    operations = Map.get(context, :operations, %{})

    scoped =
      Enum.filter(operations, fn {_sequence, operation} ->
        operation[:cancellation_scope_id] == scope_id
      end)

    canceled_unsent_ids =
      for {_sequence, %{status: :start_command_created, id: id}} <- scoped, do: id

    operations =
      Enum.reduce(scoped, operations, fn {sequence, operation}, acc ->
        Map.put(acc, sequence, cancel_scoped_operation(operation))
      end)

    pending =
      context
      |> Map.get(:pending_commands, [])
      |> Enum.reject(fn command ->
        Enum.any?(canceled_unsent_ids, &start_timer_command?(command, &1))
      end)

    cancel_commands =
      for {_sequence, %{status: :started, id: id}} <- scoped,
          not Enum.any?(pending, &cancel_timer_command?(&1, id)),
          do: cancel_timer_command(id)

    put_context(%{context | operations: operations, pending_commands: pending ++ cancel_commands})
    :ok
  end

  def cancel_scope(_scope),
    do: raise(ArgumentError, "cancel_scope/1 expects a Workflow cancellation scope")

  @spec execute_activity(String.t(), term(), keyword()) :: term()
  def execute_activity(activity_type, argument, options)
      when is_binary(activity_type) and is_list(options) do
    context =
      Process.get(@context_key) ||
        raise ArgumentError, "execute_activity/3 may only be called from Workflow execution"

    index = Map.get(context, :activity_index, 0) + 1
    put_context(Map.put(context, :activity_index, index))

    case activity_outcome(context, index) do
      {:ok, result} ->
        result

      {:error, exception} when is_exception(exception) ->
        raise exception

      {:error, reason} ->
        raise Temporal.ActivityError, cause: reason

      nil ->
        block_on_activity(context, index, activity_type, argument, options)
    end
  end

  @doc """
  Requests cancellation of the Activity identified by its scheduled event ID.

  Cancellation is not acknowledged until the Activity worker receives
  `cancel_requested` from a heartbeat and responds with
  `RespondActivityTaskCanceled`.
  """
  @spec request_cancel_activity(pos_integer()) :: no_return()
  def request_cancel_activity(scheduled_event_id)
      when is_integer(scheduled_event_id) and scheduled_event_id > 0 do
    Process.get(@context_key) ||
      raise ArgumentError, "request_cancel_activity/1 may only be called from Workflow execution"

    command = %Command{
      command_type: :COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
      attributes:
        {:request_cancel_activity_task_command_attributes,
         %RequestCancelActivityTaskCommandAttributes{scheduled_event_id: scheduled_event_id}}
    }

    throw({:temporal_workflow_blocked, command})
  end

  def request_cancel_activity(_scheduled_event_id),
    do: raise(ArgumentError, "scheduled Activity event ID must be a positive integer")

  @dialyzer {:nowarn_function, continue_as_new: 1}
  @dialyzer {:nowarn_function, continue_as_new: 2}
  @spec continue_as_new(term(), keyword()) :: no_return()
  def continue_as_new(input, options \\ []) when is_list(options) do
    context =
      Process.get(@context_key) ||
        raise ArgumentError, "continue_as_new/2 may only be called from Workflow execution"

    ensure_signal_handlers_finished!(context)
    reject_unsupported_continue_options!(options)

    workflow_type = Keyword.get(options, :workflow_type, context.workflow_type)
    task_queue = Keyword.get(options, :task_queue, context.task_queue)

    command = %Command{
      command_type: :COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
      attributes:
        {:continue_as_new_workflow_execution_command_attributes,
         %ContinueAsNewWorkflowExecutionCommandAttributes{
           workflow_type: %WorkflowType{name: workflow_type},
           task_queue: %TaskQueue{name: task_queue},
           input: Temporal.Payload.encode(input),
           workflow_run_timeout: optional_duration!(options, :workflow_run_timeout),
           workflow_task_timeout: optional_duration!(options, :workflow_task_timeout),
           backoff_start_interval: optional_duration!(options, :backoff_start_interval),
           retry_policy: Keyword.get(options, :retry_policy),
           header: Keyword.get(options, :header),
           memo: Keyword.get(options, :memo),
           search_attributes: Keyword.get(options, :search_attributes)
         }}
    }

    throw({:temporal_workflow_blocked, command})
  end

  @doc false
  def put_context(context), do: Process.put(@context_key, context)

  @doc false
  def context, do: Process.get(@context_key)

  @doc false
  def clear_context, do: Process.delete(@context_key)

  defp workflow_context!(api) do
    Process.get(@context_key) ||
      raise(ArgumentError, "#{api} may only be called from Workflow execution")
  end

  defp drive_signal_handlers(context, dispatcher) do
    put_context(%{context | signal_dispatcher: dispatcher})

    case Dispatcher.run_all(dispatcher) do
      {:ok, finished} ->
        latest = workflow_context!("signal handler")
        put_context(%{latest | signal_dispatcher: finished})
        :ok

      {:error, reason} ->
        throw({:temporal_signal_failed, reason})
    end
  end

  defp ensure_signal_handlers_finished!(%{signal_dispatcher: dispatcher}) do
    unless Dispatcher.continue_as_new_ready?(dispatcher) do
      throw(
        {:temporal_signal_failed,
         {:unfinished_signal_handlers,
          %{
            pending: Dispatcher.pending_count(dispatcher),
            buffered_event_ids: Enum.map(Dispatcher.buffered(dispatcher), & &1.event_id)
          }}}
      )
    end
  end

  defp ensure_signal_handlers_finished!(_context), do: :ok

  defp activity_outcome(context, index) do
    if Map.has_key?(context, :activity_outcomes) do
      Map.get(context.activity_outcomes, index)
    else
      Map.get(context, :activity_outcome)
    end
  end

  defp block_on_activity(context, index, activity_type, argument, options) do
    if pending_activity?(context, index) do
      throw({:temporal_workflow_blocked, Map.get(context, :pending_commands, [])})
    else
      command = schedule_command(activity_type, argument, options, index)
      existing = Map.get(context, :pending_commands, [])
      pending = existing ++ [command]
      put_context(Map.put(context, :pending_commands, pending))
      throw({:temporal_workflow_blocked, if(existing == [], do: command, else: pending)})
    end
  end

  defp pending_activity?(context, index),
    do:
      Map.get(Map.get(context, :activity_states, %{}), index) in [
        :scheduled,
        :started,
        :cancel_requested
      ]

  defp timer_options!(%TimerOptions{} = options) do
    options
    |> validate_timer_summary!()
    |> validate_timer_scope!()
  end

  defp timer_options!(options) when is_list(options) do
    options
    |> then(&struct!(TimerOptions, &1))
    |> validate_timer_summary!()
    |> validate_timer_scope!()
  end

  defp timer_options!(_options),
    do: raise(ArgumentError, "timer options must be TimerOptions or a keyword list")

  defp validate_timer_summary!(%TimerOptions{summary: nil} = options), do: options

  defp validate_timer_summary!(%TimerOptions{summary: summary} = options)
       when is_binary(summary),
       do: options

  defp validate_timer_summary!(_options),
    do: raise(ArgumentError, "timer summary must be a string")

  defp validate_timer_scope!(%TimerOptions{cancellation_scope: nil} = options), do: options

  defp validate_timer_scope!(%TimerOptions{cancellation_scope: %CancellationScope{}} = options),
    do: options

  defp validate_timer_scope!(_options),
    do: raise(ArgumentError, "timer cancellation scope must be a Workflow cancellation scope")

  defp cancel_scoped_operation(%{type: :timer, status: :start_command_created} = operation),
    do: %{operation | status: :canceled}

  defp cancel_scoped_operation(%{type: :timer, status: :started} = operation),
    do: %{operation | status: :cancel_command_created}

  defp cancel_scoped_operation(operation), do: operation

  defp timer_duration!(milliseconds) when is_integer(milliseconds) and milliseconds < 0,
    do: raise(ArgumentError, "timer duration must not be negative")

  defp timer_duration!(milliseconds)
       when is_integer(milliseconds) and milliseconds <= 315_576_000_000_999 do
    %Google.Protobuf.Duration{
      seconds: div(milliseconds, 1_000),
      nanos: rem(milliseconds, 1_000) * 1_000_000
    }
  end

  defp timer_duration!(milliseconds) when is_integer(milliseconds),
    do: raise(ArgumentError, "timer duration exceeds protobuf Duration range")

  defp timer_duration!(%Google.Protobuf.Duration{seconds: seconds, nanos: nanos})
       when is_integer(seconds) and is_integer(nanos) and seconds >= 0 and nanos >= 0 and
              seconds <= 315_576_000_000 and nanos <= 999_999_999 do
    if seconds == 0 and nanos > 0 and nanos < 1_000_000 do
      %Google.Protobuf.Duration{nanos: 1_000_000}
    else
      %Google.Protobuf.Duration{seconds: seconds, nanos: nanos}
    end
  end

  defp timer_duration!(%Google.Protobuf.Duration{seconds: seconds, nanos: nanos})
       when seconds < 0 or nanos < 0,
       do: raise(ArgumentError, "timer duration must not be negative")

  defp timer_duration!(%Google.Protobuf.Duration{}),
    do: raise(ArgumentError, "invalid protobuf Duration for timer")

  defp timer_duration!(_duration),
    do: raise(ArgumentError, "timer duration must be milliseconds or protobuf Duration")

  defp timer_future(id, sequence, :fired),
    do: %Future{id: id, sequence: sequence, type: :timer, resolution: {:ok, :fired}}

  defp timer_future(id, sequence, :canceled),
    do: %Future{
      id: id,
      sequence: sequence,
      type: :timer,
      resolution:
        {:error,
         Temporal.CanceledError.exception(
           message: "Workflow timer canceled",
           acknowledged: true
         )}
    }

  defp timer_future(id, sequence, _status),
    do: %Future{id: id, sequence: sequence, type: :timer}

  defp future_status({:ok, :fired}), do: :fired
  defp future_status({:error, %Temporal.CanceledError{}}), do: :canceled
  defp future_status(nil), do: :start_command_created

  defp start_timer_command(timer_id, timeout, summary) do
    %Command{
      command_type: :COMMAND_TYPE_START_TIMER,
      user_metadata: timer_user_metadata(summary),
      attributes:
        {:start_timer_command_attributes,
         %StartTimerCommandAttributes{
           timer_id: timer_id,
           start_to_fire_timeout: timeout
         }}
    }
  end

  defp timer_user_metadata(nil), do: nil

  defp timer_user_metadata(summary) do
    %UserMetadata{summary: Temporal.Payload.encode(summary).payloads |> List.first()}
  end

  defp cancel_timer_command(timer_id) do
    %Command{
      command_type: :COMMAND_TYPE_CANCEL_TIMER,
      attributes:
        {:cancel_timer_command_attributes, %CancelTimerCommandAttributes{timer_id: timer_id}}
    }
  end

  defp start_timer_command?(
         %Command{
           command_type: :COMMAND_TYPE_START_TIMER,
           attributes:
             {:start_timer_command_attributes, %StartTimerCommandAttributes{timer_id: timer_id}}
         },
         timer_id
       ),
       do: true

  defp start_timer_command?(_command, _timer_id), do: false

  defp cancel_timer_command?(
         %Command{
           command_type: :COMMAND_TYPE_CANCEL_TIMER,
           attributes:
             {:cancel_timer_command_attributes, %CancelTimerCommandAttributes{timer_id: timer_id}}
         },
         timer_id
       ),
       do: true

  defp cancel_timer_command?(_command, _timer_id), do: false

  defp schedule_command(activity_type, argument, options, index) do
    task_queue = required_string!(options, :task_queue)
    activity_id = Keyword.get(options, :activity_id, "activity-#{index}")
    start_to_close = duration!(options, :start_to_close_timeout)
    schedule_to_close = optional_duration!(options, :schedule_to_close_timeout)
    schedule_to_start = optional_duration!(options, :schedule_to_start_timeout)
    heartbeat = optional_duration!(options, :heartbeat_timeout)
    retry_policy = retry_policy!(Keyword.get(options, :retry_policy))

    if is_nil(start_to_close) and is_nil(schedule_to_close) do
      raise ArgumentError,
            "Activity requires :start_to_close_timeout or :schedule_to_close_timeout"
    end

    %Command{
      command_type: :COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
      attributes:
        {:schedule_activity_task_command_attributes,
         %ScheduleActivityTaskCommandAttributes{
           activity_id: activity_id,
           activity_type: %ActivityType{name: activity_type},
           task_queue: %TaskQueue{name: task_queue},
           input: Temporal.Payload.encode(argument),
           start_to_close_timeout: start_to_close,
           schedule_to_close_timeout: schedule_to_close,
           schedule_to_start_timeout: schedule_to_start,
           heartbeat_timeout: heartbeat,
           retry_policy: retry_policy
         }}
    }
  end

  defp required_string!(options, key) do
    case Keyword.fetch(options, key) do
      {:ok, value} when is_binary(value) and value != "" -> value
      _other -> raise ArgumentError, "Activity requires non-empty #{inspect(key)}"
    end
  end

  defp duration!(options, key) do
    case Keyword.fetch(options, key) do
      {:ok, seconds} -> duration(seconds, key)
      :error -> nil
    end
  end

  defp optional_duration!(options, key), do: duration!(options, key)

  defp duration(seconds, _key) when is_integer(seconds) and seconds > 0,
    do: %Google.Protobuf.Duration{seconds: seconds}

  defp duration(_seconds, key),
    do: raise(ArgumentError, "#{inspect(key)} must be a positive integer number of seconds")

  defp retry_policy!(nil), do: nil

  defp retry_policy!(%Temporal.Api.Common.V1.RetryPolicy{} = policy) do
    validate_retry_policy!(policy)
  end

  defp retry_policy!(options) when is_list(options) do
    policy = %Temporal.Api.Common.V1.RetryPolicy{
      initial_interval: retry_duration!(options, :initial_interval),
      backoff_coefficient: Keyword.get(options, :backoff_coefficient, 0.0),
      maximum_interval: retry_duration!(options, :maximum_interval),
      maximum_attempts: Keyword.get(options, :maximum_attempts, 0),
      non_retryable_error_types: Keyword.get(options, :non_retryable_error_types, [])
    }

    validate_retry_policy!(policy)
  end

  defp retry_policy!(_policy),
    do: raise(ArgumentError, ":retry_policy must be a RetryPolicy struct or keyword list")

  defp retry_duration!(options, key) do
    case Keyword.fetch(options, key) do
      {:ok, seconds} -> duration(seconds, key)
      :error -> nil
    end
  end

  defp validate_retry_policy!(%{backoff_coefficient: coefficient})
       when coefficient != 0.0 and coefficient < 1.0,
       do: raise(ArgumentError, ":backoff_coefficient must be at least 1.0")

  defp validate_retry_policy!(%{maximum_attempts: attempts}) when attempts < 0,
    do: raise(ArgumentError, ":maximum_attempts must be zero or greater")

  defp validate_retry_policy!(%{non_retryable_error_types: types} = policy)
       when is_list(types) do
    if Enum.all?(types, &(is_binary(&1) and &1 != "")) do
      policy
    else
      raise ArgumentError, ":non_retryable_error_types must contain non-empty strings"
    end
  end

  defp validate_retry_policy!(_policy),
    do: raise(ArgumentError, "invalid Activity retry policy")

  defp reject_unsupported_continue_options!(options) do
    unsupported =
      options
      |> Keyword.keys()
      |> Enum.find(&(&1 in [:workflow_execution_timeout, :cron_schedule, :failure, :initiator]))

    if unsupported do
      raise ArgumentError, "unsupported continue-as-new option #{inspect(unsupported)}"
    end
  end
end
