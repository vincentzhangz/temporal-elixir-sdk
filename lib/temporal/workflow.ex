defmodule Temporal.Workflow do
  @moduledoc """
  Deterministic APIs available while Workflow code is executing.

  Sequential Activity calls are replayed in deterministic call order.
  """

  alias Temporal.Api.Command.V1.{
    CancelTimerCommandAttributes,
    CancelWorkflowExecutionCommandAttributes,
    Command,
    ContinueAsNewWorkflowExecutionCommandAttributes,
    FailWorkflowExecutionCommandAttributes,
    ModifyWorkflowPropertiesCommandAttributes,
    RecordMarkerCommandAttributes,
    RequestCancelActivityTaskCommandAttributes,
    RequestCancelExternalWorkflowExecutionCommandAttributes,
    ScheduleActivityTaskCommandAttributes,
    ScheduleNexusOperationCommandAttributes,
    SignalExternalWorkflowExecutionCommandAttributes,
    StartChildWorkflowExecutionCommandAttributes,
    StartTimerCommandAttributes,
    UpsertWorkflowSearchAttributesCommandAttributes
  }

  alias Temporal.Api.Common.V1.{
    ActivityType,
    Memo,
    SearchAttributes,
    WorkflowExecution,
    WorkflowType
  }

  alias Temporal.Api.Sdk.V1.UserMetadata
  alias Temporal.Api.Taskqueue.V1.TaskQueue
  alias Temporal.Workflow.{CancellationScope, Future, TimerOptions}
  alias Temporal.Workflow.Signal.Dispatcher
  alias Temporal.Workflow.Update.Dispatcher, as: UpdateDispatcher

  @context_key {__MODULE__, :context}
  @query_context_key {__MODULE__, :query_context}

  @doc """
  Installs or replaces a deterministic named query handler.

  The handler receives `(query_args)` and must return the query result. It may
  read deterministic Workflow state via `signal_state/0`, `now/0`, and the
  other context APIs. The handler runs only when a query arrives and must not
  emit commands.
  """
  @spec set_query_handler(String.t(), (term() -> term())) :: :ok
  def set_query_handler(query_type, handler)
      when is_binary(query_type) and query_type != "" and is_function(handler, 1) do
    context = workflow_context!("set_query_handler/2")

    put_context(
      Map.update(
        context,
        :query_handlers,
        %{query_type => handler},
        &Map.put(&1, query_type, handler)
      )
    )

    :ok
  end

  def set_query_handler(query_type, _handler) when is_binary(query_type),
    do: raise(ArgumentError, "set_query_handler/2 expects a one-argument handler")

  def set_query_handler(_query_type, _handler),
    do: raise(ArgumentError, "set_query_handler/2 expects a non-empty query type string")

  @doc """
  Installs or replaces a deterministic named Update handler.

  The handler receives `(decoded_args, context)` and must return
  `{:ok, result}` or `{:error, reason}`. It runs during the Workflow Task that
  carries the update request and must not emit commands. Updates are
  synchronous request/response: the client's `update_workflow/3,4` returns
  after the handler completes.
  """
  @spec set_update_handler(String.t(), (term(), map() -> {:ok, term()} | {:error, term()})) :: :ok
  def set_update_handler(update_name, handler)
      when is_binary(update_name) and update_name != "" and is_function(handler, 2) do
    context = workflow_context!("set_update_handler/2")
    dispatcher = Map.get(context, :update_dispatcher, UpdateDispatcher.new())

    case UpdateDispatcher.register(dispatcher, update_name, handler) do
      {:ok, next} ->
        put_context(Map.put(context, :update_dispatcher, next))
        :ok

      {:error, reason} ->
        throw({:temporal_update_failed, reason})
    end
  end

  def set_update_handler(_update_name, _handler),
    do:
      raise(
        ArgumentError,
        "set_update_handler/2 expects a non-empty name and a two-argument handler"
      )

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

  @doc "Cancels every nonterminal timer and scoped child Workflow in a cancellation scope."
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

    child_cancel_commands = scoped_child_cancel_commands(context, scope_id)

    put_context(%{
      context
      | operations: operations,
        pending_commands: pending ++ cancel_commands ++ child_cancel_commands
    })

    :ok
  end

  def cancel_scope(_scope),
    do: raise(ArgumentError, "cancel_scope/1 expects a Workflow cancellation scope")

  @doc """
  Immediately fails the Workflow Execution with a typed failure.

  `failure` may be a `Temporal.ApplicationError`, `Temporal.CanceledError`,
  `Temporal.TimeoutError`, or any other exception; it is encoded into the
  `FailWorkflowExecution` command's Failure payload. No further Workflow code
  runs after this call.
  """
  @spec fail_workflow(Exception.t() | term()) :: no_return()
  def fail_workflow(failure) do
    Process.get(@context_key) ||
      raise ArgumentError, "fail_workflow/1 may only be called from Workflow execution"

    command = %Command{
      command_type: :COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
      attributes:
        {:fail_workflow_execution_command_attributes,
         %FailWorkflowExecutionCommandAttributes{
           failure: Temporal.Failure.to_proto(exception!(failure), [])
         }}
    }

    throw({:temporal_workflow_blocked, command})
  end

  @doc """
  Requests cancellation of the current Workflow Execution.

  Emits `COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION`; the server records a
  `WorkflowExecutionCanceled` close event. Cleanup code (timers, activities,
  signal handlers) may run before this call.
  """
  @spec cancel_workflow() :: no_return()
  def cancel_workflow do
    Process.get(@context_key) ||
      raise ArgumentError, "cancel_workflow/0 may only be called from Workflow execution"

    command = %Command{
      command_type: :COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
      attributes:
        {:cancel_workflow_execution_command_attributes,
         %CancelWorkflowExecutionCommandAttributes{}}
    }

    throw({:temporal_workflow_blocked, command})
  end

  @doc """
  Signals an external Workflow Execution from Workflow code.

  Emits `COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION` and blocks until the
  `SignalExternalWorkflowExecutionInitiated` event records the delivery (or the
  corresponding failed event). An empty `run_id` targets the current run of the
  given Workflow ID. The signal is delivered at-least-once and this call
  returns `:ok` only once the initiated event is recorded.
  """
  @spec signal_external_workflow(String.t(), String.t(), String.t(), term(), keyword()) ::
          :ok | no_return()
  def signal_external_workflow(workflow_id, run_id, signal_name, input, options \\ []) do
    Process.get(@context_key) ||
      raise ArgumentError, "signal_external_workflow/5 may only be called from Workflow execution"

    context = workflow_context!("signal_external_workflow/5")
    sequence = Map.get(context, :external_signal_index, 0) + 1
    put_context(Map.put(context, :external_signal_index, sequence))

    case external_signal_outcome(context, sequence) do
      :ok ->
        :ok

      {:error, reason} ->
        raise Temporal.ApplicationError,
          message: "external signal failed",
          type: "SignalExternalWorkflowError",
          details: reason

      nil ->
        command = %Command{
          command_type: :COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
          attributes:
            {:signal_external_workflow_execution_command_attributes,
             %SignalExternalWorkflowExecutionCommandAttributes{
               execution: %WorkflowExecution{
                 workflow_id: workflow_id,
                 run_id: run_id
               },
               signal_name: signal_name,
               input: Temporal.Payload.encode(input),
               header: Keyword.get(options, :header)
             }}
        }

        throw({:temporal_workflow_blocked, command})
    end
  end

  @doc """
  Executes a Child Workflow Execution from Workflow code.

  Emits `COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION` and blocks until the child
  reaches a terminal state. `options` accepts `:task_queue`, `:workflow_id`,
  `:workflow_execution_timeout`, `:workflow_run_timeout`,
  `:workflow_task_timeout`, `:parent_close_policy`, `:retry_policy`, `:header`,
  `:memo`, and `:search_attributes`. Returns the child's result.
  """
  @spec execute_child_workflow(String.t(), term(), keyword()) :: term() | no_return()
  def execute_child_workflow(workflow_type, input, options \\ []) do
    Process.get(@context_key) ||
      raise ArgumentError, "execute_child_workflow/3 may only be called from Workflow execution"

    context = workflow_context!("execute_child_workflow/3")
    index = Map.get(context, :child_workflow_index, 0) + 1

    context =
      context
      |> Map.put(:child_workflow_index, index)
      |> then(&record_child_scope(&1, index, workflow_type, input, options))

    put_context(context)

    case child_workflow_outcome(context, index) do
      {:ok, result} ->
        result

      {:error, exception} when is_exception(exception) ->
        raise exception

      {:error, reason} ->
        raise Temporal.ActivityError, cause: reason

      nil ->
        block_on_child_workflow(context, index, workflow_type, input, options)
    end
  end

  defp record_child_scope(context, index, workflow_type, input, options) do
    case Keyword.get(options, :cancellation_scope) do
      %CancellationScope{id: scope_id} ->
        command = start_child_command(workflow_type, input, options, index)
        workflow_id = child_workflow_id(command)

        context
        |> Map.update(
          :child_workflow_ids,
          %{index => workflow_id},
          &Map.put(&1, index, workflow_id)
        )
        |> Map.update(:child_workflow_scopes, %{scope_id => [index]}, fn scopes ->
          Map.update(scopes, scope_id, [index], &(&1 ++ [index]))
        end)

      _nil ->
        context
    end
  end

  @doc "Blocks until the Child Workflow started by `execute_child_workflow/3` resolves."
  @spec await_child_workflow(pos_integer() | String.t()) :: term() | no_return()
  def await_child_workflow(index_or_id) when is_integer(index_or_id) do
    context = workflow_context!("await_child_workflow/1")
    outcome = child_workflow_outcome(context, index_or_id)

    case outcome do
      {:ok, result} ->
        result

      {:error, exception} when is_exception(exception) ->
        raise exception

      {:error, reason} ->
        raise Temporal.ActivityError, cause: reason

      nil ->
        throw({:temporal_workflow_blocked, Map.get(context, :pending_commands, [])})
    end
  end

  def await_child_workflow(_id),
    do: raise(ArgumentError, "await_child_workflow/1 expects a positive integer index")

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
  Executes a local Activity inline in the Workflow worker.

  Local Activities run without a server round-trip: the result is recorded into
  history via a `LocalActivity` marker and returned on replay. They cannot
  heartbeat and are intended for short operations. `local_activities` on the
  worker (`%{"Name" => fun}`) supplies the implementation.
  """
  @spec execute_local_activity(String.t(), term(), keyword()) :: term()
  def execute_local_activity(activity_type, argument, options \\ [])
      when is_binary(activity_type) and is_list(options) do
    context = workflow_context!("execute_local_activity/3")
    index = Map.get(context, :local_activity_index, 0) + 1
    context = Map.put(context, :local_activity_index, index)
    put_context(context)

    case Map.get(Map.get(context, :local_activity_results, %{}), index) do
      nil ->
        result = run_local_activity(activity_type, argument, options)

        command = %Command{
          command_type: :COMMAND_TYPE_RECORD_MARKER,
          attributes:
            {:record_marker_command_attributes,
             %RecordMarkerCommandAttributes{
               marker_name: "LocalActivity",
               details: %{
                 "activity_type" => Temporal.Payload.encode(activity_type),
                 "data" => Temporal.Payload.encode(result)
               }
             }}
        }

        put_context(
          context
          |> Map.put(
            :local_activity_results,
            Map.put(context.local_activity_results, index, result)
          )
          |> Map.update(:pending_commands, [command], &(&1 ++ [command]))
        )

        result

      result ->
        result
    end
  end

  defp run_local_activity(_activity_type, argument, options) do
    case Keyword.fetch(options, :activity) do
      {:ok, activity} ->
        invoke_local(activity, argument)

      :error ->
        raise ArgumentError,
              "execute_local_activity/3 requires the :activity implementation option"
    end
  end

  defp invoke_local(activity, _argument) when is_function(activity, 0), do: activity.()

  defp invoke_local(activity, argument) when is_function(activity, 1), do: activity.(argument)

  defp invoke_local(_activity, _argument),
    do: raise(ArgumentError, "local Activity must be a zero- or one-argument function")

  @doc """
  Creates a Nexus client bound to an endpoint and service.

  Returns a map with `:endpoint` and `:service`; pass it to
  `execute_nexus_operation/3`.
  """
  @spec new_nexus_client(String.t(), String.t()) :: %{endpoint: String.t(), service: String.t()}
  def new_nexus_client(endpoint, service)
      when is_binary(endpoint) and endpoint != "" and is_binary(service) and service != "" do
    %{endpoint: endpoint, service: service}
  end

  @doc """
  Executes a Nexus operation from Workflow code.

  `client` comes from `new_nexus_client/2`; `operation` is the operation name,
  `input` its argument, and `options` accepts `:schedule_to_close_timeout`,
  `:schedule_to_start_timeout`, and `:nexus_header` (a map). Blocks until the
  operation reaches a terminal state and returns its result.
  """
  @spec execute_nexus_operation(map(), String.t(), term(), keyword()) :: term() | no_return()
  def execute_nexus_operation(client, operation, input, options \\ []) do
    Process.get(@context_key) ||
      raise ArgumentError, "execute_nexus_operation/4 may only be called from Workflow execution"

    context = workflow_context!("execute_nexus_operation/4")
    index = Map.get(context, :nexus_operation_index, 0) + 1
    context = Map.put(context, :nexus_operation_index, index)
    put_context(context)

    case Map.get(Map.get(context, :nexus_operation_outcomes, %{}), index) do
      {:ok, result} ->
        result

      {:error, exception} when is_exception(exception) ->
        raise exception

      {:error, reason} ->
        raise Temporal.ActivityError, cause: reason

      nil ->
        command = %Command{
          command_type: :COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
          attributes:
            {:schedule_nexus_operation_command_attributes,
             %ScheduleNexusOperationCommandAttributes{
               endpoint: client.endpoint,
               service: client.service,
               operation: operation,
               input: Temporal.Payload.encode(input) |> Map.fetch!(:payloads) |> List.first(),
               schedule_to_close_timeout: optional_duration!(options, :schedule_to_close_timeout),
               schedule_to_start_timeout: optional_duration!(options, :schedule_to_start_timeout),
               nexus_header: Keyword.get(options, :nexus_header, %{})
             }}
        }

        existing = Map.get(context, :pending_commands, [])
        pending = existing ++ [command]
        put_context(Map.put(context, :pending_commands, pending))
        throw({:temporal_workflow_blocked, if(existing == [], do: command, else: pending)})
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

  @doc """
  Safely versions Workflow code changes (the analogue of the official SDKs'
  `GetVersion`).

  `change_id` identifies the code change; `min_supported`/`max_supported` bound
  the returned version. The first invocation records a Version marker into
  history; replays return the recorded version so old and new code coexist
  deterministically.
  """
  @spec get_version(String.t(), integer(), integer()) :: integer()
  def get_version(change_id, min_supported, max_supported)
      when is_binary(change_id) and change_id != "" and is_integer(min_supported) and
             is_integer(max_supported) and min_supported <= max_supported do
    context = workflow_context!("get_version/3")
    versions = Map.get(context, :version_markers, %{})

    case Map.fetch(versions, change_id) do
      {:ok, version} ->
        version

      :error ->
        command = %Command{
          command_type: :COMMAND_TYPE_RECORD_MARKER,
          attributes:
            {:record_marker_command_attributes,
             %RecordMarkerCommandAttributes{
               marker_name: "Version",
               details: %{
                 change_id => Temporal.Payload.encode(max_supported),
                 "changeId" => Temporal.Payload.encode(change_id)
               }
             }}
        }

        put_context(
          context
          |> Map.put(:version_markers, Map.put(versions, change_id, max_supported))
          |> Map.update(:pending_commands, [command], &(&1 ++ [command]))
        )

        max_supported
    end
  end

  @doc "Returns the default (unversioned) version, the analogue of the official SDKs' `DefaultVersion`."
  @spec get_version_default() :: integer()
  def get_version_default, do: -1

  @doc """
  Runs a non-deterministic function once and records its result into history.

  `fun` runs exactly once (live execution); replays return the recorded result
  without re-running it. The analogue of the official SDKs' `SideEffect`.
  """
  @spec side_effect((-> term())) :: term()
  def side_effect(fun) when is_function(fun, 0) do
    context = workflow_context!("side_effect/1")
    index = Map.get(context, :side_effect_index, 0) + 1
    put_context(Map.put(context, :side_effect_index, index))

    case Map.get(Map.get(context, :side_effect_results, %{}), index) do
      nil ->
        result = fun.()

        command = %Command{
          command_type: :COMMAND_TYPE_RECORD_MARKER,
          attributes:
            {:record_marker_command_attributes,
             %RecordMarkerCommandAttributes{
               marker_name: "SideEffect",
               details: %{"data" => Temporal.Payload.encode(result)}
             }}
        }

        put_context(
          context
          |> Map.put(
            :side_effect_results,
            Map.put(Map.get(context, :side_effect_results, %{}), index, result)
          )
          |> Map.update(:pending_commands, [command], &(&1 ++ [command]))
        )

        result

      result ->
        result
    end
  end

  @doc """
  Like `side_effect/1` but keyed by an id and only re-records when the value
  changes.

  `id` identifies the value across runs; `fun` produces the candidate value and
  `equals` compares it with the recorded value. On the first run the candidate
  is recorded; on replay the recorded value is returned and the candidate is
  only re-recorded if `equals` says it changed. The analogue of the official
  SDKs' `MutableSideEffect`.
  """
  @spec mutable_side_effect(String.t(), (-> term()), (term(), term() -> boolean())) :: term()
  def mutable_side_effect(id, fun, equals)
      when is_binary(id) and id != "" and is_function(fun, 0) and is_function(equals, 2) do
    context = workflow_context!("mutable_side_effect/3")
    sequence = Map.get(context, :mutable_side_effect_index, 0) + 1
    key = {id, sequence}

    put_context(Map.put(context, :mutable_side_effect_index, sequence))

    case Map.get(Map.get(context, :mutable_side_effect_results, %{}), key) do
      nil ->
        result = fun.()

        command = %Command{
          command_type: :COMMAND_TYPE_RECORD_MARKER,
          attributes:
            {:record_marker_command_attributes,
             %RecordMarkerCommandAttributes{
               marker_name: "MutableSideEffect",
               details: %{
                 "id" => Temporal.Payload.encode(id),
                 "data" => Temporal.Payload.encode(result)
               }
             }}
        }

        put_context(
          context
          |> Map.put(
            :mutable_side_effect_results,
            Map.put(Map.get(context, :mutable_side_effect_results, %{}), key, result)
          )
          |> Map.update(:pending_commands, [command], &(&1 ++ [command]))
        )

        result

      recorded ->
        candidate = fun.()

        if equals.(candidate, recorded) do
          recorded
        else
          command = %Command{
            command_type: :COMMAND_TYPE_RECORD_MARKER,
            attributes:
              {:record_marker_command_attributes,
               %RecordMarkerCommandAttributes{
                 marker_name: "MutableSideEffect",
                 details: %{
                   "id" => Temporal.Payload.encode(id),
                   "data" => Temporal.Payload.encode(candidate)
                 }
               }}
          }

          put_context(
            context
            |> Map.put(
              :mutable_side_effect_results,
              Map.put(Map.get(context, :mutable_side_effect_results, %{}), key, candidate)
            )
            |> Map.update(:pending_commands, [command], &(&1 ++ [command]))
          )

          candidate
        end
    end
  end

  @doc "Returns metadata about the currently executing Workflow."
  @spec info() :: map()
  def info do
    context = workflow_context!("info/0")

    %{
      workflow_id: Map.get(context, :workflow_id),
      run_id: Map.get(context, :run_id),
      workflow_type: Map.get(context, :workflow_type),
      task_queue: Map.get(context, :task_queue),
      attempt: Map.get(context, :attempt, 1),
      namespace: Map.get(context, :namespace)
    }
  end

  @doc "Returns the keys of a map in deterministic (sorted) order."
  @spec deterministic_keys(map()) :: [term()]
  def deterministic_keys(map) when is_map(map), do: Enum.sort(Map.keys(map))

  @doc """
  Upserts (adds or updates) Workflow search attributes.

  `attributes` is a map of registered attribute name to a JSON-serializable
  value. Emits `COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES`.
  """
  @spec upsert_search_attributes(map()) :: :ok
  def upsert_search_attributes(attributes) when is_map(attributes) do
    context =
      Process.get(@context_key) ||
        raise ArgumentError,
              "upsert_search_attributes/1 may only be called from Workflow execution"

    command = %Command{
      command_type: :COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
      attributes:
        {:upsert_workflow_search_attributes_command_attributes,
         %UpsertWorkflowSearchAttributesCommandAttributes{
           search_attributes: %SearchAttributes{
             indexed_fields: Map.new(attributes, fn {key, value} -> {key, payload(value)} end)
           }
         }}
    }

    put_context(%{
      context
      | pending_commands: Map.get(context, :pending_commands, []) ++ [command]
    })

    :ok
  end

  @doc """
  Upserts (adds or updates) Workflow memo entries.

  `memo` is a map of key to a JSON-serializable value. Emits
  `COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES` with `upserted_memo`.
  """
  @spec upsert_memo(map()) :: :ok
  def upsert_memo(memo) when is_map(memo) do
    context =
      Process.get(@context_key) ||
        raise ArgumentError, "upsert_memo/1 may only be called from Workflow execution"

    command = %Command{
      command_type: :COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES,
      attributes:
        {:modify_workflow_properties_command_attributes,
         %ModifyWorkflowPropertiesCommandAttributes{
           upserted_memo: %Memo{
             fields: Map.new(memo, fn {key, value} -> {key, payload(value)} end)
           }
         }}
    }

    put_context(%{
      context
      | pending_commands: Map.get(context, :pending_commands, []) ++ [command]
    })

    :ok
  end

  @doc false
  def put_context(context), do: Process.put(@context_key, context)

  @doc false
  def context, do: Process.get(@context_key)

  @doc false
  def clear_context, do: Process.delete(@context_key)

  @doc false
  def capture_query_context(context), do: Process.put(@query_context_key, context)

  @doc false
  def query_context, do: Process.get(@query_context_key)

  @doc false
  def clear_query_context, do: Process.delete(@query_context_key)

  defp workflow_context!(api) do
    Process.get(@context_key) ||
      raise(ArgumentError, "#{api} may only be called from Workflow execution")
  end

  defp exception!(%_{} = exception), do: exception
  defp exception!(term), do: RuntimeError.exception(message: inspect(term))

  defp payload(value) do
    Temporal.Payload.encode(value)
    |> Map.fetch!(:payloads)
    |> List.first()
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

  defp external_signal_outcome(context, sequence) do
    case Map.get(Map.get(context, :external_signal_outcomes, %{}), sequence) do
      nil -> Map.get(context, :external_signal_outcome)
      outcome -> outcome
    end
  end

  defp child_workflow_outcome(context, index) do
    Map.get(Map.get(context, :child_workflow_outcomes, %{}), index)
  end

  defp block_on_child_workflow(context, index, workflow_type, input, options) do
    if pending_child_workflow?(context, index) do
      throw({:temporal_workflow_blocked, Map.get(context, :pending_commands, [])})
    else
      command = start_child_command(workflow_type, input, options, index)
      existing = Map.get(context, :pending_commands, [])
      pending = existing ++ [command]
      put_context(Map.put(context, :pending_commands, pending))
      throw({:temporal_workflow_blocked, if(existing == [], do: command, else: pending)})
    end
  end

  defp child_workflow_id(%Command{
         attributes:
           {:start_child_workflow_execution_command_attributes, %{workflow_id: workflow_id}}
       }),
       do: workflow_id

  defp scoped_child_cancel_commands(context, scope_id) do
    child_workflow_ids = Map.get(context, :child_workflow_ids, %{})

    context
    |> Map.get(:child_workflow_scopes, %{})
    |> Map.get(scope_id, [])
    |> Enum.flat_map(fn index ->
      case Map.get(child_workflow_ids, index) do
        nil ->
          []

        workflow_id ->
          [
            %Command{
              command_type: :COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
              attributes:
                {:request_cancel_external_workflow_execution_command_attributes,
                 %RequestCancelExternalWorkflowExecutionCommandAttributes{
                   workflow_id: workflow_id,
                   child_workflow_only: true
                 }}
            }
          ]
      end
    end)
  end

  defp pending_child_workflow?(context, index),
    do:
      Map.get(Map.get(context, :child_workflow_states, %{}), index) in [
        :initiated,
        :started
      ]

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

  defp start_child_command(workflow_type, input, options, index) do
    workflow_id = Keyword.get(options, :workflow_id, "child-#{index}")
    task_queue = Keyword.get_lazy(options, :task_queue, &context_task_queue/0)

    %Command{
      command_type: :COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
      attributes:
        {:start_child_workflow_execution_command_attributes,
         %StartChildWorkflowExecutionCommandAttributes{
           namespace: Keyword.get(options, :namespace),
           workflow_id: workflow_id,
           workflow_type: %WorkflowType{name: workflow_type},
           task_queue: %TaskQueue{name: task_queue},
           input: Temporal.Payload.encode(input),
           workflow_execution_timeout: optional_duration!(options, :workflow_execution_timeout),
           workflow_run_timeout: optional_duration!(options, :workflow_run_timeout),
           workflow_task_timeout: optional_duration!(options, :workflow_task_timeout),
           parent_close_policy: Keyword.get(options, :parent_close_policy),
           workflow_id_reuse_policy: Keyword.get(options, :workflow_id_reuse_policy),
           retry_policy: Keyword.get(options, :retry_policy),
           header: Keyword.get(options, :header),
           memo: Keyword.get(options, :memo),
           search_attributes: Keyword.get(options, :search_attributes)
         }}
    }
  end

  defp context_task_queue do
    case Process.get(@context_key) do
      %{task_queue: task_queue} when is_binary(task_queue) ->
        task_queue

      _context ->
        raise ArgumentError, "child workflow requires :task_queue when not in a Workflow"
    end
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
