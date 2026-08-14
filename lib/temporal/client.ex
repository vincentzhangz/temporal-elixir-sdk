defmodule Temporal.Client do
  @moduledoc """
  Client APIs for the supported synchronous Workflow vertical slice.

  This API supports start/result, cancellation, Workflow signals, and atomic
  Signal-With-Start. Signal request IDs are stable across transport retries;
  callers should supply `:request_id` when retrying across separate API calls.
  """

  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}
  alias Temporal.Api.Taskqueue.V1.TaskQueue

  alias Temporal.Api.Workflowservice.V1.{
    GetWorkflowExecutionHistoryRequest,
    GetWorkflowExecutionHistoryResponse,
    RequestCancelWorkflowExecutionRequest,
    RequestCancelWorkflowExecutionResponse,
    SignalWithStartWorkflowExecutionResponse,
    SignalWorkflowExecutionResponse,
    StartWorkflowExecutionRequest,
    StartWorkflowExecutionResponse
  }

  alias Temporal.Workflow.Signal.Requests

  defmodule Handle do
    @moduledoc "Identifies one started workflow execution."
    @enforce_keys [:connection, :namespace, :workflow_id, :run_id]
    defstruct [:connection, :namespace, :workflow_id, :run_id]

    @type t :: %__MODULE__{
            connection: GenServer.server(),
            namespace: String.t(),
            workflow_id: String.t(),
            run_id: String.t()
          }
  end

  @start_method "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"
  @history_method "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory"
  @cancel_method "/temporal.api.workflowservice.v1.WorkflowService/RequestCancelWorkflowExecution"
  @signal_method "/temporal.api.workflowservice.v1.WorkflowService/SignalWorkflowExecution"
  @signal_with_start_method "/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution"

  @spec start_workflow(GenServer.server(), String.t(), term(), keyword()) ::
          {:ok, Handle.t()} | {:error, term()}
  def start_workflow(connection, workflow_name, argument \\ nil, options) do
    config = Temporal.Connection.configuration(connection)
    workflow_id = Keyword.fetch!(options, :id)
    task_queue = Keyword.fetch!(options, :task_queue)

    request = %StartWorkflowExecutionRequest{
      namespace: config.namespace,
      workflow_id: workflow_id,
      workflow_type: %WorkflowType{name: workflow_name},
      task_queue: %TaskQueue{name: task_queue},
      input: Temporal.Payload.encode(argument),
      identity: config.identity,
      request_id: request_id()
    }

    with {:ok, response} <-
           call(connection, @start_method, request, StartWorkflowExecutionResponse, options) do
      {:ok,
       %Handle{
         connection: connection,
         namespace: config.namespace,
         workflow_id: workflow_id,
         run_id: response.run_id
       }}
    end
  end

  @spec execute_workflow(GenServer.server(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def execute_workflow(connection, workflow_name, argument \\ nil, options) do
    with {:ok, handle} <- start_workflow(connection, workflow_name, argument, options) do
      result(handle, options)
    end
  end

  @spec cancel_workflow(Handle.t(), keyword()) :: :ok | {:error, term()}
  def cancel_workflow(%Handle{} = handle, options \\ []) do
    config = Temporal.Connection.configuration(handle.connection)

    request = %RequestCancelWorkflowExecutionRequest{
      namespace: handle.namespace,
      workflow_execution: %WorkflowExecution{
        workflow_id: handle.workflow_id,
        run_id: handle.run_id
      },
      identity: config.identity,
      request_id: request_id(),
      first_execution_run_id: handle.run_id,
      reason: Keyword.get(options, :reason, "canceled by client")
    }

    case call(
           handle.connection,
           @cancel_method,
           request,
           RequestCancelWorkflowExecutionResponse,
           options
         ) do
      {:ok, _response} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @doc "Sends a signal to the exact Workflow Run identified by a handle."
  @spec signal_workflow(Handle.t(), String.t(), term(), keyword()) :: :ok | {:error, term()}
  def signal_workflow(%Handle{} = handle, signal_name, input),
    do: signal_workflow(handle, signal_name, input, [])

  def signal_workflow(%Handle{} = handle, signal_name, input, options) do
    signal(
      handle.connection,
      handle.namespace,
      handle.workflow_id,
      handle.run_id,
      signal_name,
      input,
      options
    )
  end

  @doc """
  Sends a signal to a Workflow Execution by ID.

  An empty `:run_id` (the default) targets the current run. Supply a stable
  `:request_id` when the caller may retry independently; transport retries
  automatically reuse the request bytes and therefore the same ID.
  """
  @spec signal_workflow(
          GenServer.server(),
          String.t(),
          String.t(),
          term(),
          keyword()
        ) :: :ok | {:error, term()}
  def signal_workflow(connection, workflow_id, signal_name, input),
    do: signal_workflow(connection, workflow_id, signal_name, input, [])

  def signal_workflow(connection, workflow_id, signal_name, input, options) do
    config = Temporal.Connection.configuration(connection)

    signal(
      connection,
      config.namespace,
      workflow_id,
      Keyword.get(options, :run_id, ""),
      signal_name,
      input,
      options
    )
  end

  @doc "Atomically starts a Workflow if needed and delivers its first signal."
  @spec signal_with_start(
          GenServer.server(),
          String.t(),
          term(),
          String.t(),
          term(),
          keyword()
        ) :: {:ok, Handle.t()} | {:error, term()}
  def signal_with_start(
        connection,
        workflow_name,
        workflow_input,
        signal_name,
        signal_input,
        options
      ) do
    config = Temporal.Connection.configuration(connection)
    workflow_id = Keyword.get(options, :id)
    task_queue = Keyword.get(options, :task_queue)

    request_options =
      Keyword.merge(options,
        namespace: config.namespace,
        identity: config.identity,
        workflow_id: workflow_id,
        workflow_type: workflow_name,
        task_queue: task_queue,
        workflow_input: workflow_input,
        signal_name: signal_name,
        signal_input: signal_input,
        request_id: Keyword.get_lazy(options, :request_id, &request_id/0)
      )

    with {:ok, request} <- Requests.signal_with_start(request_options),
         {:ok, response} <-
           call(
             connection,
             @signal_with_start_method,
             request,
             SignalWithStartWorkflowExecutionResponse,
             options
           ) do
      {:ok,
       %Handle{
         connection: connection,
         namespace: config.namespace,
         workflow_id: workflow_id,
         run_id: response.run_id
       }}
    end
  end

  @spec result(Handle.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def result(%Handle{} = handle, options \\ []) do
    case result_with_run_chain(handle, options) do
      {:ok, value, _run_ids} -> {:ok, value}
      {:error, _reason} = error -> error
    end
  end

  @spec result_with_run_chain(Handle.t(), keyword()) ::
          {:ok, term(), [String.t()]} | {:error, term()}
  def result_with_run_chain(%Handle{} = handle, options \\ []) do
    follow_result(handle, options, [], [])
  end

  defp follow_result(handle, options, seen, run_ids) do
    if handle.run_id in seen do
      {:error, {:invalid_run_chain, handle.run_id}}
    else
      fetch_result(handle, options, [handle.run_id | seen], [handle.run_id | run_ids])
    end
  end

  defp fetch_result(handle, options, seen, run_ids) do
    request = %GetWorkflowExecutionHistoryRequest{
      namespace: handle.namespace,
      execution: %WorkflowExecution{workflow_id: handle.workflow_id, run_id: handle.run_id},
      wait_new_event: true,
      history_event_filter_type: :HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
    }

    with {:ok, response} <-
           call(
             handle.connection,
             @history_method,
             request,
             GetWorkflowExecutionHistoryResponse,
             Keyword.put_new(options, :timeout, 60_000)
           ) do
      case decode_close_event(response) do
        {:continue_as_new, run_id} when run_id != "" ->
          follow_result(%{handle | run_id: run_id}, options, seen, run_ids)

        {:ok, value} ->
          {:ok, value, Enum.reverse(run_ids)}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp call(connection, method, request, response_module, options) do
    request_module = request.__struct__

    with {:ok, bytes} <-
           Temporal.Connection.unary(connection, method, request_module.encode(request), options) do
      {:ok, response_module.decode(bytes)}
    end
  end

  defp signal(connection, namespace, workflow_id, run_id, signal_name, input, options) do
    config = Temporal.Connection.configuration(connection)

    request_options =
      Keyword.merge(options,
        namespace: namespace,
        identity: config.identity,
        workflow_id: workflow_id,
        run_id: run_id,
        signal_name: signal_name,
        input: input,
        request_id: Keyword.get_lazy(options, :request_id, &request_id/0)
      )

    with {:ok, request} <- Requests.signal_workflow(request_options),
         {:ok, _response} <-
           call(connection, @signal_method, request, SignalWorkflowExecutionResponse, options) do
      :ok
    end
  end

  defp decode_close_event(%{history: %{events: events}}) do
    Enum.find_value(events, {:error, :workflow_not_completed}, fn
      %{attributes: {:workflow_execution_completed_event_attributes, attributes}} ->
        Temporal.Payload.decode(attributes.result)

      %{
        attributes:
          {:workflow_execution_continued_as_new_event_attributes, %{new_execution_run_id: run_id}}
      } ->
        {:continue_as_new, run_id}

      %{event_type: event_type}
      when event_type in [
             :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
             :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
             :EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
             :EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
           ] ->
        {:error, {:workflow_closed, event_type}}

      _ ->
        false
    end)
  end

  defp decode_close_event(_), do: {:error, :workflow_not_completed}

  defp request_id do
    "elixir-#{System.unique_integer([:positive, :monotonic])}"
  end
end
