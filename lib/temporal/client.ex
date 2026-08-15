defmodule Temporal.Client do
  @moduledoc """
  Client APIs for the supported synchronous Workflow vertical slice.

  This API supports start/result, cancellation, termination, describe, listing,
  Workflow signals, queries, and atomic Signal-With-Start. Signal request IDs
  are stable across transport retries; callers should supply `:request_id` when
  retrying across separate API calls.
  """

  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}
  alias Temporal.Api.Taskqueue.V1.TaskQueue

  alias Temporal.Api.Workflowservice.V1.{
    DescribeWorkflowExecutionRequest,
    DescribeWorkflowExecutionResponse,
    ExecuteMultiOperationRequest,
    ExecuteMultiOperationResponse,
    GetWorkflowExecutionHistoryRequest,
    GetWorkflowExecutionHistoryResponse,
    ListWorkflowExecutionsRequest,
    ListWorkflowExecutionsResponse,
    QueryWorkflowRequest,
    QueryWorkflowResponse,
    RequestCancelWorkflowExecutionRequest,
    RequestCancelWorkflowExecutionResponse,
    SignalWithStartWorkflowExecutionResponse,
    SignalWorkflowExecutionResponse,
    StartWorkflowExecutionRequest,
    StartWorkflowExecutionResponse,
    TerminateWorkflowExecutionRequest,
    TerminateWorkflowExecutionResponse,
    UpdateWorkflowExecutionRequest,
    UpdateWorkflowExecutionResponse
  }

  alias Temporal.Api.Query.V1.WorkflowQuery

  alias Temporal.Api.Update.V1.{Input, Meta, Request, WaitPolicy}

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
  @query_method "/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow"
  @terminate_method "/temporal.api.workflowservice.v1.WorkflowService/TerminateWorkflowExecution"
  @describe_method "/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkflowExecution"
  @list_method "/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions"
  @update_method "/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkflowExecution"
  @multi_operation_method "/temporal.api.workflowservice.v1.WorkflowService/ExecuteMultiOperation"

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
      input: Temporal.Payload.encode(argument, config.payload_codecs),
      identity: config.identity,
      request_id: request_id(),
      workflow_id_reuse_policy:
        Keyword.get(options, :workflow_id_reuse_policy, :WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED),
      workflow_id_conflict_policy:
        Keyword.get(
          options,
          :workflow_id_conflict_policy,
          :WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED
        ),
      retry_policy: Keyword.get(options, :retry_policy),
      cron_schedule: Keyword.get(options, :cron_schedule, ""),
      memo: Keyword.get(options, :memo),
      search_attributes: Keyword.get(options, :search_attributes),
      header: Keyword.get(options, :header),
      request_eager_execution: Keyword.get(options, :request_eager_execution, false)
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

  @doc """
  Queries the exact Workflow Run identified by a handle.

  The query executes against the workflow's deterministic state at the latest
  history event. `:query_reject_condition` defaults to
  `:QUERY_REJECT_CONDITION_NONE`.
  """
  @spec query_workflow(Handle.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def query_workflow(%Handle{} = handle, query_type, query_args),
    do: query_workflow(handle, query_type, query_args, [])

  def query_workflow(%Handle{} = handle, query_type, query_args, options) do
    query(
      handle.connection,
      handle.namespace,
      handle.workflow_id,
      handle.run_id,
      query_type,
      query_args,
      options
    )
  end

  @doc """
  Queries a Workflow Execution by ID.

  An empty `:run_id` (the default) targets the current run.
  """
  @spec query_workflow(
          GenServer.server(),
          String.t(),
          String.t(),
          term(),
          keyword()
        ) :: {:ok, term()} | {:error, term()}
  def query_workflow(connection, workflow_id, query_type, query_args),
    do: query_workflow(connection, workflow_id, query_type, query_args, [])

  def query_workflow(connection, workflow_id, query_type, query_args, options) do
    config = Temporal.Connection.configuration(connection)

    query(
      connection,
      config.namespace,
      workflow_id,
      Keyword.get(options, :run_id, ""),
      query_type,
      query_args,
      options
    )
  end

  @doc """
  Terminates a Workflow Execution by handle (with optional reason/details) or
  by Workflow ID (with default options).
  """
  @spec terminate_workflow(Handle.t(), keyword()) :: :ok | {:error, term()}
  @spec terminate_workflow(GenServer.server(), String.t()) :: :ok | {:error, term()}
  def terminate_workflow(arg, options)

  def terminate_workflow(%Handle{} = handle, options) do
    config = Temporal.Connection.configuration(handle.connection)

    request = %TerminateWorkflowExecutionRequest{
      namespace: handle.namespace,
      workflow_execution: %WorkflowExecution{
        workflow_id: handle.workflow_id,
        run_id: handle.run_id
      },
      reason: Keyword.get(options, :reason, "terminated by client"),
      details: Temporal.Payload.encode(Keyword.get(options, :details)),
      identity: config.identity,
      first_execution_run_id: handle.run_id
    }

    case call(
           handle.connection,
           @terminate_method,
           request,
           TerminateWorkflowExecutionResponse,
           options
         ) do
      {:ok, _response} -> :ok
      {:error, _reason} = error -> error
    end
  end

  def terminate_workflow(connection, workflow_id),
    do: terminate_workflow(connection, workflow_id, [])

  @doc "Terminates a Workflow Execution by ID; an empty run_id targets the current run."
  @spec terminate_workflow(GenServer.server(), String.t(), keyword()) :: :ok | {:error, term()}
  def terminate_workflow(connection, workflow_id, options) do
    config = Temporal.Connection.configuration(connection)

    request = %TerminateWorkflowExecutionRequest{
      namespace: config.namespace,
      workflow_execution: %WorkflowExecution{
        workflow_id: workflow_id,
        run_id: Keyword.get(options, :run_id, "")
      },
      reason: Keyword.get(options, :reason, "terminated by client"),
      details: Temporal.Payload.encode(Keyword.get(options, :details)),
      identity: config.identity,
      first_execution_run_id: Keyword.get(options, :first_execution_run_id, "")
    }

    case call(
           connection,
           @terminate_method,
           request,
           TerminateWorkflowExecutionResponse,
           options
         ) do
      {:ok, _response} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @doc "Terminates a Workflow Execution by handle with default options."
  @spec terminate_workflow(Handle.t()) :: :ok | {:error, term()}
  def terminate_workflow(%Handle{} = handle), do: terminate_workflow(handle, [])

  @doc "Describes a Workflow Execution and returns its current status and metadata."
  @spec describe_workflow(Handle.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def describe_workflow(%Handle{} = handle, options \\ []) do
    request = %DescribeWorkflowExecutionRequest{
      namespace: handle.namespace,
      execution: %WorkflowExecution{
        workflow_id: handle.workflow_id,
        run_id: handle.run_id
      }
    }

    case call(
           handle.connection,
           @describe_method,
           request,
           DescribeWorkflowExecutionResponse,
           options
         ) do
      {:ok, response} -> {:ok, response}
      {:error, _reason} = error -> error
    end
  end

  @doc "Lists Workflow Executions matching a query with an optional page size."
  @spec list_workflows(GenServer.server(), String.t(), keyword()) ::
          {:ok, %{executions: list(), next_page_token: String.t()}} | {:error, term()}
  def list_workflows(connection, query, options \\ []) do
    config = Temporal.Connection.configuration(connection)

    request = %ListWorkflowExecutionsRequest{
      namespace: config.namespace,
      page_size: Keyword.get(options, :page_size, 0),
      next_page_token: Keyword.get(options, :next_page_token, ""),
      query: query
    }

    with {:ok, response} <-
           call(connection, @list_method, request, ListWorkflowExecutionsResponse, options) do
      {:ok,
       %{
         executions: response.executions,
         next_page_token: response.next_page_token
       }}
    end
  end

  @doc """
  Sends an Update to a Workflow Execution and returns the decoded result.

  `options` accepts `:update_id` (default `"update-<unique>"`), `:wait_policy`
  (default `:UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED`), and
  `:request_id`. The update is handled by the handler registered with
  `Temporal.Workflow.set_update_handler/2` on the worker.
  """
  @spec update_workflow(Handle.t(), String.t(), term()) :: {:ok, term()} | {:error, term()}
  def update_workflow(%Handle{} = handle, update_name, args),
    do: update_workflow(handle, update_name, args, [])

  @spec update_workflow(Handle.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def update_workflow(%Handle{} = handle, update_name, args, options) do
    update_request = %Request{
      meta: %Meta{update_id: Keyword.get(options, :update_id, request_id())},
      input: %Input{name: update_name, args: Temporal.Payload.encode(args)},
      request_id: Keyword.get(options, :request_id, request_id())
    }

    request = %UpdateWorkflowExecutionRequest{
      namespace: handle.namespace,
      workflow_execution: %WorkflowExecution{
        workflow_id: handle.workflow_id,
        run_id: handle.run_id
      },
      first_execution_run_id: handle.run_id,
      wait_policy: %WaitPolicy{
        lifecycle_stage:
          Keyword.get(
            options,
            :wait_policy,
            :UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
          )
      },
      request: update_request
    }

    with {:ok, response} <-
           call(
             handle.connection,
             @update_method,
             request,
             UpdateWorkflowExecutionResponse,
             options
           ) do
      decode_update_response(response, update_name)
    end
  end

  @doc """
  Sends an Update to a Workflow Execution by ID and returns the decoded result.

  An empty `:run_id` (the default) targets the current run.
  """
  @spec update_workflow(GenServer.server(), String.t(), String.t(), term()) ::
          {:ok, term()} | {:error, term()}
  def update_workflow(connection, workflow_id, update_name, args),
    do: update_workflow(connection, workflow_id, update_name, args, [])

  @spec update_workflow(GenServer.server(), String.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def update_workflow(connection, workflow_id, update_name, args, options) do
    config = Temporal.Connection.configuration(connection)

    update_request = %Request{
      meta: %Meta{update_id: Keyword.get(options, :update_id, request_id())},
      input: %Input{name: update_name, args: Temporal.Payload.encode(args)},
      request_id: Keyword.get(options, :request_id, request_id())
    }

    request = %UpdateWorkflowExecutionRequest{
      namespace: config.namespace,
      workflow_execution: %WorkflowExecution{
        workflow_id: workflow_id,
        run_id: Keyword.get(options, :run_id, "")
      },
      wait_policy: %WaitPolicy{
        lifecycle_stage:
          Keyword.get(
            options,
            :wait_policy,
            :UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
          )
      },
      request: update_request
    }

    with {:ok, response} <-
           call(connection, @update_method, request, UpdateWorkflowExecutionResponse, options) do
      decode_update_response(response, update_name)
    end
  end

  defp decode_update_response(%{outcome: %{value: {:success, payloads}}}, _update_name) do
    Temporal.Payload.decode(payloads)
  end

  defp decode_update_response(
         %{outcome: %{value: {:failure, failure}}},
         _update_name
       ) do
    {:error, {:update_failed, Temporal.Failure.from_proto(failure)}}
  end

  defp decode_update_response(response, update_name),
    do: {:error, {:update_unresolved, update_name, response.stage}}

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

  @doc """
  Atomically executes a batch of operations on a Workflow in one RPC.

  `operations` is a list of `{:start, start_options}` and/or
  `{:signal, handle_or_id, signal_name, input}` tuples. Returns a
  `%ExecuteMultiOperationResponse{}` when the batch succeeds; the server
  rejects the whole batch if any operation fails.
  """
  @spec execute_multi_operation(
          GenServer.server(),
          String.t(),
          term(),
          keyword()
        ) :: {:ok, term()} | {:error, term()}
  def execute_multi_operation(connection, workflow_name, argument, options) do
    config = Temporal.Connection.configuration(connection)

    start_request = %StartWorkflowExecutionRequest{
      namespace: config.namespace,
      workflow_id: Keyword.fetch!(options, :id),
      workflow_type: %WorkflowType{name: workflow_name},
      task_queue: %TaskQueue{name: Keyword.fetch!(options, :task_queue)},
      input: Temporal.Payload.encode(argument),
      identity: config.identity,
      request_id: request_id()
    }

    update_request =
      case Keyword.fetch(options, :update) do
        {:ok, {update_name, update_args}} ->
          %UpdateWorkflowExecutionRequest{
            namespace: config.namespace,
            workflow_execution: %WorkflowExecution{workflow_id: Keyword.fetch!(options, :id)},
            request: %Request{
              meta: %Meta{update_id: Keyword.get(options, :update_id, request_id())},
              input: %Input{name: update_name, args: Temporal.Payload.encode(update_args)},
              request_id: request_id()
            },
            wait_policy: %WaitPolicy{
              lifecycle_stage: :UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
            }
          }

        :error ->
          nil
      end

    operations =
      [%ExecuteMultiOperationRequest.Operation{operation: {:start_workflow, start_request}}] ++
        if update_request do
          [%ExecuteMultiOperationRequest.Operation{operation: {:update_workflow, update_request}}]
        else
          []
        end

    request = %ExecuteMultiOperationRequest{
      namespace: config.namespace,
      operations: operations
    }

    call(connection, @multi_operation_method, request, ExecuteMultiOperationResponse, options)
  end

  @doc """
  Atomically starts a Workflow if needed and delivers its first Update.

  This is the Update-With-Start semantic from the official SDKs, implemented as
  a single `ExecuteMultiOperation` RPC containing a start + update pair. On
  success returns `{:ok, result, %Handle{}}` where `result` is the decoded
  update outcome.
  """
  @spec update_with_start(
          GenServer.server(),
          String.t(),
          term(),
          String.t(),
          term(),
          keyword()
        ) :: {:ok, term(), Handle.t()} | {:error, term()}
  def update_with_start(
        connection,
        workflow_name,
        workflow_input,
        update_name,
        update_args,
        options
      ) do
    config = Temporal.Connection.configuration(connection)
    workflow_id = Keyword.fetch!(options, :id)

    with {:ok, response} <-
           execute_multi_operation(
             connection,
             workflow_name,
             workflow_input,
             Keyword.merge(options,
               update: {update_name, update_args},
               update_id: Keyword.get(options, :update_id, request_id())
             )
           ) do
      case decode_multi_update(response) do
        {:ok, result} ->
          {:ok, result,
           %Handle{
             connection: connection,
             namespace: config.namespace,
             workflow_id: workflow_id,
             run_id: first_run_id(response)
           }}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp decode_multi_update(%ExecuteMultiOperationResponse{responses: responses}) do
    Enum.find_value(responses, {:error, :update_result_missing}, fn
      %{response: {:update_workflow, %{outcome: outcome}}} -> decode_update_outcome(outcome)
      _other -> false
    end)
  end

  defp decode_update_outcome(nil), do: {:error, :update_unresolved}

  defp decode_update_outcome(%{value: {:success, payloads}}) do
    case Temporal.Payload.decode(payloads) do
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_update_outcome(%{value: {:failure, failure}}) do
    {:error, {:update_failed, Temporal.Failure.from_proto(failure)}}
  end

  defp first_run_id(%ExecuteMultiOperationResponse{responses: responses}) do
    Enum.find_value(responses, "", fn
      %{response: {:start_workflow, %{run_id: run_id}}} -> run_id
      _other -> false
    end)
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

  defp query(
         connection,
         namespace,
         workflow_id,
         run_id,
         query_type,
         query_args,
         options
       ) do
    request = %QueryWorkflowRequest{
      namespace: namespace,
      execution: %WorkflowExecution{workflow_id: workflow_id, run_id: run_id},
      query: %WorkflowQuery{
        query_type: query_type,
        query_args: Temporal.Payload.encode(query_args)
      },
      query_reject_condition:
        Keyword.get(options, :query_reject_condition, :QUERY_REJECT_CONDITION_NONE)
    }

    with {:ok, response} <-
           call(connection, @query_method, request, QueryWorkflowResponse, options) do
      decode_query_response(response, query_type)
    end
  end

  defp decode_query_response(
         %{query_result: %Temporal.Api.Common.V1.Payloads{} = payloads},
         _query_type
       ) do
    case Temporal.Payload.decode(payloads) do
      {:ok, value} -> {:ok, value}
      {:error, _reason} = error -> error
    end
  end

  defp decode_query_response(
         %{query_rejected: %{status: status}},
         _query_type
       )
       when not is_nil(status) and status != :QUERY_REJECT_CONDITION_UNSPECIFIED do
    {:error, {:query_rejected, status}}
  end

  defp decode_query_response(_response, query_type),
    do: {:error, {:query_failed, query_type}}

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
