defmodule Temporal.Worker.Runtime do
  @moduledoc false

  alias Temporal.Api.Workflowservice.V1.{
    PollWorkflowTaskQueueResponse,
    RespondQueryTaskCompletedRequest,
    RespondWorkflowTaskFailedRequest
  }

  alias Temporal.Api.Failure.V1.Failure
  alias Temporal.Api.Query.V1.WorkflowQueryResult
  alias Temporal.Workflow.HistoryCursor
  alias Temporal.Workflow.TaskKernel.{Completion, Reducer, State}

  @spec complete(struct(), map(), String.t()) :: {:ok, struct()} | {:error, term()}
  def complete(%PollWorkflowTaskQueueResponse{} = task, workflows, identity) do
    with {:ok, completion, _state} <- prepare(task, workflows, identity, nil) do
      {:ok, completion}
    end
  end

  def prepare_response(task, workflows, identity, previous_cursor, options \\ []) do
    case prepare(task, workflows, identity, previous_cursor, options) do
      {:ok, completion, state} ->
        {:completed, completion, state}

      {:error, reason} ->
        {:failed,
         %RespondWorkflowTaskFailedRequest{
           task_token: task.task_token,
           identity: identity,
           namespace: Keyword.get(options, :namespace, ""),
           cause: failure_cause(reason),
           failure: %Failure{
             message: failure_message(reason),
             source: Temporal.SDKMetadata.name()
           }
         }, reason}
    end
  end

  @spec prepare_query_response(PollWorkflowTaskQueueResponse.t(), map(), String.t(), keyword()) ::
          {:ok, RespondQueryTaskCompletedRequest.t()} | {:error, term()}
  def prepare_query_response(task, workflows, identity, options \\ []) do
    with {:ok, state} <- fresh_state(task, options),
         {:ok, reduced} <- Reducer.reduce_query(state, task, workflows) do
      results = State.query_results(reduced)

      case Map.get(results, "0") do
        %WorkflowQueryResult{} = result ->
          {:ok, respond_query_task(task, identity, result, options)}

        _other ->
          {:error, :query_result_missing}
      end
    end
  end

  def prepare(task, workflows, identity, previous),
    do: prepare(task, workflows, identity, previous, [])

  def prepare(
        %PollWorkflowTaskQueueResponse{} = task,
        workflows,
        identity,
        previous,
        options
      ) do
    with {:ok, state} <- state_for(previous, task, options),
         {:ok, reduced} <- Reducer.reduce_task(state, task, workflows) do
      {:ok,
       Completion.request(
         task.task_token,
         identity,
         State.commands(reduced),
         State.query_results(reduced),
         Map.get(reduced, :update_messages, [])
       ), reduced}
    end
  end

  defp state_for(%State{} = state, _task, _options), do: {:ok, state}

  defp state_for(
         %HistoryCursor{} = cursor,
         task,
         options
       ) do
    with :ok <- cursor_identity(cursor, task),
         {:ok, state} <- fresh_state(task, options) do
      {:ok,
       %{
         state
         | cursor: cursor,
           task_token: cursor.task_token,
           workflow_task_started_event_id: cursor.workflow_task_started_event_id,
           next_event_id: cursor.next_event_id,
           last_event_id: cursor.last_event_id
       }}
    end
  end

  defp state_for(nil, task, options), do: fresh_state(task, options)

  defp cursor_identity(
         %HistoryCursor{workflow_id: workflow_id, run_id: run_id},
         %{workflow_execution: %{workflow_id: workflow_id, run_id: run_id}}
       ),
       do: :ok

  defp cursor_identity(
         %HistoryCursor{workflow_id: expected},
         %{workflow_execution: %{workflow_id: actual}}
       )
       when expected != actual,
       do:
         {:error,
          {:workflow_identity_mismatch,
           %{field: :workflow_id, expected: expected, actual: actual}}}

  defp cursor_identity(
         %HistoryCursor{run_id: expected},
         %{workflow_execution: %{run_id: actual}}
       ),
       do:
         {:error,
          {:workflow_identity_mismatch, %{field: :run_id, expected: expected, actual: actual}}}

  defp fresh_state(
         %{workflow_execution: %{workflow_id: workflow_id, run_id: run_id}},
         options
       )
       when workflow_id != "" and run_id != "" do
    {:ok,
     State.new(
       namespace: Keyword.get(options, :namespace, ""),
       workflow_id: workflow_id,
       run_id: run_id
     )}
  end

  defp fresh_state(_task, _options), do: {:error, :missing_workflow_execution_identity}

  defp failure_cause({kind, _diagnostic}) when kind in [:invalid_history, :nondeterminism],
    do: :WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR

  defp failure_cause(_reason),
    do: :WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE

  defp failure_message({_kind, %{message: message}}) when is_binary(message), do: message
  defp failure_message(reason), do: inspect(reason)

  defp respond_query_task(task, _identity, result, options) do
    %RespondQueryTaskCompletedRequest{
      task_token: task.task_token,
      completed_type: result.result_type,
      query_result: result.answer,
      error_message: result.error_message,
      namespace: Keyword.get(options, :namespace, "")
    }
  end
end
