defmodule Temporal.Workflow.TaskKernel.Query do
  @moduledoc false

  alias Temporal.Api.Query.V1.WorkflowQueryResult

  @spec run(map(), map() | nil) :: %{optional(String.t()) => WorkflowQueryResult.t()}
  def run(queries, query_context) when is_map(queries) and is_map(query_context) do
    Map.new(queries, fn {query_id, workflow_query} ->
      {query_id, answer(workflow_query, query_context)}
    end)
  end

  def run(_queries, _query_context), do: %{}

  defp answer(workflow_query, query_context) do
    query_handlers = Map.get(query_context, :query_handlers, %{})

    case Map.fetch(query_handlers, workflow_query.query_type) do
      {:ok, handler} ->
        execute(handler, workflow_query, query_context)

      :error ->
        failed_result(
          workflow_query,
          "query handler #{inspect(workflow_query.query_type)} is not registered"
        )
    end
  end

  defp execute(handler, workflow_query, query_context) do
    with {:ok, args} <- Temporal.Payload.decode(workflow_query.query_args),
         {:ok, result} <- invoke(handler, args, query_context) do
      %WorkflowQueryResult{
        result_type: :QUERY_RESULT_TYPE_ANSWERED,
        answer: Temporal.Payload.encode(result)
      }
    else
      {:error, reason} -> failed_result(workflow_query, format_error(reason))
    end
  end

  defp invoke(handler, args, query_context) do
    Temporal.Workflow.put_context(query_context)

    try do
      {:ok, handler.(args)}
    rescue
      exception -> {:error, {:query_handler_failed, exception, __STACKTRACE__}}
    after
      Temporal.Workflow.clear_context()
    end
  end

  defp failed_result(_workflow_query, message) do
    %WorkflowQueryResult{
      result_type: :QUERY_RESULT_TYPE_FAILED,
      error_message: message
    }
  end

  defp format_error({:query_handler_failed, exception, _stacktrace}),
    do: Exception.message(exception)

  defp format_error(reason), do: inspect(reason)
end
