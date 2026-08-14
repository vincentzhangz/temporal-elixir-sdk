defmodule Temporal.Workflow.TaskKernel.HistoryPaginator do
  @moduledoc false

  alias Temporal.Api.History.V1.History
  alias Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse

  @spec assemble(PollWorkflowTaskQueueResponse.t(), (binary() ->
                                                       {:ok, struct()} | {:error, term()})) ::
          {:ok, PollWorkflowTaskQueueResponse.t()} | {:error, term()}
  def assemble(%PollWorkflowTaskQueueResponse{next_page_token: ""} = task, _fetch_page),
    do: {:ok, task}

  def assemble(%PollWorkflowTaskQueueResponse{} = task, fetch_page)
      when is_function(fetch_page, 1) do
    events = (task.history && task.history.events) || []

    with {:ok, all_events} <-
           fetch_remaining(task.next_page_token, fetch_page, events, %{}) do
      {:ok, %{task | history: %History{events: all_events}, next_page_token: ""}}
    end
  end

  defp fetch_remaining("", _fetch_page, events, _seen), do: {:ok, events}

  defp fetch_remaining(token, fetch_page, events, seen) do
    if Map.has_key?(seen, token) do
      {:error, {:invalid_history_pagination, :repeated_page_token}}
    else
      with {:ok, page} <- fetch_page.(token),
           page_events <- (page.history && page.history.events) || [] do
        fetch_remaining(
          page.next_page_token,
          fetch_page,
          events ++ page_events,
          Map.put(seen, token, true)
        )
      end
    end
  end
end
