defmodule Temporal.Workflow.Replay do
  @moduledoc """
  Offline deterministic replay of an official Temporal Workflow history.

  Replay and live Workflow Task execution use the same Task Kernel reducer.
  """

  alias Temporal.Api.History.V1.History
  alias Temporal.Workflow.HistoryCursor
  alias Temporal.Workflow.TaskKernel.{Reducer, State}

  @type replay_error ::
          {:invalid_history, map()}
          | {:nondeterminism, map()}
          | {:unsupported_history_event, map()}
          | {:workflow_identity_mismatch, map()}
          | term()

  @spec replay(struct() | iodata(), function(), keyword()) ::
          {:ok, HistoryCursor.t()} | {:error, replay_error()}
  def replay(history_or_json, workflow, options) do
    with {:ok, history} <- decode_history(history_or_json),
         state <-
           State.new(
             namespace: Keyword.get(options, :namespace, ""),
             workflow_id: Keyword.fetch!(options, :workflow_id),
             run_id: Keyword.fetch!(options, :run_id),
             mode: :offline
           ),
         {:ok, reduced} <- Reducer.reduce_history(state, history, workflow),
         :ok <- require_terminal(reduced.cursor) do
      {:ok, reduced.cursor}
    end
  end

  defp decode_history(%History{} = history), do: {:ok, history}

  defp decode_history(json) when is_binary(json) or is_list(json) do
    case Protobuf.JSON.decode(json, History) do
      {:ok, history} ->
        {:ok, history}

      {:error, error} ->
        {:error,
         {:invalid_history,
          %{message: "invalid official History protobuf JSON: #{Exception.message(error)}"}}}
    end
  end

  defp decode_history(_),
    do: {:error, {:invalid_history, %{message: "expected History protobuf or protobuf JSON"}}}

  defp require_terminal(%{status: status})
       when status in [:completed, :continued_as_new, :failed, :canceled],
       do: :ok

  defp require_terminal(cursor) do
    {:error,
     {:invalid_history,
      %{
        event_id: cursor.next_event_id,
        message: "offline replay requires terminal Workflow history"
      }}}
  end
end
