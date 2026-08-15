defmodule Temporal.Activity.Async do
  @moduledoc """
  Asynchronously complete or fail an Activity Task from any process.

  The Activity worker registers every polled task token with itself. An
  Activity (or any other process holding the token) can call `complete_async/2`
  or `fail_async/3` to respond on the worker's behalf. This is the BEAM
  analogue of the official SDKs' async Activity completion.
  """

  @spec complete_async(GenServer.server(), binary(), term()) :: :ok | {:error, term()}
  def complete_async(worker, task_token, result)
      when is_binary(task_token) and task_token != "" do
    message = {:activity_async_complete, task_token, Temporal.Payload.encode(result)}
    GenServer.cast(worker, message)
  end

  def complete_async(_worker, _task_token, _result),
    do: {:error, :invalid_activity_task_token}

  @spec fail_async(GenServer.server(), binary(), Exception.t() | term()) :: :ok | {:error, term()}
  def fail_async(worker, task_token, failure)
      when is_binary(task_token) and task_token != "" do
    failure = Temporal.Failure.to_proto(exception!(failure), [])
    message = {:activity_async_fail, task_token, failure}
    GenServer.cast(worker, message)
  end

  def fail_async(_worker, _task_token, _failure),
    do: {:error, :invalid_activity_task_token}

  defp exception!(%_{} = exception), do: exception
  defp exception!(term), do: RuntimeError.exception(message: inspect(term))
end
