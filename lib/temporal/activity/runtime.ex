defmodule Temporal.Activity.Runtime do
  @moduledoc false

  alias Temporal.Activity.{Context, Info}

  alias Temporal.Api.Workflowservice.V1.{
    PollActivityTaskQueueResponse,
    RespondActivityTaskCanceledRequest,
    RespondActivityTaskCompletedRequest,
    RespondActivityTaskFailedRequest
  }

  @spec prepare(PollActivityTaskQueueResponse.t(), map(), String.t(), map() | nil) ::
          {:ok, RespondActivityTaskCompletedRequest.t(), map()}
          | {:canceled, RespondActivityTaskCanceledRequest.t(), map()}
          | {:error_response, RespondActivityTaskFailedRequest.t(), map()}
          | {:error, term()}
  def prepare(%PollActivityTaskQueueResponse{} = task, activities, identity, previous_fence) do
    prepare(task, activities, identity, previous_fence, [])
  end

  @spec prepare(PollActivityTaskQueueResponse.t(), map(), String.t(), map() | nil, keyword()) ::
          {:ok, RespondActivityTaskCompletedRequest.t(), map()}
          | {:canceled, RespondActivityTaskCanceledRequest.t(), map()}
          | {:error_response, RespondActivityTaskFailedRequest.t(), map()}
          | {:error, term()}
  def prepare(
        %PollActivityTaskQueueResponse{} = task,
        activities,
        identity,
        previous_fence,
        options
      ) do
    with :ok <- validate_task(task),
         :ok <- fence(previous_fence, task),
         {:ok, activity} <- fetch_activity(activities, task.activity_type),
         {:ok, argument} <-
           Temporal.Payload.decode(task.input, Keyword.get(options, :payload_codecs, [])) do
      task_fence = task_fence(task)
      namespace = Keyword.get(options, :namespace, task.workflow_namespace)

      context = %Context{
        info: activity_info(task, namespace, Keyword.get(options, :task_queue)),
        task_token: task.task_token,
        identity: identity,
        heartbeat: Keyword.get(options, :heartbeat, &heartbeat_unavailable/1),
        throttle_ms: heartbeat_throttle_ms(task.heartbeat_timeout)
      }

      case invoke(activity, argument, context) do
        {:ok, result} ->
          {:ok,
           %RespondActivityTaskCompletedRequest{
             task_token: task.task_token,
             result: Temporal.Payload.encode(result, Keyword.get(options, :payload_codecs, [])),
             identity: identity,
             namespace: namespace
           }, task_fence}

        {:error, %Temporal.CanceledError{acknowledged: true} = exception, _stacktrace} ->
          {:canceled,
           %RespondActivityTaskCanceledRequest{
             task_token: task.task_token,
             details: Temporal.Payload.encode(exception.details),
             identity: identity,
             namespace: namespace
           }, task_fence}

        {:error, exception, stacktrace} ->
          failure = Temporal.Failure.to_proto(exception, stacktrace)

          {:error_response,
           %RespondActivityTaskFailedRequest{
             task_token: task.task_token,
             failure: failure,
             identity: identity,
             namespace: namespace
           }, task_fence}
      end
    end
  end

  defp validate_task(%{task_token: ""}), do: {:error, :empty_activity_task}
  defp validate_task(%{activity_id: ""}), do: {:error, :missing_activity_id}

  defp validate_task(%{
         workflow_execution: %{workflow_id: workflow_id, run_id: run_id},
         attempt: attempt
       })
       when workflow_id != "" and run_id != "" and attempt > 0,
       do: :ok

  defp validate_task(%{workflow_execution: nil}),
    do: {:error, :missing_workflow_execution_identity}

  defp validate_task(%{workflow_execution: %{workflow_id: ""}}),
    do: {:error, :missing_workflow_execution_identity}

  defp validate_task(%{workflow_execution: %{run_id: ""}}),
    do: {:error, :missing_workflow_execution_identity}

  defp validate_task(%{attempt: attempt}) when attempt < 1,
    do: {:error, :invalid_activity_attempt}

  defp validate_task(_task), do: :ok

  defp fence(nil, _task), do: :ok

  defp fence(previous, task) do
    current = task_fence(task)
    identity_fields = [:workflow_id, :run_id, :activity_id, :attempt]

    if Enum.all?(identity_fields, &(Map.get(previous, &1) == Map.get(current, &1))) do
      if previous.task_token == current.task_token do
        {:error, {:stale_activity_task, %{activity_id: current.activity_id}}}
      else
        {:error, {:activity_task_token_mismatch, %{activity_id: current.activity_id}}}
      end
    else
      :ok
    end
  end

  defp task_fence(task) do
    %{
      workflow_id: task.workflow_execution.workflow_id,
      run_id: task.workflow_execution.run_id,
      activity_id: task.activity_id,
      attempt: task.attempt,
      task_token: task.task_token
    }
  end

  defp fetch_activity(activities, %{name: name}) do
    case Map.fetch(activities, name) do
      {:ok, activity} -> {:ok, activity}
      :error -> {:error, {:activity_not_registered, name}}
    end
  end

  defp fetch_activity(_activities, _type), do: {:error, :missing_activity_type}

  defp invoke(activity, _argument, context) when is_function(activity, 0),
    do: safe_invoke(activity, [], context)

  defp invoke(activity, argument, context) when is_function(activity, 1),
    do: safe_invoke(activity, [argument], context)

  defp invoke(_activity, _argument, _context),
    do: {:error, ArgumentError.exception("unsupported Activity arity"), []}

  defp safe_invoke(activity, arguments, context) do
    Temporal.Activity.put_context(context)

    try do
      {:ok, apply(activity, arguments)}
    rescue
      exception -> {:error, exception, __STACKTRACE__}
    after
      Temporal.Activity.clear_context()
    end
  end

  defp activity_info(task, namespace, task_queue) do
    %Info{
      namespace: namespace,
      workflow_id: task.workflow_execution.workflow_id,
      run_id: task.workflow_execution.run_id,
      workflow_type: task.workflow_type && task.workflow_type.name,
      activity_id: task.activity_id,
      activity_type: task.activity_type.name,
      task_queue: task_queue,
      attempt: task.attempt,
      scheduled_time: task.scheduled_time,
      current_attempt_scheduled_time: task.current_attempt_scheduled_time,
      started_time: task.started_time,
      schedule_to_close_timeout: task.schedule_to_close_timeout,
      start_to_close_timeout: task.start_to_close_timeout,
      heartbeat_timeout: task.heartbeat_timeout,
      retry_policy: task.retry_policy,
      heartbeat_details: task.heartbeat_details
    }
  end

  defp heartbeat_throttle_ms(nil), do: 60_000

  defp heartbeat_throttle_ms(%Google.Protobuf.Duration{seconds: seconds, nanos: nanos}) do
    timeout_ms = seconds * 1_000 + div(nanos, 1_000_000)
    max(1, min(60_000, trunc(timeout_ms * 0.8)))
  end

  defp heartbeat_unavailable(_request), do: {:error, :heartbeat_transport_unavailable}
end
