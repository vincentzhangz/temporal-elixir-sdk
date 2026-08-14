defmodule Temporal.Activity.Worker do
  @moduledoc """
  Bounded long-poll executor for registered zero- or one-argument Activities.

  Activity tasks are executed serially by this process. Async completion,
  eager execution, local Activities, and worker versioning are not implemented.
  """

  use GenServer

  alias Temporal.Activity.Runtime
  alias Temporal.Api.Taskqueue.V1.TaskQueue

  alias Temporal.Api.Workflowservice.V1.{
    PollActivityTaskQueueRequest,
    PollActivityTaskQueueResponse,
    RecordActivityTaskHeartbeatResponse,
    RespondActivityTaskCanceledResponse,
    RespondActivityTaskCompletedResponse,
    RespondActivityTaskFailedResponse
  }

  @poll_method "/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue"
  @complete_method "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted"
  @fail_method "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailed"
  @heartbeat_method "/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeat"
  @cancel_method "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCanceled"

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @spec stop(GenServer.server()) :: :ok
  def stop(worker), do: GenServer.stop(worker, :normal)

  @impl true
  def init(options) do
    connection = Keyword.fetch!(options, :connection)
    config = Temporal.Connection.configuration(connection)

    state = %{
      connection: connection,
      namespace: config.namespace,
      identity: config.identity,
      task_queue: Keyword.fetch!(options, :task_queue),
      activities: Keyword.fetch!(options, :activities),
      poll_timeout: Keyword.get(options, :poll_timeout, 30_000),
      fence_ttl: Keyword.get(options, :fence_ttl, 60_000),
      fences: %{}
    }

    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    request = %PollActivityTaskQueueRequest{
      namespace: state.namespace,
      identity: state.identity,
      task_queue: %TaskQueue{name: state.task_queue}
    }

    next_state =
      case rpc(state, @poll_method, request, PollActivityTaskQueueResponse) do
        {:ok, %{task_token: ""}} -> state
        {:ok, task} -> execute_task(task, state)
        {:error, _reason} -> state
      end

    schedule_poll()
    {:noreply, next_state}
  end

  @impl true
  def handle_info({:evict_fence, key, task_token}, state) do
    fences =
      case Map.get(state.fences, key) do
        %{task_token: ^task_token} -> Map.delete(state.fences, key)
        _fence -> state.fences
      end

    {:noreply, %{state | fences: fences}}
  end

  defp execute_task(task, state) do
    key = fence_key(task)
    fence = Map.get(state.fences, key)

    heartbeat = fn request ->
      rpc(state, @heartbeat_method, request, RecordActivityTaskHeartbeatResponse)
    end

    case Runtime.prepare(task, state.activities, state.identity, fence,
           namespace: state.namespace,
           task_queue: state.task_queue,
           heartbeat: heartbeat
         ) do
      {:ok, completion, next_fence} ->
        respond(
          task,
          key,
          state,
          @complete_method,
          completion,
          RespondActivityTaskCompletedResponse,
          next_fence
        )

      {:error_response, failure, next_fence} ->
        respond(
          task,
          key,
          state,
          @fail_method,
          failure,
          RespondActivityTaskFailedResponse,
          next_fence
        )

      {:canceled, cancellation, next_fence} ->
        respond(
          task,
          key,
          state,
          @cancel_method,
          cancellation,
          RespondActivityTaskCanceledResponse,
          next_fence
        )

      {:error, _reason} ->
        %{state | fences: Map.delete(state.fences, key)}
    end
  end

  defp respond(task, key, state, method, request, response_module, fence) do
    case rpc(state, method, request, response_module) do
      {:ok, _response} ->
        Process.send_after(
          self(),
          {:evict_fence, key, task.task_token},
          state.fence_ttl
        )

        %{state | fences: Map.put(state.fences, key, fence)}

      {:error, _reason} ->
        %{state | fences: Map.delete(state.fences, key)}
    end
  end

  defp fence_key(task) do
    execution = task.workflow_execution
    {execution.workflow_id, execution.run_id, task.activity_id, task.attempt}
  end

  defp rpc(state, method, request, response_module) do
    request_module = request.__struct__

    with {:ok, bytes} <-
           Temporal.Connection.unary(
             state.connection,
             method,
             request_module.encode(request),
             timeout: state.poll_timeout
           ) do
      {:ok, response_module.decode(bytes)}
    end
  end

  defp schedule_poll, do: Process.send_after(self(), :poll, 10)
end
