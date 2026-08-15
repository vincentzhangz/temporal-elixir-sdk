defmodule Temporal.Activity.Worker do
  @moduledoc """
  Bounded long-poll executor for registered zero- or one-argument Activities.

  Activity tasks are executed concurrently: each polled task runs in its own
  spawned process, so independent Activities proceed in parallel while each
  retains its own heartbeat/cancellation context. Task tokens are registered
  with the worker, enabling asynchronous completion via
  `Temporal.Activity.Async.complete_async/3` / `fail_async/3`. Eager
  execution, local Activities, and worker versioning are not implemented.
  """

  use GenServer

  alias Temporal.Activity.Runtime
  alias Temporal.Api.Taskqueue.V1.TaskQueue

  alias Temporal.Api.Workflowservice.V1.{
    PollActivityTaskQueueRequest,
    PollActivityTaskQueueResponse,
    RecordActivityTaskHeartbeatResponse,
    RespondActivityTaskCanceledResponse,
    RespondActivityTaskCompletedRequest,
    RespondActivityTaskCompletedResponse,
    RespondActivityTaskFailedRequest,
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

  @doc "Dispatches an eager Activity task (from a Workflow Task completion) without polling."
  @spec submit_eager(GenServer.server(), PollActivityTaskQueueResponse.t()) :: :ok
  def submit_eager(worker, %PollActivityTaskQueueResponse{} = task) do
    GenServer.cast(worker, {:eager_activity_task, task})
  end

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
      payload_codecs: config.payload_codecs,
      fences: %{},
      tokens: %{}
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

  @impl true
  def handle_info({:activity_task_result, task_token, {:ok, request}}, state) do
    case Map.pop(state.tokens, task_token) do
      {nil, _tokens} ->
        {:noreply, state}

      {_task, tokens} ->
        state =
          case rpc(state, @complete_method, request, RespondActivityTaskCompletedResponse) do
            {:ok, _response} -> state
            {:error, _reason} -> state
          end

        {:noreply, %{state | tokens: tokens}}
    end
  end

  def handle_info({:activity_task_result, task_token, {:error_response, request}}, state) do
    case Map.pop(state.tokens, task_token) do
      {nil, _tokens} ->
        {:noreply, state}

      {_task, tokens} ->
        state =
          case rpc(state, @fail_method, request, RespondActivityTaskFailedResponse) do
            {:ok, _response} -> state
            {:error, _reason} -> state
          end

        {:noreply, %{state | tokens: tokens}}
    end
  end

  def handle_info({:activity_task_result, task_token, {:canceled, request}}, state) do
    case Map.pop(state.tokens, task_token) do
      {nil, _tokens} ->
        {:noreply, state}

      {_task, tokens} ->
        state =
          case rpc(state, @cancel_method, request, RespondActivityTaskCanceledResponse) do
            {:ok, _response} -> state
            {:error, _reason} -> state
          end

        {:noreply, %{state | tokens: tokens}}
    end
  end

  def handle_info({:activity_task_result, task_token, :dropped}, state) do
    {:noreply, %{state | tokens: Map.delete(state.tokens, task_token)}}
  end

  @impl true
  def handle_cast({:activity_async_complete, task_token, payload}, state) do
    case Map.get(state.tokens, task_token) do
      nil ->
        {:noreply, state}

      task ->
        request = %RespondActivityTaskCompletedRequest{
          task_token: task_token,
          result: payload,
          identity: state.identity,
          namespace: state.namespace
        }

        handle_async_respond(task_token, task, request, :complete, state)
    end
  end

  def handle_cast({:activity_async_fail, task_token, failure}, state) do
    case Map.get(state.tokens, task_token) do
      nil ->
        {:noreply, state}

      task ->
        request = %RespondActivityTaskFailedRequest{
          task_token: task_token,
          failure: failure,
          identity: state.identity,
          namespace: state.namespace
        }

        handle_async_respond(task_token, task, request, :fail, state)
    end
  end

  @impl true
  def handle_cast({:eager_activity_task, %PollActivityTaskQueueResponse{} = task}, state) do
    {:noreply, execute_task(task, state)}
  end

  defp execute_task(task, state) do
    key = fence_key(task)
    fence = Map.get(state.fences, key)
    tokens = Map.put(state.tokens, task.task_token, task)
    Process.send_after(self(), {:evict_fence, key, task.task_token}, state.fence_ttl)

    worker = self()
    connection = state.connection
    namespace = state.namespace
    identity = state.identity
    activities = state.activities
    task_queue = state.task_queue
    poll_timeout = state.poll_timeout
    payload_codecs = state.payload_codecs

    spawn(fn ->
      heartbeat = &heartbeat_rpc(connection, poll_timeout, &1)

      result =
        case Runtime.prepare(task, activities, identity, fence,
               namespace: namespace,
               task_queue: task_queue,
               heartbeat: heartbeat,
               payload_codecs: payload_codecs
             ) do
          {:ok, completion, _next_fence} ->
            {:ok, completion}

          {:error_response, failure, _next_fence} ->
            {:error_response, failure}

          {:canceled, cancellation, _next_fence} ->
            {:canceled, cancellation}

          {:error, _reason} ->
            :dropped
        end

      send(worker, {:activity_task_result, task.task_token, result})
    end)

    %{state | tokens: tokens, fences: Map.put(state.fences, key, fence)}
  end

  defp heartbeat_rpc(connection, timeout, request) do
    request_module = request.__struct__

    with {:ok, bytes} <-
           Temporal.Connection.unary(
             connection,
             @heartbeat_method,
             request_module.encode(request),
             timeout: timeout
           ) do
      {:ok, RecordActivityTaskHeartbeatResponse.decode(bytes)}
    end
  end

  defp handle_async_respond(task_token, _task, request, kind, state) do
    case rpc(state, respond_method(kind), request, response_module(kind)) do
      {:ok, _response} ->
        {:noreply, %{state | tokens: Map.delete(state.tokens, task_token)}}

      {:error, _reason} ->
        {:noreply, %{state | tokens: Map.delete(state.tokens, task_token)}}
    end
  end

  defp respond_method(:complete), do: @complete_method
  defp respond_method(:fail), do: @fail_method

  defp response_module(:complete), do: RespondActivityTaskCompletedResponse
  defp response_module(:fail), do: RespondActivityTaskFailedResponse

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
