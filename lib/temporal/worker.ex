defmodule Temporal.Worker do
  @moduledoc """
  Worker facade for bounded Workflow and Activity Task polling.

  Pass `:workflows` to run the Workflow poller. Adding an `:activities` map
  starts separately supervised Workflow and Activity pollers under one facade.
  Named functions currently accept zero or one argument.
  """

  use GenServer

  alias Temporal.Activity.Worker, as: ActivityWorker
  alias Temporal.Api.Common.V1.{WorkerVersionCapabilities, WorkerVersionStamp}
  alias Temporal.Api.Taskqueue.V1.{StickyExecutionAttributes, TaskQueue}

  alias Temporal.Api.Workflowservice.V1.{
    GetWorkflowExecutionHistoryRequest,
    GetWorkflowExecutionHistoryResponse,
    PollWorkflowTaskQueueRequest,
    PollWorkflowTaskQueueResponse,
    RespondQueryTaskCompletedResponse,
    RespondWorkflowTaskCompletedResponse,
    RespondWorkflowTaskFailedResponse
  }

  alias Temporal.Worker.Runtime
  alias Temporal.Workflow.TaskKernel.HistoryPaginator

  @poll_method "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue"
  @complete_method "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted"
  @fail_method "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskFailed"
  @query_complete_method "/temporal.api.workflowservice.v1.WorkflowService/RespondQueryTaskCompleted"
  @history_method "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory"

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) do
    if Keyword.has_key?(options, :activities) and not Keyword.get(options, :internal, false) do
      Temporal.Worker.Supervisor.start_link(options)
    else
      GenServer.start_link(__MODULE__, options)
    end
  end

  @doc """
  Gracefully stops the worker, waiting up to `timeout` ms for in-flight work.

  Polling stops and any currently-processing Workflow Task is allowed to
  complete (or time out) before the process exits. Returns `:ok`.
  """
  @spec stop(GenServer.server(), timeout()) :: :ok
  def stop(worker, timeout \\ 5_000) do
    if Process.alive?(worker), do: GenServer.stop(worker, :normal, timeout)
    :ok
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init(options) do
    connection = Keyword.fetch!(options, :connection)
    config = Temporal.Connection.configuration(connection)

    sticky = Keyword.get(options, :sticky, false)

    state = %{
      connection: connection,
      namespace: config.namespace,
      identity: config.identity,
      task_queue: Keyword.fetch!(options, :task_queue),
      workflows: Keyword.fetch!(options, :workflows),
      poll_timeout: Keyword.get(options, :poll_timeout, 30_000),
      run_state_ttl: Keyword.get(options, :run_state_ttl, 60_000),
      sticky: sticky,
      sticky_task_queue:
        if(sticky, do: "sticky-#{System.unique_integer([:positive])}", else: nil),
      eager_activity_worker: Keyword.get(options, :eager_activity_worker),
      build_id: Keyword.get(options, :build_id),
      use_versioning: Keyword.get(options, :use_versioning, false),
      runs: %{}
    }

    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    result = poll_once(state)

    case result do
      {:ok, %{task_token: ""}} ->
        schedule_poll()
        {:noreply, state}

      {:ok, task} ->
        # Process concurrently so independent runs proceed in parallel; the
        # run cache + token fencing preserve per-run serialization.
        spawn_process_task(task, state)
        schedule_poll()
        {:noreply, state}

      {:error, _reason} ->
        schedule_poll()
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:task_evicted, key}, state) do
    {:noreply, %{state | runs: Map.delete(state.runs, key)}}
  end

  def handle_info({:task_completed, _key, runs_update}, state) do
    {:noreply, %{state | runs: Map.merge(state.runs, runs_update)}}
  end

  @impl true
  def handle_info({:evict_run, key, task_token}, state) do
    runs =
      case Map.get(state.runs, key) do
        %{task_token: ^task_token} -> Map.delete(state.runs, key)
        _cursor -> state.runs
      end

    {:noreply, %{state | runs: runs}}
  end

  defp spawn_process_task(task, state) do
    worker = self()
    runs = state.runs
    namespace = state.namespace

    spawn(fn ->
      key = run_key(task, namespace)
      previous_run = Map.get(runs, key)

      processed =
        complete_task(task, %{state | runs: runs}, previous_run, key)

      case Map.fetch(processed.runs, key) do
        {:ok, run} -> send(worker, {:task_completed, key, %{key => run}})
        :error -> send(worker, {:task_evicted, key})
      end
    end)
  end

  defp poll_once(%{sticky: true, sticky_task_queue: sticky_queue} = state) do
    sticky_request = %PollWorkflowTaskQueueRequest{
      namespace: state.namespace,
      identity: state.identity,
      task_queue: %TaskQueue{name: sticky_queue, kind: :TASK_QUEUE_KIND_STICKY},
      worker_version_capabilities: version_capabilities(state)
    }

    case rpc(state, @poll_method, sticky_request, PollWorkflowTaskQueueResponse) do
      {:ok, %{task_token: ""}} -> poll_normal(state)
      other -> other
    end
  end

  defp poll_once(state), do: poll_normal(state)

  defp poll_normal(state) do
    request = %PollWorkflowTaskQueueRequest{
      namespace: state.namespace,
      identity: state.identity,
      task_queue: %TaskQueue{name: state.task_queue},
      worker_version_capabilities: version_capabilities(state)
    }

    rpc(state, @poll_method, request, PollWorkflowTaskQueueResponse)
  end

  defp version_capabilities(%{use_versioning: true, build_id: build_id})
       when is_binary(build_id) and build_id != "" do
    %WorkerVersionCapabilities{build_id: build_id, use_versioning: true}
  end

  defp version_capabilities(_state), do: nil

  defp complete_task(task, state, previous_run, key) do
    if query_only_task?(task) do
      complete_query_task(task, state)
    else
      case assemble_history(task, state) do
        {:ok, task} ->
          prepare_workflow_task(task, previous_run, key, state)

        {:error, _reason} ->
          %{state | runs: Map.delete(state.runs, key)}
      end
    end
  end

  defp query_only_task?(%{query: query}) when not is_nil(query), do: true

  defp query_only_task?(%{queries: queries}) when is_map(queries) and map_size(queries) > 0,
    do: false

  defp query_only_task?(_task), do: false

  defp complete_query_task(task, state) do
    case assemble_history(task, state) do
      {:ok, task} ->
        case Runtime.prepare_query_response(task, state.workflows, state.identity,
               namespace: state.namespace
             ) do
          {:ok, response} ->
            _rpc = rpc(state, @query_complete_method, response, RespondQueryTaskCompletedResponse)
            state

          {:error, _reason} ->
            state
        end

      {:error, _reason} ->
        state
    end
  end

  defp prepare_workflow_task(task, previous_run, key, state) do
    case Runtime.prepare_response(
           task,
           state.workflows,
           state.identity,
           previous_run,
           namespace: state.namespace
         ) do
      {:completed, completion, next_cursor} ->
        complete_workflow_task(completion, next_cursor, key, state)

      {:failed, failure, _reason} ->
        _response = rpc(state, @fail_method, failure, RespondWorkflowTaskFailedResponse)
        %{state | runs: Map.delete(state.runs, key)}
    end
  end

  defp complete_workflow_task(completion, next_run, key, state) do
    completion = maybe_attach_sticky(completion, state)

    case rpc(state, @complete_method, completion, RespondWorkflowTaskCompletedResponse) do
      {:ok, response} ->
        dispatch_eager_activities(response, state)

        Process.send_after(
          self(),
          {:evict_run, key, next_run.task_token},
          state.run_state_ttl
        )

        %{state | runs: Map.put(state.runs, key, next_run)}

      {:error, _reason} ->
        %{state | runs: Map.delete(state.runs, key)}
    end
  end

  defp dispatch_eager_activities(%{activity_tasks: activity_tasks}, %{
         eager_activity_worker: worker
       })
       when is_list(activity_tasks) and activity_tasks != [] and not is_nil(worker) do
    Enum.each(activity_tasks, &ActivityWorker.submit_eager(worker, &1))
  end

  defp dispatch_eager_activities(_response, _state), do: :ok

  defp maybe_attach_sticky(
         completion,
         %{sticky: true, sticky_task_queue: sticky_queue} = state
       ) do
    completion
    |> Map.put(:sticky_attributes, %StickyExecutionAttributes{
      worker_task_queue: %TaskQueue{name: sticky_queue, kind: :TASK_QUEUE_KIND_STICKY}
    })
    |> maybe_attach_version_stamp(state)
  end

  defp maybe_attach_sticky(completion, state), do: maybe_attach_version_stamp(completion, state)

  defp maybe_attach_version_stamp(completion, %{use_versioning: true, build_id: build_id})
       when is_binary(build_id) and build_id != "" do
    %{
      completion
      | worker_version_stamp: %WorkerVersionStamp{build_id: build_id, use_versioning: true}
    }
  end

  defp maybe_attach_version_stamp(completion, _state), do: completion

  defp assemble_history(task, state) do
    HistoryPaginator.assemble(task, fn page_token ->
      request = %GetWorkflowExecutionHistoryRequest{
        namespace: state.namespace,
        execution: task.workflow_execution,
        next_page_token: page_token,
        wait_new_event: false,
        history_event_filter_type: :HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
      }

      rpc(state, @history_method, request, GetWorkflowExecutionHistoryResponse)
    end)
  end

  defp run_key(%{workflow_execution: %{run_id: run_id}}, namespace), do: {namespace, run_id}

  defp run_key(_task, _namespace), do: :unknown_run

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
