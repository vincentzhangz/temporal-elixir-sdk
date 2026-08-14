defmodule Temporal.Worker do
  @moduledoc """
  Worker facade for bounded Workflow and Activity Task polling.

  Pass `:workflows` to run the Workflow poller. Adding an `:activities` map
  starts separately supervised Workflow and Activity pollers under one facade.
  Named functions currently accept zero or one argument.
  """

  use GenServer

  alias Temporal.Api.Taskqueue.V1.TaskQueue

  alias Temporal.Api.Workflowservice.V1.{
    GetWorkflowExecutionHistoryRequest,
    GetWorkflowExecutionHistoryResponse,
    PollWorkflowTaskQueueRequest,
    PollWorkflowTaskQueueResponse,
    RespondWorkflowTaskCompletedResponse,
    RespondWorkflowTaskFailedResponse
  }

  alias Temporal.Worker.Runtime
  alias Temporal.Workflow.TaskKernel.HistoryPaginator

  @poll_method "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue"
  @complete_method "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted"
  @fail_method "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskFailed"
  @history_method "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory"

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) do
    if Keyword.has_key?(options, :activities) and not Keyword.get(options, :internal, false) do
      Temporal.Worker.Supervisor.start_link(options)
    else
      GenServer.start_link(__MODULE__, options)
    end
  end

  @spec stop(GenServer.server()) :: :ok
  def stop(worker) do
    if Process.alive?(worker), do: GenServer.stop(worker, :normal)
    :ok
  catch
    :exit, _reason -> :ok
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
      workflows: Keyword.fetch!(options, :workflows),
      poll_timeout: Keyword.get(options, :poll_timeout, 30_000),
      run_state_ttl: Keyword.get(options, :run_state_ttl, 60_000),
      runs: %{}
    }

    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    request = %PollWorkflowTaskQueueRequest{
      namespace: state.namespace,
      identity: state.identity,
      task_queue: %TaskQueue{name: state.task_queue}
    }

    result = rpc(state, @poll_method, request, PollWorkflowTaskQueueResponse)

    case result do
      {:ok, %{task_token: ""}} ->
        schedule_poll()
        {:noreply, state}

      {:ok, task} ->
        state = complete_task(task, state)
        schedule_poll()
        {:noreply, state}

      {:error, _reason} ->
        schedule_poll()
        {:noreply, state}
    end
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

  defp complete_task(task, state) do
    key = run_key(task, state.namespace)
    previous_run = Map.get(state.runs, key)

    case assemble_history(task, state) do
      {:ok, task} ->
        prepare_workflow_task(task, previous_run, key, state)

      {:error, _reason} ->
        %{state | runs: Map.delete(state.runs, key)}
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
    case rpc(state, @complete_method, completion, RespondWorkflowTaskCompletedResponse) do
      {:ok, _response} ->
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
