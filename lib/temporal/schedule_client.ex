defmodule Temporal.ScheduleClient do
  @moduledoc """
  Client APIs for Temporal Schedules.

  Schedules run a Workflow (or other action) on a cron/interval schedule. This
  client covers create, describe, delete, and list. All functions return
  `{:ok, result}` or `{:error, term()}`.
  """

  alias Temporal.Api.Common.V1.WorkflowType
  alias Temporal.Api.Schedule.V1.{Schedule, ScheduleAction, ScheduleSpec}
  alias Temporal.Api.Taskqueue.V1.TaskQueue
  alias Temporal.Api.Workflow.V1.NewWorkflowExecutionInfo

  alias Temporal.Api.Workflowservice.V1.{
    CreateScheduleRequest,
    CreateScheduleResponse,
    DeleteScheduleRequest,
    DeleteScheduleResponse,
    DescribeScheduleRequest,
    DescribeScheduleResponse,
    ListSchedulesRequest,
    ListSchedulesResponse
  }

  @create_method "/temporal.api.workflowservice.v1.WorkflowService/CreateSchedule"
  @describe_method "/temporal.api.workflowservice.v1.WorkflowService/DescribeSchedule"
  @delete_method "/temporal.api.workflowservice.v1.WorkflowService/DeleteSchedule"
  @list_method "/temporal.api.workflowservice.v1.WorkflowService/ListSchedules"

  @doc """
  Creates a Schedule that starts a Workflow on a cron schedule.

  `schedule_id` is the Schedule's ID. `workflow_type` is the Workflow Type name,
  `argument` its input, and `options` accepts `:task_queue`, `:cron` (cron
  string, e.g. `"0 * * * *"`), `:interval` (seconds), `:workflow_id`,
  `:memo`, and `:search_attributes`.
  """
  @spec create_schedule(GenServer.server(), String.t(), String.t(), term(), keyword()) ::
          {:ok, CreateScheduleResponse.t()} | {:error, term()}
  def create_schedule(connection, schedule_id, workflow_type, argument, options \\ []) do
    config = Temporal.Connection.configuration(connection)

    workflow = %NewWorkflowExecutionInfo{
      workflow_id: Keyword.get(options, :workflow_id, schedule_id),
      workflow_type: %WorkflowType{name: workflow_type},
      task_queue: %TaskQueue{name: Keyword.fetch!(options, :task_queue)},
      input: Temporal.Payload.encode(argument)
    }

    request = %CreateScheduleRequest{
      namespace: config.namespace,
      schedule_id: schedule_id,
      identity: config.identity,
      request_id: request_id(),
      memo: Keyword.get(options, :memo),
      search_attributes: Keyword.get(options, :search_attributes),
      schedule: %Schedule{
        spec: schedule_spec(options),
        action: %ScheduleAction{action: {:start_workflow, workflow}}
      }
    }

    call(connection, @create_method, request, CreateScheduleResponse, options)
  end

  @doc "Describes a Schedule by ID and returns the full Schedule description."
  @spec describe_schedule(GenServer.server(), String.t(), keyword()) ::
          {:ok, DescribeScheduleResponse.t()} | {:error, term()}
  def describe_schedule(connection, schedule_id, options \\ []) do
    config = Temporal.Connection.configuration(connection)

    request = %DescribeScheduleRequest{
      namespace: config.namespace,
      schedule_id: schedule_id
    }

    call(connection, @describe_method, request, DescribeScheduleResponse, options)
  end

  @doc "Deletes a Schedule by ID."
  @spec delete_schedule(GenServer.server(), String.t(), keyword()) ::
          {:ok, DeleteScheduleResponse.t()} | {:error, term()}
  def delete_schedule(connection, schedule_id, options \\ []) do
    config = Temporal.Connection.configuration(connection)

    request = %DeleteScheduleRequest{
      namespace: config.namespace,
      schedule_id: schedule_id,
      identity: config.identity
    }

    call(connection, @delete_method, request, DeleteScheduleResponse, options)
  end

  @doc "Lists Schedules in the namespace, with optional page size and token."
  @spec list_schedules(GenServer.server(), keyword()) ::
          {:ok, %{schedules: list(), next_page_token: String.t()}} | {:error, term()}
  def list_schedules(connection, options \\ []) do
    config = Temporal.Connection.configuration(connection)

    request = %ListSchedulesRequest{
      namespace: config.namespace,
      maximum_page_size: Keyword.get(options, :page_size, 0),
      next_page_token: Keyword.get(options, :next_page_token, "")
    }

    with {:ok, response} <-
           call(connection, @list_method, request, ListSchedulesResponse, options) do
      {:ok,
       %{
         schedules: response.schedules,
         next_page_token: response.next_page_token
       }}
    end
  end

  defp schedule_spec(options) do
    case {Keyword.get(options, :cron), Keyword.get(options, :interval)} do
      {cron, _} when is_binary(cron) and cron != "" ->
        %ScheduleSpec{cron_string: [cron]}

      {nil, seconds} when is_integer(seconds) and seconds > 0 ->
        %ScheduleSpec{
          interval: [
            %Temporal.Api.Schedule.V1.IntervalSpec{
              interval: %Google.Protobuf.Duration{seconds: seconds}
            }
          ]
        }

      _other ->
        raise ArgumentError, "Schedule requires :cron (string) or :interval (seconds)"
    end
  end

  defp call(connection, method, request, response_module, options) do
    request_module = request.__struct__

    with {:ok, bytes} <-
           Temporal.Connection.unary(connection, method, request_module.encode(request), options) do
      {:ok, response_module.decode(bytes)}
    end
  end

  defp request_id, do: "elixir-#{System.unique_integer([:positive, :monotonic])}"
end
