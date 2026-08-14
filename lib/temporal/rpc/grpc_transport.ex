defmodule Temporal.RPC.GRPCTransport do
  @moduledoc """
  Pure-BEAM gRPC transport backed by grpc-elixir's Mint HTTP/2 adapter.
  """

  @behaviour Temporal.RPC.Transport

  alias Temporal.Connection.Options
  alias Temporal.RPC.Error

  @rpc_types %{
    "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo" =>
      {:get_system_info, Temporal.Api.Workflowservice.V1.GetSystemInfoRequest,
       Temporal.Api.Workflowservice.V1.GetSystemInfoResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution" =>
      {:start_workflow_execution, Temporal.Api.Workflowservice.V1.StartWorkflowExecutionRequest,
       Temporal.Api.Workflowservice.V1.StartWorkflowExecutionResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory" =>
      {:get_workflow_execution_history,
       Temporal.Api.Workflowservice.V1.GetWorkflowExecutionHistoryRequest,
       Temporal.Api.Workflowservice.V1.GetWorkflowExecutionHistoryResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/RequestCancelWorkflowExecution" =>
      {:request_cancel_workflow_execution,
       Temporal.Api.Workflowservice.V1.RequestCancelWorkflowExecutionRequest,
       Temporal.Api.Workflowservice.V1.RequestCancelWorkflowExecutionResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/SignalWorkflowExecution" =>
      {:signal_workflow_execution, Temporal.Api.Workflowservice.V1.SignalWorkflowExecutionRequest,
       Temporal.Api.Workflowservice.V1.SignalWorkflowExecutionResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution" =>
      {:signal_with_start_workflow_execution,
       Temporal.Api.Workflowservice.V1.SignalWithStartWorkflowExecutionRequest,
       Temporal.Api.Workflowservice.V1.SignalWithStartWorkflowExecutionResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue" =>
      {:poll_workflow_task_queue, Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueRequest,
       Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted" =>
      {:respond_workflow_task_completed,
       Temporal.Api.Workflowservice.V1.RespondWorkflowTaskCompletedRequest,
       Temporal.Api.Workflowservice.V1.RespondWorkflowTaskCompletedResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskFailed" =>
      {:respond_workflow_task_failed,
       Temporal.Api.Workflowservice.V1.RespondWorkflowTaskFailedRequest,
       Temporal.Api.Workflowservice.V1.RespondWorkflowTaskFailedResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue" =>
      {:poll_activity_task_queue, Temporal.Api.Workflowservice.V1.PollActivityTaskQueueRequest,
       Temporal.Api.Workflowservice.V1.PollActivityTaskQueueResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted" =>
      {:respond_activity_task_completed,
       Temporal.Api.Workflowservice.V1.RespondActivityTaskCompletedRequest,
       Temporal.Api.Workflowservice.V1.RespondActivityTaskCompletedResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailed" =>
      {:respond_activity_task_failed,
       Temporal.Api.Workflowservice.V1.RespondActivityTaskFailedRequest,
       Temporal.Api.Workflowservice.V1.RespondActivityTaskFailedResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeat" =>
      {:record_activity_task_heartbeat,
       Temporal.Api.Workflowservice.V1.RecordActivityTaskHeartbeatRequest,
       Temporal.Api.Workflowservice.V1.RecordActivityTaskHeartbeatResponse},
    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCanceled" =>
      {:respond_activity_task_canceled,
       Temporal.Api.Workflowservice.V1.RespondActivityTaskCanceledRequest,
       Temporal.Api.Workflowservice.V1.RespondActivityTaskCanceledResponse}
  }

  @spec connect(Options.t(), keyword()) :: {:ok, map()} | {:error, Error.t()}
  def connect(options, raw_options) do
    driver = Keyword.get(raw_options, :grpc_driver, __MODULE__.Driver)

    case driver.connect(options) do
      {:ok, channel} -> {:ok, %{channel: channel, options: options, driver: driver}}
      {:error, reason} -> {:error, Error.from(reason)}
    end
  end

  @impl true
  def unary(%{channel: channel, options: config, driver: driver}, method, request, options) do
    with :ok <- within_limit(request, config.max_message_size, "request"),
         {:ok, {rpc, request_module, response_module}} <- rpc(method) do
      request = request_module.decode(request)
      timeout = Keyword.get(options, :timeout, config.default_deadline)

      call_options = [
        timeout: timeout,
        metadata: call_metadata(config, options)
      ]

      case driver.call(channel, rpc, request, call_options) do
        {:ok, response} ->
          encode_response(response_module, response, config.max_message_size)

        {:ok, response, _headers_and_trailers} ->
          encode_response(response_module, response, config.max_message_size)

        {:error, reason} ->
          {:error, Error.from(reason)}
      end
    end
  catch
    :exit, {:timeout, _} -> {:error, Error.from(:timeout)}
    :exit, {:shutdown, _} -> {:error, Error.from(:cancelled)}
  end

  defp call_metadata(config, options) do
    config
    |> Options.metadata()
    |> Map.merge(Keyword.get(options, :metadata, %{}))
  end

  @spec close(map()) :: :ok
  def close(%{channel: channel, driver: driver}) do
    _ = driver.disconnect(channel)
    :ok
  end

  defp rpc(method) do
    case Map.fetch(@rpc_types, method) do
      {:ok, rpc} -> {:ok, rpc}
      :error -> {:error, %Error{status: :unimplemented, message: "unsupported RPC #{method}"}}
    end
  end

  defp encode_response(module, response, limit) do
    bytes = module.encode(response)

    case within_limit(bytes, limit, "response") do
      :ok -> {:ok, bytes}
      error -> error
    end
  end

  defp within_limit(bytes, limit, _direction) when byte_size(bytes) <= limit, do: :ok

  defp within_limit(_bytes, _limit, direction) do
    {:error,
     %Error{status: :resource_exhausted, message: "#{direction} exceeds message size limit"}}
  end

  defmodule Driver do
    @moduledoc false
    @stub Temporal.Api.Workflowservice.V1.WorkflowService.Stub

    def connect(options) do
      credential =
        if options.tls, do: %GRPC.Credential{ssl: options.tls_options}, else: nil

      GRPC.Stub.connect(options.target,
        adapter: GRPC.Client.Adapters.Mint,
        cred: credential,
        connect_timeout: options.connect_timeout,
        adapter_opts: adapter_opts(options)
      )
    end

    def call(channel, rpc, request, options), do: apply(@stub, rpc, [channel, request, options])
    def disconnect(channel), do: GRPC.Stub.disconnect(channel)

    defp adapter_opts(options) do
      []
      |> maybe_put(:keepalive, options.keepalive)
      |> maybe_put(:transport_opts, transport_opts(options))
    end

    defp transport_opts(%{tls: true} = options) do
      [hostname: options.target |> String.replace_prefix("dns://", "") |> host_from_target()]
    end

    defp transport_opts(_options), do: []

    defp host_from_target(target) do
      case String.split(target, ":") do
        [host] -> host
        [host, _port] -> host
        _ -> target
      end
    end

    defp maybe_put(opts, _key, nil), do: opts
    defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
  end
end
