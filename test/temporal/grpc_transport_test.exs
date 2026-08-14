defmodule Temporal.GRPCTransportTest do
  use ExUnit.Case, async: false

  alias Temporal.Api.Workflowservice.V1.{
    GetSystemInfoRequest,
    GetSystemInfoResponse,
    GetWorkflowExecutionHistoryRequest,
    PollActivityTaskQueueRequest,
    PollWorkflowTaskQueueRequest,
    RespondActivityTaskCompletedRequest,
    RespondActivityTaskFailedRequest,
    RespondWorkflowTaskCompletedRequest,
    SignalWithStartWorkflowExecutionRequest,
    SignalWorkflowExecutionRequest,
    StartWorkflowExecutionRequest
  }

  alias Temporal.Connection.Options
  alias Temporal.RPC.GRPCTransport

  defmodule Driver do
    def connect(_options), do: {:ok, :channel}

    def call(:channel, :get_system_info, %GetSystemInfoRequest{}, options) do
      send(Process.whereis(:grpc_transport_test), {:call_options, options})
      {:ok, %GetSystemInfoResponse{server_version: "test"}, %{trailers: %{"grpc-status" => "0"}}}
    end

    def disconnect(:channel) do
      send(Process.whereis(:grpc_transport_test), :disconnected)
      :ok
    end
  end

  defmodule MetadataDriver do
    def connect(_options), do: {:ok, :metadata_channel}

    def call(:metadata_channel, rpc, _request, options) do
      send(Process.whereis(:grpc_transport_test), {:metadata_call, rpc, options[:metadata]})
      {:ok, response(rpc)}
    end

    def disconnect(:metadata_channel), do: :ok

    defp response(:get_system_info),
      do: %Temporal.Api.Workflowservice.V1.GetSystemInfoResponse{}

    defp response(:start_workflow_execution),
      do: %Temporal.Api.Workflowservice.V1.StartWorkflowExecutionResponse{}

    defp response(:get_workflow_execution_history),
      do: %Temporal.Api.Workflowservice.V1.GetWorkflowExecutionHistoryResponse{}

    defp response(:signal_workflow_execution),
      do: %Temporal.Api.Workflowservice.V1.SignalWorkflowExecutionResponse{}

    defp response(:signal_with_start_workflow_execution),
      do: %Temporal.Api.Workflowservice.V1.SignalWithStartWorkflowExecutionResponse{}

    defp response(:poll_workflow_task_queue),
      do: %Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse{}

    defp response(:respond_workflow_task_completed),
      do: %Temporal.Api.Workflowservice.V1.RespondWorkflowTaskCompletedResponse{}

    defp response(:poll_activity_task_queue),
      do: %Temporal.Api.Workflowservice.V1.PollActivityTaskQueueResponse{}

    defp response(:respond_activity_task_completed),
      do: %Temporal.Api.Workflowservice.V1.RespondActivityTaskCompletedResponse{}

    defp response(:respond_activity_task_failed),
      do: %Temporal.Api.Workflowservice.V1.RespondActivityTaskFailedResponse{}
  end

  test "uses generated stub request types, refreshes metadata, and closes channel" do
    Process.register(self(), :grpc_transport_test)

    on_exit(fn ->
      if Process.whereis(:grpc_transport_test), do: Process.unregister(:grpc_transport_test)
    end)

    {:ok, credentials} = Agent.start_link(fn -> "first" end)

    {:ok, connection} =
      Temporal.Connection.open(
        target: "localhost:7233",
        grpc_driver: Driver,
        api_key: fn -> Agent.get(credentials, & &1) end
      )

    assert {:ok, %GetSystemInfoResponse{server_version: "test"}} =
             Temporal.RPC.system_info(connection)

    assert_receive {:call_options, options}
    assert options[:metadata]["authorization"] == "Bearer first"
    assert options[:metadata]["client-name"] == "temporal-elixir-community"
    assert options[:metadata]["client-version"] == Mix.Project.config()[:version]

    Agent.update(credentials, fn _ -> "second" end)
    assert {:ok, %GetSystemInfoResponse{}} = Temporal.RPC.system_info(connection)
    assert_receive {:call_options, options}
    assert options[:metadata]["authorization"] == "Bearer second"
    assert options[:metadata]["client-name"] == "temporal-elixir-community"
    assert options[:metadata]["client-version"] == Mix.Project.config()[:version]

    assert :ok = Temporal.Connection.close(connection)
    assert_receive :disconnected
  end

  test "adds SDK headers to every supported unary poll and completion RPC" do
    Process.register(self(), :grpc_transport_test)

    on_exit(fn ->
      if Process.whereis(:grpc_transport_test), do: Process.unregister(:grpc_transport_test)
    end)

    assert {:ok, options} = Options.new(target: "localhost:7233")

    assert {:ok, state} =
             GRPCTransport.connect(options, grpc_driver: MetadataDriver)

    calls = [
      {"/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo", GetSystemInfoRequest},
      {"/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
       StartWorkflowExecutionRequest},
      {"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory",
       GetWorkflowExecutionHistoryRequest},
      {"/temporal.api.workflowservice.v1.WorkflowService/SignalWorkflowExecution",
       SignalWorkflowExecutionRequest},
      {"/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution",
       SignalWithStartWorkflowExecutionRequest},
      {"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
       PollWorkflowTaskQueueRequest},
      {"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted",
       RespondWorkflowTaskCompletedRequest},
      {"/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue",
       PollActivityTaskQueueRequest},
      {"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
       RespondActivityTaskCompletedRequest},
      {"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailed",
       RespondActivityTaskFailedRequest}
    ]

    for {method, request_module} <- calls do
      assert {:ok, _bytes} =
               GRPCTransport.unary(
                 state,
                 method,
                 request_module.encode(struct(request_module)),
                 []
               )

      assert_receive {:metadata_call, _rpc,
                      %{
                        "client-name" => "temporal-elixir-community",
                        "client-version" => version
                      }}

      assert version == Mix.Project.config()[:version]
    end

    assert :ok = GRPCTransport.close(state)
  end

  test "merges per-call metadata over SDK and connection metadata" do
    Process.register(self(), :grpc_transport_test)

    on_exit(fn ->
      if Process.whereis(:grpc_transport_test), do: Process.unregister(:grpc_transport_test)
    end)

    assert {:ok, options} = Options.new(target: "localhost:7233")

    assert {:ok, state} =
             GRPCTransport.connect(options, grpc_driver: MetadataDriver)

    request = GetSystemInfoRequest.encode(%GetSystemInfoRequest{})

    assert {:ok, _bytes} =
             GRPCTransport.unary(
               state,
               "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
               request,
               metadata: %{"trace-id" => "abc"}
             )

    assert_receive {:metadata_call, _rpc,
                    %{
                      "client-name" => "temporal-elixir-community",
                      "trace-id" => "abc"
                    }}

    assert :ok = GRPCTransport.close(state)
  end

  test "does not surface the fake grpc-status trailer from mocks" do
    Process.register(self(), :grpc_transport_test)

    on_exit(fn ->
      if Process.whereis(:grpc_transport_test), do: Process.unregister(:grpc_transport_test)
    end)

    assert {:ok, options} = Options.new(target: "localhost:7233")

    assert {:ok, state} =
             GRPCTransport.connect(options, grpc_driver: Driver)

    assert {:ok, bytes} =
             GRPCTransport.unary(
               state,
               "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
               GetSystemInfoRequest.encode(%GetSystemInfoRequest{}),
               []
             )

    assert %GetSystemInfoResponse{server_version: "test"} =
             GetSystemInfoResponse.decode(bytes)

    assert :ok = GRPCTransport.close(state)
  end
end
