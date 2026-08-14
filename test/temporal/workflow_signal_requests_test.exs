defmodule Temporal.WorkflowSignalRequestsTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{Header, Payload}

  alias Temporal.Api.Workflowservice.V1.{
    SignalWithStartWorkflowExecutionRequest,
    SignalWorkflowExecutionRequest
  }

  alias Temporal.Workflow.Signal.Requests

  test "builds SignalWorkflowExecution with execution identity headers and encoded input" do
    header = %Header{fields: %{"trace" => %Payload{data: "abc"}}}

    assert {:ok, %SignalWorkflowExecutionRequest{} = request} =
             Requests.signal_workflow(
               namespace: "payments",
               identity: "elixir-client",
               workflow_id: "account-1",
               run_id: "",
               signal_name: "deposit",
               input: 42,
               request_id: "signal-request-1",
               header: header
             )

    assert request.workflow_execution.workflow_id == "account-1"
    assert request.workflow_execution.run_id == ""
    assert request.signal_name == "deposit"
    assert request.request_id == "signal-request-1"
    assert request.header == header
    assert {:ok, 42} = Temporal.Payload.decode(request.input)
  end

  test "supports an encoder hook and validates its result" do
    payloads = Temporal.Payload.encode("custom")

    assert {:ok, request} =
             Requests.signal_workflow(
               base_signal_options(encoder: fn :argument -> {:ok, payloads} end)
             )

    assert request.input == payloads

    assert {:error, {:payload_conversion_failed, :bad_input}} =
             Requests.signal_workflow(
               base_signal_options(encoder: fn _value -> {:error, :bad_input} end)
             )
  end

  test "requires non-empty signal request fields while allowing an empty run ID" do
    for key <- [:namespace, :identity, :workflow_id, :signal_name, :request_id] do
      options = Keyword.put(base_signal_options(), key, "")
      assert {:error, {:invalid_option, ^key, ""}} = Requests.signal_workflow(options)
    end

    assert {:ok, request} =
             base_signal_options()
             |> Keyword.put(:run_id, "")
             |> Requests.signal_workflow()

    assert request.workflow_execution.run_id == ""
  end

  test "builds SignalWithStartWorkflowExecution atomically from start and signal inputs" do
    header = %Header{fields: %{"trace" => %Payload{data: "xyz"}}}

    assert {:ok, %SignalWithStartWorkflowExecutionRequest{} = request} =
             Requests.signal_with_start(
               namespace: "payments",
               identity: "elixir-client",
               workflow_id: "account-1",
               workflow_type: "AccountWorkflow",
               task_queue: "accounts",
               workflow_input: %{balance: 0},
               signal_name: "deposit",
               signal_input: 42,
               request_id: "signal-with-start-1",
               header: header
             )

    assert request.workflow_type.name == "AccountWorkflow"
    assert request.task_queue.name == "accounts"
    assert request.signal_name == "deposit"
    assert request.header == header
    assert {:ok, %{"balance" => 0}} = Temporal.Payload.decode(request.input)
    assert {:ok, 42} = Temporal.Payload.decode(request.signal_input)
  end

  test "validates SignalWithStart required fields and independently converts both payloads" do
    base = [
      namespace: "payments",
      identity: "elixir-client",
      workflow_id: "account-1",
      workflow_type: "AccountWorkflow",
      task_queue: "accounts",
      signal_name: "deposit",
      request_id: "signal-with-start-1"
    ]

    for key <- [
          :namespace,
          :identity,
          :workflow_id,
          :workflow_type,
          :task_queue,
          :signal_name,
          :request_id
        ] do
      assert {:error, {:invalid_option, ^key, ""}} =
               base
               |> Keyword.put(key, "")
               |> Requests.signal_with_start()
    end

    encoder = fn
      :workflow -> {:ok, Temporal.Payload.encode("workflow")}
      :signal -> {:error, :malformed_signal}
    end

    assert {:error, {:payload_conversion_failed, :malformed_signal}} =
             base
             |> Keyword.merge(
               workflow_input: :workflow,
               signal_input: :signal,
               encoder: encoder
             )
             |> Requests.signal_with_start()
  end

  defp base_signal_options(extra \\ []) do
    Keyword.merge(
      [
        namespace: "payments",
        identity: "elixir-client",
        workflow_id: "account-1",
        run_id: "",
        signal_name: "deposit",
        input: :argument,
        request_id: "signal-request-1"
      ],
      extra
    )
  end
end
