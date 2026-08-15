defmodule Temporal.CloudConformanceTest do
  use ExUnit.Case, async: false

  @moduletag :cloud_live
  @moduletag timeout: 120_000

  # Opt-in via TEMPORAL_CLOUD_ADDRESS (with TEMPORAL_NAMESPACE, TEMPORAL_API_KEY,
  # and optional TEMPORAL_CLOUD_TLS=true). Runs the standard live workflows
  # against a Temporal Cloud namespace to verify TLS/API-key conformance.
  # The README row stays Experimental until a recorded run is captured.

  test "Temporal Cloud: starts and completes a synchronous workflow over TLS" do
    address = System.fetch_env!("TEMPORAL_CLOUD_ADDRESS")

    {:ok, connection} =
      Temporal.Connection.open(
        target: address,
        namespace: System.get_env("TEMPORAL_NAMESPACE", "default"),
        tls: System.get_env("TEMPORAL_CLOUD_TLS", "true") == "true",
        api_key: System.get_env("TEMPORAL_API_KEY")
      )

    on_exit(fn ->
      if Process.alive?(connection), do: Temporal.Connection.close(connection)
    end)

    assert {:ok, system_info} = Temporal.RPC.system_info(connection)
    assert system_info.server_version != ""

    task_queue = "temporal-elixir-cloud-#{System.unique_integer([:positive])}"

    {:ok, worker} =
      Temporal.Worker.start_link(
        connection: connection,
        task_queue: task_queue,
        workflows: %{"Greeting" => fn name -> "hello #{name}" end}
      )

    on_exit(fn ->
      if Process.alive?(worker), do: Temporal.Worker.stop(worker)
    end)

    assert {:ok, "hello Temporal"} =
             Temporal.Client.execute_workflow(connection, "Greeting", "Temporal",
               id: "temporal-elixir-cloud-#{System.unique_integer([:positive])}",
               task_queue: task_queue,
               timeout: 60_000
             )
  end

  test "Temporal Cloud: a workflow with signals and queries completes" do
    address = System.fetch_env!("TEMPORAL_CLOUD_ADDRESS")

    {:ok, connection} =
      Temporal.Connection.open(
        target: address,
        namespace: System.get_env("TEMPORAL_NAMESPACE", "default"),
        tls: System.get_env("TEMPORAL_CLOUD_TLS", "true") == "true",
        api_key: System.get_env("TEMPORAL_API_KEY")
      )

    on_exit(fn ->
      if Process.alive?(connection), do: Temporal.Connection.close(connection)
    end)

    task_queue = "temporal-elixir-cloud-#{System.unique_integer([:positive])}"

    workflow = fn _input ->
      :ok =
        Temporal.Workflow.set_signal_handler("add", fn amount, _ctx, state ->
          {:ok, Map.update(state, "balance", amount, &(&1 + amount))}
        end)

      Temporal.Workflow.set_query_handler("balance", fn ->
        Temporal.Workflow.signal_state()["balance"]
      end)

      state = Temporal.Workflow.await_signal_state(&(Map.get(&1, "balance", 0) >= 10))
      state["balance"]
    end

    {:ok, worker} =
      Temporal.Worker.start_link(
        connection: connection,
        task_queue: task_queue,
        workflows: %{"BalanceWorkflow" => workflow}
      )

    on_exit(fn ->
      if Process.alive?(worker), do: Temporal.Worker.stop(worker)
    end)

    workflow_id = "temporal-elixir-cloud-#{System.unique_integer([:positive])}"

    {:ok, handle} =
      Temporal.Client.start_workflow(connection, "BalanceWorkflow", nil,
        id: workflow_id,
        task_queue: task_queue
      )

    assert :ok = Temporal.Client.signal_workflow(handle, "add", 7)
    assert {:ok, 7} = Temporal.Client.query_workflow(handle, "balance", nil)
    assert :ok = Temporal.Client.signal_workflow(handle, "add", 3)
    assert {:ok, 10} = Temporal.Client.result(handle, timeout: 60_000)
  end
end
