defmodule Temporal.LiveSignalTest do
  use ExUnit.Case, async: false

  @moduletag :live_server
  @moduletag timeout: 120_000

  test "delivers signals after registration in FIFO order and deduplicates request IDs" do
    {connection, task_queue} = live_connection_and_queue("fifo")

    workflow = fn _input ->
      :ok =
        Temporal.Workflow.set_signal_handler("append", fn value, _context, state ->
          {:ok, Map.update(state, "seen", [value], &(&1 ++ [value]))}
        end)

      :ok =
        Temporal.Workflow.set_dynamic_signal_handler(fn value, context, state ->
          entry = %{"name" => context.signal_name, "value" => value}
          {:ok, Map.update(state, "seen", [entry], &(&1 ++ [entry]))}
        end)

      state = Temporal.Workflow.await_signal_state(&(length(Map.get(&1, "seen", [])) == 4))
      state["seen"]
    end

    live_worker(connection, task_queue, workflows: %{"SignalFIFO" => workflow})
    handle = start(connection, task_queue, "SignalFIFO", "fifo")

    assert :ok = Temporal.Client.signal_workflow(handle, "append", 1, request_id: "fifo-1")
    assert :ok = Temporal.Client.signal_workflow(handle, "append", 2, request_id: "fifo-2")
    assert :ok = Temporal.Client.signal_workflow(handle, "append", 99, request_id: "fifo-2")
    assert :ok = Temporal.Client.signal_workflow(handle, "append", 3, request_id: "fifo-3")
    assert :ok = Temporal.Client.signal_workflow(handle, "unknown", 4, request_id: "fifo-4")

    assert {:ok, [1, 2, 3, %{"name" => "unknown", "value" => 4}]} =
             Temporal.Client.result(handle, timeout: 60_000)
  end

  test "buffers a signal until registration after a durable timer" do
    {connection, task_queue} = live_connection_and_queue("buffer")

    workflow = fn _input ->
      :ok = Temporal.Workflow.sleep(100)

      :ok =
        Temporal.Workflow.set_signal_handler("finish", fn value, context, _state ->
          {:ok, %{"value" => value, "identity" => context.identity}}
        end)

      Temporal.Workflow.await_signal_state(&Map.has_key?(&1, "value"))
    end

    live_worker(connection, task_queue, workflows: %{"BufferedSignal" => workflow})
    handle = start(connection, task_queue, "BufferedSignal", nil)

    assert :ok = Temporal.Client.signal_workflow(handle, "finish", "ready")

    assert {:ok, %{"value" => "ready", "identity" => identity}} =
             Temporal.Client.result(handle, timeout: 60_000)

    assert is_binary(identity) and identity != ""
  end

  test "Signal-With-Start atomically starts and signals a workflow" do
    {connection, task_queue} = live_connection_and_queue("with-start")

    workflow = fn initial ->
      :ok =
        Temporal.Workflow.set_signal_handler("set", fn value, _context, _state ->
          {:ok, %{"initial" => initial, "signal" => value}}
        end)

      Temporal.Workflow.await_signal_state(&Map.has_key?(&1, "signal"))
    end

    live_worker(connection, task_queue, workflows: %{"SignalWithStart" => workflow})

    assert {:ok, handle} =
             Temporal.Client.signal_with_start(
               connection,
               "SignalWithStart",
               "workflow-input",
               "set",
               "signal-input",
               id: live_id("with-start"),
               task_queue: task_queue,
               request_id: "sws-#{unique_suffix()}"
             )

    assert {:ok, %{"initial" => "workflow-input", "signal" => "signal-input"}} =
             Temporal.Client.result(handle, timeout: 60_000)
  end

  test "a signal handler can block deterministically on a timer and Activity" do
    {connection, task_queue} = live_connection_and_queue("blocking")

    workflow = fn _input ->
      :ok =
        Temporal.Workflow.set_signal_handler("run", fn value, _context, _state ->
          :ok = Temporal.Workflow.sleep(20)

          result =
            Temporal.Workflow.execute_activity("Echo", value,
              task_queue: task_queue,
              start_to_close_timeout: 10
            )

          {:ok, %{"result" => result}}
        end)

      Temporal.Workflow.await_signal_state(&Map.has_key?(&1, "result"))
    end

    live_worker(connection, task_queue,
      activities: %{"Echo" => fn value -> "echo-#{value}" end},
      workflows: %{"BlockingSignal" => workflow}
    )

    handle = start(connection, task_queue, "BlockingSignal", nil)
    assert :ok = Temporal.Client.signal_workflow(handle, "run", "value")
    assert {:ok, %{"result" => "echo-value"}} = Temporal.Client.result(handle, timeout: 60_000)
  end

  test "routes a signal to the new run after Continue-As-New with explicit carry-forward" do
    {connection, task_queue} = live_connection_and_queue("continue")

    workflow = fn %{"generation" => generation, "carried" => carried} ->
      :ok =
        Temporal.Workflow.set_signal_handler("append", fn value, _context, state ->
          seen = Map.get(state, "seen", carried)
          {:ok, Map.put(state, "seen", seen ++ [value])}
        end)

      state =
        Temporal.Workflow.await_signal_state(
          &(length(Map.get(&1, "seen", carried)) > length(carried))
        )

      if generation == 0 do
        # Temporal does not transfer buffered signals to the new run. Carry the
        # application-level state explicitly in Continue-As-New input.
        Temporal.Workflow.continue_as_new(%{"generation" => 1, "carried" => state["seen"]})
      else
        state["seen"]
      end
    end

    live_worker(connection, task_queue, workflows: %{"ContinueSignals" => workflow})

    handle =
      start(connection, task_queue, "ContinueSignals", %{"generation" => 0, "carried" => []})

    assert :ok = Temporal.Client.signal_workflow(handle, "append", "first")
    assert {:ok, new_run_id} = continued_run(handle)
    new_handle = %{handle | run_id: new_run_id}
    assert :ok = Temporal.Client.signal_workflow(new_handle, "append", "second")

    assert {:ok, ["first", "second"], [first_run, ^new_run_id]} =
             Temporal.Client.result_with_run_chain(handle, timeout: 60_000)

    assert first_run == handle.run_id
  end

  defp live_connection_and_queue(prefix) do
    {:ok, connection} =
      Temporal.Connection.open(
        target: System.get_env("TEMPORAL_ADDRESS", "127.0.0.1:7233"),
        namespace: System.get_env("TEMPORAL_NAMESPACE", "default"),
        tls: System.get_env("TEMPORAL_TLS") == "true",
        api_key: System.get_env("TEMPORAL_API_KEY")
      )

    on_exit(fn ->
      if Process.alive?(connection), do: Temporal.Connection.close(connection)
    end)

    {connection, "temporal-elixir-signal-#{prefix}-#{unique_suffix()}"}
  end

  defp live_worker(connection, task_queue, options) do
    {:ok, worker} =
      Temporal.Worker.start_link(
        Keyword.merge(options, connection: connection, task_queue: task_queue)
      )

    on_exit(fn -> if Process.alive?(worker), do: Temporal.Worker.stop(worker) end)
    worker
  end

  defp start(connection, task_queue, workflow, input) do
    assert {:ok, handle} =
             Temporal.Client.start_workflow(connection, workflow, input,
               id: live_id(workflow),
               task_queue: task_queue
             )

    handle
  end

  defp continued_run(handle) do
    alias Temporal.Api.Common.V1.WorkflowExecution

    alias Temporal.Api.Workflowservice.V1.{
      GetWorkflowExecutionHistoryRequest,
      GetWorkflowExecutionHistoryResponse
    }

    request = %GetWorkflowExecutionHistoryRequest{
      namespace: handle.namespace,
      execution: %WorkflowExecution{
        workflow_id: handle.workflow_id,
        run_id: handle.run_id
      },
      wait_new_event: true,
      history_event_filter_type: :HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
    }

    method = "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory"

    with {:ok, bytes} <-
           Temporal.Connection.unary(
             handle.connection,
             method,
             GetWorkflowExecutionHistoryRequest.encode(request),
             timeout: 60_000
           ) do
      response = GetWorkflowExecutionHistoryResponse.decode(bytes)

      Enum.find_value(response.history.events, {:error, :not_continued}, fn
        %{
          attributes:
            {:workflow_execution_continued_as_new_event_attributes,
             %{new_execution_run_id: run_id}}
        } ->
          {:ok, run_id}

        _event ->
          false
      end)
    end
  end

  defp live_id(prefix), do: "temporal-elixir-signal-#{prefix}-#{unique_suffix()}"

  defp unique_suffix do
    "#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive, :monotonic])}"
  end
end
