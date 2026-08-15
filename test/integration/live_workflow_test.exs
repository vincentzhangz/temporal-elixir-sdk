defmodule Temporal.LiveWorkflowTest do
  use ExUnit.Case, async: false

  @moduletag :live_server
  @moduletag timeout: 90_000

  test "starts and completes one synchronous workflow against Temporal" do
    address = System.fetch_env!("TEMPORAL_ADDRESS")

    {:ok, connection} =
      Temporal.Connection.open(
        target: address,
        namespace: System.get_env("TEMPORAL_NAMESPACE", "default"),
        tls: System.get_env("TEMPORAL_TLS") == "true",
        api_key: System.get_env("TEMPORAL_API_KEY")
      )

    on_exit(fn ->
      if Process.alive?(connection), do: Temporal.Connection.close(connection)
    end)

    assert {:ok, system_info} = Temporal.RPC.system_info(connection)
    assert system_info.server_version != ""

    task_queue = "temporal-elixir-live-#{System.unique_integer([:positive])}"

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
               id: "temporal-elixir-#{System.unique_integer([:positive])}",
               task_queue: task_queue,
               timeout: 60_000
             )
  end

  test "reports the SDK name and version in WorkflowTaskCompleted events" do
    {connection, task_queue} = live_connection_and_queue("sdk-metadata")

    live_worker(connection, task_queue,
      workflows: %{"Greeting" => fn name -> "hello #{name}" end}
    )

    assert {:ok, handle} =
             Temporal.Client.start_workflow(
               connection,
               "Greeting",
               "Temporal",
               id: live_id("sdk-metadata"),
               task_queue: task_queue
             )

    assert {:ok, "hello Temporal"} = Temporal.Client.result(handle, timeout: 60_000)

    alias Temporal.Api.Common.V1.WorkflowExecution

    alias Temporal.Api.History.V1.{
      HistoryEvent,
      WorkflowTaskCompletedEventAttributes
    }

    alias Temporal.Api.Workflowservice.V1.{
      GetWorkflowExecutionHistoryRequest,
      GetWorkflowExecutionHistoryResponse
    }

    request = %GetWorkflowExecutionHistoryRequest{
      namespace: handle.namespace,
      execution: %WorkflowExecution{
        workflow_id: handle.workflow_id,
        run_id: handle.run_id
      }
    }

    method = "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory"

    assert {:ok, bytes} =
             Temporal.Connection.unary(
               handle.connection,
               method,
               GetWorkflowExecutionHistoryRequest.encode(request),
               timeout: 5_000
             )

    response = GetWorkflowExecutionHistoryResponse.decode(bytes)

    metadata =
      response.history.events
      |> Enum.filter(&(&1.event_type == :EVENT_TYPE_WORKFLOW_TASK_COMPLETED))
      |> Enum.map(fn %HistoryEvent{attributes: attributes} ->
        case attributes do
          {:workflow_task_completed_event_attributes, %WorkflowTaskCompletedEventAttributes{} = a} ->
            a.sdk_metadata

          _ ->
            nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    assert [%{sdk_name: name, sdk_version: version}] = metadata
    assert name == Temporal.SDKMetadata.name()
    assert version == Temporal.SDKMetadata.version()
  end

  test "answers live queries against a running workflow" do
    {connection, task_queue} = live_connection_and_queue("query")

    _worker =
      live_worker(connection, task_queue, workflows: %{"Queryable" => &queryable_workflow/1})

    assert {:ok, handle} =
             Temporal.Client.start_workflow(
               connection,
               "Queryable",
               nil,
               id: live_id("query"),
               task_queue: task_queue
             )

    assert {:ok, "queryable-ready"} =
             Temporal.Client.query_workflow(handle, "get_status", nil)

    assert {:ok, "arg-value"} = Temporal.Client.query_workflow(handle, "echo", "arg-value")

    assert :ok = Temporal.Client.signal_workflow(handle, "finish", nil)

    assert {:ok, "complete"} = Temporal.Client.result(handle, timeout: 60_000)
  end

  defp queryable_workflow(_args) do
    Temporal.Workflow.set_query_handler("get_status", fn _args -> "queryable-ready" end)
    Temporal.Workflow.set_query_handler("echo", fn args -> args end)

    Temporal.Workflow.set_signal_handler("finish", fn _input, _context, _state ->
      {:ok, :complete}
    end)

    Temporal.Workflow.await_signal_state(fn state -> state == :complete end)
    :complete
  end

  test "schedules an Activity and completes with its decoded result" do
    address = System.fetch_env!("TEMPORAL_ADDRESS")

    {:ok, connection} =
      Temporal.Connection.open(
        target: address,
        namespace: System.get_env("TEMPORAL_NAMESPACE", "default"),
        tls: System.get_env("TEMPORAL_TLS") == "true",
        api_key: System.get_env("TEMPORAL_API_KEY")
      )

    on_exit(fn ->
      if Process.alive?(connection), do: Temporal.Connection.close(connection)
    end)

    task_queue = "temporal-elixir-activity-live-#{System.unique_integer([:positive])}"

    {:ok, worker} =
      Temporal.Worker.start_link(
        connection: connection,
        task_queue: task_queue,
        activities: %{"ComposeGreeting" => fn name -> "hello #{name}" end},
        workflows: %{
          "ActivityGreeting" => fn name ->
            Temporal.Workflow.execute_activity("ComposeGreeting", name,
              task_queue: task_queue,
              start_to_close_timeout: 10,
              schedule_to_close_timeout: 30
            )
          end
        }
      )

    on_exit(fn ->
      if Process.alive?(worker), do: Temporal.Worker.stop(worker)
    end)

    assert {:ok, "hello Temporal"} =
             Temporal.Client.execute_workflow(connection, "ActivityGreeting", "Temporal",
               id: "temporal-elixir-activity-#{System.unique_integer([:positive])}",
               task_queue: task_queue,
               timeout: 60_000
             )
  end

  test "follows Continue-As-New runs to the final result" do
    address = System.fetch_env!("TEMPORAL_ADDRESS")

    {:ok, connection} =
      Temporal.Connection.open(
        target: address,
        namespace: System.get_env("TEMPORAL_NAMESPACE", "default"),
        tls: System.get_env("TEMPORAL_TLS") == "true",
        api_key: System.get_env("TEMPORAL_API_KEY")
      )

    on_exit(fn ->
      if Process.alive?(connection), do: Temporal.Connection.close(connection)
    end)

    task_queue = "temporal-elixir-continue-live-#{System.unique_integer([:positive])}"

    workflow = fn %{"remaining" => remaining} = state ->
      if remaining > 0 do
        Temporal.Workflow.continue_as_new(%{state | "remaining" => remaining - 1})
      else
        "continued to completion"
      end
    end

    {:ok, worker} =
      Temporal.Worker.start_link(
        connection: connection,
        task_queue: task_queue,
        workflows: %{"ContinueCounter" => workflow}
      )

    on_exit(fn ->
      if Process.alive?(worker), do: Temporal.Worker.stop(worker)
    end)

    assert {:ok, handle} =
             Temporal.Client.start_workflow(
               connection,
               "ContinueCounter",
               %{"remaining" => 2},
               id: "temporal-elixir-continue-#{System.unique_integer([:positive])}",
               task_queue: task_queue
             )

    assert {:ok, "continued to completion", run_ids} =
             Temporal.Client.result_with_run_chain(handle, timeout: 60_000)

    assert length(run_ids) == 3
    assert length(Enum.uniq(run_ids)) == 3
    assert hd(run_ids) == handle.run_id
  end

  test "resumes a retrying Activity from heartbeat details" do
    {connection, task_queue} = live_connection_and_queue("heartbeat")

    activity = fn _input ->
      case Temporal.Activity.info().attempt do
        1 ->
          Temporal.Activity.heartbeat(%{"offset" => 17})
          raise Temporal.ApplicationError, message: "retry", type: "Transient"

        2 ->
          {:ok, checkpoint} = Temporal.Activity.heartbeat_details()
          checkpoint
      end
    end

    workflow = fn input ->
      Temporal.Workflow.execute_activity("Checkpoint", input,
        task_queue: task_queue,
        start_to_close_timeout: 10,
        heartbeat_timeout: 2,
        retry_policy: [initial_interval: 1, maximum_attempts: 2]
      )
    end

    worker =
      live_worker(connection, task_queue,
        activities: %{"Checkpoint" => activity},
        workflows: %{"CheckpointWorkflow" => workflow}
      )

    assert {:ok, %{"offset" => 17}} =
             Temporal.Client.execute_workflow(connection, "CheckpointWorkflow", nil,
               id: live_id("heartbeat"),
               task_queue: task_queue,
               timeout: 60_000
             )

    assert Process.alive?(worker)
  end

  test "does not retry non-retryable ApplicationError" do
    {connection, task_queue} = live_connection_and_queue("non-retryable")
    {:ok, attempts} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(attempts), do: Agent.stop(attempts) end)

    activity = fn _input ->
      Agent.update(attempts, &(&1 + 1))

      raise Temporal.ApplicationError,
        message: "invalid",
        type: "InvalidInput",
        non_retryable: true
    end

    workflow = fn input ->
      try do
        Temporal.Workflow.execute_activity("Reject", input,
          task_queue: task_queue,
          start_to_close_timeout: 10,
          retry_policy: [initial_interval: 1, maximum_attempts: 5]
        )
      rescue
        error in Temporal.ActivityError -> [error.cause.type, error.retry_state]
      end
    end

    live_worker(connection, task_queue,
      activities: %{"Reject" => activity},
      workflows: %{"RejectWorkflow" => workflow}
    )

    assert {:ok, ["InvalidInput", "RETRY_STATE_NON_RETRYABLE_FAILURE"]} =
             Temporal.Client.execute_workflow(connection, "RejectWorkflow", nil,
               id: live_id("non-retryable"),
               task_queue: task_queue,
               timeout: 60_000
             )

    assert Agent.get(attempts, & &1) == 1
  end

  test "reports retry exhaustion after the configured attempts" do
    {connection, task_queue} = live_connection_and_queue("retry-exhaustion")
    {:ok, attempts} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(attempts), do: Agent.stop(attempts) end)

    activity = fn _input ->
      Agent.update(attempts, &(&1 + 1))
      raise Temporal.ApplicationError, message: "still failing", type: "Transient"
    end

    workflow = fn input ->
      try do
        Temporal.Workflow.execute_activity("AlwaysFails", input,
          task_queue: task_queue,
          start_to_close_timeout: 10,
          retry_policy: [initial_interval: 1, maximum_attempts: 3]
        )
      rescue
        error in Temporal.ActivityError -> error.retry_state
      end
    end

    live_worker(connection, task_queue,
      activities: %{"AlwaysFails" => activity},
      workflows: %{"RetryWorkflow" => workflow}
    )

    assert {:ok, "RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED"} =
             Temporal.Client.execute_workflow(connection, "RetryWorkflow", nil,
               id: live_id("retry-exhaustion"),
               task_queue: task_queue,
               timeout: 60_000
             )

    assert Agent.get(attempts, & &1) == 3
  end

  test "executes three sequential Activities in command order" do
    {connection, task_queue} = live_connection_and_queue("sequential")

    workflow = fn input ->
      Enum.reduce(1..3, input, fn _step, value ->
        Temporal.Workflow.execute_activity("Append", value,
          task_queue: task_queue,
          start_to_close_timeout: 10
        )
      end)
    end

    live_worker(connection, task_queue,
      activities: %{"Append" => fn value -> value <> "!" end},
      workflows: %{"SequentialWorkflow" => workflow}
    )

    assert {:ok, "start!!!"} =
             Temporal.Client.execute_workflow(connection, "SequentialWorkflow", "start",
               id: live_id("sequential"),
               task_queue: task_queue,
               timeout: 60_000
             )
  end

  test "reports a nondeterministic replay by failing the Workflow Task" do
    {connection, task_queue} = live_connection_and_queue("nondeterminism")
    {:ok, executions} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(executions), do: Agent.stop(executions) end)

    workflow = fn input ->
      activity_type =
        Agent.get_and_update(executions, fn
          0 -> {"FirstActivity", 1}
          count -> {"ChangedActivity", count + 1}
        end)

      Temporal.Workflow.execute_activity(activity_type, input,
        task_queue: task_queue,
        start_to_close_timeout: 10
      )
    end

    live_worker(connection, task_queue,
      activities: %{"FirstActivity" => fn value -> value end},
      workflows: %{"NondeterministicWorkflow" => workflow}
    )

    assert {:ok, handle} =
             Temporal.Client.start_workflow(connection, "NondeterministicWorkflow", "input",
               id: live_id("nondeterminism"),
               task_queue: task_queue
             )

    assert {:ok, event} = wait_for_workflow_task_failure(handle, 100)
    assert event.event_type == :EVENT_TYPE_WORKFLOW_TASK_FAILED

    {:workflow_task_failed_event_attributes, attributes} = event.attributes
    assert attributes.cause == :WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR
  end

  test "server retries after one intentional Workflow Task failure" do
    {connection, task_queue} = live_connection_and_queue("workflow-task-retry")
    {:ok, executions} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(executions), do: Agent.stop(executions) end)

    workflow = fn _input ->
      case Agent.get_and_update(executions, fn count -> {count, count + 1} end) do
        0 -> raise "intentional Workflow Task failure"
        _attempt -> "recovered"
      end
    end

    live_worker(connection, task_queue, workflows: %{"RetryWorkflowTask" => workflow})

    assert {:ok, "recovered"} =
             Temporal.Client.execute_workflow(connection, "RetryWorkflowTask", nil,
               id: live_id("workflow-task-retry"),
               task_queue: task_queue,
               timeout: 60_000
             )

    assert Agent.get(executions, & &1) >= 2
  end

  test "acknowledges cancellation of a running heartbeating Activity" do
    {connection, task_queue} = live_connection_and_queue("cancel")
    parent = self()

    activity = fn _input ->
      send(parent, :activity_started)

      Enum.each(1..100, fn heartbeat ->
        Temporal.Activity.heartbeat(%{"heartbeat" => heartbeat})
        Process.sleep(50)
      end)

      "not canceled"
    end

    workflow = fn input ->
      try do
        Temporal.Workflow.execute_activity("Cancelable", input,
          task_queue: task_queue,
          start_to_close_timeout: 15,
          heartbeat_timeout: 2
        )
      rescue
        error in Temporal.ActivityError ->
          %Temporal.CanceledError{} = error.cause
          "cancellation acknowledged"
      end
    end

    live_worker(connection, task_queue,
      activities: %{"Cancelable" => activity},
      workflows: %{"CancelWorkflow" => workflow}
    )

    assert {:ok, handle} =
             Temporal.Client.start_workflow(connection, "CancelWorkflow", nil,
               id: live_id("cancel"),
               task_queue: task_queue
             )

    assert_receive :activity_started, 10_000
    assert :ok = Temporal.Client.cancel_workflow(handle)
    assert {:ok, "cancellation acknowledged"} = Temporal.Client.result(handle, timeout: 60_000)
  end

  test "runs durable sleep and local zero or negative timer edges" do
    {connection, task_queue} = live_connection_and_queue("timers-basic")

    workflow = fn duration ->
      try do
        :ok = Temporal.Workflow.sleep(duration, summary: "live timer edge")
        ["slept", duration]
      rescue
        error in ArgumentError -> ["error", error.message]
      end
    end

    live_worker(connection, task_queue, workflows: %{"TimerEdges" => workflow})

    for {duration, expected} <- [
          {25, ["slept", 25]},
          {0, ["slept", 0]},
          {-1, ["error", "timer duration must not be negative"]}
        ] do
      assert {:ok, ^expected} =
               Temporal.Client.execute_workflow(connection, "TimerEdges", duration,
                 id: live_id("timer-edge"),
                 task_queue: task_queue,
                 timeout: 60_000
               )
    end
  end

  test "cancels a started timer after a beside Activity" do
    {connection, task_queue} = live_connection_and_queue("timer-cancel")

    workflow = fn input ->
      timer = Temporal.Workflow.new_timer(5_000)

      Temporal.Workflow.execute_activity("Pause", input,
        task_queue: task_queue,
        start_to_close_timeout: 10
      )

      :ok = Temporal.Workflow.cancel_timer(timer)

      try do
        Temporal.Workflow.await(timer)
      rescue
        Temporal.CanceledError -> "timer canceled"
      end
    end

    live_worker(connection, task_queue,
      activities: %{
        "Pause" => fn value ->
          Process.sleep(25)
          value
        end
      },
      workflows: %{"CancelableTimer" => workflow}
    )

    assert {:ok, "timer canceled"} =
             Temporal.Client.execute_workflow(connection, "CancelableTimer", "value",
               id: live_id("timer-cancel"),
               task_queue: task_queue,
               timeout: 60_000
             )
  end

  test "runs timers after and beside Activities and wakes multiple timers deterministically" do
    {connection, task_queue} = live_connection_and_queue("timer-order")

    workflow = fn _input ->
      beside = Temporal.Workflow.new_timer(20)

      value =
        Temporal.Workflow.execute_activity("Echo", "activity",
          task_queue: task_queue,
          start_to_close_timeout: 10
        )

      :ok = Temporal.Workflow.await(beside)
      :ok = Temporal.Workflow.sleep(10)
      slow = Temporal.Workflow.new_timer(30)
      fast = Temporal.Workflow.new_timer(5)
      :ok = Temporal.Workflow.await(fast)
      :ok = Temporal.Workflow.await(slow)
      value <> "-timers"
    end

    live_worker(connection, task_queue,
      activities: %{"Echo" => & &1},
      workflows: %{"TimerOrder" => workflow}
    )

    assert {:ok, "activity-timers"} =
             Temporal.Client.execute_workflow(connection, "TimerOrder", nil,
               id: live_id("timer-order"),
               task_queue: task_queue,
               timeout: 60_000
             )
  end

  test "continues as new after a durable timer" do
    {connection, task_queue} = live_connection_and_queue("timer-continue")

    workflow = fn remaining ->
      :ok = Temporal.Workflow.sleep(10)

      if remaining > 0 do
        Temporal.Workflow.continue_as_new(remaining - 1)
      else
        "timer chain complete"
      end
    end

    live_worker(connection, task_queue, workflows: %{"TimerContinue" => workflow})

    assert {:ok, handle} =
             Temporal.Client.start_workflow(connection, "TimerContinue", 1,
               id: live_id("timer-continue"),
               task_queue: task_queue
             )

    assert {:ok, "timer chain complete", run_ids} =
             Temporal.Client.result_with_run_chain(handle, timeout: 60_000)

    assert length(run_ids) == 2
  end

  defp live_connection_and_queue(prefix) do
    {:ok, connection} =
      Temporal.Connection.open(
        target: System.fetch_env!("TEMPORAL_ADDRESS"),
        namespace: System.get_env("TEMPORAL_NAMESPACE", "default"),
        tls: System.get_env("TEMPORAL_TLS") == "true",
        api_key: System.get_env("TEMPORAL_API_KEY")
      )

    on_exit(fn ->
      if Process.alive?(connection), do: Temporal.Connection.close(connection)
    end)

    {connection, "temporal-elixir-#{prefix}-#{unique_suffix()}"}
  end

  defp live_worker(connection, task_queue, options) do
    {:ok, worker} =
      Temporal.Worker.start_link(
        Keyword.merge(options, connection: connection, task_queue: task_queue)
      )

    on_exit(fn -> if Process.alive?(worker), do: Temporal.Worker.stop(worker) end)
    worker
  end

  defp live_id(prefix), do: "temporal-elixir-#{prefix}-#{unique_suffix()}"

  defp unique_suffix do
    "#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp wait_for_workflow_task_failure(_handle, 0), do: {:error, :timeout}

  defp wait_for_workflow_task_failure(handle, attempts) do
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
      history_event_filter_type: :HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
    }

    method = "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory"

    with {:ok, bytes} <-
           Temporal.Connection.unary(
             handle.connection,
             method,
             GetWorkflowExecutionHistoryRequest.encode(request),
             timeout: 5_000
           ) do
      response = GetWorkflowExecutionHistoryResponse.decode(bytes)

      case workflow_task_failure(response.history.events) do
        nil ->
          Process.sleep(50)
          wait_for_workflow_task_failure(handle, attempts - 1)

        event ->
          {:ok, event}
      end
    end
  end

  defp workflow_task_failure(events) do
    Enum.find(events, &(&1.event_type == :EVENT_TYPE_WORKFLOW_TASK_FAILED))
  end
end
