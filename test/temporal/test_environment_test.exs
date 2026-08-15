defmodule Temporal.TestEnvironmentAndHooksTest do
  use ExUnit.Case, async: true

  defmodule RecordingInterceptor do
    use Temporal.Interceptor

    @impl true
    def on_call(connection, operation, options) do
      send(self(), {:on_call, connection, operation})
      Keyword.put(options, :intercepted, true)
    end

    @impl true
    def on_response(connection, operation, result, _options) do
      send(self(), {:on_response, connection, operation, result})
      :ok
    end
  end

  setup do
    :ok
  end

  test "TestEnvironment.run_workflow returns a completed workflow result" do
    assert {:ok, "hello Temporal"} =
             Temporal.TestEnvironment.run_workflow(fn name -> "hello #{name}" end, "Temporal")
  end

  test "TestEnvironment.run_workflow supports zero-arity workflows" do
    assert {:ok, "fixed"} = Temporal.TestEnvironment.run_workflow(fn -> "fixed" end, nil)
  end

  test "TestEnvironment reports a blocked workflow when history is exhausted" do
    workflow = fn _input ->
      Temporal.Workflow.sleep(1_000)
      :never
    end

    assert {:error, {:workflow_blocked, :history_exhausted}} =
             Temporal.TestEnvironment.run_workflow(workflow, nil)
  end

  test "TestEnvironment.advance_time completes a workflow that sleeps" do
    workflow = fn _input ->
      Temporal.Workflow.sleep(1_000)
      "done_after_sleep"
    end

    assert {:ok, "done_after_sleep"} = Temporal.TestEnvironment.advance_time(workflow, nil)
  end

  test "TestEnvironment.advance_time resumes after a timer and returns the result" do
    workflow = fn _input ->
      timer = Temporal.Workflow.new_timer(5_000)
      Temporal.Workflow.await(timer)
      "timer-done"
    end

    assert {:ok, "timer-done"} = Temporal.TestEnvironment.advance_time(workflow, nil)
  end

  test "Interceptor hooks fire around on_call/on_response" do
    options = Temporal.Interceptor.on_call(RecordingInterceptor, self(), :start_workflow, [])
    assert options[:intercepted] == true
    assert_receive {:on_call, _, :start_workflow}

    :ok =
      Temporal.Interceptor.on_response(
        RecordingInterceptor,
        self(),
        :start_workflow,
        {:ok, :result},
        []
      )

    assert_receive {:on_response, _, :start_workflow, {:ok, :result}}
  end

  test "Telemetry.time_client emits the client call event" do
    handler = fn event, measurements, metadata, _config ->
      send(self(), {:telemetry, event, measurements, metadata})
    end

    :ok = :telemetry.attach("test-handler", Temporal.Telemetry.client_event(), handler, nil)

    assert {:ok, 42} = Temporal.Telemetry.time_client(:start_workflow, fn -> {:ok, 42} end)

    assert_receive {:telemetry, [:temporal, :client, :call], %{duration: duration},
                    %{
                      operation: :start_workflow,
                      result: :ok
                    }}

    assert is_integer(duration)
    :telemetry.detach("test-handler")
  end
end
