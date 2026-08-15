defmodule Temporal.WorkerTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Sdk.V1.WorkflowTaskCompletedMetadata
  alias Temporal.Api.Taskqueue.V1.StickyExecutionAttributes

  alias Temporal.Api.Workflowservice.V1.{
    PollWorkflowTaskQueueRequest,
    PollWorkflowTaskQueueResponse,
    RespondWorkflowTaskCompletedRequest,
    RespondWorkflowTaskCompletedResponse
  }

  defmodule WorkerTransport do
    @behaviour Temporal.RPC.Transport

    def unary({test_pid, agent}, method, bytes, _options) do
      case method do
        "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue" ->
          send(test_pid, {:poll, PollWorkflowTaskQueueRequest.decode(bytes)})

          Agent.get_and_update(agent, fn
            :ready ->
              task = %PollWorkflowTaskQueueResponse{
                task_token: "owned-token",
                workflow_execution: %WorkflowExecution{
                  workflow_id: "workflow-id",
                  run_id: "run-id"
                },
                workflow_type: %WorkflowType{name: "Greeting"},
                started_event_id: 3,
                history: %History{
                  events: [
                    %HistoryEvent{
                      event_id: 1,
                      event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
                      attributes:
                        {:workflow_execution_started_event_attributes,
                         %WorkflowExecutionStartedEventAttributes{
                           workflow_id: "workflow-id",
                           workflow_type: %WorkflowType{name: "Greeting"},
                           input: Temporal.Payload.encode("Temporal")
                         }}
                    },
                    %HistoryEvent{
                      event_id: 2,
                      event_type: :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
                      attributes:
                        {:workflow_task_scheduled_event_attributes,
                         %WorkflowTaskScheduledEventAttributes{attempt: 1}}
                    },
                    %HistoryEvent{
                      event_id: 3,
                      event_type: :EVENT_TYPE_WORKFLOW_TASK_STARTED,
                      attributes:
                        {:workflow_task_started_event_attributes,
                         %WorkflowTaskStartedEventAttributes{scheduled_event_id: 2}}
                    }
                  ]
                }
              }

              {{:ok, PollWorkflowTaskQueueResponse.encode(task)}, :empty}

            :empty ->
              {{:ok, PollWorkflowTaskQueueResponse.encode(%PollWorkflowTaskQueueResponse{})},
               :empty}
          end)

        "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted" ->
          completion = RespondWorkflowTaskCompletedRequest.decode(bytes)
          send(test_pid, {:completed, completion})

          {:ok,
           RespondWorkflowTaskCompletedResponse.encode(%RespondWorkflowTaskCompletedResponse{})}
      end
    end
  end

  defmodule FailureRecoveryTransport do
    @behaviour Temporal.RPC.Transport

    alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

    alias Temporal.Api.History.V1.{
      History,
      HistoryEvent,
      WorkflowExecutionStartedEventAttributes,
      WorkflowTaskScheduledEventAttributes,
      WorkflowTaskStartedEventAttributes
    }

    alias Temporal.Api.Workflowservice.V1.{
      PollWorkflowTaskQueueResponse,
      RespondWorkflowTaskCompletedResponse
    }

    def unary({test_pid, agent}, method, _bytes, _options) do
      case method do
        "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue" ->
          Agent.get_and_update(agent, fn
            :first_poll ->
              {{:ok, PollWorkflowTaskQueueResponse.encode(task())}, :first_completion}

            :retry_poll ->
              {{:ok, PollWorkflowTaskQueueResponse.encode(task())}, :second_completion}

            state ->
              {{:ok, PollWorkflowTaskQueueResponse.encode(%PollWorkflowTaskQueueResponse{})},
               state}
          end)

        "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted" ->
          Agent.get_and_update(agent, fn
            :first_completion ->
              send(test_pid, {:completion_attempt, :failed})
              {{:error, :simulated_completion_failure}, :retry_poll}

            :second_completion ->
              send(test_pid, {:completion_attempt, :succeeded})

              {{:ok,
                RespondWorkflowTaskCompletedResponse.encode(
                  %RespondWorkflowTaskCompletedResponse{}
                )}, :empty}
          end)
      end
    end

    defp task do
      %PollWorkflowTaskQueueResponse{
        task_token: "retry-token",
        workflow_execution: %WorkflowExecution{workflow_id: "retry-workflow", run_id: "retry-run"},
        workflow_type: %WorkflowType{name: "Greeting"},
        started_event_id: 3,
        history: %History{
          events: [
            %HistoryEvent{
              event_id: 1,
              event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
              attributes:
                {:workflow_execution_started_event_attributes,
                 %WorkflowExecutionStartedEventAttributes{
                   workflow_id: "retry-workflow",
                   workflow_type: %WorkflowType{name: "Greeting"},
                   input: Temporal.Payload.encode("Temporal")
                 }}
            },
            %HistoryEvent{
              event_id: 2,
              event_type: :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
              attributes:
                {:workflow_task_scheduled_event_attributes,
                 %WorkflowTaskScheduledEventAttributes{attempt: 1}}
            },
            %HistoryEvent{
              event_id: 3,
              event_type: :EVENT_TYPE_WORKFLOW_TASK_STARTED,
              attributes:
                {:workflow_task_started_event_attributes,
                 %WorkflowTaskStartedEventAttributes{scheduled_event_id: 2}}
            }
          ]
        }
      }
    end
  end

  test "bounded poll dispatches once and graceful shutdown stops further polls" do
    {:ok, agent} = Agent.start_link(fn -> :ready end)

    {:ok, connection} =
      Temporal.Connection.open(
        transport: WorkerTransport,
        transport_state: {self(), agent},
        namespace: "default",
        identity: "worker-test"
      )

    {:ok, worker} =
      Temporal.Worker.start_link(
        connection: connection,
        task_queue: "elixir",
        workflows: %{"Greeting" => fn name -> "hello #{name}" end},
        poll_timeout: 100
      )

    assert_receive {:poll, %PollWorkflowTaskQueueRequest{identity: "worker-test"}}

    assert_receive {:completed,
                    %RespondWorkflowTaskCompletedRequest{
                      task_token: "owned-token",
                      sdk_metadata: %WorkflowTaskCompletedMetadata{
                        sdk_name: "temporal-elixir-community",
                        sdk_version: version
                      }
                    }}

    assert is_binary(version) and version != ""

    assert :ok = Temporal.Worker.stop(worker)
    refute Process.alive?(worker)
    Temporal.Connection.close(connection)
  end

  test "with use_versioning and build_id, polls carry capabilities and completions carry the stamp" do
    {:ok, agent} = Agent.start_link(fn -> :ready end)

    {:ok, connection} =
      Temporal.Connection.open(
        transport: WorkerTransport,
        transport_state: {self(), agent},
        namespace: "default",
        identity: "worker-test"
      )

    {:ok, worker} =
      Temporal.Worker.start_link(
        connection: connection,
        task_queue: "elixir",
        workflows: %{"Greeting" => fn name -> "hello #{name}" end},
        poll_timeout: 100,
        build_id: "build-1.0",
        use_versioning: true
      )

    assert_receive {:poll,
                    %PollWorkflowTaskQueueRequest{
                      worker_version_capabilities:
                        %Temporal.Api.Common.V1.WorkerVersionCapabilities{
                          build_id: "build-1.0",
                          use_versioning: true
                        }
                    }}

    assert_receive {:completed,
                    %RespondWorkflowTaskCompletedRequest{
                      worker_version_stamp: %Temporal.Api.Common.V1.WorkerVersionStamp{
                        build_id: "build-1.0",
                        use_versioning: true
                      }
                    }}

    Temporal.Worker.stop(worker)
    Temporal.Connection.close(connection)
  end

  test "evicts failed run state so the same task can be safely redelivered" do
    {:ok, agent} = Agent.start_link(fn -> :first_poll end)

    {:ok, connection} =
      Temporal.Connection.open(
        transport: FailureRecoveryTransport,
        transport_state: {self(), agent},
        namespace: "default",
        identity: "worker-test"
      )

    {:ok, worker} =
      Temporal.Worker.start_link(
        connection: connection,
        task_queue: "elixir",
        workflows: %{"Greeting" => fn name -> "hello #{name}" end},
        poll_timeout: 100,
        run_state_ttl: 60_000
      )

    assert_receive {:completion_attempt, :failed}
    assert_receive {:completion_attempt, :succeeded}

    # The concurrent task processing merges the run back asynchronously.
    state = wait_for_run(worker, {"default", "retry-run"})
    assert Map.has_key?(state.runs, {"default", "retry-run"})

    send(worker, {:evict_run, {"default", "retry-run"}, "retry-token"})
    assert %{runs: %{}} = :sys.get_state(worker)

    Temporal.Worker.stop(worker)
    Temporal.Connection.close(connection)
  end

  defp wait_for_run(worker, key, attempts \\ 100) do
    state = :sys.get_state(worker)

    if Map.has_key?(state.runs, key) or attempts == 0 do
      state
    else
      Process.sleep(10)
      wait_for_run(worker, key, attempts - 1)
    end
  end

  defmodule StickyTransport do
    @behaviour Temporal.RPC.Transport

    def unary({test_pid, agent}, method, bytes, _options) do
      case method do
        "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue" ->
          request = PollWorkflowTaskQueueRequest.decode(bytes)
          send(test_pid, {:poll, request})

          Agent.get_and_update(agent, fn state ->
            sticky_response(state, request)
          end)

        "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted" ->
          completion = RespondWorkflowTaskCompletedRequest.decode(bytes)
          send(test_pid, {:completed, completion})

          {:ok,
           RespondWorkflowTaskCompletedResponse.encode(%RespondWorkflowTaskCompletedResponse{})}
      end
    end

    defp sticky_response(:sticky, %{task_queue: %{kind: :TASK_QUEUE_KIND_STICKY}}) do
      # Sticky poll is empty; fall through to normal.
      {{:ok, PollWorkflowTaskQueueResponse.encode(%PollWorkflowTaskQueueResponse{})}, :deliver}
    end

    defp sticky_response(:deliver, _request) do
      # First normal (non-sticky) poll delivers the task.
      {{:ok, PollWorkflowTaskQueueResponse.encode(sticky_task())}, :done}
    end

    defp sticky_response(_state, _request) do
      {{:ok, PollWorkflowTaskQueueResponse.encode(%PollWorkflowTaskQueueResponse{})}, :done}
    end

    defp sticky_task do
      %PollWorkflowTaskQueueResponse{
        task_token: "sticky-token",
        workflow_execution: %WorkflowExecution{workflow_id: "workflow-id", run_id: "run-id"},
        workflow_type: %WorkflowType{name: "Greeting"},
        started_event_id: 3,
        history: %History{
          events: [
            %HistoryEvent{
              event_id: 1,
              event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
              attributes:
                {:workflow_execution_started_event_attributes,
                 %WorkflowExecutionStartedEventAttributes{
                   workflow_id: "workflow-id",
                   workflow_type: %WorkflowType{name: "Greeting"},
                   input: Temporal.Payload.encode("Temporal")
                 }}
            },
            %HistoryEvent{
              event_id: 2,
              event_type: :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
              attributes:
                {:workflow_task_scheduled_event_attributes,
                 %WorkflowTaskScheduledEventAttributes{attempt: 1}}
            },
            %HistoryEvent{
              event_id: 3,
              event_type: :EVENT_TYPE_WORKFLOW_TASK_STARTED,
              attributes:
                {:workflow_task_started_event_attributes,
                 %WorkflowTaskStartedEventAttributes{scheduled_event_id: 2}}
            }
          ]
        }
      }
    end
  end

  test "with sticky enabled, polls the sticky queue first, then normal, and attaches sticky attributes" do
    {:ok, agent} = Agent.start_link(fn -> :sticky end)

    {:ok, connection} =
      Temporal.Connection.open(
        transport: StickyTransport,
        transport_state: {self(), agent},
        namespace: "default",
        identity: "worker-test"
      )

    {:ok, worker} =
      Temporal.Worker.start_link(
        connection: connection,
        task_queue: "elixir",
        workflows: %{"Greeting" => fn name -> "hello #{name}" end},
        poll_timeout: 100,
        sticky: true
      )

    # First: the sticky queue poll.
    assert_receive {:poll, %PollWorkflowTaskQueueRequest{task_queue: %{kind: kind}}} = _first
    assert kind == :TASK_QUEUE_KIND_STICKY

    # Then: the normal queue poll (no sticky kind).
    assert_receive {:poll, %PollWorkflowTaskQueueRequest{task_queue: %{name: "elixir"}}}

    # The completion carries sticky_attributes pointing at the worker's sticky queue.
    assert_receive {:completed,
                    %RespondWorkflowTaskCompletedRequest{
                      sticky_attributes: %StickyExecutionAttributes{
                        worker_task_queue: %{kind: :TASK_QUEUE_KIND_STICKY}
                      }
                    }}

    Temporal.Worker.stop(worker)
    Temporal.Connection.close(connection)
  end
end
