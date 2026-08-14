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
    assert_receive {:completed, %RespondWorkflowTaskCompletedRequest{task_token: "owned-token"}}

    assert :ok = Temporal.Worker.stop(worker)
    refute Process.alive?(worker)
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

    state = :sys.get_state(worker)
    assert Map.has_key?(state.runs, {"default", "retry-run"})

    send(worker, {:evict_run, {"default", "retry-run"}, "retry-token"})
    assert %{runs: %{}} = :sys.get_state(worker)

    Temporal.Worker.stop(worker)
    Temporal.Connection.close(connection)
  end
end
