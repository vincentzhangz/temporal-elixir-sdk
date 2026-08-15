defmodule Temporal.TestEnvironment do
  @moduledoc """
  Offline Workflow test harness built on the replay kernel.

  `run_workflow/3` executes a Workflow function against a synthetic Workflow
  Execution history and returns its final result — the offline analogue of the
  official SDKs' test environments. It shares the exact same typed reducer,
  machine registry, and payload conversion as live execution, so a passing test
  here is strong evidence the same code path works against a real server.

  The environment does not (yet) skip wall-clock time; timers scheduled by the
  Workflow are resolved by advancing the synthetic history directly.
  """

  alias Temporal.Api.Common.V1.{WorkflowExecution, WorkflowType}

  alias Temporal.Api.History.V1.{
    History,
    HistoryEvent,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes
  }

  alias Temporal.Api.Workflowservice.V1.PollWorkflowTaskQueueResponse
  alias Temporal.Workflow.TaskKernel.{Reducer, State}

  @doc """
  Runs `workflow` (a zero- or one-argument function) with `input` against a
  synthetic single-task history and returns `{:ok, result}`.
  """
  @spec run_workflow((term() -> term()), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def run_workflow(workflow, input, options \\ []) do
    workflow_id = Keyword.get(options, :workflow_id, "test-workflow")
    run_id = Keyword.get(options, :run_id, "test-run")
    workflow_type = Keyword.get(options, :workflow_type, "TestWorkflow")

    base_events = [
      started_event(workflow_id, run_id, workflow_type, input),
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

    state =
      State.new(
        namespace: Keyword.get(options, :namespace, "default"),
        workflow_id: workflow_id,
        run_id: run_id,
        mode: :live
      )

    task = %PollWorkflowTaskQueueResponse{
      task_token: "test-token",
      workflow_execution: %WorkflowExecution{workflow_id: workflow_id, run_id: run_id},
      workflow_type: %WorkflowType{name: workflow_type},
      started_event_id: 3,
      history: %History{events: base_events}
    }

    # First pass (live mode): reduce the task to discover whether the workflow
    # completes immediately or blocks.
    with {:ok, reduced} <- Reducer.reduce_task(state, task, %{workflow_type => workflow}) do
      case completion_command(State.commands(reduced)) do
        nil ->
          {:error, {:workflow_blocked, :history_exhausted}}

        command ->
          terminal_reduce(%{state | mode: :offline}, base_events, command, workflow)
      end
    end
  end

  @doc """
  Runs a Workflow that blocks on a timer, advancing time by synthesizing the
  history a real server would record for the timer (`StartTimer` →
  `TimerFired`), then returns the final completed result.

  This is the offline time-skipping analogue of the official SDKs' test
  environments. The Workflow should use `Temporal.Workflow.sleep/1` (or
  `new_timer/1` + `await/1`).
  """
  @spec advance_time((term() -> term()), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def advance_time(workflow, input, options \\ []) do
    workflow_id = Keyword.get(options, :workflow_id, "test-workflow")
    run_id = Keyword.get(options, :run_id, "test-run")
    workflow_type = Keyword.get(options, :workflow_type, "TestWorkflow")

    state =
      State.new(
        namespace: Keyword.get(options, :namespace, "default"),
        workflow_id: workflow_id,
        run_id: run_id,
        mode: :live
      )

    # First pass: run the workflow on a base task (live, events 1-3 only) and
    # read the emitted StartTimer command so the synthesized TimerStarted event
    # matches exactly.
    first_events = Enum.take(timer_history_events(workflow_id, run_id, workflow_type, input), 3)
    first = %History{events: first_events}

    with {:ok, reduced} <- Reducer.reduce_history(state, first, workflow) do
      case start_timer_command(State.commands(reduced)) do
        nil ->
          {:error, {:workflow_blocked, :no_timer}}

        %{
          attributes:
            {:start_timer_command_attributes,
             %{timer_id: timer_id, start_to_fire_timeout: timeout}}
        } ->
          advance_timer(
            workflow,
            input,
            workflow_id,
            run_id,
            workflow_type,
            state,
            timer_id,
            timeout
          )
      end
    end
  end

  defp advance_timer(
         workflow,
         input,
         workflow_id,
         run_id,
         workflow_type,
         state,
         timer_id,
         timeout
       ) do
    base_events = Enum.take(timer_history_events(workflow_id, run_id, workflow_type, input), 4)

    history = %History{
      events:
        base_events ++
          [
            timer_started_event(timer_id, timeout, 4),
            timer_fired_event(timer_id, 5),
            %HistoryEvent{
              event_id: 7,
              event_type: :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
              attributes:
                {:workflow_task_scheduled_event_attributes,
                 %WorkflowTaskScheduledEventAttributes{attempt: 1}}
            },
            %HistoryEvent{
              event_id: 8,
              event_type: :EVENT_TYPE_WORKFLOW_TASK_STARTED,
              attributes:
                {:workflow_task_started_event_attributes,
                 %WorkflowTaskStartedEventAttributes{scheduled_event_id: 7}}
            }
          ]
    }

    with {:ok, resumed} <- Reducer.reduce_history(state, history, workflow) do
      case completion_command(State.commands(resumed)) do
        nil -> {:error, {:workflow_blocked, :history_exhausted}}
        command -> resume_and_close(state, history, command, workflow)
      end
    end
  end

  defp resume_and_close(state, history, command, workflow) do
    task_completed_event = %HistoryEvent{
      event_id: 9,
      event_type: :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
      attributes:
        {:workflow_task_completed_event_attributes,
         %WorkflowTaskCompletedEventAttributes{scheduled_event_id: 7, started_event_id: 8}}
    }

    full_history = %History{
      events: history.events ++ [task_completed_event, close_event(10, 9, command)]
    }

    state = %{state | mode: :offline}

    case Reducer.reduce_history(state, full_history, workflow) do
      {:ok, done} ->
        case completion_result(State.commands(done)) do
          {:ok, result} -> {:ok, result}
          _other -> {:error, :workflow_not_completed}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp start_timer_command([%{command_type: :COMMAND_TYPE_START_TIMER} = command | _rest]),
    do: command

  defp start_timer_command(_commands), do: nil

  defp timer_started_event(timer_id, timeout, task_completed_id) do
    %HistoryEvent{
      event_id: 5,
      event_type: :EVENT_TYPE_TIMER_STARTED,
      attributes:
        {:timer_started_event_attributes,
         %Temporal.Api.History.V1.TimerStartedEventAttributes{
           timer_id: timer_id,
           start_to_fire_timeout: timeout,
           workflow_task_completed_event_id: task_completed_id
         }}
    }
  end

  defp timer_fired_event(timer_id, started_id) do
    %HistoryEvent{
      event_id: 6,
      event_type: :EVENT_TYPE_TIMER_FIRED,
      attributes:
        {:timer_fired_event_attributes,
         %Temporal.Api.History.V1.TimerFiredEventAttributes{
           timer_id: timer_id,
           started_event_id: started_id
         }}
    }
  end

  defp timer_history_events(workflow_id, run_id, workflow_type, input) do
    [
      started_event(workflow_id, run_id, workflow_type, input),
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
      },
      %HistoryEvent{
        event_id: 4,
        event_type: :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
        attributes:
          {:workflow_task_completed_event_attributes,
           %WorkflowTaskCompletedEventAttributes{scheduled_event_id: 2, started_event_id: 3}}
      },
      %HistoryEvent{
        event_id: 5,
        event_type: :EVENT_TYPE_TIMER_STARTED,
        attributes:
          {:timer_started_event_attributes,
           %Temporal.Api.History.V1.TimerStartedEventAttributes{
             timer_id: "timer-1",
             start_to_fire_timeout: %Google.Protobuf.Duration{seconds: 1},
             workflow_task_completed_event_id: 4
           }}
      },
      %HistoryEvent{
        event_id: 6,
        event_type: :EVENT_TYPE_TIMER_FIRED,
        attributes:
          {:timer_fired_event_attributes,
           %Temporal.Api.History.V1.TimerFiredEventAttributes{
             timer_id: "timer-1",
             started_event_id: 5
           }}
      },
      %HistoryEvent{
        event_id: 7,
        event_type: :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
        attributes:
          {:workflow_task_scheduled_event_attributes,
           %WorkflowTaskScheduledEventAttributes{attempt: 1}}
      },
      %HistoryEvent{
        event_id: 8,
        event_type: :EVENT_TYPE_WORKFLOW_TASK_STARTED,
        attributes:
          {:workflow_task_started_event_attributes,
           %WorkflowTaskStartedEventAttributes{scheduled_event_id: 7}}
      },
      %HistoryEvent{
        event_id: 9,
        event_type: :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
        attributes:
          {:workflow_task_completed_event_attributes,
           %WorkflowTaskCompletedEventAttributes{scheduled_event_id: 7, started_event_id: 8}}
      }
    ]
  end

  defp terminal_reduce(state, base_events, command, workflow) do
    history = %History{events: base_events ++ completed_event(base_events, command)}

    case Reducer.reduce_history(state, history, workflow) do
      {:ok, final} ->
        case completion_result(State.commands(final)) do
          {:ok, result} -> {:ok, result}
          _other -> {:error, :workflow_not_completed}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp completion_command([%{command_type: :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION} = command]),
    do: command

  defp completion_command(_commands), do: nil

  defp completed_event(_base_events, command) do
    result =
      case command.attributes do
        {:complete_workflow_execution_command_attributes, %{result: payloads}} -> payloads
        _other -> Temporal.Payload.encode(:ok)
      end

    task_completed = %HistoryEvent{
      event_id: 4,
      event_type: :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
      attributes:
        {:workflow_task_completed_event_attributes,
         %WorkflowTaskCompletedEventAttributes{scheduled_event_id: 2, started_event_id: 3}}
    }

    close = %HistoryEvent{
      event_id: 5,
      event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
      attributes:
        {:workflow_execution_completed_event_attributes,
         %Temporal.Api.History.V1.WorkflowExecutionCompletedEventAttributes{
           workflow_task_completed_event_id: 4,
           result: result
         }}
    }

    [task_completed, close]
  end

  defp close_event(event_id, task_completed_id, command) do
    result =
      case command.attributes do
        {:complete_workflow_execution_command_attributes, %{result: payloads}} -> payloads
        _other -> Temporal.Payload.encode(:ok)
      end

    %HistoryEvent{
      event_id: event_id,
      event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
      attributes:
        {:workflow_execution_completed_event_attributes,
         %Temporal.Api.History.V1.WorkflowExecutionCompletedEventAttributes{
           workflow_task_completed_event_id: task_completed_id,
           result: result
         }}
    }
  end

  defp completion_result([%{command_type: :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION} = command]) do
    {:ok, decode_completion(command)}
  end

  defp completion_result(_commands), do: {:blocked, :no_completion}

  defp decode_completion(%{
         attributes: {:complete_workflow_execution_command_attributes, %{result: payloads}}
       }) do
    case Temporal.Payload.decode(payloads) do
      {:ok, value} -> value
      {:error, _reason} -> nil
    end
  end

  defp decode_completion(_command), do: nil

  defp started_event(workflow_id, _run_id, workflow_type, input) do
    %HistoryEvent{
      event_id: 1,
      event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
      attributes:
        {:workflow_execution_started_event_attributes,
         %WorkflowExecutionStartedEventAttributes{
           workflow_id: workflow_id,
           workflow_type: %WorkflowType{name: workflow_type},
           input: Temporal.Payload.encode(input)
         }}
    }
  end
end
