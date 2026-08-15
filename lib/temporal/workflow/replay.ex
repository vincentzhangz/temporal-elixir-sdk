defmodule Temporal.Workflow.TaskKernel.EventReducer do
  @moduledoc false

  alias Temporal.Api.Command.V1.{
    Command,
    CompleteWorkflowExecutionCommandAttributes,
    ContinueAsNewWorkflowExecutionCommandAttributes,
    RequestCancelActivityTaskCommandAttributes,
    ScheduleActivityTaskCommandAttributes
  }

  alias Temporal.Api.History.V1.{
    ActivityTaskCanceledEventAttributes,
    ActivityTaskCancelRequestedEventAttributes,
    ActivityTaskCompletedEventAttributes,
    ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    ActivityTaskStartedEventAttributes,
    ActivityTaskTimedOutEventAttributes,
    ChildWorkflowExecutionCanceledEventAttributes,
    ChildWorkflowExecutionCompletedEventAttributes,
    ChildWorkflowExecutionFailedEventAttributes,
    ChildWorkflowExecutionStartedEventAttributes,
    ChildWorkflowExecutionTimedOutEventAttributes,
    ExternalWorkflowExecutionSignaledEventAttributes,
    History,
    MarkerRecordedEventAttributes,
    NexusOperationCanceledEventAttributes,
    NexusOperationCompletedEventAttributes,
    NexusOperationFailedEventAttributes,
    NexusOperationScheduledEventAttributes,
    NexusOperationStartedEventAttributes,
    NexusOperationTimedOutEventAttributes,
    RequestCancelExternalWorkflowExecutionInitiatedEventAttributes,
    SignalExternalWorkflowExecutionFailedEventAttributes,
    SignalExternalWorkflowExecutionInitiatedEventAttributes,
    StartChildWorkflowExecutionInitiatedEventAttributes,
    TimerCanceledEventAttributes,
    TimerFiredEventAttributes,
    TimerStartedEventAttributes,
    UpsertWorkflowSearchAttributesEventAttributes,
    WorkflowExecutionCanceledEventAttributes,
    WorkflowExecutionCancelRequestedEventAttributes,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionContinuedAsNewEventAttributes,
    WorkflowExecutionFailedEventAttributes,
    WorkflowExecutionSignaledEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowExecutionUpdateAcceptedEventAttributes,
    WorkflowExecutionUpdateCompletedEventAttributes,
    WorkflowExecutionUpdateRejectedEventAttributes,
    WorkflowPropertiesModifiedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskFailedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes,
    WorkflowTaskTimedOutEventAttributes
  }

  alias Temporal.Workflow.CommandBatch
  alias Temporal.Workflow.HistoryCursor
  alias Temporal.Workflow.Machines.{ChildWorkflow, ExternalSignal, Timer}
  alias Temporal.Workflow.Signal.Dispatcher
  alias Temporal.Workflow.TaskKernel.MachineRegistry

  @supported_events ~w(
    EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
    EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    EVENT_TYPE_WORKFLOW_TASK_STARTED
    EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    EVENT_TYPE_WORKFLOW_TASK_FAILED
    EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
    EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
    EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
    EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
    EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
    EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    EVENT_TYPE_ACTIVITY_TASK_STARTED
    EVENT_TYPE_ACTIVITY_TASK_COMPLETED
    EVENT_TYPE_ACTIVITY_TASK_FAILED
    EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
    EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
    EVENT_TYPE_ACTIVITY_TASK_CANCELED
    EVENT_TYPE_TIMER_STARTED
    EVENT_TYPE_TIMER_FIRED
    EVENT_TYPE_TIMER_CANCELED
    EVENT_TYPE_MARKER_RECORDED
    EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED
    EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED
    EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED
    EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
    EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
    EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED
    EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
    EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
    EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED
    EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
    EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
    EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
    EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED
    EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED
    EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT
    EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
    EVENT_TYPE_NEXUS_OPERATION_STARTED
    EVENT_TYPE_NEXUS_OPERATION_COMPLETED
    EVENT_TYPE_NEXUS_OPERATION_FAILED
    EVENT_TYPE_NEXUS_OPERATION_CANCELED
    EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT
    EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES
    EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED
  )a

  @unsupported_patterns [
    {"ACTIVITY_TASK", :activities},
    {"QUERY", :queries},
    {"CONTINUED_AS_NEW", :continue_as_new}
  ]

  @type diagnostic :: %{required(:message) => String.t(), optional(atom()) => term()}
  @type replay_error ::
          {:invalid_history, diagnostic()}
          | {:nondeterminism, diagnostic()}
          | {:unsupported_history_event, diagnostic()}
          | {:workflow_identity_mismatch, diagnostic()}
          | term()

  @spec reduce(History.t(), function(), HistoryCursor.t(), MachineRegistry.t(), :live | :offline) ::
          {:ok, HistoryCursor.t(), MachineRegistry.t()} | {:error, replay_error()}
  def reduce(
        %History{events: events},
        workflow,
        %HistoryCursor{} = cursor,
        registry,
        mode
      )
      when mode in [:live, :offline] do
    initial = %{cursor | status: :replaying}

    reduction =
      Enum.reduce_while(events, {:ok, initial, :execution_started, registry}, fn event,
                                                                                 {:ok, acc, phase,
                                                                                  machines} ->
        case apply_event(event, acc, phase, workflow, machines, mode) do
          {:ok, next, next_phase, next_machines} ->
            {:cont, {:ok, next, next_phase, next_machines}}

          {:error, _reason} = error ->
            {:halt, error}
        end
      end)

    case normalize_reduction(reduction) do
      {:ok, final_cursor, phase, machines} ->
        case finish(final_cursor, phase, mode) do
          {:ok, cursor} -> {:ok, cursor, machines}
          {:error, _reason} = error -> error
        end

      {:error, _reason} = error ->
        error
    end
  end

  def reduce(_history, _workflow, _cursor, _registry, _mode),
    do: {:error, {:invalid_history, %{message: "expected a History protobuf"}}}

  defp normalize_reduction({:ok, cursor, phase, machines}),
    do: {:ok, cursor, phase, machines}

  defp normalize_reduction({:error, _reason} = error), do: error

  defp apply_event(event, cursor, phase, workflow, registry, mode) do
    with :ok <- validate_event_id(event, cursor),
         {:ok, next, next_phase} <- transition(event, cursor, phase, workflow),
         {:ok, machines} <- reduce_machine_event(registry, cursor, event, mode) do
      {:ok, %{next | last_event_id: event.event_id, next_event_id: event.event_id + 1},
       next_phase, machines}
    end
  end

  defp validate_event_id(%{event_id: actual}, %{next_event_id: expected})
       when actual == expected,
       do: :ok

  defp validate_event_id(%{event_id: actual}, %{next_event_id: expected})
       when actual < expected do
    {:error,
     {:invalid_history,
      %{
        event_id: actual,
        expected_event_id: expected,
        message:
          "duplicate history event ID #{actual}; expected the next serial event ID #{expected}"
      }}}
  end

  defp validate_event_id(%{event_id: actual}, %{next_event_id: expected}) do
    {:error,
     {:invalid_history,
      %{
        event_id: actual,
        expected_event_id: expected,
        message: "history event ID gap at #{actual}; expected #{expected}"
      }}}
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
           attributes:
             {:workflow_execution_started_event_attributes,
              %WorkflowExecutionStartedEventAttributes{} = attributes}
         } = event,
         cursor,
         :execution_started,
         workflow
       ) do
    with :ok <- validate_started_identity(attributes, cursor),
         {:ok, argument} <- Temporal.Payload.decode(attributes.input),
         {:ok, command} <-
           workflow_command(workflow, argument, %{}, %{
             workflow_type: workflow_type_name(attributes.workflow_type),
             task_queue: task_queue_name(attributes.task_queue),
             logical_time: event.event_time
           }) do
      {:ok,
       %{
         cursor
         | workflow_type: workflow_type_name(attributes.workflow_type),
           workflow_task_queue: task_queue_name(attributes.task_queue),
           input: argument,
           logical_time: event.event_time,
           command: command
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED} = event,
         _cursor,
         :execution_started,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowExecutionStarted")

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
           attributes:
             {:workflow_task_scheduled_event_attributes, %WorkflowTaskScheduledEventAttributes{}}
         } = event,
         cursor,
         :workflow_task_scheduled,
         _workflow
       ) do
    {:ok,
     %{
       cursor
       | workflow_task_scheduled_event_id: event.event_id,
         workflow_task_started_event_id: nil,
         workflow_task_completed_event_id: nil
     }, :workflow_task_started}
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED} = event,
         _cursor,
         :workflow_task_scheduled,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowTaskScheduled")

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_TASK_STARTED,
           attributes:
             {:workflow_task_started_event_attributes,
              %WorkflowTaskStartedEventAttributes{} = attributes}
         } = event,
         cursor,
         :workflow_task_started,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :scheduled_event_id,
             cursor.workflow_task_scheduled_event_id,
             attributes.scheduled_event_id
           ) do
      {:ok, %{cursor | workflow_task_started_event_id: event.event_id}, :workflow_task_closed}
    end
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_TASK_STARTED} = event,
         _cursor,
         :workflow_task_started,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowTaskStarted")

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
           attributes:
             {:workflow_task_completed_event_attributes,
              %WorkflowTaskCompletedEventAttributes{} = attributes}
         } = event,
         cursor,
         :workflow_task_closed,
         _workflow
       ) do
    with :ok <- correlate_task(event.event_id, attributes, cursor) do
      next = %{cursor | workflow_task_completed_event_id: event.event_id}

      if current_command(cursor.command) do
        {:ok, next, :command_event}
      else
        {:ok, %{next | signal_resume_phase: nil}, waiting_phase(cursor)}
      end
    end
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_TASK_COMPLETED} = event,
         _cursor,
         :workflow_task_closed,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowTaskCompleted")

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_TASK_FAILED,
           attributes:
             {:workflow_task_failed_event_attributes,
              %WorkflowTaskFailedEventAttributes{} = attributes}
         } = event,
         cursor,
         :workflow_task_closed,
         _workflow
       ) do
    with :ok <- correlate_task(event.event_id, attributes, cursor) do
      {:ok, cursor, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_TASK_FAILED} = event,
         _cursor,
         :workflow_task_closed,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowTaskFailed")

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
           attributes:
             {:workflow_task_timed_out_event_attributes,
              %WorkflowTaskTimedOutEventAttributes{} = attributes}
         } = event,
         cursor,
         :workflow_task_closed,
         _workflow
       ) do
    with :ok <- correlate_task(event.event_id, attributes, cursor) do
      {:ok, cursor, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT} = event,
         _cursor,
         :workflow_task_closed,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowTaskTimedOut")

  defp transition(
         %{
           event_type: :EVENT_TYPE_TIMER_STARTED,
           attributes:
             {:timer_started_event_attributes, %TimerStartedEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with {:ok, sequence} <- timer_sequence(event.event_id, attributes.timer_id),
         :ok <- match_timer_command(event.event_id, current_command(cursor.command), attributes),
         :ok <-
           match_timer_metadata(
             event.event_id,
             current_command(cursor.command),
             event.user_metadata
           ),
         :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ) do
      next =
        cursor
        |> Map.put(
          :timer_started_event_ids,
          Map.put(cursor.timer_started_event_ids, attributes.timer_id, event.event_id)
        )
        |> Map.put(
          :timer_states,
          Map.put(cursor.timer_states, sequence, %{
            type: :timer,
            id: attributes.timer_id,
            status: :started
          })
        )
        |> advance_command()

      phase = if current_command(next.command), do: :command_event, else: :timer_resolution
      {:ok, next, phase}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_TIMER_FIRED,
           attributes: {:timer_fired_event_attributes, %TimerFiredEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [
              :timer_resolution,
              :activity_started,
              :activity_resolution,
              :command_event,
              :workflow_task_scheduled
            ] do
    direct_command_resolution = cancel_timer_command?(current_command(cursor.command))

    with {:ok, sequence} <- timer_sequence(event.event_id, attributes.timer_id),
         :ok <-
           correlate(
             event.event_id,
             :started_event_id,
             Map.get(cursor.timer_started_event_ids, attributes.timer_id),
             attributes.started_event_id
           ),
         outcomes <- Map.put(cursor.timer_outcomes, sequence, :fired),
         timer_states <-
           Map.put(cursor.timer_states, sequence, %{
             type: :timer,
             id: attributes.timer_id,
             status: :fired
           }),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_outcomes: outcomes,
             timer_states: timer_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             logical_time: event.event_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           timer_outcomes: outcomes,
           timer_states: timer_states,
           logical_time: event.event_time
       }, if(direct_command_resolution, do: :command_event, else: :workflow_task_scheduled)}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_TIMER_CANCELED,
           attributes:
             {:timer_canceled_event_attributes, %TimerCanceledEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:timer_resolution, :activity_started, :activity_resolution, :command_event] do
    with {:ok, sequence} <- timer_sequence(event.event_id, attributes.timer_id),
         :ok <-
           match_cancel_timer_command(
             event.event_id,
             current_command(cursor.command),
             attributes.timer_id
           ),
         :ok <-
           correlate(
             event.event_id,
             :started_event_id,
             Map.get(cursor.timer_started_event_ids, attributes.timer_id),
             attributes.started_event_id
           ),
         :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         outcomes <- Map.put(cursor.timer_outcomes, sequence, :canceled),
         timer_states <-
           Map.put(cursor.timer_states, sequence, %{
             type: :timer,
             id: attributes.timer_id,
             status: :canceled
           }),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_outcomes: outcomes,
             timer_states: timer_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             logical_time: event.event_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           timer_outcomes: outcomes,
           timer_states: timer_states,
           logical_time: event.event_time
       }, :command_event}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_MARKER_RECORDED,
           attributes:
             {:marker_recorded_event_attributes, %MarkerRecordedEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <- match_marker(event.event_id, current_command(cursor.command), attributes) do
      next =
        cursor
        |> record_marker_result(attributes)
        |> advance_command()

      phase = if current_command(next.command), do: :command_event, else: waiting_phase(next)
      {:ok, next, phase}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
           attributes:
             {:activity_task_scheduled_event_attributes,
              %ActivityTaskScheduledEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <-
           match_activity_command(event.event_id, current_command(cursor.command), attributes) do
      next =
        cursor
        |> Map.put(:activity_scheduled_event_id, event.event_id)
        |> Map.put(:activity_started_event_id, nil)
        |> Map.put(:activity_completed_event_id, nil)
        |> Map.put(:current_activity_index, map_size(cursor.activity_outcomes) + 1)
        |> Map.put(
          :activity_states,
          Map.put(cursor.activity_states, map_size(cursor.activity_outcomes) + 1, :scheduled)
        )
        |> advance_command()

      phase = if current_command(next.command), do: :command_event, else: :activity_started
      {:ok, next, phase}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
           attributes:
             {:signal_external_workflow_execution_initiated_event_attributes,
              %SignalExternalWorkflowExecutionInitiatedEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <-
           match_external_signal_command(
             event.event_id,
             current_command(cursor.command),
             attributes
           ) do
      next =
        cursor
        |> Map.put(:external_signal_initiated_event_id, event.event_id)
        |> Map.put(
          :external_signal_states,
          Map.put(
            cursor.external_signal_states,
            map_size(cursor.external_signal_states) + 1,
            :initiated
          )
        )
        |> advance_command()

      phase =
        if current_command(next.command), do: :command_event, else: :external_signal_resolution

      {:ok, next, phase}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
           attributes:
             {:request_cancel_external_workflow_execution_initiated_event_attributes,
              %RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <-
           match_request_cancel_external(
             event.event_id,
             current_command(cursor.command),
             attributes
           ) do
      next = advance_command(cursor)
      phase = if current_command(next.command), do: :command_event, else: waiting_phase(next)
      {:ok, next, phase}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
           attributes:
             {:start_child_workflow_execution_initiated_event_attributes,
              %StartChildWorkflowExecutionInitiatedEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <-
           match_child_workflow_command(
             event.event_id,
             current_command(cursor.command),
             attributes
           ) do
      next =
        cursor
        |> Map.put(:child_workflow_initiated_event_id, event.event_id)
        |> Map.put(
          :child_workflow_states,
          Map.put(
            cursor.child_workflow_states,
            map_size(cursor.child_workflow_states) + 1,
            :initiated
          )
        )
        |> advance_command()

      phase = if current_command(next.command), do: :command_event, else: :child_workflow_started
      {:ok, next, phase}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
           attributes:
             {:nexus_operation_scheduled_event_attributes,
              %NexusOperationScheduledEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <- match_nexus_operation(event.event_id, current_command(cursor.command), attributes) do
      next =
        cursor
        |> Map.put(:nexus_operation_scheduled_event_id, event.event_id)
        |> Map.put(
          :nexus_operation_states,
          Map.put(
            cursor.nexus_operation_states,
            map_size(cursor.nexus_operation_states) + 1,
            :scheduled
          )
        )
        |> advance_command()

      phase =
        if current_command(next.command), do: :command_event, else: :nexus_operation_resolution

      {:ok, next, phase}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_NEXUS_OPERATION_STARTED,
           attributes:
             {:nexus_operation_started_event_attributes,
              %NexusOperationStartedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         _workflow
       )
       when phase in [:nexus_operation_resolution, :command_event] do
    with :ok <-
           correlate(
             event.event_id,
             :scheduled_event_id,
             cursor.nexus_operation_scheduled_event_id,
             attributes.scheduled_event_id
           ) do
      {:ok,
       %{
         cursor
         | nexus_operation_started_event_id: event.event_id,
           nexus_operation_states:
             Map.put(
               cursor.nexus_operation_states,
               map_size(cursor.nexus_operation_states),
               :started
             )
       }, :nexus_operation_resolution}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
           attributes:
             {:nexus_operation_completed_event_attributes,
              %NexusOperationCompletedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:nexus_operation_resolution, :command_event] do
    with :ok <- correlate_nexus(event.event_id, attributes, cursor),
         {:ok, result} <- Temporal.Payload.decode(attributes.result),
         outcomes <-
           Map.put(
             cursor.nexus_operation_outcomes,
             map_size(cursor.nexus_operation_outcomes) + 1,
             {:ok, result}
           ),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             nexus_operation_outcomes: outcomes,
             child_workflow_outcomes: cursor.child_workflow_outcomes,
             external_signal_outcomes: cursor.external_signal_outcomes,
             logical_time: event.event_time || cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           nexus_operation_outcomes: outcomes,
           logical_time: event.event_time || cursor.logical_time
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_NEXUS_OPERATION_FAILED,
           attributes:
             {:nexus_operation_failed_event_attributes,
              %NexusOperationFailedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:nexus_operation_resolution, :command_event] do
    with :ok <- correlate_nexus(event.event_id, attributes, cursor),
         error <-
           Temporal.ActivityError.exception(
             message: "Nexus operation failed",
             cause: Temporal.Failure.from_proto(Map.get(attributes, :failure)),
             scheduled_event_id: attributes.scheduled_event_id
           ),
         outcomes <-
           Map.put(
             cursor.nexus_operation_outcomes,
             map_size(cursor.nexus_operation_outcomes) + 1,
             {:error, error}
           ),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             nexus_operation_outcomes: outcomes,
             child_workflow_outcomes: cursor.child_workflow_outcomes,
             external_signal_outcomes: cursor.external_signal_outcomes,
             logical_time: event.event_time || cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           nexus_operation_outcomes: outcomes,
           logical_time: event.event_time || cursor.logical_time
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
           attributes:
             {:nexus_operation_timed_out_event_attributes,
              %NexusOperationTimedOutEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:nexus_operation_resolution, :command_event] do
    with :ok <- correlate_nexus(event.event_id, attributes, cursor),
         error <-
           Temporal.TimeoutError.exception(
             message: "Nexus operation timed out",
             cause: Temporal.Failure.from_proto(Map.get(attributes, :failure))
           ),
         outcomes <-
           Map.put(
             cursor.nexus_operation_outcomes,
             map_size(cursor.nexus_operation_outcomes) + 1,
             {:error, error}
           ),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             nexus_operation_outcomes: outcomes,
             child_workflow_outcomes: cursor.child_workflow_outcomes,
             external_signal_outcomes: cursor.external_signal_outcomes,
             logical_time: event.event_time || cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           nexus_operation_outcomes: outcomes,
           logical_time: event.event_time || cursor.logical_time
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_NEXUS_OPERATION_CANCELED,
           attributes:
             {:nexus_operation_canceled_event_attributes,
              %NexusOperationCanceledEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:nexus_operation_resolution, :command_event] do
    with :ok <- correlate_nexus(event.event_id, attributes, cursor),
         outcomes <-
           Map.put(
             cursor.nexus_operation_outcomes,
             map_size(cursor.nexus_operation_outcomes) + 1,
             {:error, :nexus_operation_canceled}
           ),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             nexus_operation_outcomes: outcomes,
             child_workflow_outcomes: cursor.child_workflow_outcomes,
             external_signal_outcomes: cursor.external_signal_outcomes,
             logical_time: event.event_time || cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           nexus_operation_outcomes: outcomes,
           logical_time: event.event_time || cursor.logical_time
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_ACTIVITY_TASK_STARTED,
           attributes:
             {:activity_task_started_event_attributes,
              %ActivityTaskStartedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         _workflow
       )
       when phase in [:activity_started, :activity_resolution, :command_event] do
    with :ok <-
           correlate(
             event.event_id,
             :scheduled_event_id,
             cursor.activity_scheduled_event_id,
             attributes.scheduled_event_id
           ) do
      {:ok,
       %{
         cursor
         | activity_started_event_id: event.event_id,
           activity_states:
             Map.put(cursor.activity_states, cursor.current_activity_index, :started)
       }, :activity_resolution}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
           attributes:
             {:workflow_execution_cancel_requested_event_attributes,
              %WorkflowExecutionCancelRequestedEventAttributes{}}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [
              :external_event,
              :workflow_task_scheduled,
              :workflow_task_started,
              :workflow_task_closed,
              :command_event,
              :activity_started,
              :activity_resolution,
              :timer_resolution
            ] do
    next = %{
      cursor
      | workflow_cancel_requested: true,
        logical_time: event.event_time || cursor.logical_time
    }

    with {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             workflow_cancel_requested: true,
             logical_time: next.logical_time
           }) do
      next_phase =
        if current_command(command) do
          :workflow_task_scheduled
        else
          waiting_phase(next)
        end

      {:ok, %{next | command: command}, next_phase}
    end
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED} = event,
         _cursor,
         _phase,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowExecutionCancelRequested")

  defp transition(
         %{
           event_type: :EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
           attributes:
             {:external_workflow_execution_signaled_event_attributes,
              %ExternalWorkflowExecutionSignaledEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:external_signal_resolution, :command_event] do
    with :ok <-
           correlate(
             event.event_id,
             :initiated_event_id,
             cursor.external_signal_initiated_event_id,
             attributes.initiated_event_id
           ),
         outcomes <-
           Map.put(
             cursor.external_signal_outcomes,
             map_size(cursor.external_signal_outcomes) + 1,
             :ok
           ),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             external_signal_outcomes: outcomes,
             child_workflow_outcomes: cursor.child_workflow_outcomes,
             logical_time: event.event_time || cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           external_signal_outcomes: outcomes,
           logical_time: event.event_time || cursor.logical_time
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
           attributes:
             {:signal_external_workflow_execution_failed_event_attributes,
              %SignalExternalWorkflowExecutionFailedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:external_signal_resolution, :command_event] do
    with :ok <-
           correlate(
             event.event_id,
             :initiated_event_id,
             cursor.external_signal_initiated_event_id,
             attributes.initiated_event_id
           ),
         outcomes <-
           Map.put(
             cursor.external_signal_outcomes,
             map_size(cursor.external_signal_outcomes) + 1,
             {:error, {:external_signal_failed, attributes.cause}}
           ),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             external_signal_outcomes: outcomes,
             child_workflow_outcomes: cursor.child_workflow_outcomes,
             logical_time: event.event_time || cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           external_signal_outcomes: outcomes,
           logical_time: event.event_time || cursor.logical_time
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
           attributes:
             {:child_workflow_execution_started_event_attributes,
              %ChildWorkflowExecutionStartedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         _workflow
       )
       when phase in [:child_workflow_started, :command_event] do
    with :ok <-
           correlate(
             event.event_id,
             :initiated_event_id,
             cursor.child_workflow_initiated_event_id,
             attributes.initiated_event_id
           ) do
      {:ok,
       %{
         cursor
         | child_workflow_started_event_id: event.event_id,
           child_workflow_states:
             Map.put(
               cursor.child_workflow_states,
               map_size(cursor.child_workflow_states),
               :started
             )
       }, :child_workflow_resolution}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
           attributes:
             {:child_workflow_execution_completed_event_attributes,
              %ChildWorkflowExecutionCompletedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:child_workflow_resolution, :child_workflow_started, :command_event] do
    with :ok <- correlate_child_resolution(event.event_id, attributes, cursor),
         {:ok, result} <- Temporal.Payload.decode(attributes.result),
         outcomes <-
           Map.put(
             cursor.child_workflow_outcomes,
             map_size(cursor.child_workflow_outcomes) + 1,
             {:ok, result}
           ),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             external_signal_outcomes: cursor.external_signal_outcomes,
             child_workflow_outcomes: outcomes,
             child_workflow_states: cursor.child_workflow_states,
             logical_time: event.event_time || cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           child_workflow_outcomes: outcomes,
           logical_time: event.event_time || cursor.logical_time
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
           attributes:
             {:child_workflow_execution_failed_event_attributes,
              %ChildWorkflowExecutionFailedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:child_workflow_resolution, :child_workflow_started, :command_event] do
    with :ok <- correlate_child_resolution(event.event_id, attributes, cursor),
         error <- child_workflow_error(attributes, cursor),
         outcomes <-
           Map.put(
             cursor.child_workflow_outcomes,
             map_size(cursor.child_workflow_outcomes) + 1,
             {:error, error}
           ),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             external_signal_outcomes: cursor.external_signal_outcomes,
             child_workflow_outcomes: outcomes,
             child_workflow_states: cursor.child_workflow_states,
             logical_time: event.event_time || cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           child_workflow_outcomes: outcomes,
           logical_time: event.event_time || cursor.logical_time
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
           attributes:
             {:child_workflow_execution_canceled_event_attributes,
              %ChildWorkflowExecutionCanceledEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:child_workflow_resolution, :child_workflow_started, :command_event] do
    with :ok <- correlate_child_resolution(event.event_id, attributes, cursor),
         outcomes <-
           Map.put(
             cursor.child_workflow_outcomes,
             map_size(cursor.child_workflow_outcomes) + 1,
             {:error, :child_workflow_canceled}
           ),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             external_signal_outcomes: cursor.external_signal_outcomes,
             child_workflow_outcomes: outcomes,
             child_workflow_states: cursor.child_workflow_states,
             logical_time: event.event_time || cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           child_workflow_outcomes: outcomes,
           logical_time: event.event_time || cursor.logical_time
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
           attributes:
             {:child_workflow_execution_timed_out_event_attributes,
              %ChildWorkflowExecutionTimedOutEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:child_workflow_resolution, :child_workflow_started, :command_event] do
    with :ok <- correlate_child_resolution(event.event_id, attributes, cursor),
         error <- child_workflow_timeout(attributes),
         outcomes <-
           Map.put(
             cursor.child_workflow_outcomes,
             map_size(cursor.child_workflow_outcomes) + 1,
             {:error, error}
           ),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             external_signal_outcomes: cursor.external_signal_outcomes,
             child_workflow_outcomes: outcomes,
             child_workflow_states: cursor.child_workflow_states,
             logical_time: event.event_time || cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | command: command,
           child_workflow_outcomes: outcomes,
           logical_time: event.event_time || cursor.logical_time
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
           attributes:
             {:upsert_workflow_search_attributes_event_attributes,
              %UpsertWorkflowSearchAttributesEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <-
           match_upsert_attributes(event.event_id, current_command(cursor.command), attributes) do
      next = advance_command(cursor)
      phase = if current_command(next.command), do: :command_event, else: waiting_phase(next)
      {:ok, next, phase}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED,
           attributes:
             {:workflow_properties_modified_event_attributes,
              %WorkflowPropertiesModifiedEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <-
           match_modify_properties(event.event_id, current_command(cursor.command), attributes) do
      next = advance_command(cursor)
      phase = if current_command(next.command), do: :command_event, else: waiting_phase(next)
      {:ok, next, phase}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
           attributes:
             {:activity_task_completed_event_attributes,
              %ActivityTaskCompletedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [:activity_resolution, :command_event] do
    with :ok <- correlate_activity(event.event_id, attributes, cursor),
         {:ok, result} <- Temporal.Payload.decode(attributes.result),
         outcomes <-
           Map.put(cursor.activity_outcomes, cursor.current_activity_index, {:ok, result}),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             logical_time: cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | activity_completed_event_id: event.event_id,
           activity_outcome: {:ok, result},
           activity_outcomes: outcomes,
           activity_states:
             Map.put(cursor.activity_states, cursor.current_activity_index, :completed),
           command: command
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_ACTIVITY_TASK_FAILED,
           attributes:
             {:activity_task_failed_event_attributes,
              %ActivityTaskFailedEventAttributes{} = attributes}
         } = event,
         cursor,
         :activity_resolution,
         workflow
       ) do
    with :ok <- correlate_activity(event.event_id, attributes, cursor),
         error <- activity_error(attributes, cursor),
         outcomes <-
           Map.put(cursor.activity_outcomes, cursor.current_activity_index, {:error, error}),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             logical_time: cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | activity_outcome: {:error, error},
           activity_outcomes: outcomes,
           command: command
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
           attributes:
             {:activity_task_timed_out_event_attributes,
              %ActivityTaskTimedOutEventAttributes{} = attributes}
         } = event,
         cursor,
         :activity_resolution,
         workflow
       ) do
    with :ok <- correlate_activity(event.event_id, attributes, cursor),
         timeout <- timeout_error(attributes),
         error <- activity_error(attributes, cursor, timeout),
         outcomes <-
           Map.put(cursor.activity_outcomes, cursor.current_activity_index, {:error, error}),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             logical_time: cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | activity_outcome: {:error, error},
           activity_outcomes: outcomes,
           command: command
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
           attributes:
             {:activity_task_cancel_requested_event_attributes,
              %ActivityTaskCancelRequestedEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :scheduled_event_id,
             cursor.activity_scheduled_event_id,
             attributes.scheduled_event_id
           ),
         :ok <- match_cancel_activity_command(event.event_id, cursor.command, attributes) do
      {:ok, cursor, :activity_resolution}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_ACTIVITY_TASK_CANCELED,
           attributes:
             {:activity_task_canceled_event_attributes,
              %ActivityTaskCanceledEventAttributes{} = attributes}
         } = event,
         cursor,
         :activity_resolution,
         workflow
       ) do
    with :ok <- correlate_activity(event.event_id, attributes, cursor),
         {:ok, details} <- Temporal.Payload.decode(attributes.details),
         canceled <-
           Temporal.CanceledError.exception(
             details: details,
             acknowledged: true,
             message: "Activity canceled"
           ),
         error <- activity_error(attributes, cursor, canceled),
         outcomes <-
           Map.put(cursor.activity_outcomes, cursor.current_activity_index, {:error, error}),
         {:ok, command} <-
           workflow_command(workflow, cursor.input, outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: cursor.signal_events,
             marker_results: cursor.marker_results,
             logical_time: cursor.logical_time
           }) do
      {:ok,
       %{
         cursor
         | activity_outcome: {:error, error},
           activity_outcomes: outcomes,
           command: command
       }, :workflow_task_scheduled}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
           attributes:
             {:workflow_execution_signaled_event_attributes,
              %WorkflowExecutionSignaledEventAttributes{}}
         } = event,
         cursor,
         phase,
         workflow
       )
       when phase in [
              :external_event,
              :workflow_task_scheduled,
              :workflow_task_started,
              :workflow_task_closed,
              :activity_started,
              :activity_resolution,
              :timer_resolution
            ] do
    signal_events = cursor.signal_events ++ [event]

    with {:ok, command} <-
           workflow_command(workflow, cursor.input, cursor.activity_outcomes, %{
             workflow_type: cursor.workflow_type,
             task_queue: cursor.workflow_task_queue,
             timer_states: cursor.timer_states,
             timer_outcomes: cursor.timer_outcomes,
             activity_states: cursor.activity_states,
             signal_events: signal_events,
             logical_time: event.event_time || cursor.logical_time
           }) do
      resume_phase =
        if phase in [:activity_started, :activity_resolution, :timer_resolution],
          do: phase,
          else: cursor.signal_resume_phase

      next_phase =
        if phase in [:workflow_task_started, :workflow_task_closed],
          do: phase,
          else: :workflow_task_scheduled

      {:ok,
       %{
         cursor
         | command: command,
           signal_events: signal_events,
           signal_resume_phase: resume_phase,
           logical_time: event.event_time || cursor.logical_time
       }, next_phase}
    end
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED} = event,
         _cursor,
         _phase,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowExecutionSignaled")

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
           attributes:
             {:workflow_execution_update_accepted_event_attributes,
              %WorkflowExecutionUpdateAcceptedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         _workflow
       )
       when phase in [
              :external_event,
              :workflow_task_scheduled,
              :workflow_task_started,
              :workflow_task_closed,
              :command_event,
              :activity_started,
              :activity_resolution,
              :timer_resolution,
              :external_signal_resolution,
              :child_workflow_started,
              :child_workflow_resolution
            ] do
    update_id = accepted_update_id(attributes)

    {:ok,
     %{
       cursor
       | update_states: Map.put(cursor.update_states, update_id, :accepted),
         logical_time: event.event_time || cursor.logical_time
     }, phase}
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED,
           attributes:
             {:workflow_execution_update_completed_event_attributes,
              %WorkflowExecutionUpdateCompletedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         _workflow
       )
       when phase in [
              :external_event,
              :workflow_task_scheduled,
              :workflow_task_started,
              :workflow_task_closed,
              :command_event,
              :activity_started,
              :activity_resolution,
              :timer_resolution,
              :external_signal_resolution,
              :child_workflow_started,
              :child_workflow_resolution
            ] do
    update_id = completed_update_id(attributes)
    outcome = decode_update_outcome(attributes.outcome)

    {:ok,
     %{
       cursor
       | update_states: Map.put(cursor.update_states, update_id, outcome),
         logical_time: event.event_time || cursor.logical_time
     }, phase}
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED,
           attributes:
             {:workflow_execution_update_rejected_event_attributes,
              %WorkflowExecutionUpdateRejectedEventAttributes{} = attributes}
         } = event,
         cursor,
         phase,
         _workflow
       )
       when phase in [
              :external_event,
              :workflow_task_scheduled,
              :workflow_task_started,
              :workflow_task_closed,
              :command_event,
              :activity_started,
              :activity_resolution,
              :timer_resolution,
              :external_signal_resolution,
              :child_workflow_started,
              :child_workflow_resolution
            ] do
    update_id = rejected_update_id(attributes)

    {:ok,
     %{
       cursor
       | update_states:
           Map.put(
             cursor.update_states,
             update_id,
             {:error, Temporal.Failure.from_proto(attributes.failure)}
           ),
         logical_time: event.event_time || cursor.logical_time
     }, phase}
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
           attributes:
             {:workflow_execution_continued_as_new_event_attributes,
              %WorkflowExecutionContinuedAsNewEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <- match_continue_as_new(event.event_id, cursor.command, attributes) do
      {:ok,
       %{
         cursor
         | status: :continued_as_new,
           new_execution_run_id: attributes.new_execution_run_id
       }, :continued_as_new}
    end
  end

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
           attributes:
             {:workflow_execution_completed_event_attributes,
              %WorkflowExecutionCompletedEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <- match_completion(event.event_id, cursor.command, attributes) do
      {:ok, %{cursor | status: :completed}, :completed}
    end
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED} = event,
         _cursor,
         :command_event,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowExecutionCompleted")

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
           attributes:
             {:workflow_execution_failed_event_attributes,
              %WorkflowExecutionFailedEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <- match_workflow_failure(event.event_id, cursor.command, attributes) do
      {:ok, %{cursor | status: :failed}, :failed}
    end
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED} = event,
         _cursor,
         :command_event,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowExecutionFailed")

  defp transition(
         %{
           event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
           attributes:
             {:workflow_execution_canceled_event_attributes,
              %WorkflowExecutionCanceledEventAttributes{} = attributes}
         } = event,
         cursor,
         :command_event,
         _workflow
       ) do
    with :ok <-
           correlate(
             event.event_id,
             :workflow_task_completed_event_id,
             cursor.workflow_task_completed_event_id,
             attributes.workflow_task_completed_event_id
           ),
         :ok <- match_workflow_cancel(event.event_id, cursor.command, attributes) do
      {:ok, %{cursor | status: :canceled}, :canceled}
    end
  end

  defp transition(
         %{event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED} = event,
         _cursor,
         :command_event,
         _workflow
       ),
       do: missing_attributes(event, "WorkflowExecutionCanceled")

  defp transition(event, _cursor, phase, _workflow) do
    case unsupported_feature(event.event_type) do
      nil ->
        {:error,
         {:nondeterminism,
          %{
            event_id: event.event_id,
            expected_event_type: expected_event_type(phase),
            actual_event_type: event.event_type,
            message:
              "history event #{event.event_id} is #{event.event_type}; expected " <>
                "#{expected_event_type(phase)} while replaying #{phase}"
          }}}

      feature ->
        {:error,
         {:unsupported_history_event,
          %{event_id: event.event_id, event_type: event.event_type, feature: feature}}}
    end
  end

  defp validate_started_identity(%{workflow_id: ""}, _cursor), do: :ok

  defp validate_started_identity(%{workflow_id: workflow_id}, %{workflow_id: workflow_id}),
    do: :ok

  defp validate_started_identity(%{workflow_id: actual}, %{workflow_id: expected}) do
    {:error,
     {:workflow_identity_mismatch, %{field: :workflow_id, expected: expected, actual: actual}}}
  end

  defp correlate_task(event_id, attributes, cursor) do
    case correlate(
           event_id,
           :scheduled_event_id,
           cursor.workflow_task_scheduled_event_id,
           attributes.scheduled_event_id
         ) do
      :ok ->
        correlate(
          event_id,
          :started_event_id,
          cursor.workflow_task_started_event_id,
          attributes.started_event_id
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp correlate_activity(event_id, attributes, cursor) do
    case correlate(
           event_id,
           :scheduled_event_id,
           cursor.activity_scheduled_event_id,
           attributes.scheduled_event_id
         ) do
      :ok ->
        correlate(
          event_id,
          :started_event_id,
          cursor.activity_started_event_id,
          attributes.started_event_id
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp correlate(_event_id, _field, value, value), do: :ok

  defp correlate(event_id, field, expected, actual) do
    {:error,
     {:invalid_history,
      %{
        event_id: event_id,
        field: field,
        expected: expected,
        actual: actual,
        message:
          "history event #{event_id} has #{field}=#{actual}; expected correlated event #{expected}"
      }}}
  end

  defp match_activity_command(
         event_id,
         %Command{
           command_type: :COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
           attributes:
             {:schedule_activity_task_command_attributes,
              %ScheduleActivityTaskCommandAttributes{} = expected}
         },
         actual
       ) do
    comparisons = [
      activity_id: {expected.activity_id, actual.activity_id},
      activity_type:
        {expected.activity_type && expected.activity_type.name,
         actual.activity_type && actual.activity_type.name},
      task_queue:
        {expected.task_queue && expected.task_queue.name,
         actual.task_queue && actual.task_queue.name},
      start_to_close_timeout: {expected.start_to_close_timeout, actual.start_to_close_timeout},
      schedule_to_close_timeout:
        {expected.schedule_to_close_timeout,
         defaulted_duration(expected.schedule_to_close_timeout, actual.schedule_to_close_timeout)},
      schedule_to_start_timeout:
        {expected.schedule_to_start_timeout,
         defaulted_duration(expected.schedule_to_start_timeout, actual.schedule_to_start_timeout)},
      heartbeat_timeout:
        {expected.heartbeat_timeout,
         defaulted_duration(expected.heartbeat_timeout, actual.heartbeat_timeout)}
    ]

    case Enum.find(comparisons, fn
           {field, {nil, _server_default}}
           when field in [
                  :schedule_to_start_timeout,
                  :schedule_to_close_timeout,
                  :heartbeat_timeout
                ] ->
             false

           {_field, {left, right}} ->
             left != right
         end) do
      {field, {expected_value, actual_value}} ->
        activity_mismatch(event_id, field, expected_value, actual_value)

      nil ->
        match_activity_payload_and_retry(event_id, expected, actual)
    end
  end

  defp match_activity_command(event_id, %Command{command_type: command_type}, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        actual_event_type: :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        message: "recorded ActivityTaskScheduled does not match the emitted command"
      }}}
  end

  defp match_timer_command(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_START_TIMER,
           attributes:
             {:start_timer_command_attributes,
              %{timer_id: timer_id, start_to_fire_timeout: timeout}}
         },
         %{timer_id: timer_id, start_to_fire_timeout: timeout}
       ),
       do: :ok

  defp match_timer_command(event_id, command, actual) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command && command.command_type,
        expected_event_type: :EVENT_TYPE_TIMER_STARTED,
        timer_id: actual.timer_id,
        message: "recorded TimerStarted does not match the emitted StartTimer command"
      }}}
  end

  defp match_timer_metadata(
         _event_id,
         %Command{user_metadata: metadata},
         metadata
       ),
       do: :ok

  defp match_timer_metadata(event_id, %Command{user_metadata: expected}, actual) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        field: :user_metadata,
        expected: expected,
        actual: actual,
        message: "StartTimer summary metadata does not match recorded TimerStarted metadata"
      }}}
  end

  defp match_cancel_timer_command(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_CANCEL_TIMER,
           attributes: {:cancel_timer_command_attributes, %{timer_id: timer_id}}
         },
         timer_id
       ),
       do: :ok

  defp match_cancel_timer_command(event_id, command, timer_id) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        timer_id: timer_id,
        command_type: command && command.command_type,
        expected_event_type: :EVENT_TYPE_TIMER_CANCELED,
        message: "recorded TimerCanceled does not match the emitted CancelTimer command"
      }}}
  end

  defp cancel_timer_command?(%Command{command_type: :COMMAND_TYPE_CANCEL_TIMER}), do: true
  defp cancel_timer_command?(_command), do: false

  defp timer_sequence(event_id, "timer-" <> sequence) do
    case Integer.parse(sequence) do
      {value, ""} when value > 0 -> {:ok, value}
      _other -> invalid_timer_id(event_id, "timer-" <> sequence)
    end
  end

  defp timer_sequence(event_id, timer_id), do: invalid_timer_id(event_id, timer_id)

  defp invalid_timer_id(event_id, timer_id) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        timer_id: timer_id,
        message: "timer ID does not contain a deterministic sequence"
      }}}
  end

  defp match_activity_payload_and_retry(event_id, expected, actual) do
    with :ok <- match_retry_policy(event_id, expected.retry_policy, actual.retry_policy) do
      match_payloads(expected.input, actual.input, fn expected_input, actual_input ->
        activity_mismatch(event_id, :input, expected_input, actual_input)
      end)
    end
  end

  defp match_cancel_activity_command(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
           attributes:
             {:request_cancel_activity_task_command_attributes,
              %RequestCancelActivityTaskCommandAttributes{scheduled_event_id: scheduled_event_id}}
         },
         %{scheduled_event_id: scheduled_event_id}
       ),
       do: :ok

  defp match_cancel_activity_command(event_id, command, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command.command_type,
        expected_event_type: :EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
        message: "ActivityTaskCancelRequested does not match the emitted command"
      }}}
  end

  defp match_external_signal_command(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
           attributes:
             {:signal_external_workflow_execution_command_attributes,
              %{execution: execution, signal_name: signal_name, input: input}}
         },
         %{
           workflow_execution: actual_execution,
           signal_name: actual_signal_name,
           input: actual_input
         }
       ) do
    comparisons = [
      workflow_execution:
        {execution && execution.workflow_id, actual_execution && actual_execution.workflow_id},
      signal_name: {signal_name, actual_signal_name}
    ]

    mismatch = Enum.find(comparisons, fn {_field, {left, right}} -> left != right end)

    case mismatch do
      nil ->
        match_payloads(input, actual_input, fn expected_input, actual_input ->
          external_signal_mismatch(:input, expected_input, actual_input)
        end)

      {field, {expected_value, actual_value}} ->
        external_signal_mismatch(field, expected_value, actual_value)
    end
  end

  defp match_external_signal_command(event_id, %Command{command_type: command_type}, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
        message: "SignalExternalWorkflowExecutionInitiated does not match the emitted command"
      }}}
  end

  defp match_nexus_operation(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
           attributes:
             {:schedule_nexus_operation_command_attributes,
              %{endpoint: endpoint, service: service, operation: operation}}
         },
         %{endpoint: endpoint, service: service, operation: operation}
       ),
       do: :ok

  defp match_nexus_operation(event_id, %Command{command_type: command_type}, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
        message: "NexusOperationScheduled does not match the emitted command"
      }}}
  end

  defp correlate_nexus(event_id, attributes, cursor) do
    correlate(
      event_id,
      :scheduled_event_id,
      cursor.nexus_operation_scheduled_event_id,
      Map.get(attributes, :scheduled_event_id)
    )
  end

  defp match_request_cancel_external(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION,
           attributes:
             {:request_cancel_external_workflow_execution_command_attributes,
              %{workflow_id: workflow_id}}
         },
         %{workflow_execution: %{workflow_id: workflow_id}}
       ),
       do: :ok

  defp match_request_cancel_external(
         event_id,
         %Command{command_type: command_type},
         _attributes
       ) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
        message:
          "RequestCancelExternalWorkflowExecutionInitiated does not match the emitted command"
      }}}
  end

  defp match_child_workflow_command(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
           attributes:
             {:start_child_workflow_execution_command_attributes,
              %{
                workflow_id: workflow_id,
                workflow_type: %{name: workflow_type},
                task_queue: %{name: task_queue},
                input: input
              }}
         },
         %{
           workflow_id: workflow_id,
           workflow_type: %{name: workflow_type},
           task_queue: %{name: task_queue},
           input: actual_input
         }
       ) do
    match_payloads(input, actual_input, fn expected_input, actual_input ->
      child_workflow_mismatch(:input, expected_input, actual_input)
    end)
  end

  defp match_child_workflow_command(event_id, %Command{command_type: command_type}, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
        message: "StartChildWorkflowExecutionInitiated does not match the emitted command"
      }}}
  end

  defp correlate_child_resolution(
         event_id,
         attributes,
         %{child_workflow_initiated_event_id: initiated_event_id}
       ) do
    correlate(
      event_id,
      :initiated_event_id,
      initiated_event_id,
      attributes.initiated_event_id
    )
  end

  defp match_upsert_attributes(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
           attributes:
             {:upsert_workflow_search_attributes_command_attributes,
              %{search_attributes: expected}}
         },
         %{search_attributes: actual}
       ) do
    if equivalent_search_attributes?(expected, actual) do
      :ok
    else
      {:error,
       {:nondeterminism,
        %{
          command_type: :COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
          field: :search_attributes,
          expected: actual,
          actual: expected,
          message: "UpsertWorkflowSearchAttributes does not match the emitted command"
        }}}
    end
  end

  defp match_upsert_attributes(event_id, %Command{command_type: command_type}, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
        message: "UpsertWorkflowSearchAttributes does not match the emitted command"
      }}}
  end

  defp match_modify_properties(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES,
           attributes:
             {:modify_workflow_properties_command_attributes, %{upserted_memo: expected}}
         },
         %{upserted_memo: actual}
       ) do
    if equivalent_memo?(expected, actual) do
      :ok
    else
      {:error,
       {:nondeterminism,
        %{
          command_type: :COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES,
          field: :upserted_memo,
          expected: actual,
          actual: expected,
          message: "WorkflowPropertiesModified does not match the emitted command"
        }}}
    end
  end

  defp match_modify_properties(event_id, %Command{command_type: command_type}, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED,
        message: "WorkflowPropertiesModified does not match the emitted command"
      }}}
  end

  defp record_marker_result(cursor, %{marker_name: marker_name, details: details}) do
    case marker_result(marker_name, details) do
      nil ->
        cursor

      result ->
        %{cursor | marker_results: Map.put(cursor.marker_results, marker_name, result)}
    end
  end

  defp marker_result("SideEffect", %{"data" => payloads}) do
    decode_marker_payload(payloads)
  end

  defp marker_result("LocalActivity", %{"data" => payloads}) do
    {:local_activity, decode_marker_payload(payloads)}
  end

  defp marker_result("MutableSideEffect", %{"id" => id, "data" => payloads}) do
    case decode_marker_payload(id) do
      {:ok, decoded_id} -> {decoded_id, decode_marker_payload(payloads)}
      _other -> nil
    end
  end

  defp marker_result("Version", details) do
    case decode_marker_payload(Map.get(details, "changeId")) do
      {:ok, change_id} ->
        {change_id, decode_marker_payload(Map.get(details, change_id))}

      _other ->
        nil
    end
  end

  defp marker_result(_marker_name, _details), do: nil

  defp decode_marker_payload(nil), do: nil

  defp decode_marker_payload(payloads) do
    case Temporal.Payload.decode(payloads) do
      {:ok, value} -> {:ok, value}
      {:error, _reason} -> nil
    end
  end

  defp match_marker(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_RECORD_MARKER,
           attributes:
             {:record_marker_command_attributes, %{marker_name: marker_name, details: details}}
         },
         %{marker_name: marker_name, details: actual_details}
       ) do
    if equivalent_marker_details?(details, actual_details) do
      :ok
    else
      {:error,
       {:nondeterminism,
        %{
          command_type: :COMMAND_TYPE_RECORD_MARKER,
          field: :details,
          message: "RecordMarker details do not match recorded history"
        }}}
    end
  end

  defp match_marker(event_id, %Command{command_type: command_type}, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_MARKER_RECORDED,
        message: "MarkerRecorded does not match the emitted command"
      }}}
  end

  defp equivalent_marker_details?(left, right) do
    decode_marker_values(left) == decode_marker_values(right)
  end

  defp decode_marker_values(map) when is_map(map) do
    Map.new(map, fn {key, payloads} -> {key, decode_payloads(payloads)} end)
  end

  defp decode_marker_values(_other), do: nil

  defp decode_payloads(payloads) do
    case Temporal.Payload.decode(payloads) do
      {:ok, value} -> value
      {:error, _reason} -> payloads
    end
  end

  defp equivalent_search_attributes?(nil, nil), do: true

  defp equivalent_search_attributes?(%{indexed_fields: left}, %{indexed_fields: right}) do
    Map.new(left, fn {k, v} -> {k, payload_value(v)} end) ==
      Map.new(right, fn {k, v} -> {k, payload_value(v)} end)
  end

  defp equivalent_search_attributes?(_left, _right), do: false

  defp equivalent_memo?(nil, nil), do: true

  defp equivalent_memo?(%{fields: left}, %{fields: right}) do
    Map.new(left, fn {k, v} -> {k, payload_value(v)} end) ==
      Map.new(right, fn {k, v} -> {k, payload_value(v)} end)
  end

  defp equivalent_memo?(_left, _right), do: false

  defp payload_value(payload) do
    case Temporal.Payload.decode(%Temporal.Api.Common.V1.Payloads{payloads: [payload]}) do
      {:ok, value} -> value
      {:error, _reason} -> payload
    end
  end

  defp child_workflow_error(attributes, cursor) do
    cause = Temporal.Failure.from_proto(Map.get(attributes, :failure))

    Temporal.ActivityError.exception(
      message: "Child workflow failed",
      cause: cause,
      scheduled_event_id: attributes.initiated_event_id,
      started_event_id: attributes.started_event_id,
      retry_state: Map.get(attributes, :retry_state),
      activity_id: nil,
      activity_type: child_workflow_type(cursor)
    )
  end

  defp child_workflow_timeout(attributes) do
    Temporal.TimeoutError.exception(
      message: "Child workflow timed out",
      retry_state: Map.get(attributes, :retry_state)
    )
  end

  defp child_workflow_type(%{child_workflow_states: states}) when map_size(states) > 0 do
    nil
  end

  defp child_workflow_type(_cursor), do: nil

  defp accepted_update_id(%{accepted_request: %{meta: %{update_id: update_id}}})
       when is_binary(update_id) and update_id != "",
       do: update_id

  defp accepted_update_id(%{protocol_instance_id: protocol_instance_id}),
    do: protocol_instance_id

  defp completed_update_id(%{meta: %{update_id: update_id}}) when is_binary(update_id),
    do: update_id

  defp completed_update_id(_attributes), do: "update-#{System.unique_integer([:positive])}"

  defp rejected_update_id(%{rejected_request: %{meta: %{update_id: update_id}}})
       when is_binary(update_id) and update_id != "",
       do: update_id

  defp rejected_update_id(%{protocol_instance_id: protocol_instance_id}),
    do: protocol_instance_id

  defp decode_update_outcome(nil), do: nil

  defp decode_update_outcome(%{value: {:success, payloads}}) do
    case Temporal.Payload.decode(payloads) do
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_update_outcome(%{value: {:failure, failure}}),
    do: {:error, Temporal.Failure.from_proto(failure)}

  defp external_signal_mismatch(field, expected, actual) do
    {:error,
     {:nondeterminism,
      %{
        command_type: :COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
        field: field,
        expected: actual,
        actual: expected,
        message: "SignalExternalWorkflowExecution #{field} does not match recorded history"
      }}}
  end

  defp child_workflow_mismatch(field, expected, actual) do
    {:error,
     {:nondeterminism,
      %{
        command_type: :COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
        field: field,
        expected: actual,
        actual: expected,
        message: "StartChildWorkflowExecution #{field} does not match recorded history"
      }}}
  end

  defp activity_mismatch(event_id, field, expected, actual) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: :COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
        field: field,
        expected: actual,
        actual: expected,
        message: "ScheduleActivityTask #{field} does not match recorded history"
      }}}
  end

  defp defaulted_duration(nil, %Google.Protobuf.Duration{seconds: 0, nanos: 0}), do: nil
  defp defaulted_duration(_expected, actual), do: actual

  defp match_retry_policy(_event_id, nil, _actual), do: :ok

  defp match_retry_policy(event_id, expected, actual) do
    fields = [
      initial_interval: {expected.initial_interval, actual && actual.initial_interval},
      backoff_coefficient: {expected.backoff_coefficient, actual && actual.backoff_coefficient},
      maximum_interval: {expected.maximum_interval, actual && actual.maximum_interval},
      maximum_attempts: {expected.maximum_attempts, actual && actual.maximum_attempts},
      non_retryable_error_types:
        {expected.non_retryable_error_types, actual && actual.non_retryable_error_types}
    ]

    mismatch =
      Enum.find(fields, fn
        {_field, {nil, _actual}} -> false
        {:backoff_coefficient, {+0.0, _actual}} -> false
        {:maximum_attempts, {0, _actual}} -> false
        {:non_retryable_error_types, {[], _actual}} -> false
        {_field, {left, right}} -> left != right
      end)

    case mismatch do
      nil ->
        :ok

      {field, {left, right}} ->
        activity_mismatch(event_id, "retry_policy.#{field}", left, right)
    end
  end

  defp match_continue_as_new(
         event_id,
         %Command{
           command_type: :COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
           attributes:
             {:continue_as_new_workflow_execution_command_attributes,
              %ContinueAsNewWorkflowExecutionCommandAttributes{} = expected}
         },
         actual
       ) do
    comparisons = [
      workflow_type:
        {expected.workflow_type && expected.workflow_type.name,
         actual.workflow_type && actual.workflow_type.name},
      task_queue:
        {expected.task_queue && expected.task_queue.name,
         actual.task_queue && actual.task_queue.name},
      workflow_run_timeout: {expected.workflow_run_timeout, actual.workflow_run_timeout},
      workflow_task_timeout: {expected.workflow_task_timeout, actual.workflow_task_timeout},
      backoff_start_interval: {expected.backoff_start_interval, actual.backoff_start_interval},
      header: {expected.header, actual.header},
      memo: {expected.memo, actual.memo},
      search_attributes: {expected.search_attributes, actual.search_attributes}
    ]

    mismatch =
      Enum.find(comparisons, fn
        {_field, {nil, _inherited_or_defaulted}} -> false
        {_field, {left, right}} -> left != right
      end)

    case mismatch do
      {field, {expected_value, actual_value}} ->
        continue_mismatch(event_id, field, expected_value, actual_value)

      nil ->
        match_payloads(expected.input, actual.input, fn expected_input, actual_input ->
          continue_mismatch(event_id, :input, expected_input, actual_input)
        end)
    end
  end

  defp match_continue_as_new(event_id, %Command{command_type: command_type}, _actual) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
        message: "WorkflowExecutionContinuedAsNew does not match the emitted command"
      }}}
  end

  defp match_payloads(expected, actual, mismatch) do
    with {:ok, expected_value} <- Temporal.Payload.decode(expected),
         {:ok, actual_value} <- Temporal.Payload.decode(actual) do
      if expected_value == actual_value, do: :ok, else: mismatch.(expected_value, actual_value)
    end
  end

  defp continue_mismatch(event_id, field, expected, actual) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: :COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
        field: field,
        expected: actual,
        actual: expected,
        message: "ContinueAsNewWorkflowExecution #{field} does not match recorded history"
      }}}
  end

  defp match_completion(
         event_id,
         %Command{
           command_type: :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
           attributes:
             {:complete_workflow_execution_command_attributes,
              %CompleteWorkflowExecutionCommandAttributes{result: expected}}
         },
         %{result: actual}
       ) do
    with {:ok, expected_value} <- Temporal.Payload.decode(expected),
         {:ok, actual_value} <- Temporal.Payload.decode(actual) do
      if expected_value == actual_value do
        :ok
      else
        {:error,
         {:nondeterminism,
          %{
            event_id: event_id,
            expected_event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
            command_type: :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
            expected_result: actual_value,
            actual_result: expected_value,
            message:
              "CompleteWorkflowExecution result does not match recorded result at event #{event_id}"
          }}}
      end
    end
  end

  defp match_completion(event_id, %Command{command_type: command_type}, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        expected_event_type: :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        actual_event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
        command_type: command_type,
        message: "workflow completed while replay expected the emitted command event"
      }}}
  end

  defp match_workflow_failure(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
           attributes: {:fail_workflow_execution_command_attributes, %{failure: expected}}
         },
         %{failure: actual}
       ) do
    if equivalent_failure?(expected, actual) do
      :ok
    else
      {:error,
       {:nondeterminism,
        %{
          command_type: :COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
          field: :failure,
          expected: actual,
          actual: expected,
          message: "FailWorkflowExecution failure does not match recorded history"
        }}}
    end
  end

  defp match_workflow_failure(event_id, %Command{command_type: command_type}, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
        message: "WorkflowExecutionFailed does not match the emitted command"
      }}}
  end

  defp match_workflow_cancel(
         _event_id,
         %Command{
           command_type: :COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
           attributes: {:cancel_workflow_execution_command_attributes, %{details: expected}}
         },
         %{details: actual}
       ) do
    if equivalent_details?(expected, actual) do
      :ok
    else
      {:error,
       {:nondeterminism,
        %{
          command_type: :COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION,
          field: :details,
          expected: actual,
          actual: expected,
          message: "CancelWorkflowExecution details do not match recorded history"
        }}}
    end
  end

  defp match_workflow_cancel(event_id, %Command{command_type: command_type}, _attributes) do
    {:error,
     {:nondeterminism,
      %{
        event_id: event_id,
        command_type: command_type,
        expected_event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
        message: "WorkflowExecutionCanceled does not match the emitted command"
      }}}
  end

  defp equivalent_failure?(nil, nil), do: true

  defp equivalent_failure?(%{message: left, failure_info: left_info}, %{
         message: right,
         failure_info: right_info
       }) do
    left == right and left_info == right_info
  end

  defp equivalent_failure?(_left, _right), do: false

  defp equivalent_details?(left, right) do
    with {:ok, left_value} <- Temporal.Payload.decode(left),
         {:ok, right_value} <- Temporal.Payload.decode(right) do
      left_value == right_value
    else
      _error -> false
    end
  end

  defp completion_command(result) do
    %Command{
      command_type: :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
      attributes:
        {:complete_workflow_execution_command_attributes,
         %CompleteWorkflowExecutionCommandAttributes{result: Temporal.Payload.encode(result)}}
    }
  end

  defp side_effect_results(defaults) do
    case Map.get(defaults, :marker_results, %{}) do
      %{"SideEffect" => {:ok, value}} -> %{1 => value}
      _other -> %{}
    end
  end

  defp mutable_side_effect_results(defaults) do
    defaults
    |> Map.get(:marker_results, %{})
    |> Enum.reduce(%{}, fn
      {"MutableSideEffect", {id, {:ok, value}}}, acc -> Map.put(acc, {id, 1}, value)
      _entry, acc -> acc
    end)
  end

  defp version_markers(defaults) do
    defaults
    |> Map.get(:marker_results, %{})
    |> Enum.reduce(%{}, fn
      {"Version", {change_id, {:ok, version}}}, acc -> Map.put(acc, change_id, version)
      _entry, acc -> acc
    end)
  end

  defp local_activity_results(defaults) do
    defaults
    |> Map.get(:marker_results, %{})
    |> Enum.reduce(%{}, fn
      {"LocalActivity", {:local_activity, {:ok, value}}}, acc -> Map.put(acc, 1, value)
      _entry, acc -> acc
    end)
  end

  defp workflow_command(workflow, argument, activity_outcomes, defaults) do
    latest =
      activity_outcomes
      |> Enum.max_by(fn {index, _outcome} -> index end, fn -> {0, nil} end)
      |> elem(1)

    timer_operations =
      Map.get_lazy(defaults, :timer_states, fn ->
        defaults
        |> Map.get(:timer_outcomes, %{})
        |> Map.new(fn {sequence, status} ->
          {sequence, %{type: :timer, id: "timer-#{sequence}", status: status}}
        end)
      end)

    with {:ok, signal_dispatcher} <- signal_dispatcher(defaults) do
      Temporal.Workflow.put_context(
        Map.merge(defaults, %{
          activity_outcome: latest,
          activity_outcomes: activity_outcomes,
          activity_states: Map.get(defaults, :activity_states, %{}),
          activity_index: 0,
          operation_index: 0,
          operations: timer_operations,
          pending_commands: [],
          query_handlers: Map.get(defaults, :query_handlers, %{}),
          signal_dispatcher: signal_dispatcher,
          workflow_cancel_requested: Map.get(defaults, :workflow_cancel_requested, false),
          attempt: Map.get(defaults, :attempt, 1),
          side_effect_results: side_effect_results(defaults),
          mutable_side_effect_results: mutable_side_effect_results(defaults),
          version_markers: version_markers(defaults),
          local_activity_results: local_activity_results(defaults),
          nexus_operation_outcomes: Map.get(defaults, :nexus_operation_outcomes, %{})
        })
      )

      try do
        with {:ok, result} <- invoke(workflow, argument),
             :ok <- signal_completion_policy() do
          commands =
            Temporal.Workflow.context()
            |> Map.get(:pending_commands, [])
            |> Kernel.++([completion_command(result)])

          {:ok, command_result(commands)}
        end
      catch
        {:temporal_workflow_blocked, %Command{} = command} ->
          {:ok, command}

        {:temporal_workflow_blocked, [%Command{} = command]} ->
          {:ok, command}

        {:temporal_workflow_blocked, []} ->
          {:ok, nil}

        {:temporal_workflow_blocked, commands} when is_list(commands) ->
          if Enum.all?(commands, &match?(%Command{}, &1)) do
            {:ok, %CommandBatch{commands: commands}}
          else
            {:error, {:invalid_workflow_commands, commands}}
          end

        {:temporal_signal_failed, reason} ->
          {:error, reason}
      after
        capture_query_context()
        Temporal.Workflow.clear_context()
      end
    end
  end

  defp capture_query_context do
    case Temporal.Workflow.context() do
      %{query_handlers: query_handlers} = context when map_size(query_handlers) > 0 ->
        Temporal.Workflow.capture_query_context(context)

      %{update_dispatcher: update_dispatcher} = context when not is_nil(update_dispatcher) ->
        Temporal.Workflow.capture_query_context(context)

      _context ->
        :ok
    end
  end

  defp signal_dispatcher(defaults) do
    Enum.reduce_while(
      Map.get(defaults, :signal_events, []),
      {:ok, Dispatcher.new(state: %{})},
      fn event, {:ok, dispatcher} ->
        case Dispatcher.ingest(dispatcher, event, :replay) do
          {:ok, next, _resolution} -> {:cont, {:ok, next}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end
    )
  end

  defp signal_completion_policy do
    dispatcher = Temporal.Workflow.context().signal_dispatcher

    if Dispatcher.completable?(dispatcher) do
      :ok
    else
      {:error,
       {:unfinished_signal_handlers,
        %{
          pending: Dispatcher.pending_count(dispatcher),
          buffered_event_ids: Enum.map(Dispatcher.buffered(dispatcher), & &1.event_id)
        }}}
    end
  end

  defp command_result([command]), do: command
  defp command_result(commands), do: %CommandBatch{commands: commands}

  defp current_command(%CommandBatch{commands: [command | _]}), do: command
  defp current_command(%Command{} = command), do: command
  defp current_command(nil), do: nil

  defp advance_command(%{command: %CommandBatch{commands: [_current | [next]]}} = cursor),
    do: %{cursor | command: next}

  defp advance_command(%{command: %CommandBatch{commands: [_current | rest]}} = cursor),
    do: %{cursor | command: %CommandBatch{commands: rest}}

  defp advance_command(%{command: %Command{}} = cursor), do: %{cursor | command: nil}

  defp invoke(workflow, _argument) when is_function(workflow, 0), do: safe_invoke(workflow, [])

  defp invoke(workflow, argument) when is_function(workflow, 1),
    do: safe_invoke(workflow, [argument])

  defp invoke(_workflow, _argument), do: {:error, :unsupported_workflow_arity}

  defp safe_invoke(workflow, arguments) do
    {:ok, apply(workflow, arguments)}
  rescue
    exception -> {:error, {:workflow_failed, exception, __STACKTRACE__}}
  end

  defp activity_error(attributes, cursor, cause \\ nil) do
    cause = cause || Temporal.Failure.from_proto(Map.get(attributes, :failure))

    Temporal.ActivityError.exception(
      message: "Activity failed",
      cause: cause,
      scheduled_event_id: attributes.scheduled_event_id,
      started_event_id: attributes.started_event_id,
      retry_state: Map.get(attributes, :retry_state),
      activity_id: activity_id(cursor.command),
      activity_type: activity_type(cursor.command)
    )
  end

  defp timeout_error(attributes) do
    case Temporal.Failure.from_proto(attributes.failure) do
      %Temporal.TimeoutError{} = error ->
        %{error | retry_state: attributes.retry_state}

      other ->
        Temporal.TimeoutError.exception(
          cause: other,
          retry_state: attributes.retry_state,
          message: "Activity timed out"
        )
    end
  end

  defp activity_id(%Command{
         attributes: {:schedule_activity_task_command_attributes, attributes}
       }),
       do: attributes.activity_id

  defp activity_id(_command), do: nil

  defp activity_type(%Command{
         attributes: {:schedule_activity_task_command_attributes, attributes}
       }),
       do: attributes.activity_type && attributes.activity_type.name

  defp activity_type(_command), do: nil

  defp reduce_machine_event(registry, cursor, event, mode) do
    with {:ok, registered} <- ensure_command_machine_for_event(registry, cursor, event) do
      dispatch_machine_event(registered, event, registry_mode(mode))
    end
  end

  defp ensure_command_machine_for_event(
         registry,
         %{command: command} = cursor,
         %{event_type: event_type}
       )
       when event_type not in [:EVENT_TYPE_TIMER_FIRED, :EVENT_TYPE_TIMER_CANCELED] do
    if cancel_timer_command?(current_command(command)) do
      {:ok, registry}
    else
      ensure_command_machine(registry, cursor)
    end
  end

  defp ensure_command_machine_for_event(registry, cursor, _event),
    do: ensure_command_machine(registry, cursor)

  defp ensure_command_machine(registry, %{command: nil}), do: {:ok, registry}

  defp ensure_command_machine(registry, %{command: %CommandBatch{} = batch} = cursor),
    do: ensure_command_machine(registry, %{cursor | command: current_command(batch)})

  defp ensure_command_machine(
         registry,
         %{
           command: %Command{
             command_type: :COMMAND_TYPE_CANCEL_TIMER,
             attributes: {:cancel_timer_command_attributes, %{timer_id: timer_id}}
           },
           workflow_task_completed_event_id: completed_event_id
         }
       )
       when is_integer(completed_event_id) and completed_event_id > 0 do
    with {:ok, %Timer{} = timer} <- MachineRegistry.fetch(registry, :timer, timer_id) do
      ensure_timer_cancel_state(registry, timer, timer_id, completed_event_id)
    end
  end

  defp ensure_command_machine(
         registry,
         %{
           command: %Command{command_type: :COMMAND_TYPE_START_TIMER},
           workflow_task_completed_event_id: completed_event_id
         }
       )
       when not (is_integer(completed_event_id) and completed_event_id > 0),
       do: {:ok, registry}

  defp ensure_command_machine(
         registry,
         %{
           command: %Command{
             command_type: :COMMAND_TYPE_START_TIMER,
             attributes:
               {:start_timer_command_attributes,
                %{timer_id: timer_id, start_to_fire_timeout: timeout}}
           },
           workflow_task_completed_event_id: completed_event_id
         }
       ) do
    with {:ok, sequence} <- timer_sequence(completed_event_id || 0, timer_id) do
      case MachineRegistry.fetch(registry, :timer, timer_id) do
        {:ok,
         %Timer{
           state: :start_command_created,
           start_workflow_task_completed_event_id: ^completed_event_id
         }} ->
          {:ok, registry}

        existing ->
          replace_timer_machine(
            registry,
            existing,
            sequence,
            timer_id,
            timeout,
            completed_event_id
          )
      end
    end
  end

  defp ensure_command_machine(
         registry,
         %{
           command:
             %Command{
               command_type: :COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION
             } = command
         }
       ) do
    case MachineRegistry.fetch(registry, :child_workflow, "execution") do
      {:ok, _machine} ->
        {:ok, registry}

      :error ->
        MachineRegistry.register(
          registry,
          :child_workflow,
          "execution",
          ChildWorkflow.new("execution", command, 1)
        )
    end
  end

  defp ensure_command_machine(
         registry,
         %{
           command:
             %Command{
               command_type: :COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION
             } = command
         }
       ) do
    case MachineRegistry.fetch(registry, :external_signal, "execution") do
      {:ok, _machine} ->
        {:ok, registry}

      :error ->
        MachineRegistry.register(
          registry,
          :external_signal,
          "execution",
          ExternalSignal.new("execution", command, 1)
        )
    end
  end

  defp ensure_command_machine(
         registry,
         %{
           command:
             %Command{
               command_type: :COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION
             } = command
         }
       ) do
    case MachineRegistry.fetch(registry, :workflow, "cancel-external") do
      {:ok, _machine} ->
        {:ok, registry}

      :error ->
        MachineRegistry.register(registry, :workflow, "cancel-external", %{
          command_type: command.command_type,
          command: command,
          state: :command_created
        })
    end
  end

  defp ensure_command_machine(
         registry,
         %{
           command:
             %Command{
               command_type: :COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION
             } = command
         }
       ) do
    case MachineRegistry.fetch(registry, :workflow, "nexus-operation") do
      {:ok, _machine} ->
        {:ok, registry}

      :error ->
        MachineRegistry.register(registry, :workflow, "nexus-operation", %{
          command_type: command.command_type,
          command: command,
          state: :command_created
        })
    end
  end

  defp ensure_command_machine(registry, %{command: %Command{} = command}) do
    case command_machine_identity(command) do
      nil ->
        {:ok, registry}

      {type, id} ->
        case MachineRegistry.fetch(registry, type, id) do
          {:ok, _machine} ->
            {:ok, registry}

          :error ->
            MachineRegistry.register(registry, type, id, %{
              command_type: command.command_type,
              command: command,
              state: :command_created
            })
        end
    end
  end

  defp ensure_timer_cancel_state(registry, %Timer{state: :started} = timer, timer_id, event_id) do
    with {:ok, canceling, [_command], nil} <- Timer.cancel(timer, event_id) do
      {:ok, put_in(registry, [:machines, {:timer, timer_id}], canceling)}
    end
  end

  defp ensure_timer_cancel_state(registry, %Timer{state: state}, _timer_id, _event_id)
       when state in [:cancel_command_created, :fired, :canceled],
       do: {:ok, registry}

  defp replace_timer_machine(registry, existing, sequence, timer_id, timeout, event_id) do
    with {:ok, timer} <- Timer.new(sequence, timeout, timer_id: timer_id),
         {:ok, timer, [_command], nil} <- Timer.start(timer, event_id) do
      store_timer_machine(registry, existing, timer_id, timer)
    end
  end

  defp store_timer_machine(registry, :error, timer_id, timer),
    do: MachineRegistry.register(registry, :timer, timer_id, timer)

  defp store_timer_machine(registry, {:ok, _placeholder}, timer_id, timer),
    do: {:ok, put_in(registry, [:machines, {:timer, timer_id}], timer)}

  defp command_machine_identity(%Command{
         attributes: {:schedule_activity_task_command_attributes, %{activity_id: id}}
       }),
       do: {:activity, id}

  defp command_machine_identity(%Command{
         attributes: {:start_timer_command_attributes, %{timer_id: id}}
       }),
       do: {:timer, id}

  defp command_machine_identity(%Command{
         command_type: :COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION
       }),
       do: {:continue_as_new, "execution"}

  defp command_machine_identity(%Command{command_type: command_type})
       when command_type in [
              :COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
              :COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
              :COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION
            ],
       do: {:terminal, "execution"}

  defp command_machine_identity(_command), do: nil

  defp dispatch_machine_event(
         registry,
         %{
           event_type: :EVENT_TYPE_TIMER_STARTED,
           attributes: {:timer_started_event_attributes, %{timer_id: timer_id}}
         } = event,
         mode
       ),
       do: dispatch_if_present(registry, :timer, timer_id, event, mode)

  defp dispatch_machine_event(
         registry,
         %{event_type: event_type, attributes: {_tag, %{timer_id: timer_id}}} = event,
         mode
       )
       when event_type in [:EVENT_TYPE_TIMER_FIRED, :EVENT_TYPE_TIMER_CANCELED],
       do: dispatch_if_present(registry, :timer, timer_id, event, mode)

  defp dispatch_machine_event(
         registry,
         %{
           event_type: :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
           attributes: {:activity_task_scheduled_event_attributes, %{activity_id: activity_id}}
         } = event,
         mode
       ),
       do: dispatch_if_present(registry, :activity, activity_id, event, mode)

  defp dispatch_machine_event(registry, %{event_type: event_type} = event, mode)
       when event_type in [
              :EVENT_TYPE_ACTIVITY_TASK_STARTED,
              :EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
              :EVENT_TYPE_ACTIVITY_TASK_FAILED,
              :EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
              :EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
              :EVENT_TYPE_ACTIVITY_TASK_CANCELED
            ] do
    scheduled_event_id =
      case event.attributes do
        {_tag, attributes} -> Map.get(attributes || %{}, :scheduled_event_id)
        _other -> nil
      end

    case find_activity_machine(registry, scheduled_event_id) do
      {:ok, activity_id} -> dispatch_if_present(registry, :activity, activity_id, event, mode)
      :error -> {:ok, registry}
    end
  end

  defp dispatch_machine_event(
         registry,
         %{event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW} = event,
         mode
       ),
       do: dispatch_if_present(registry, :continue_as_new, "execution", event, mode)

  defp dispatch_machine_event(
         registry,
         %{event_type: :EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED} = event,
         mode
       ),
       do: dispatch_if_present(registry, :external_signal, "execution", event, mode)

  defp dispatch_machine_event(registry, %{event_type: event_type} = event, mode)
       when event_type in [
              :EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
              :EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
            ],
       do: dispatch_if_present(registry, :external_signal, "execution", event, mode)

  defp dispatch_machine_event(
         registry,
         %{event_type: :EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED} = event,
         mode
       ),
       do: dispatch_if_present(registry, :workflow, "cancel-external", event, mode)

  defp dispatch_machine_event(registry, %{event_type: event_type} = event, mode)
       when event_type in [
              :EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
              :EVENT_TYPE_NEXUS_OPERATION_STARTED,
              :EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
              :EVENT_TYPE_NEXUS_OPERATION_FAILED,
              :EVENT_TYPE_NEXUS_OPERATION_CANCELED,
              :EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT
            ],
       do: dispatch_if_present(registry, :workflow, "nexus-operation", event, mode)

  defp dispatch_machine_event(
         registry,
         %{event_type: :EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED} = event,
         mode
       ),
       do: dispatch_if_present(registry, :child_workflow, "execution", event, mode)

  defp dispatch_machine_event(registry, %{event_type: event_type} = event, mode)
       when event_type in [
              :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
              :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
              :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
              :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
              :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT
            ],
       do: dispatch_if_present(registry, :child_workflow, "execution", event, mode)

  defp dispatch_machine_event(registry, %{event_type: event_type} = event, mode)
       when event_type in [
              :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
              :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
              :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
            ],
       do: dispatch_if_present(registry, :terminal, "execution", event, mode)

  defp dispatch_machine_event(registry, _event, _mode), do: {:ok, registry}

  defp dispatch_if_present(registry, type, id, event, mode) do
    case MachineRegistry.fetch(registry, type, id) do
      {:ok, _machine} ->
        case MachineRegistry.dispatch(registry, type, id, event, mode) do
          {:ok, next, _resolution} -> {:ok, next}
          {:error, _reason} = error -> error
        end

      :error ->
        {:ok, registry}
    end
  end

  defp find_activity_machine(%{machines: machines}, scheduled_event_id) do
    Enum.find_value(machines, :error, fn
      {{:activity, activity_id}, %{scheduled_event_id: ^scheduled_event_id}} ->
        {:ok, activity_id}

      _entry ->
        false
    end)
  end

  defp registry_mode(:offline), do: :replay
  defp registry_mode(:live), do: :live

  defp finish(cursor, :workflow_task_closed, :live),
    do: {:ok, %{cursor | status: :awaiting_live_completion}}

  defp finish(cursor, phase, :live)
       when phase in [
              :external_event,
              :timer_resolution,
              :activity_resolution,
              :external_signal_resolution,
              :child_workflow_resolution,
              :child_workflow_started,
              :nexus_operation_resolution
            ],
       do: {:ok, %{cursor | status: :awaiting_live_completion}}

  defp finish(cursor, :completed, _mode), do: {:ok, cursor}
  defp finish(cursor, :continued_as_new, _mode), do: {:ok, cursor}
  defp finish(cursor, :failed, _mode), do: {:ok, cursor}
  defp finish(cursor, :canceled, _mode), do: {:ok, cursor}

  defp finish(cursor, phase, _mode) do
    {:error,
     {:invalid_history,
      %{
        event_id: cursor.next_event_id,
        expected_event_type: expected_event_type(phase),
        message:
          "history ended after event #{cursor.last_event_id}; expected #{expected_event_type(phase)}"
      }}}
  end

  defp missing_attributes(event, name) do
    {:error,
     {:invalid_history, %{event_id: event.event_id, message: "missing #{name} attributes"}}}
  end

  defp workflow_type_name(%{name: name}), do: name
  defp workflow_type_name(_), do: nil
  defp task_queue_name(%{name: name}), do: name
  defp task_queue_name(_), do: nil

  defp expected_event_type(:execution_started), do: :EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
  defp expected_event_type(:workflow_task_scheduled), do: :EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
  defp expected_event_type(:workflow_task_started), do: :EVENT_TYPE_WORKFLOW_TASK_STARTED
  defp expected_event_type(:workflow_task_closed), do: :EVENT_TYPE_WORKFLOW_TASK_COMPLETED
  defp expected_event_type(:command_event), do: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
  defp expected_event_type(:activity_started), do: :EVENT_TYPE_ACTIVITY_TASK_STARTED
  defp expected_event_type(:activity_resolution), do: :EVENT_TYPE_ACTIVITY_TASK_COMPLETED
  defp expected_event_type(:timer_resolution), do: :EVENT_TYPE_TIMER_FIRED
  defp expected_event_type(:external_event), do: :EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED

  defp expected_event_type(:external_signal_resolution),
    do: :EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED

  defp expected_event_type(:child_workflow_started),
    do: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED

  defp expected_event_type(:child_workflow_resolution),
    do: :EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED

  defp expected_event_type(:nexus_operation_resolution),
    do: :EVENT_TYPE_NEXUS_OPERATION_COMPLETED

  defp expected_event_type(:completed), do: :EVENT_TYPE_UNSPECIFIED
  defp expected_event_type(:continued_as_new), do: :EVENT_TYPE_UNSPECIFIED

  defp waiting_phase(%{signal_resume_phase: phase}) when not is_nil(phase), do: phase

  defp waiting_phase(cursor) do
    cond do
      Enum.any?(cursor.activity_states, fn {_index, status} ->
        status in [:scheduled, :started, :cancel_requested]
      end) ->
        :activity_resolution

      Enum.any?(cursor.timer_states, fn {_index, timer} ->
        timer.status in [:start_command_created, :started, :cancel_command_created]
      end) ->
        :timer_resolution

      Enum.any?(cursor.external_signal_states, fn {_index, status} ->
        status == :initiated
      end) ->
        :external_signal_resolution

      Enum.any?(cursor.child_workflow_states, fn {_index, status} ->
        status in [:initiated, :started]
      end) ->
        if Map.has_key?(cursor.child_workflow_states, map_size(cursor.child_workflow_states)) do
          :child_workflow_resolution
        else
          :child_workflow_started
        end

      Enum.any?(cursor.nexus_operation_states, fn {_index, status} ->
        status in [:scheduled, :started]
      end) ->
        :nexus_operation_resolution

      true ->
        :external_event
    end
  end

  defp unsupported_feature(:EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT), do: :workflow_timeout
  defp unsupported_feature(:EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED), do: :workflow_termination
  defp unsupported_feature(event_type) when event_type in @supported_events, do: nil

  defp unsupported_feature(event_type) do
    name = Atom.to_string(event_type)

    Enum.find_value(@unsupported_patterns, :unimplemented, fn {pattern, feature} ->
      if String.contains?(name, pattern), do: feature
    end)
  end
end
