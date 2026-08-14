defmodule Temporal.Workflow.HistoryCursor do
  @moduledoc """
  Ordered replay position and Workflow Task identity for one Workflow Run.

  A cursor belongs to exactly one `{workflow_id, run_id}` pair. It is safe to
  retain only after the corresponding Workflow Task completion RPC succeeds.
  """

  @enforce_keys [:workflow_id, :run_id]
  defstruct [
    :workflow_id,
    :run_id,
    :workflow_type,
    :workflow_task_queue,
    :input,
    :command,
    :workflow_task_scheduled_event_id,
    :workflow_task_started_event_id,
    :workflow_task_completed_event_id,
    :activity_scheduled_event_id,
    :activity_started_event_id,
    :activity_completed_event_id,
    :activity_outcome,
    :current_activity_index,
    :logical_time,
    :workflow_cancel_requested,
    :new_execution_run_id,
    :signal_resume_phase,
    :task_token,
    next_event_id: 1,
    last_event_id: 0,
    status: :new,
    activity_outcomes: %{},
    activity_states: %{},
    timer_outcomes: %{},
    timer_states: %{},
    timer_started_event_ids: %{},
    signal_events: []
  ]

  @type status ::
          :new | :replaying | :awaiting_live_completion | :completed | :continued_as_new

  @type t :: %__MODULE__{
          workflow_id: String.t(),
          run_id: String.t(),
          workflow_type: String.t() | nil,
          workflow_task_queue: String.t() | nil,
          input: term(),
          command: struct() | nil,
          workflow_task_scheduled_event_id: non_neg_integer() | nil,
          workflow_task_started_event_id: non_neg_integer() | nil,
          workflow_task_completed_event_id: non_neg_integer() | nil,
          activity_scheduled_event_id: non_neg_integer() | nil,
          activity_started_event_id: non_neg_integer() | nil,
          activity_completed_event_id: non_neg_integer() | nil,
          activity_outcome: {:ok, term()} | {:error, term()} | nil,
          current_activity_index: pos_integer() | nil,
          workflow_cancel_requested: boolean() | nil,
          activity_outcomes: %{optional(pos_integer()) => {:ok, term()} | {:error, term()}},
          activity_states: %{optional(pos_integer()) => atom()},
          timer_outcomes: %{optional(pos_integer()) => :fired | :canceled},
          timer_states: %{optional(pos_integer()) => map()},
          timer_started_event_ids: %{optional(String.t()) => pos_integer()},
          signal_events: [struct()],
          logical_time: Google.Protobuf.Timestamp.t() | nil,
          new_execution_run_id: String.t() | nil,
          signal_resume_phase: atom() | nil,
          task_token: binary() | nil,
          next_event_id: pos_integer(),
          last_event_id: non_neg_integer(),
          status: status()
        }

  @spec new(keyword()) :: t()
  def new(options) do
    %__MODULE__{
      workflow_id: Keyword.fetch!(options, :workflow_id),
      run_id: Keyword.fetch!(options, :run_id)
    }
  end
end
