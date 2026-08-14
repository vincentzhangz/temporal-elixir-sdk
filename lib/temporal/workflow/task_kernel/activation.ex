defmodule Temporal.Workflow.TaskKernel.Activation do
  @moduledoc false

  defmodule Job do
    @moduledoc false
    @enforce_keys [:sequence, :type, :payload]
    defstruct [:sequence, :type, :payload]
  end

  @enforce_keys [:workflow_task_started_event_id, :task_token]
  defstruct [:workflow_task_started_event_id, :task_token, jobs: [], next_sequence: 1]

  @type job_type :: :workflow | :activity | :terminal | :continue_as_new | :timer | :signal
  @type t :: %__MODULE__{}

  @spec new(pos_integer(), binary()) :: t()
  def new(started_event_id, task_token)
      when is_integer(started_event_id) and started_event_id > 0 and is_binary(task_token) do
    %__MODULE__{workflow_task_started_event_id: started_event_id, task_token: task_token}
  end

  @spec add_job(t(), job_type(), term()) :: t()
  def add_job(%__MODULE__{} = activation, type, payload) do
    job = %Job{sequence: activation.next_sequence, type: type, payload: payload}

    %{
      activation
      | jobs: activation.jobs ++ [job],
        next_sequence: activation.next_sequence + 1
    }
  end
end
