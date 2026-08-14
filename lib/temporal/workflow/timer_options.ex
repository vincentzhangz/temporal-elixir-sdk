defmodule Temporal.Workflow.TimerOptions do
  @moduledoc """
  Options for a durable Workflow timer.

  `summary` is encoded into the command's Temporal `UserMetadata` and recorded
  with `TimerStarted`. It does not affect timer identity.
  """

  defstruct [:summary, :cancellation_scope]

  @type t :: %__MODULE__{
          summary: String.t() | nil,
          cancellation_scope: Temporal.Workflow.CancellationScope.t() | nil
        }
end
