defmodule Temporal.Workflow.CancellationScope do
  @moduledoc """
  Deterministic group for canceling Workflow operations together.

  Attach a scope through `Temporal.Workflow.TimerOptions` or the
  `:cancellation_scope` timer option, then call
  `Temporal.Workflow.cancel_scope/1` from Workflow code.
  """

  @enforce_keys [:id, :sequence]
  defstruct [:id, :sequence]

  @type t :: %__MODULE__{id: String.t(), sequence: pos_integer()}
end
