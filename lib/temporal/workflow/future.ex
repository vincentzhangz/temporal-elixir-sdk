defmodule Temporal.Workflow.Future do
  @moduledoc """
  Deterministic handle for an operation started by Workflow code.

  Futures are meaningful only during the Workflow invocation that created (or
  deterministically recreated) them. Use `Temporal.Workflow.await/1` to wait
  for their durable resolution.
  """

  @enforce_keys [:id, :sequence, :type]
  defstruct [:id, :sequence, :type, :resolution]

  @type t :: %__MODULE__{
          id: String.t(),
          sequence: pos_integer(),
          type: :timer,
          resolution: {:ok, :fired} | {:error, Exception.t()} | nil
        }
end
