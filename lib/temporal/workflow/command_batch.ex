defmodule Temporal.Workflow.CommandBatch do
  @moduledoc false

  @enforce_keys [:commands]
  defstruct [:commands]
end
