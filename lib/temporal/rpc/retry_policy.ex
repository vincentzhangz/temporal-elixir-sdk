defmodule Temporal.RPC.RetryPolicy do
  @moduledoc """
  Opt-in retry policy for individually identified idempotent RPCs.

  Classification alone never causes replay: callers must pass
  `idempotent: true` for the specific operation.
  """

  alias Temporal.RPC.Error

  defstruct max_attempts: 1, backoff: nil

  @type backoff :: (pos_integer() -> pos_integer())
  @type t :: %__MODULE__{max_attempts: pos_integer(), backoff: backoff()}

  @retryable ~w(cancelled unknown deadline_exceeded resource_exhausted aborted internal unavailable)a

  @spec new(keyword()) :: %__MODULE__{}
  def new(options \\ []) do
    %__MODULE__{
      max_attempts: Keyword.get(options, :max_attempts, 1),
      backoff: Keyword.get(options, :backoff, &default_backoff/1)
    }
  end

  @spec retryable_status?(atom()) :: boolean()
  def retryable_status?(status), do: status in @retryable

  @spec run(%__MODULE__{}, (-> {:ok, term()} | {:error, term()}), keyword()) ::
          {:ok, term()} | {:error, term()}
  def run(%__MODULE__{} = policy, operation, options) do
    attempts = if Keyword.get(options, :idempotent, false), do: policy.max_attempts, else: 1
    run_attempt(operation, policy, attempts, 1)
  end

  defp run_attempt(operation, policy, remaining, attempt) do
    case operation.() do
      {:error, %Error{status: status}} = error when remaining > 1 ->
        if retryable_status?(status) do
          Process.sleep(policy.backoff.(attempt))
          run_attempt(operation, policy, remaining - 1, attempt + 1)
        else
          error
        end

      result ->
        result
    end
  end

  defp default_backoff(attempt), do: min(25 * Integer.pow(2, attempt - 1), 1_000)
end
