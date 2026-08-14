defmodule Temporal.RetryPolicyTest do
  use ExUnit.Case, async: true

  alias Temporal.RPC.{Error, RetryPolicy}

  test "classifies only transient gRPC statuses as retryable" do
    for status <- [
          :cancelled,
          :unknown,
          :deadline_exceeded,
          :resource_exhausted,
          :aborted,
          :internal,
          :unavailable
        ] do
      assert RetryPolicy.retryable_status?(status)
    end

    refute RetryPolicy.retryable_status?(:invalid_argument)
    refute RetryPolicy.retryable_status?(:permission_denied)
  end

  test "never retries unless the individual RPC is explicitly idempotent" do
    policy = RetryPolicy.new(max_attempts: 3, backoff: fn _ -> 0 end)
    operation = fn -> {:error, %Error{status: :unavailable, message: "down"}} end

    assert {:error, %Error{}} = RetryPolicy.run(policy, operation, idempotent: false)

    Process.put(:attempts, 0)

    operation = fn ->
      attempts = Process.get(:attempts) + 1
      Process.put(:attempts, attempts)
      if attempts < 3, do: {:error, %Error{status: :unavailable}}, else: {:ok, :done}
    end

    assert {:ok, :done} = RetryPolicy.run(policy, operation, idempotent: true)
    assert Process.get(:attempts) == 3
  end
end
