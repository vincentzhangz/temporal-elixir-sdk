defmodule Temporal.ApplicationError do
  @moduledoc """
  A typed application failure returned by an Activity.
  """

  defexception [
    :type,
    :details,
    :cause,
    :next_retry_delay,
    message: "application failure",
    non_retryable: false
  ]

  @impl true
  def exception(options) do
    struct!(__MODULE__, options)
  end
end

defmodule Temporal.ActivityError do
  @moduledoc "Raised when an Activity reaches a terminal unsuccessful resolution."

  defexception [
    :cause,
    :activity_id,
    :activity_type,
    :identity,
    :scheduled_event_id,
    :started_event_id,
    :retry_state,
    message: "Activity failed"
  ]

  @impl true
  def exception(options) do
    options = Keyword.put_new(options, :message, "Activity failed")
    struct!(__MODULE__, options)
  end
end

defmodule Temporal.TimeoutError do
  @moduledoc "A typed Temporal timeout failure."

  defexception [
    :timeout_type,
    :last_heartbeat_details,
    :cause,
    :retry_state,
    message: "operation timed out"
  ]

  @impl true
  def exception(options), do: struct!(__MODULE__, options)
end

defmodule Temporal.CanceledError do
  @moduledoc "A typed Temporal cancellation failure."

  defexception [
    :details,
    :cause,
    :retry_state,
    acknowledged: false,
    message: "operation canceled"
  ]

  @impl true
  def exception(options), do: struct!(__MODULE__, options)
end
