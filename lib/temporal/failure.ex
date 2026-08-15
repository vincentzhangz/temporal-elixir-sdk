defmodule Temporal.Failure do
  @moduledoc false

  alias Temporal.Api.Failure.V1.{
    ActivityFailureInfo,
    ApplicationFailureInfo,
    CanceledFailureInfo,
    Failure,
    TimeoutFailureInfo
  }

  @spec from_proto(Failure.t() | nil) :: Exception.t() | nil
  def from_proto(nil), do: nil

  def from_proto(%Failure{
        message: message,
        cause: cause,
        failure_info: {:application_failure_info, %ApplicationFailureInfo{} = info}
      }) do
    Temporal.ApplicationError.exception(
      message: message,
      type: info.type,
      details: decode_details(info.details),
      non_retryable: info.non_retryable,
      next_retry_delay: duration_seconds(info.next_retry_delay),
      cause: from_proto(cause)
    )
  end

  def from_proto(%Failure{
        message: message,
        cause: cause,
        failure_info: {:timeout_failure_info, %TimeoutFailureInfo{} = info}
      }) do
    Temporal.TimeoutError.exception(
      message: present_message(message, "Activity timed out"),
      timeout_type: info.timeout_type,
      last_heartbeat_details: decode_details(info.last_heartbeat_details),
      cause: from_proto(cause)
    )
  end

  def from_proto(%Failure{
        message: message,
        cause: cause,
        failure_info: {:canceled_failure_info, %CanceledFailureInfo{} = info}
      }) do
    Temporal.CanceledError.exception(
      message: present_message(message, "Activity canceled"),
      details: decode_details(info.details),
      cause: from_proto(cause),
      acknowledged: true
    )
  end

  def from_proto(%Failure{
        message: message,
        cause: cause,
        failure_info: {:activity_failure_info, %ActivityFailureInfo{} = info}
      }) do
    Temporal.ActivityError.exception(
      message: present_message(message, "Activity failed"),
      activity_id: info.activity_id,
      activity_type: info.activity_type && info.activity_type.name,
      identity: info.identity,
      scheduled_event_id: info.scheduled_event_id,
      started_event_id: info.started_event_id,
      retry_state: info.retry_state,
      cause: from_proto(cause)
    )
  end

  def from_proto(%Failure{message: message, cause: cause}) do
    Temporal.ApplicationError.exception(
      message: present_message(message, "Activity failed"),
      type: "Failure",
      cause: from_proto(cause)
    )
  end

  @spec to_proto(Exception.t(), list()) :: Failure.t()
  def to_proto(%Temporal.ApplicationError{} = exception, stacktrace) do
    %Failure{
      message: exception.message,
      source: "ElixirSDK",
      stack_trace: Exception.format(:error, exception, stacktrace),
      cause: cause_to_proto(exception.cause),
      failure_info:
        {:application_failure_info,
         %ApplicationFailureInfo{
           type: exception.type || "Temporal.ApplicationError",
           non_retryable: exception.non_retryable,
           details: Temporal.Payload.encode(exception.details),
           next_retry_delay: to_duration(exception.next_retry_delay)
         }}
    }
  end

  def to_proto(%Temporal.CanceledError{} = exception, stacktrace) do
    %Failure{
      message: exception.message,
      source: "ElixirSDK",
      stack_trace: Exception.format(:error, exception, stacktrace),
      cause: cause_to_proto(exception.cause),
      failure_info:
        {:canceled_failure_info,
         %CanceledFailureInfo{details: Temporal.Payload.encode(exception.details)}}
    }
  end

  def to_proto(%Temporal.TimeoutError{} = exception, stacktrace) do
    %Failure{
      message: exception.message,
      source: "ElixirSDK",
      stack_trace: Exception.format(:error, exception, stacktrace),
      cause: cause_to_proto(exception.cause),
      failure_info:
        {:timeout_failure_info, %TimeoutFailureInfo{timeout_type: exception.timeout_type}}
    }
  end

  def to_proto(exception, stacktrace) do
    %Failure{
      message: Exception.message(exception),
      source: "ElixirSDK",
      stack_trace: Exception.format(:error, exception, stacktrace),
      failure_info:
        {:application_failure_info,
         %ApplicationFailureInfo{type: inspect(exception.__struct__), non_retryable: false}}
    }
  end

  defp cause_to_proto(nil), do: nil
  defp cause_to_proto(exception), do: to_proto(exception, [])

  defp to_duration(nil), do: nil

  defp to_duration(seconds) when is_integer(seconds) and seconds > 0,
    do: %Google.Protobuf.Duration{seconds: seconds}

  defp to_duration(_seconds), do: nil

  defp decode_details(nil), do: nil

  defp decode_details(payloads) do
    case Temporal.Payload.decode(payloads) do
      {:ok, value} -> value
      {:error, _reason} -> nil
    end
  end

  defp duration_seconds(nil), do: nil
  defp duration_seconds(%Google.Protobuf.Duration{seconds: seconds}), do: seconds
  defp present_message("", fallback), do: fallback
  defp present_message(message, _fallback), do: message
end
