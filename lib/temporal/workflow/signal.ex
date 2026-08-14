defmodule Temporal.Workflow.Signal do
  @moduledoc """
  A decoded, immutable Workflow signal taken from one ordered history event.

  `request_id` is the server RPC idempotency key. Application-level exactly-once
  behavior still requires an idempotency key in the signal input because
  request-id deduplication is a client/server transport concern.
  """

  alias Temporal.Api.History.V1.{
    HistoryEvent,
    WorkflowExecutionSignaledEventAttributes
  }

  @enforce_keys [:event_id, :signal_name, :input, :headers, :identity, :request_id]
  defstruct [:event_id, :signal_name, :input, :headers, :identity, :request_id]

  @type t :: %__MODULE__{
          event_id: pos_integer(),
          signal_name: String.t(),
          input: term(),
          headers: struct() | nil,
          identity: String.t(),
          request_id: String.t()
        }

  @spec from_history_event(struct(), (struct() | nil -> {:ok, term()} | {:error, term()})) ::
          {:ok, t()} | {:error, term()}
  def from_history_event(
        %HistoryEvent{
          event_id: event_id,
          event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
          attributes:
            {:workflow_execution_signaled_event_attributes,
             %WorkflowExecutionSignaledEventAttributes{} = attributes}
        },
        decoder
      )
      when is_function(decoder, 1) do
    cond do
      not (is_integer(event_id) and event_id > 0) ->
        {:error, {:invalid_signal_event_id, event_id}}

      not (is_binary(attributes.signal_name) and attributes.signal_name != "") ->
        {:error, {:invalid_signal_name, attributes.signal_name}}

      true ->
        decode(attributes.input, decoder, event_id, attributes)
    end
  end

  def from_history_event(%HistoryEvent{event_id: event_id}, _decoder),
    do: {:error, {:malformed_signal_event, event_id}}

  defp decode(input, decoder, event_id, attributes) do
    case decoder.(input) do
      {:ok, value} ->
        {:ok,
         %__MODULE__{
           event_id: event_id,
           signal_name: attributes.signal_name,
           input: value,
           headers: attributes.header,
           identity: attributes.identity,
           request_id: attributes.request_id
         }}

      {:error, reason} ->
        {:error, {:payload_conversion_failed, %{event_id: event_id, reason: reason}}}

      other ->
        {:error, {:invalid_decoder_result, other}}
    end
  end
end
