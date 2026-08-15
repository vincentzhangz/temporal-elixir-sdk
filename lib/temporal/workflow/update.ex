defmodule Temporal.Workflow.Update do
  @moduledoc false

  alias Temporal.Api.Protocol.V1.Message
  alias Temporal.Api.Update.V1.{Acceptance, Input, Outcome, Request, Response}
  alias Temporal.Workflow.Update.Dispatcher

  @protocol_instance "temporal.api.update.v1"

  @spec process([Message.t()], Dispatcher.t()) ::
          {:ok, [Message.t()], [Message.t()]}
          | {:error, term()}
  def process(messages, dispatcher) do
    Enum.reduce_while(messages, {:ok, [], []}, fn message, {:ok, accepted, responses} ->
      case process_message(message, dispatcher) do
        {:ok, acceptance, response} ->
          {:cont, {:ok, [acceptance | accepted], [response | responses]}}

        :skip ->
          {:cont, {:ok, accepted, responses}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, accepted, responses} -> {:ok, Enum.reverse(accepted), Enum.reverse(responses)}
      error -> error
    end
  end

  defp process_message(%Message{body: %Google.Protobuf.Any{type_url: type_url} = any}, dispatcher)
       when is_binary(type_url) do
    with {:ok, request} <- decode_any(any, Request),
         {:ok, handler} <- fetch_handler(dispatcher, request) do
      case run_handler(handler, request) do
        {:ok, result} ->
          message_id = request.meta && request.meta.update_id

          acceptance = %Message{
            id: message_id,
            protocol_instance_id: @protocol_instance,
            body: %Google.Protobuf.Any{
              type_url: "type.googleapis.com/temporal.api.update.v1.Acceptance",
              value: Acceptance.encode(%Acceptance{accepted_request_message_id: message_id})
            }
          }

          response = %Message{
            id: message_id,
            protocol_instance_id: @protocol_instance,
            body: %Google.Protobuf.Any{
              type_url: "type.googleapis.com/temporal.api.update.v1.Response",
              value:
                Response.encode(%Response{
                  meta: request.meta,
                  outcome: %Outcome{value: {:success, Temporal.Payload.encode(result)}}
                })
            }
          }

          {:ok, acceptance, response}

        {:error, reason} ->
          {:error, {:update_handler_failed, request.input.name, reason}}
      end
    end
  end

  defp process_message(_message, _dispatcher), do: :skip

  defp decode_any(%Google.Protobuf.Any{value: value}, module) do
    {:ok, module.decode(value)}
  rescue
    error -> {:error, {:invalid_update_message, Exception.message(error)}}
  end

  defp fetch_handler(dispatcher, %Request{input: %Input{name: name}})
       when is_binary(name) and name != "" do
    case Dispatcher.fetch(dispatcher, name) do
      {:ok, handler} -> {:ok, handler}
      :error -> {:error, {:update_handler_not_registered, name}}
    end
  end

  defp fetch_handler(_dispatcher, _request), do: {:error, :invalid_update_request}

  defp run_handler(handler, %Request{input: %Input{args: args}} = request) do
    with {:ok, decoded} <- Temporal.Payload.decode(args) do
      handler.(decoded, %{update_id: request.meta && request.meta.update_id})
    end
  end
end
