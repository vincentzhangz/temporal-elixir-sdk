defmodule Temporal.Payload do
  @moduledoc """
  Minimal `json/plain` payload converter used by the initial workflow slice.
  """

  alias Temporal.Api.Common.V1.{Payload, Payloads}

  @spec encode(term()) :: struct()
  def encode(value) do
    %Payloads{
      payloads: [
        %Payload{metadata: %{"encoding" => "json/plain"}, data: Jason.encode!(value)}
      ]
    }
  end

  @spec decode(struct() | nil) :: {:ok, term()} | {:error, term()}
  def decode(nil), do: {:ok, nil}
  def decode(%Payloads{payloads: []}), do: {:ok, nil}

  def decode(%Payloads{payloads: [%Payload{metadata: %{"encoding" => "json/plain"}, data: data}]}) do
    Jason.decode(data)
  end

  def decode(%Payloads{payloads: [%Payload{metadata: metadata}]}) do
    {:error, {:unsupported_payload_encoding, metadata["encoding"]}}
  end

  def decode(%Payloads{}), do: {:error, :unsupported_payload_arity}
end
