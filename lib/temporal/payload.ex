defmodule Temporal.Payload do
  @moduledoc """
  Minimal `json/plain` payload converter used by the initial workflow slice.

  `encode/2` and `decode/2` accept an optional codec chain (`[module()]`) that
  wraps the JSON payload — see `Temporal.Codec` for the behaviour and
  `Temporal.Codec.Base64` for an example.
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

  @spec encode(term(), [module()]) :: struct()
  def encode(value, codecs) when is_list(codecs),
    do: Temporal.Converter.encode(value, Temporal.Converter, codecs)

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

  @spec decode(struct() | nil, [module()]) :: {:ok, term()} | {:error, term()}
  def decode(payloads, codecs) when is_list(codecs),
    do: Temporal.Converter.decode(payloads, Temporal.Converter, codecs)
end
