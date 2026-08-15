defmodule Temporal.Converter do
  @moduledoc """
  Pluggable payload conversion for Workflow input, results, and Activity args.

  The default implementation is the built-in JSON/plain converter
  (`Temporal.Payload`). A custom converter may implement `encode/1` and
  `decode/1` to apply codecs (e.g. encryption) before the standard JSON
  encoding.
  """

  alias Temporal.Api.Common.V1.{Payload, Payloads}

  @callback encode(term()) :: Payloads.t()
  @callback decode(Payloads.t() | nil) :: {:ok, term()} | {:error, term()}

  @spec encode(term(), module()) :: Payloads.t()
  def encode(value, converter), do: converter.encode(value)

  @spec decode(Payloads.t() | nil, module()) :: {:ok, term()} | {:error, term()}
  def decode(payloads, converter), do: converter.decode(payloads)

  @spec encode(term(), module(), [module()]) :: Payloads.t()
  def encode(value, converter, codecs) do
    converter.encode(value)
    |> Temporal.Codec.encode_payloads(codecs)
  end

  @spec decode(Payloads.t() | nil, module(), [module()]) :: {:ok, term()} | {:error, term()}
  def decode(nil, converter, _codecs), do: converter.decode(nil)

  def decode(payloads, converter, codecs) do
    case Temporal.Codec.decode_payloads(payloads, codecs) do
      {:ok, decoded} -> converter.decode(decoded)
      {:error, _reason} = error -> error
    end
  end

  @spec encode(term()) :: Payloads.t()
  def encode(value) do
    %Payloads{
      payloads: [
        %Payload{metadata: %{"encoding" => "json/plain"}, data: Jason.encode!(value)}
      ]
    }
  end

  @spec decode(Payloads.t() | nil) :: {:ok, term()} | {:error, term()}
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
