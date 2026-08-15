defmodule Temporal.Codec do
  @moduledoc """
  Payload-codec chain for encrypting/encoding payload data.

  A codec transforms a payloads container (the wire unit for Workflow input,
  results, and Activity args) and back. Codecs compose: `Temporal.Converter`
  applies them in order on encode and reverse order on decode. This mirrors the
  official SDKs' payload-codec stack used for encryption.
  """

  alias Temporal.Api.Common.V1.Payloads

  @callback encode_payloads(Payloads.t()) :: Payloads.t()
  @callback decode_payloads(Payloads.t()) :: {:ok, Payloads.t()} | {:error, term()}

  @spec encode_payloads(Payloads.t(), [module()]) :: Payloads.t()
  def encode_payloads(payloads, codecs),
    do: Enum.reduce(codecs, payloads, & &1.encode_payloads(&2))

  @spec decode_payloads(Payloads.t(), [module()]) :: {:ok, Payloads.t()} | {:error, term()}
  def decode_payloads(payloads, []), do: {:ok, payloads}

  def decode_payloads(payloads, [codec | rest]) do
    with {:ok, decoded} <- decode_payloads(payloads, rest) do
      codec.decode_payloads(decoded)
    end
  end
end

defmodule Temporal.Codec.Base64 do
  @moduledoc """
  Example codec that base64-encodes each payload's data.

  Demonstrates the codec chain; real deployments would use an encryption codec.
  """

  @behaviour Temporal.Codec

  alias Temporal.Api.Common.V1.Payloads

  @impl true
  def encode_payloads(%Payloads{payloads: payloads} = container) do
    %{container | payloads: Enum.map(payloads, &%{&1 | data: Base.encode64(&1.data)})}
  end

  @impl true
  def decode_payloads(%Payloads{payloads: payloads} = container) do
    {:ok, %{container | payloads: Enum.map(payloads, &%{&1 | data: Base.decode64!(&1.data)})}}
  rescue
    error -> {:error, {:codec_decode_failed, Exception.message(error)}}
  end
end
