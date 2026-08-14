defmodule Temporal.SDKMetadata do
  @moduledoc """
  Truthful SDK identification sent with every Temporal gRPC request.

  The `client-name` and `client-version` keys are Temporal's standard SDK
  headers. The name deliberately identifies this package as a community SDK.
  """

  @sdk_name "temporal-elixir-community"

  @spec name() :: String.t()
  def name, do: @sdk_name

  @spec version() :: String.t()
  def version do
    :temporal
    |> Application.spec(:vsn)
    |> to_string()
  end

  @spec headers() :: %{String.t() => String.t()}
  def headers do
    %{"client-name" => name(), "client-version" => version()}
  end
end
