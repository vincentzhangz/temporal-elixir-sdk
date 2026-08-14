defmodule Temporal.RPC.Transport do
  @moduledoc """
  Boundary implemented by unary gRPC transports.

  Requests and successful responses are protobuf wire binaries. Transport
  implementations own HTTP/2 and gRPC framing; application RPCs are never
  replayed by this behaviour.
  """

  @type method :: String.t()
  @type wire_message :: binary()
  @type options :: keyword()

  @callback unary(
              transport_state :: term(),
              method(),
              request :: wire_message(),
              options()
            ) :: {:ok, response :: wire_message()} | {:error, term()}
end
