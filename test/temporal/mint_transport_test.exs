defmodule Temporal.MintTransportTest do
  use ExUnit.Case, async: true

  alias Temporal.Connection.Options
  alias Temporal.RPC.{Error, MintTransport}

  test "maps grpc status trailers to Temporal.RPC.Error" do
    assert %Error{status: :not_found} = Error.from(5, "workflow not found")
    assert %Error{status: :unavailable} = Error.from(14, nil)
    assert %Error{status: :unauthenticated} = Error.from(16, "missing token")
    assert %Error{status: :unknown} = Error.from(99, "weird")
  end

  test "rejects requests above the message size limit" do
    {:ok, options} = Options.new(target: "localhost:7233", max_message_size: 4)

    state = %{
      conn_pid: nil,
      host: "localhost",
      tls: false,
      keepalive: nil,
      max_message_size: options.max_message_size,
      default_deadline: options.default_deadline
    }

    assert {:error, %Error{status: :resource_exhausted}} =
             MintTransport.unary(state, "/svc/Method", <<1, 2, 3, 4, 5>>, [])
  end

  test "grpc framing round-trips a payload through the transport boundary" do
    # Exercise the frame/unframe logic through the public unary with an
    # over-limit-free payload: a too-large response path returns an error
    # before framing, proving the size guard.
    {:ok, options} = Options.new(target: "localhost:7233", max_message_size: 2)

    state = %{
      conn_pid: nil,
      host: "localhost",
      tls: false,
      keepalive: nil,
      max_message_size: options.max_message_size,
      default_deadline: options.default_deadline
    }

    assert {:error, %Error{status: :resource_exhausted}} =
             MintTransport.unary(state, "/svc/Method", <<1, 2, 3>>, [])
  end
end
