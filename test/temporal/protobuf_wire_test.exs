defmodule Temporal.ProtobufWireTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Workflowservice.V1.GetSystemInfoRequest
  alias Temporal.Api.Workflowservice.V1.GetSystemInfoResponse
  alias Temporal.Api.Workflowservice.V1.GetSystemInfoResponse.Capabilities

  test "official system-info messages have deterministic protobuf wire encoding" do
    assert GetSystemInfoRequest.encode(%GetSystemInfoRequest{}) == <<>>

    message = %GetSystemInfoResponse{
      server_version: "1.63.3",
      capabilities: %Capabilities{signal_and_query_header: true}
    }

    expected_wire = <<10, 6, "1.63.3", 18, 2, 8, 1>>

    assert GetSystemInfoResponse.encode(message) == expected_wire
    assert GetSystemInfoResponse.decode(expected_wire) == message
  end
end
