defmodule Temporal.RPC do
  @moduledoc """
  Minimal typed RPC facade over official Temporal protobuf messages.

  Only `GetSystemInfo` is exposed in this milestone.
  """

  alias Temporal.Api.Workflowservice.V1.GetSystemInfoRequest
  alias Temporal.Api.Workflowservice.V1.GetSystemInfoResponse

  @system_info_method "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"

  @spec system_info(GenServer.server(), keyword()) ::
          {:ok, GetSystemInfoResponse.t()} | {:error, term()}
  def system_info(connection, options \\ []) do
    request = GetSystemInfoRequest.encode(%GetSystemInfoRequest{})

    with {:ok, response} <-
           Temporal.Connection.unary(connection, @system_info_method, request, options) do
      {:ok, GetSystemInfoResponse.decode(response)}
    end
  end
end
