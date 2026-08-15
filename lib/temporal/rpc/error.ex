defmodule Temporal.RPC.Error do
  @moduledoc "Structured error returned by the Temporal gRPC boundary."

  defexception status: :unknown, message: nil, details: nil, metadata: %{}

  @type t :: %__MODULE__{
          status: atom(),
          message: String.t() | nil,
          details: term(),
          metadata: map()
        }

  @spec from(term()) :: t()
  def from(%__MODULE__{} = error), do: error

  if Code.ensure_loaded?(GRPC.RPCError) do
    def from(%GRPC.RPCError{} = error) do
      %__MODULE__{
        status: status_atom(error.status),
        message: error.message,
        details: Map.get(error, :details)
      }
    end
  end

  def from(:timeout), do: %__MODULE__{status: :deadline_exceeded, message: "deadline exceeded"}
  def from(:cancelled), do: %__MODULE__{status: :cancelled, message: "caller cancelled"}
  def from(reason), do: %__MODULE__{status: :unknown, message: inspect(reason), details: reason}

  @spec from(atom() | integer(), String.t() | nil) :: t()
  def from(status, message) do
    %__MODULE__{status: status_atom(status), message: message}
  end

  @spec status_atom(atom() | integer()) :: atom()
  def status_atom(status) when is_atom(status), do: status

  def status_atom(status) when is_integer(status) do
    %{
      0 => :ok,
      1 => :cancelled,
      2 => :unknown,
      3 => :invalid_argument,
      4 => :deadline_exceeded,
      5 => :not_found,
      6 => :already_exists,
      7 => :permission_denied,
      8 => :resource_exhausted,
      9 => :failed_precondition,
      10 => :aborted,
      11 => :out_of_range,
      12 => :unimplemented,
      13 => :internal,
      14 => :unavailable,
      15 => :data_loss,
      16 => :unauthenticated
    }[status] || :unknown
  end
end
