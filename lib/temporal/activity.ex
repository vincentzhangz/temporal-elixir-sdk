defmodule Temporal.Activity.Info do
  @moduledoc "Immutable metadata for the currently executing Activity attempt."

  @enforce_keys [
    :namespace,
    :workflow_id,
    :run_id,
    :activity_id,
    :activity_type,
    :task_queue,
    :attempt
  ]
  defstruct [
    :namespace,
    :workflow_id,
    :run_id,
    :workflow_type,
    :activity_id,
    :activity_type,
    :task_queue,
    :attempt,
    :scheduled_time,
    :current_attempt_scheduled_time,
    :started_time,
    :schedule_to_close_timeout,
    :start_to_close_timeout,
    :heartbeat_timeout,
    :retry_policy,
    :heartbeat_details
  ]

  @type t :: %__MODULE__{}
end

defmodule Temporal.Activity.Context do
  @moduledoc "Execution context for one Activity attempt."

  @enforce_keys [:info, :task_token, :identity, :heartbeat]
  defstruct [
    :info,
    :task_token,
    :identity,
    :heartbeat,
    :last_heartbeat_at,
    :last_heartbeat_details,
    :throttle_ms,
    cancel_requested: false
  ]

  @type t :: %__MODULE__{}
end

defmodule Temporal.Activity do
  @moduledoc """
  APIs available to a running Activity.

  Heartbeats are encoded with the configured payload converter. A heartbeat
  response that requests cancellation interrupts the Activity with
  `Temporal.CanceledError`; that exception is the worker's cancellation
  acknowledgement.
  """

  alias Temporal.Activity.Context
  alias Temporal.Api.Workflowservice.V1.RecordActivityTaskHeartbeatRequest

  @context_key {__MODULE__, :context}

  @spec info() :: Temporal.Activity.Info.t()
  def info, do: context!().info

  @spec context() :: Context.t()
  def context, do: context!()

  @spec heartbeat_details() :: {:ok, term()} | {:error, term()}
  def heartbeat_details do
    context!().info.heartbeat_details
    |> Temporal.Payload.decode()
  end

  @spec heartbeat(term()) :: :ok
  def heartbeat(details) do
    context = context!()
    now = System.monotonic_time(:millisecond)

    if due?(context, now) do
      request = %RecordActivityTaskHeartbeatRequest{
        task_token: context.task_token,
        details: Temporal.Payload.encode(details),
        identity: context.identity,
        namespace: context.info.namespace
      }

      case context.heartbeat.(request) do
        {:ok, %{cancel_requested: true}} ->
          put_context(%{
            context
            | cancel_requested: true,
              last_heartbeat_at: now,
              last_heartbeat_details: details
          })

          raise Temporal.CanceledError,
            details: details,
            acknowledged: true,
            message: "Activity cancellation requested"

        {:ok, _response} ->
          put_context(%{
            context
            | last_heartbeat_at: now,
              last_heartbeat_details: details
          })

          :ok

        {:error, reason} ->
          raise Temporal.ApplicationError,
            message: "Activity heartbeat failed: #{inspect(reason)}",
            type: "HeartbeatError"
      end
    else
      put_context(%{context | last_heartbeat_details: details})
      :ok
    end
  end

  @doc false
  def put_context(%Context{} = context), do: Process.put(@context_key, context)

  @doc false
  def clear_context, do: Process.delete(@context_key)

  defp context! do
    Process.get(@context_key) ||
      raise ArgumentError, "Activity APIs may only be called from Activity execution"
  end

  defp due?(%{last_heartbeat_at: nil}, _now), do: true
  defp due?(%{last_heartbeat_at: last, throttle_ms: throttle}, now), do: now - last >= throttle
end
