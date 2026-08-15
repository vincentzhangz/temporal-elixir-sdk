defmodule Temporal.Telemetry do
  @moduledoc """
  Emits `:telemetry` events for client operations and worker activity.

  Uses the standard `:telemetry` application (a BEAM dependency, no extra Hex
  dep). Events:

    * `[:temporal, :client, :call]` — `measurements: %{duration: ms}`, metadata
      `%{operation: atom, result: :ok | :error}`
    * `[:temporal, :worker, :poll]` — `measurements: %{duration: ms}`, metadata
      `%{task: :workflow | :activity, outcome: :task | :empty | :error}`

  Attach handlers with `:telemetry.attach/4`.
  """

  @client_event [:temporal, :client, :call]
  @worker_event [:temporal, :worker, :poll]

  @spec client_event() :: [atom()]
  def client_event, do: @client_event

  @spec worker_event() :: [atom()]
  def worker_event, do: @worker_event

  @spec time_client(atom(), (-> {:ok, term()} | {:error, term()})) ::
          {:ok, term()} | {:error, term()}
  def time_client(operation, fun) do
    started = System.monotonic_time(:microsecond)
    result = fun.()

    measurements = %{duration: System.monotonic_time(:microsecond) - started}
    metadata = %{operation: operation, result: result_tag(result)}

    if function_exported?(:telemetry, :execute, 3) do
      :telemetry.execute(@client_event, measurements, metadata)
    end

    result
  end

  @spec time_worker(atom(), (-> term())) :: term()
  def time_worker(task, fun) do
    started = System.monotonic_time(:microsecond)
    result = fun.()

    measurements = %{duration: System.monotonic_time(:microsecond) - started}
    metadata = %{task: task, outcome: result}

    if function_exported?(:telemetry, :execute, 3) do
      :telemetry.execute(@worker_event, measurements, metadata)
    end

    result
  end

  defp result_tag({:ok, _}), do: :ok
  defp result_tag({:error, _}), do: :error
  defp result_tag(_other), do: :other
end
