defmodule Temporal.Interceptor do
  @moduledoc """
  Client and Worker interceptor hooks, mirroring the official SDKs' interceptors.

  Implement this behaviour to observe or decorate client operations. Each
  callback receives the connection, the operation name (e.g. `:start_workflow`),
  and an options keyword, and returns the options (possibly augmented) plus a
  result. Interceptors are pure hooks: they must not change SDK behavior beyond
  the options they return.
  """

  @callback on_call(connection :: GenServer.server(), operation :: atom(), options :: keyword()) ::
              keyword()

  @callback on_response(
              connection :: GenServer.server(),
              operation :: atom(),
              result :: {:ok, term()} | {:error, term()},
              options :: keyword()
            ) :: :ok

  defmacro __using__(_opts) do
    quote do
      @behaviour Temporal.Interceptor

      @impl true
      def on_call(_connection, _operation, options), do: options

      @impl true
      def on_response(_connection, _operation, _result, _options), do: :ok

      defoverridable on_call: 3, on_response: 4
    end
  end

  @spec on_call(module() | nil, GenServer.server(), atom(), keyword()) :: keyword()
  def on_call(nil, _connection, _operation, options), do: options

  def on_call(interceptor, connection, operation, options),
    do: interceptor.on_call(connection, operation, options)

  @spec on_response(
          module() | nil,
          GenServer.server(),
          atom(),
          {:ok, term()} | {:error, term()},
          keyword()
        ) :: :ok
  def on_response(nil, _connection, _operation, _result, _options), do: :ok

  def on_response(interceptor, connection, operation, result, options),
    do: interceptor.on_response(connection, operation, result, options)
end
