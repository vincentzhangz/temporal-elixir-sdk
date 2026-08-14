defmodule Temporal.Connection do
  @moduledoc """
  Supervised owner for one Temporal gRPC channel.

  Calls execute outside the owner process. If a caller exits while an RPC is in
  flight, its transport task is cancelled; terminating the connection cancels
  every in-flight call and closes the channel.
  """

  use GenServer

  alias Temporal.Connection.Options
  alias Temporal.RPC.{Error, GRPCTransport, RetryPolicy}

  @type option ::
          {:transport, module()}
          | {:transport_state, term()}
          | {:target, String.t()}
          | {:namespace, String.t()}
          | {:identity, String.t()}
          | {:tls, boolean()}
          | {:tls_options, keyword()}
          | {:ca_cert, binary()}
          | {:ca_certs, [binary()]}
          | {:server_name, String.t()}
          | {:verify, :verify_peer | :verify_none}
          | {:client_cert, binary()}
          | {:client_key, binary()}
          | {:api_key, binary() | (-> binary())}
          | {:metadata, map() | (-> map())}
          | {:default_deadline, pos_integer()}
          | {:connect_timeout, pos_integer()}
          | {:keepalive, [interval: pos_integer(), timeout: pos_integer()]}
          | {:max_message_size, pos_integer()}
          | {:retry_policy, Temporal.RPC.RetryPolicy.t()}

  @spec open([option()]) :: DynamicSupervisor.on_start_child()
  def open(options) do
    DynamicSupervisor.start_child(Temporal.ConnectionSupervisor, {__MODULE__, options})
  end

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @spec close(GenServer.server()) :: :ok
  def close(connection), do: GenServer.stop(connection, :normal)

  @doc false
  def configuration(connection), do: GenServer.call(connection, :configuration)

  @doc false
  @spec unary(GenServer.server(), String.t(), binary(), keyword()) ::
          {:ok, binary()} | {:error, term()}
  def unary(connection, method, request, options) do
    GenServer.call(connection, {:unary, method, request, options}, call_timeout(options))
  catch
    :exit, {:timeout, _} -> {:error, Error.from(:timeout)}
    :exit, {:noproc, _} -> {:error, %Error{status: :unavailable, message: "connection closed"}}
    :exit, {:normal, _} -> {:error, %Error{status: :unavailable, message: "connection closed"}}
  end

  @impl true
  def init(options) do
    transport = Keyword.get(options, :transport, GRPCTransport)

    with {:ok, config} <- Options.new(options),
         {:ok, transport_state} <- init_transport(transport, config, options) do
      {:ok,
       %{
         transport: transport,
         transport_state: transport_state,
         config: config,
         inflight: %{},
         caller_monitors: %{}
       }}
    end
  end

  @impl true
  def handle_call({:unary, method, request, options}, from, state) do
    owner = self()
    reference = make_ref()

    {task_pid, task_monitor} =
      spawn_monitor(fn ->
        send(owner, {:rpc_result, reference, execute_rpc(state, method, request, options)})
      end)

    caller_monitor = Process.monitor(elem(from, 0))
    timer = deadline_timer(options, reference)

    inflight =
      Map.put(state.inflight, reference, %{
        from: from,
        task_pid: task_pid,
        task_monitor: task_monitor,
        caller_monitor: caller_monitor,
        timer: timer
      })

    caller_monitors = Map.put(state.caller_monitors, caller_monitor, reference)
    {:noreply, %{state | inflight: inflight, caller_monitors: caller_monitors}}
  end

  def handle_call(:configuration, _from, state), do: {:reply, state.config, state}

  @impl true
  def handle_info({:rpc_result, reference, result}, state) do
    case Map.pop(state.inflight, reference) do
      {nil, _inflight} ->
        {:noreply, state}

      {%{
         from: from,
         task_monitor: task_monitor,
         caller_monitor: caller_monitor,
         timer: timer
       }, inflight} ->
        Process.demonitor(task_monitor, [:flush])
        Process.demonitor(caller_monitor, [:flush])
        cancel_timer(timer)
        GenServer.reply(from, result)

        {:noreply,
         %{
           state
           | inflight: inflight,
             caller_monitors: Map.delete(state.caller_monitors, caller_monitor)
         }}
    end
  end

  def handle_info({:rpc_timeout, reference}, state) do
    case Map.pop(state.inflight, reference) do
      {nil, _inflight} ->
        {:noreply, state}

      {%{from: from} = call, inflight} ->
        Process.exit(call.task_pid, :kill)
        Process.demonitor(call.task_monitor, [:flush])
        Process.demonitor(call.caller_monitor, [:flush])
        GenServer.reply(from, {:error, Error.from(:timeout)})

        {:noreply,
         %{
           state
           | inflight: inflight,
             caller_monitors: Map.delete(state.caller_monitors, call.caller_monitor)
         }}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Map.pop(state.caller_monitors, monitor) do
      {nil, caller_monitors} ->
        {:noreply, %{state | caller_monitors: caller_monitors}}

      {reference, caller_monitors} ->
        {call, inflight} = Map.pop(state.inflight, reference)

        if call do
          cancel_timer(call.timer)
          Process.exit(call.task_pid, :kill)
        end

        {:noreply, %{state | inflight: inflight, caller_monitors: caller_monitors}}
    end
  end

  def child_spec(options) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [options]},
      restart: :temporary
    }
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.inflight, fn {_reference, call} ->
      Process.exit(call.task_pid, :shutdown)
    end)

    if function_exported?(state.transport, :close, 1) do
      state.transport.close(state.transport_state)
    end

    :ok
  end

  defp call_timeout(options) do
    case Keyword.get(options, :timeout, 5_000) do
      :infinity -> :infinity
      timeout when is_integer(timeout) and timeout >= 0 -> timeout + 100
    end
  end

  defp init_transport(transport, config, options) do
    Code.ensure_loaded!(transport)

    cond do
      Keyword.has_key?(options, :transport_state) ->
        {:ok, Keyword.get(options, :transport_state)}

      function_exported?(transport, :connect, 2) ->
        transport.connect(config, options)

      true ->
        {:ok, nil}
    end
  end

  defp deadline_timer(options, reference) do
    case Keyword.get(options, :timeout, 5_000) do
      :infinity -> nil
      timeout -> Process.send_after(self(), {:rpc_timeout, reference}, timeout)
    end
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer), do: Process.cancel_timer(timer, async: true, info: false)

  defp execute_rpc(state, method, request, options) do
    operation = fn ->
      case state.transport.unary(state.transport_state, method, request, options) do
        {:error, reason} -> {:error, Error.from(reason)}
        result -> result
      end
    end

    case state.config.retry_policy do
      %RetryPolicy{} = policy -> RetryPolicy.run(policy, operation, options)
      nil -> operation.()
    end
  end
end
