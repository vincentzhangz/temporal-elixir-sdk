defmodule Temporal.Workflow.Signal.Dispatcher do
  @moduledoc """
  Deterministic signal handler registry and pure Workflow-state dispatcher.

  Handlers receive `(decoded_input, context, workflow_state)` and must return
  `{:ok, new_workflow_state}` or `{:error, reason}`. They are scheduled one at a
  time in history-event order. A named handler takes precedence over the
  dynamic fallback.
  """

  alias Temporal.Workflow.Machines.SignalInbox
  alias Temporal.Workflow.Machines.SignalInbox.Invocation
  alias Temporal.Workflow.Signal

  @unknown_policies [:buffer, :drop, :fail]

  defstruct handlers: %{},
            dynamic_handler: nil,
            decoder: &Temporal.Payload.decode/1,
            unknown_signal: :buffer,
            workflow_state: %{},
            inbox: SignalInbox.new()

  @type mode :: :live | :replay
  @type handler :: (term(), map(), term() -> {:ok, term()} | {:error, term()})
  @type t :: %__MODULE__{}

  @spec new(keyword()) :: t()
  def new(options \\ []) do
    decoder = Keyword.get(options, :decoder, &Temporal.Payload.decode/1)
    unknown_signal = Keyword.get(options, :unknown_signal, :buffer)

    unless is_function(decoder, 1) do
      raise ArgumentError, ":decoder must be a one-argument function"
    end

    unless unknown_signal in @unknown_policies do
      raise ArgumentError, ":unknown_signal must be :buffer, :drop, or :fail"
    end

    %__MODULE__{
      decoder: decoder,
      unknown_signal: unknown_signal,
      workflow_state: Keyword.get(options, :state, %{})
    }
  end

  @spec register(t(), String.t(), handler()) ::
          {:ok, t(), non_neg_integer()} | {:error, {:invalid_handler_name, term()}}
  def register(%__MODULE__{} = dispatcher, signal_name, handler)
      when is_binary(signal_name) and signal_name != "" and is_function(handler, 3) do
    handlers = Map.put(dispatcher.handlers, signal_name, handler)

    {inbox, promoted} =
      SignalInbox.promote(
        dispatcher.inbox,
        &(&1.signal_name == signal_name),
        handler
      )

    {:ok, %{dispatcher | handlers: handlers, inbox: inbox}, promoted}
  end

  def register(%__MODULE__{}, signal_name, _handler),
    do: {:error, {:invalid_handler_name, signal_name}}

  @spec register_dynamic(t(), handler()) :: {:ok, t(), non_neg_integer()}
  def register_dynamic(%__MODULE__{} = dispatcher, handler) when is_function(handler, 3) do
    {inbox, promoted} =
      SignalInbox.promote(
        dispatcher.inbox,
        &(not Map.has_key?(dispatcher.handlers, &1.signal_name)),
        handler
      )

    {:ok, %{dispatcher | dynamic_handler: handler, inbox: inbox}, promoted}
  end

  @spec remove(t(), String.t()) :: {:ok, t(), handler() | nil}
  def remove(%__MODULE__{} = dispatcher, signal_name) when is_binary(signal_name) do
    {removed, handlers} = Map.pop(dispatcher.handlers, signal_name)
    {:ok, %{dispatcher | handlers: handlers}, removed}
  end

  @spec remove_dynamic(t()) :: {:ok, t(), handler() | nil}
  def remove_dynamic(%__MODULE__{} = dispatcher) do
    {:ok, %{dispatcher | dynamic_handler: nil}, dispatcher.dynamic_handler}
  end

  @spec ingest(t(), struct(), mode()) ::
          {:ok, t(), :buffered | :dropped | :scheduled | :duplicate} | {:error, term()}
  def ingest(%__MODULE__{} = dispatcher, event, mode) when mode in [:live, :replay] do
    with {:ok, signal} <- Signal.from_history_event(event, dispatcher.decoder) do
      dispatch(dispatcher, signal)
    end
  end

  def ingest(%__MODULE__{}, _event, mode), do: {:error, {:invalid_signal_mode, mode}}

  @spec start_next(t()) ::
          {:ok, t(), Invocation.t()} | {:empty, t()} | {:error, :handler_already_running}
  def start_next(%__MODULE__{} = dispatcher) do
    case SignalInbox.start_next(dispatcher.inbox) do
      {:ok, inbox, invocation} -> {:ok, %{dispatcher | inbox: inbox}, invocation}
      {:empty, _inbox} -> {:empty, dispatcher}
      {:error, _reason} = error -> error
    end
  end

  @spec run(Invocation.t(), t()) :: {:ok, t()} | {:error, term()}
  def run(%Invocation{} = invocation, %__MODULE__{} = dispatcher) do
    case dispatcher.inbox.running do
      %Invocation{event_id: event_id} when event_id == invocation.event_id ->
        execute(invocation, dispatcher)

      _other ->
        {:error, {:invocation_not_running, invocation.event_id}}
    end
  end

  @spec run_all(t()) :: {:ok, t()} | {:error, term()}
  def run_all(%__MODULE__{} = dispatcher) do
    case start_next(dispatcher) do
      {:ok, running, invocation} ->
        with {:ok, next} <- run(invocation, running), do: run_all(next)

      {:empty, idle} ->
        {:ok, idle}

      {:error, _reason} = error ->
        error
    end
  end

  @spec completable?(t()) :: boolean()
  def completable?(%__MODULE__{} = dispatcher), do: SignalInbox.idle?(dispatcher.inbox)

  @doc """
  Signals are not transferred to the next run by Temporal automatically.

  A caller may Continue-As-New only when no handler is running or scheduled and
  no signal remains buffered, or it must explicitly encode buffered application
  messages into the next run's input.
  """
  @spec continue_as_new_ready?(t()) :: boolean()
  def continue_as_new_ready?(%__MODULE__{} = dispatcher), do: completable?(dispatcher)

  @spec pending_count(t()) :: non_neg_integer()
  def pending_count(%__MODULE__{} = dispatcher), do: SignalInbox.pending_count(dispatcher.inbox)

  @spec buffered(t()) :: [Signal.t()]
  def buffered(%__MODULE__{} = dispatcher), do: dispatcher.inbox.buffered

  @spec scheduled(t()) :: [Invocation.t()]
  def scheduled(%__MODULE__{} = dispatcher), do: dispatcher.inbox.scheduled

  defp dispatch(dispatcher, signal) do
    case Map.fetch(dispatcher.handlers, signal.signal_name) do
      {:ok, handler} ->
        accept(dispatcher, signal, {:schedule, handler})

      :error when is_function(dispatcher.dynamic_handler, 3) ->
        accept(dispatcher, signal, {:schedule, dispatcher.dynamic_handler})

      :error ->
        unknown(dispatcher, signal)
    end
  end

  defp unknown(%{unknown_signal: :buffer} = dispatcher, signal),
    do: accept(dispatcher, signal, :buffer)

  defp unknown(%{unknown_signal: :drop} = dispatcher, signal),
    do: accept(dispatcher, signal, :drop)

  defp unknown(%{unknown_signal: :fail}, signal) do
    {:error, {:unknown_signal, %{event_id: signal.event_id, signal_name: signal.signal_name}}}
  end

  defp accept(dispatcher, signal, destination) do
    case SignalInbox.accept(dispatcher.inbox, signal, destination) do
      {:ok, inbox, outcome} -> {:ok, %{dispatcher | inbox: inbox}, outcome}
      {:error, _reason} = error -> error
    end
  end

  defp execute(invocation, dispatcher) do
    context = %{
      event_id: invocation.event_id,
      signal_name: invocation.signal_name,
      headers: invocation.headers,
      identity: invocation.identity,
      request_id: invocation.request_id
    }

    case invocation.handler.(invocation.input, context, dispatcher.workflow_state) do
      {:ok, workflow_state} ->
        with {:ok, inbox} <- SignalInbox.finish(dispatcher.inbox, invocation.event_id) do
          {:ok, %{dispatcher | inbox: inbox, workflow_state: workflow_state}}
        end

      {:error, reason} ->
        {:error,
         {:signal_handler_failed,
          %{event_id: invocation.event_id, signal_name: invocation.signal_name, reason: reason}}}

      other ->
        {:error,
         {:invalid_signal_handler_result,
          %{event_id: invocation.event_id, signal_name: invocation.signal_name, result: other}}}
    end
  end
end
