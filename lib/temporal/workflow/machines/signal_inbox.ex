defmodule Temporal.Workflow.Machines.SignalInbox do
  @moduledoc """
  Pure ordered storage and single-lane scheduling for Workflow signals.

  The inbox has no process, clock, or transport dependencies. The same history
  event sequence therefore produces the same buffered and runnable sequence in
  live execution and replay.
  """

  alias Temporal.Workflow.Signal

  defmodule Invocation do
    @moduledoc "One signal handler invocation selected in history-event order."
    @enforce_keys [
      :event_id,
      :signal_name,
      :input,
      :headers,
      :identity,
      :request_id,
      :handler
    ]
    defstruct [
      :event_id,
      :signal_name,
      :input,
      :headers,
      :identity,
      :request_id,
      :handler
    ]

    @type t :: %__MODULE__{
            event_id: pos_integer(),
            signal_name: String.t(),
            input: term(),
            headers: struct() | nil,
            identity: String.t(),
            request_id: String.t(),
            handler: function()
          }
  end

  defstruct buffered: [],
            scheduled: [],
            running: nil,
            seen_event_ids: MapSet.new(),
            seen_request_ids: MapSet.new(),
            last_event_id: 0

  @type t :: %__MODULE__{}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec accept(t(), Signal.t(), :buffer | :drop | {:schedule, function()}) ::
          {:ok, t(), :buffered | :dropped | :scheduled | :duplicate} | {:error, term()}
  def accept(%__MODULE__{} = inbox, %Signal{} = signal, destination) do
    cond do
      MapSet.member?(inbox.seen_event_ids, signal.event_id) ->
        {:error, {:duplicate_event_id, signal.event_id}}

      signal.event_id < inbox.last_event_id ->
        {:error,
         {:out_of_order_signal_event,
          %{event_id: signal.event_id, last_event_id: inbox.last_event_id}}}

      duplicate_request?(inbox, signal.request_id) ->
        {:ok, remember(inbox, signal), :duplicate}

      true ->
        store(remember(inbox, signal), signal, destination)
    end
  end

  @spec promote(t(), (Signal.t() -> boolean()), function()) :: {t(), non_neg_integer()}
  def promote(%__MODULE__{} = inbox, predicate, handler)
      when is_function(predicate, 1) and is_function(handler, 3) do
    {selected, remaining} = Enum.split_with(inbox.buffered, predicate)
    promoted = Enum.map(selected, &invocation(&1, handler))

    next = %{
      inbox
      | buffered: remaining,
        scheduled: sort_by_event(inbox.scheduled ++ promoted)
    }

    {next, length(promoted)}
  end

  @spec start_next(t()) ::
          {:ok, t(), Invocation.t()} | {:empty, t()} | {:error, :handler_already_running}
  def start_next(%__MODULE__{running: %Invocation{}}),
    do: {:error, :handler_already_running}

  def start_next(%__MODULE__{scheduled: []} = inbox), do: {:empty, inbox}

  def start_next(%__MODULE__{scheduled: [next | rest]} = inbox) do
    {:ok, %{inbox | scheduled: rest, running: next}, next}
  end

  @spec finish(t(), pos_integer()) :: {:ok, t()} | {:error, {:invocation_not_running, integer()}}
  def finish(%__MODULE__{running: %Invocation{event_id: event_id}} = inbox, event_id),
    do: {:ok, %{inbox | running: nil}}

  def finish(%__MODULE__{}, event_id),
    do: {:error, {:invocation_not_running, event_id}}

  @spec pending_count(t()) :: non_neg_integer()
  def pending_count(%__MODULE__{} = inbox) do
    length(inbox.buffered) + length(inbox.scheduled) + if(inbox.running, do: 1, else: 0)
  end

  @spec idle?(t()) :: boolean()
  def idle?(%__MODULE__{} = inbox), do: pending_count(inbox) == 0

  defp store(inbox, signal, :buffer) do
    {:ok, %{inbox | buffered: sort_by_event([signal | inbox.buffered])}, :buffered}
  end

  defp store(inbox, _signal, :drop), do: {:ok, inbox, :dropped}

  defp store(inbox, signal, {:schedule, handler}) when is_function(handler, 3) do
    scheduled = sort_by_event([invocation(signal, handler) | inbox.scheduled])
    {:ok, %{inbox | scheduled: scheduled}, :scheduled}
  end

  defp invocation(%Signal{} = signal, handler) do
    struct!(Invocation, Map.from_struct(signal) |> Map.put(:handler, handler))
  end

  defp remember(inbox, signal) do
    request_ids =
      if signal.request_id == "" do
        inbox.seen_request_ids
      else
        MapSet.put(inbox.seen_request_ids, signal.request_id)
      end

    %{
      inbox
      | seen_event_ids: MapSet.put(inbox.seen_event_ids, signal.event_id),
        seen_request_ids: request_ids,
        last_event_id: max(inbox.last_event_id, signal.event_id)
    }
  end

  defp duplicate_request?(_inbox, ""), do: false

  defp duplicate_request?(inbox, request_id),
    do: MapSet.member?(inbox.seen_request_ids, request_id)

  defp sort_by_event(entries), do: Enum.sort_by(entries, & &1.event_id)
end
