defmodule Temporal.WorkflowSignalDispatcherTest do
  use ExUnit.Case, async: true

  alias Temporal.Api.Common.V1.{Header, Payload, Payloads}

  alias Temporal.Api.History.V1.{
    HistoryEvent,
    WorkflowExecutionSignaledEventAttributes
  }

  alias Temporal.Workflow.Signal.Dispatcher

  defp handler(tag) do
    fn value, context, state ->
      {:ok,
       Map.update(state, tag, [{context.event_id, context.signal_name, value}], fn entries ->
         entries ++ [{context.event_id, context.signal_name, value}]
       end)}
    end
  end

  test "buffers before registration then schedules matching signals in event order" do
    dispatcher = Dispatcher.new(state: %{})

    assert {:ok, dispatcher, :buffered} =
             Dispatcher.ingest(dispatcher, signal(7, "deposit", 10), :live)

    assert {:ok, dispatcher, :buffered} =
             Dispatcher.ingest(dispatcher, signal(9, "deposit", 20), :live)

    assert Dispatcher.pending_count(dispatcher) == 2
    refute Dispatcher.completable?(dispatcher)

    assert {:ok, dispatcher, 2} =
             Dispatcher.register(dispatcher, "deposit", handler(:deposits))

    assert {:ok, dispatcher, first} = Dispatcher.start_next(dispatcher)
    assert first.event_id == 7
    assert first.headers.fields["trace"].data == "trace-7"
    assert {:ok, dispatcher} = Dispatcher.run(first, dispatcher)

    assert {:ok, dispatcher, second} = Dispatcher.start_next(dispatcher)
    assert second.event_id == 9
    assert {:ok, dispatcher} = Dispatcher.run(second, dispatcher)

    assert dispatcher.workflow_state == %{
             deposits: [{7, "deposit", 10}, {9, "deposit", 20}]
           }

    assert Dispatcher.completable?(dispatcher)
  end

  test "uses named handlers before dynamic fallback and replacement affects future signals" do
    assert {:ok, dispatcher, 0} =
             Dispatcher.new(state: %{})
             |> Dispatcher.register_dynamic(handler(:dynamic))

    assert {:ok, dispatcher, 0} =
             Dispatcher.register(dispatcher, "known", handler(:named))

    assert {:ok, dispatcher, :scheduled} =
             Dispatcher.ingest(dispatcher, signal(1, "known", "one"), :replay)

    assert {:ok, dispatcher, :scheduled} =
             Dispatcher.ingest(dispatcher, signal(2, "other", "two"), :replay)

    assert {:ok, dispatcher, _old_handler} =
             Dispatcher.register(dispatcher, "known", handler(:replacement))

    assert {:ok, dispatcher, :scheduled} =
             Dispatcher.ingest(dispatcher, signal(3, "known", "three"), :replay)

    assert {:ok, dispatcher} = Dispatcher.run_all(dispatcher)

    assert dispatcher.workflow_state == %{
             named: [{1, "known", "one"}],
             dynamic: [{2, "other", "two"}],
             replacement: [{3, "known", "three"}]
           }
  end

  test "removing a named handler exposes the dynamic fallback" do
    assert {:ok, dispatcher, 0} =
             Dispatcher.new(state: %{})
             |> Dispatcher.register_dynamic(handler(:dynamic))

    assert {:ok, dispatcher, 0} =
             Dispatcher.register(dispatcher, "known", handler(:named))

    assert {:ok, dispatcher, _removed} = Dispatcher.remove(dispatcher, "known")

    assert {:ok, dispatcher, :scheduled} =
             Dispatcher.ingest(dispatcher, signal(1, "known", "fallback"), :live)

    assert {:ok, dispatcher} = Dispatcher.run_all(dispatcher)
    assert dispatcher.workflow_state.dynamic == [{1, "known", "fallback"}]
  end

  test "supports explicit unknown-signal buffer drop and fail policies" do
    for mode <- [:live, :replay] do
      assert {:ok, buffered, :buffered} =
               Dispatcher.new(unknown_signal: :buffer)
               |> Dispatcher.ingest(signal(1, "unknown", nil), mode)

      assert Dispatcher.pending_count(buffered) == 1

      assert {:ok, dropped, :dropped} =
               Dispatcher.new(unknown_signal: :drop)
               |> Dispatcher.ingest(signal(1, "unknown", nil), mode)

      assert Dispatcher.pending_count(dropped) == 0

      assert {:error, {:unknown_signal, %{event_id: 1, signal_name: "unknown"}}} =
               Dispatcher.new(unknown_signal: :fail)
               |> Dispatcher.ingest(signal(1, "unknown", nil), mode)
    end
  end

  test "rejects duplicate event IDs and idempotently ignores duplicate non-empty request IDs" do
    assert {:ok, dispatcher, 0} =
             Dispatcher.new()
             |> Dispatcher.register("signal", handler(:signals))

    assert {:ok, dispatcher, :scheduled} =
             Dispatcher.ingest(dispatcher, signal(4, "signal", 1, request_id: "request-1"), :live)

    assert {:error, {:duplicate_event_id, 4}} =
             Dispatcher.ingest(dispatcher, signal(4, "signal", 1, request_id: "request-2"), :live)

    assert {:ok, dispatcher, :duplicate} =
             Dispatcher.ingest(
               dispatcher,
               signal(5, "signal", 1, request_id: "request-1"),
               :live
             )

    assert Dispatcher.pending_count(dispatcher) == 1
  end

  test "payload conversion is injectable and malformed payloads do not mutate the inbox" do
    decoder = fn
      %Payloads{payloads: [%Payload{data: "ok"}]} -> {:ok, :converted}
      _payloads -> {:error, :malformed}
    end

    assert {:ok, dispatcher, 0} =
             Dispatcher.new(decoder: decoder, state: %{})
             |> Dispatcher.register("signal", handler(:signals))

    assert {:error, {:payload_conversion_failed, %{event_id: 1, reason: :malformed}}} =
             Dispatcher.ingest(dispatcher, raw_signal(1, "signal", "bad"), :replay)

    assert Dispatcher.pending_count(dispatcher) == 0

    assert {:ok, dispatcher, :scheduled} =
             Dispatcher.ingest(dispatcher, raw_signal(2, "signal", "ok"), :replay)

    assert {:ok, dispatcher} = Dispatcher.run_all(dispatcher)
    assert dispatcher.workflow_state.signals == [{2, "signal", :converted}]
  end

  test "running handlers and buffered signals block completion and continue-as-new readiness" do
    assert {:ok, dispatcher, 0} =
             Dispatcher.new()
             |> Dispatcher.register("known", handler(:known))

    assert {:ok, dispatcher, :scheduled} =
             Dispatcher.ingest(dispatcher, signal(1, "known", 1), :live)

    assert {:ok, dispatcher, invocation} = Dispatcher.start_next(dispatcher)
    refute Dispatcher.completable?(dispatcher)
    refute Dispatcher.continue_as_new_ready?(dispatcher)

    assert {:ok, dispatcher} = Dispatcher.run(invocation, dispatcher)
    assert Dispatcher.continue_as_new_ready?(dispatcher)

    assert {:ok, dispatcher, :buffered} =
             Dispatcher.ingest(dispatcher, signal(2, "later", 2), :live)

    refute Dispatcher.continue_as_new_ready?(dispatcher)
    assert Enum.map(Dispatcher.buffered(dispatcher), & &1.event_id) == [2]
  end

  test "live and replay ingest produce equivalent deterministic scheduling" do
    build = fn mode ->
      assert {:ok, dispatcher, 0} =
               Dispatcher.new(state: %{})
               |> Dispatcher.register_dynamic(handler(:seen))

      Enum.reduce([signal(3, "a", 1), signal(5, "b", 2)], dispatcher, fn event, acc ->
        assert {:ok, next, :scheduled} = Dispatcher.ingest(acc, event, mode)
        next
      end)
    end

    live = build.(:live)
    replay = build.(:replay)

    assert Enum.map(Dispatcher.scheduled(live), &{&1.event_id, &1.signal_name, &1.input}) ==
             Enum.map(Dispatcher.scheduled(replay), &{&1.event_id, &1.signal_name, &1.input})
  end

  test "serial scheduler rejects the wrong or repeated invocation completion" do
    assert {:ok, dispatcher, 0} =
             Dispatcher.new()
             |> Dispatcher.register("signal", handler(:signals))

    assert {:ok, dispatcher, :scheduled} =
             Dispatcher.ingest(dispatcher, signal(1, "signal", 1), :live)

    assert {:ok, running, invocation} = Dispatcher.start_next(dispatcher)
    wrong = %{invocation | event_id: 99}

    assert {:error, {:invocation_not_running, 99}} = Dispatcher.run(wrong, running)
    assert {:ok, finished} = Dispatcher.run(invocation, running)
    assert {:error, {:invocation_not_running, 1}} = Dispatcher.run(invocation, finished)
  end

  test "property: arbitrary monotonic signal streams preserve FIFO delivery" do
    for seed <- 1..50 do
      :rand.seed(:exsss, {seed, seed * 2, seed * 3})
      count = :rand.uniform(25)

      {events, _last_id} =
        Enum.map_reduce(1..count, 0, fn value, last_id ->
          event_id = last_id + :rand.uniform(4)
          {signal(event_id, "item", value), event_id}
        end)

      dispatcher =
        Enum.reduce(events, Dispatcher.new(state: %{}), fn event, dispatcher ->
          assert {:ok, next, :buffered} = Dispatcher.ingest(dispatcher, event, :replay)
          next
        end)

      assert {:ok, dispatcher, ^count} =
               Dispatcher.register(dispatcher, "item", handler(:items))

      assert {:ok, dispatcher} = Dispatcher.run_all(dispatcher)

      assert Enum.map(dispatcher.workflow_state.items, fn {_id, _name, value} -> value end) ==
               Enum.to_list(1..count)
    end
  end

  defp signal(event_id, name, value, options \\ []) do
    payloads = Temporal.Payload.encode(value)
    request_id = Keyword.get(options, :request_id, "")

    history_signal(event_id, name, payloads, request_id)
  end

  defp raw_signal(event_id, name, data) do
    history_signal(event_id, name, %Payloads{payloads: [%Payload{data: data}]}, "")
  end

  defp history_signal(event_id, name, input, request_id) do
    %HistoryEvent{
      event_id: event_id,
      event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
      attributes:
        {:workflow_execution_signaled_event_attributes,
         %WorkflowExecutionSignaledEventAttributes{
           signal_name: name,
           input: input,
           identity: "test-client",
           request_id: request_id,
           header: %Header{
             fields: %{"trace" => %Payload{data: "trace-#{event_id}"}}
           }
         }}
    }
  end
end
