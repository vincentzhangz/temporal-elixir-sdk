defmodule Temporal.GRPCKeepaliveTest do
  use ExUnit.Case

  alias GRPC.Client.Adapters.Mint.ConnectionProcess.State

  describe "ConnectionProcess keepalive state" do
    test "tracks ping refs and cancels ack timers" do
      {ref, ack_timer} = {make_ref(), make_ref()}
      state = State.new(%{mint: :conn}, keepalive: %{interval: 100, timeout: 50})

      assert State.keepalive_enabled?(state)
      refute State.keepalive_matching_ping?(state, ref)

      pinged = State.put_keepalive_ping(state, ref, ack_timer)
      assert State.keepalive_matching_ping?(pinged, ref)

      cleared = State.clear_keepalive_ping(pinged)
      refute State.keepalive_matching_ping?(cleared, ref)
      assert cleared.keepalive_ack_timer == nil
    end

    test "keepalive is disabled when option is absent" do
      state = State.new(%{mint: :conn}, [])
      refute State.keepalive_enabled?(state)
      assert State.clear_keepalive_timers(state) == state
    end

    test "clear_keepalive_timers drops pending ping and ack timers" do
      state = State.new(%{mint: :conn}, keepalive: %{interval: 100, timeout: 50})
      state = State.set_keepalive_ping_timer(state, make_ref())
      state = State.put_keepalive_ping(state, make_ref(), make_ref())

      cleared = State.clear_keepalive_timers(state)
      assert cleared.keepalive_ping_timer == nil
      assert cleared.keepalive_ack_timer == nil
      assert cleared.keepalive_ping_ref == nil
    end
  end

  @tag :live_server
  @tag timeout: 60_000
  test "keeps a keepalive-enabled connection healthy across idle pings" do
    address = System.fetch_env!("TEMPORAL_ADDRESS")

    {:ok, connection} =
      Temporal.Connection.open(
        target: address,
        namespace: System.get_env("TEMPORAL_NAMESPACE", "default"),
        tls: System.get_env("TEMPORAL_TLS") == "true",
        api_key: System.get_env("TEMPORAL_API_KEY"),
        keepalive: [interval: 1_000, timeout: 3_000]
      )

    on_exit(fn ->
      if Process.alive?(connection), do: Temporal.Connection.close(connection)
    end)

    assert {:ok, %{server_version: version}} = Temporal.RPC.system_info(connection)
    assert version != ""

    Process.sleep(3_500)

    assert {:ok, %{server_version: ^version}} = Temporal.RPC.system_info(connection)
  end
end
