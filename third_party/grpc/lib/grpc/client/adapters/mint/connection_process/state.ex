if Code.ensure_loaded?(Mint.HTTP) do
  defmodule GRPC.Client.Adapters.Mint.ConnectionProcess.State do
    @moduledoc false

    defstruct [
      :conn,
      :parent,
      :scheme,
      :host,
      :port,
      :connect_opts,
      :retry_timeout_ms,
      :keepalive,
      :keepalive_ping_ref,
      :keepalive_ping_timer,
      :keepalive_ack_timer,
      requests: %{},
      request_stream_queue: :queue.new(),
      retry: 0,
      retry_attempt: 0
    ]

    @type keepalive :: %{interval: pos_integer(), timeout: pos_integer()} | nil
    @type t :: %__MODULE__{
            conn: Mint.HTTP.t(),
            requests: map(),
            parent: pid(),
            scheme: Mint.Types.scheme() | nil,
            host: Mint.Types.address() | nil,
            port: :inet.port_number() | nil,
            connect_opts: keyword(),
            retry_timeout_ms: non_neg_integer() | nil,
            keepalive: keepalive(),
            keepalive_ping_ref: Mint.Types.request_ref() | nil,
            keepalive_ping_timer: reference() | nil,
            keepalive_ack_timer: reference() | nil,
            retry: non_neg_integer(),
            retry_attempt: non_neg_integer()
          }

    def new(conn, opts) do
      %__MODULE__{
        conn: conn,
        request_stream_queue: :queue.new(),
        parent: opts[:parent],
        scheme: opts[:scheme],
        host: opts[:host],
        port: opts[:port],
        connect_opts: opts[:connect_opts] || [],
        keepalive: opts[:keepalive],
        retry: opts[:retry] || 0
      }
    end

    def update_conn(state, conn) do
      %{state | conn: conn}
    end

    def keepalive_enabled?(state), do: not is_nil(state.keepalive)

    def put_keepalive_ping(state, ref, ack_timer) do
      %{state | keepalive_ping_ref: ref, keepalive_ping_timer: nil, keepalive_ack_timer: ack_timer}
    end

    def clear_keepalive_ping(state) do
      if state.keepalive_ack_timer,
        do: Process.cancel_timer(state.keepalive_ack_timer, async: true, info: false)

      %{state | keepalive_ping_ref: nil, keepalive_ack_timer: nil}
    end

    def set_keepalive_ping_timer(state, timer) do
      %{state | keepalive_ping_timer: timer}
    end

    def clear_keepalive_timers(state) do
      if state.keepalive_ping_timer,
        do: Process.cancel_timer(state.keepalive_ping_timer, async: true, info: false)

      if state.keepalive_ack_timer,
        do: Process.cancel_timer(state.keepalive_ack_timer, async: true, info: false)

      %{state | keepalive_ping_ref: nil, keepalive_ping_timer: nil, keepalive_ack_timer: nil}
    end

    def keepalive_matching_ping?(state, ref), do: state.keepalive_ping_ref == ref

    def update_request_stream_queue(state, queue) do
      %{state | request_stream_queue: queue}
    end

    def put_empty_ref_state(state, ref, response_pid) do
      put_in(state.requests[ref], %{
        stream_response_pid: response_pid,
        done: false,
        response: %{}
      })
    end

    def update_response_status(state, ref, status) do
      put_in(state.requests[ref].response[:status], status)
    end

    def update_response_headers(state, ref, headers) do
      put_in(state.requests[ref].response[:headers], headers)
    end

    def empty_headers?(state, ref) do
      is_nil(state.requests[ref].response[:headers])
    end

    def stream_response_pid(state, ref) do
      state.requests[ref].stream_response_pid
    end

    defguard has_request_ref(state, ref) when is_map_key(state.requests, ref)

    def pop_ref(state, ref) do
      pop_in(state.requests[ref])
    end

    def append_response_data(state, ref, new_data) do
      update_in(state.requests[ref].response[:data], fn data -> (data || "") <> new_data end)
    end
  end
end
