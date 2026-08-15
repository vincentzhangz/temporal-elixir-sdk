defmodule Temporal.RPC.MintTransport do
  @moduledoc """
  Native pure-BEAM gRPC unary transport over Mint HTTP/2.

  Implements `Temporal.RPC.Transport` without the `grpc`/`grpc_core`
  dependencies. Supports only unary calls (the full surface the SDK uses),
  TLS/mTLS via `Temporal.Connection.Options`, HTTP/2 PING keepalive, per-call
  metadata, deadlines, and the 128 MiB message-size limit.

  A `Connection` GenServer owns the Mint HTTP/2 connection and routes response
  messages to the process that issued each request, so concurrent unary calls
  (including long polling) share one connection.
  """

  @behaviour Temporal.RPC.Transport

  alias Temporal.RPC.Error

  @content_type "application/grpc"
  @grpc_encoding "identity"

  defmodule Connection do
    @moduledoc false

    use GenServer

    def start_link(host, port, options) do
      GenServer.start_link(__MODULE__, {host, port, options})
    end

    def request(pid, path, headers, body, timeout) do
      GenServer.call(pid, {:request, path, headers, body, timeout}, timeout + 1_000)
    end

    def close(pid) do
      GenServer.call(pid, :close)
    catch
      :exit, _reason -> :ok
    end

    @impl true
    def init({host, port, options}) do
      conn_opts = connect_opts(host, options)
      scheme = if options.tls, do: :https, else: :http

      case Mint.HTTP.connect(scheme, host, port, conn_opts) do
        {:ok, conn} ->
          state = %{
            conn: conn,
            requests: %{},
            keepalive: options.keepalive,
            keepalive_ping_ref: nil,
            keepalive_ack_timer: nil,
            keepalive_ping_timer: nil
          }

          state = schedule_keepalive(state)
          {:ok, state}

        {:error, reason} ->
          {:stop, reason}
      end
    end

    defp connect_opts(host, options) do
      [
        protocols: [:http2],
        transport_opts: transport_opts(host, options),
        client_settings: [initial_window_size: 8_000_000, max_frame_size: 8_000_000],
        mode: :passive
      ]
    end

    defp transport_opts(host, %{tls: true} = options) do
      opts = options.tls_options

      opts =
        if Keyword.has_key?(opts, :server_name_indication) do
          opts
        else
          Keyword.put(opts, :server_name_indication, String.to_charlist(host))
        end

      Keyword.put_new(opts, :verify, :verify_peer)
    end

    defp transport_opts(_host, _options), do: [timeout: :infinity]

    @impl true
    def handle_call({:request, path, headers, body, _timeout}, from, state) do
      case Mint.HTTP.request(state.conn, "POST", path, headers, body) do
        {:ok, conn, ref} ->
          monitor = Process.monitor(elem(from, 0))

          requests =
            Map.put(state.requests, ref, %{
              from: from,
              monitor: monitor,
              data: "",
              trailers: nil
            })

          {:noreply, %{state | conn: conn, requests: requests}}

        {:error, conn, reason} ->
          {:reply, {:error, Error.from(reason)}, %{state | conn: conn}}
      end
    end

    def handle_call(:close, _from, state) do
      {:ok, conn} = Mint.HTTP.close(state.conn)
      {:stop, :normal, :ok, %{state | conn: conn}}
    end

    @impl true
    def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
      case Enum.find(state.requests, fn {_ref, req} -> req.monitor == monitor end) do
        {ref, _req} ->
          Process.demonitor(monitor, [:flush])
          requests = Map.delete(state.requests, ref)

          case Mint.HTTP2.cancel_request(state.conn, ref) do
            {:ok, conn} -> {:noreply, %{state | conn: conn, requests: requests}}
            {:error, conn, _reason} -> {:noreply, %{state | conn: conn, requests: requests}}
          end

        nil ->
          {:noreply, state}
      end
    end

    def handle_info({:mint, conn, _ref, _event} = message, state) do
      case Mint.HTTP.stream(conn, message) do
        {:ok, conn, responses} -> handle_stream(conn, responses, state)
        {:error, conn, _reason, _responses} -> {:noreply, %{state | conn: conn}}
      end
    end

    def handle_info(:keepalive_ping, state) do
      if state.keepalive do
        {:ok, conn, ref} = Mint.HTTP2.ping(state.conn)

        ack_timer =
          Process.send_after(self(), {:keepalive_ack_timeout, ref}, state.keepalive.timeout)

        state = %{
          state
          | conn: conn,
            keepalive_ping_ref: ref,
            keepalive_ack_timer: ack_timer
        }

        {:noreply, state}
      else
        {:noreply, state}
      end
    end

    def handle_info({:keepalive_ack_timeout, ref}, state) do
      if state.keepalive_ping_ref == ref do
        # No PONG within the timeout: close the connection.
        {:ok, conn} = Mint.HTTP.close(state.conn)
        {:stop, :normal, %{state | conn: conn}}
      else
        {:noreply, state}
      end
    end

    @impl true
    def handle_info(_message, state), do: {:noreply, state}

    defp handle_stream(conn, responses, state) do
      case reduce_stream(responses, state.requests) do
        {:ok, requests, replies} ->
          Enum.each(replies, fn {ref, result} ->
            %{from: from, monitor: monitor} = state.requests[ref]
            Process.demonitor(monitor, [:flush])
            GenServer.reply(from, result)
          end)

          {:noreply, %{state | conn: conn, requests: requests}}

        {:ok, requests} ->
          {:noreply, %{state | conn: conn, requests: requests}}
      end
    end

    defp reduce_stream(responses, requests) do
      Enum.reduce_while(responses, {:ok, requests, []}, fn response, {:ok, requests, replies} ->
        {:cont, step_response(response, requests, replies)}
      end)
      |> case do
        {:ok, requests, []} -> {:ok, requests}
        {:ok, requests, replies} -> {:ok, requests, replies}
      end
    end

    defp step_response({:data, ref, data}, requests, replies) do
      case Map.get(requests, ref) do
        nil -> {:ok, requests, replies}
        req -> {:ok, Map.put(requests, ref, %{req | data: req.data <> data}), replies}
      end
    end

    defp step_response({:trailers, ref, trailers}, requests, replies) do
      case Map.get(requests, ref) do
        nil -> {:ok, requests, replies}
        req -> {:ok, Map.put(requests, ref, %{req | trailers: trailers}), replies}
      end
    end

    defp step_response({:done, ref}, requests, replies) do
      case Map.pop(requests, ref) do
        {nil, _requests} ->
          {:ok, requests, replies}

        {%{data: data, trailers: trailers}, requests} ->
          {:ok, requests, [{ref, {:ok, data, trailers || %{}}} | replies]}
      end
    end

    defp step_response(_other, requests, replies), do: {:ok, requests, replies}

    defp schedule_keepalive(%{keepalive: nil} = state), do: state

    defp schedule_keepalive(%{keepalive: %{interval: interval}} = state) do
      timer = Process.send_after(self(), :keepalive_ping, interval)
      %{state | keepalive_ping_timer: timer}
    end
  end

  def connect(options, _raw_options) do
    with {:ok, host, port} <- parse_target(options.target),
         {:ok, pid} <- Connection.start_link(host, port, options) do
      {:ok,
       %{
         conn_pid: pid,
         host: host,
         tls: options.tls,
         keepalive: options.keepalive,
         max_message_size: options.max_message_size,
         default_deadline: options.default_deadline
       }}
    else
      {:error, reason} -> {:error, Error.from(reason)}
    end
  end

  @impl true
  def unary(
        %{conn_pid: pid, host: host, tls: tls, max_message_size: max_size} = transport,
        method,
        request,
        options
      ) do
    timeout = Keyword.get(options, :timeout, transport.default_deadline)
    metadata = Keyword.get(options, :metadata, %{})
    scheme = if tls, do: "https", else: "http"

    with :ok <- within_limit(request, max_size, "request") do
      body = frame(request)

      case Connection.request(pid, method, request_headers(host, scheme, metadata), body, timeout) do
        {:ok, data, trailers} -> handle_response(data, trailers, max_size)
        {:error, _} = error -> error
      end
    end
  end

  defp handle_response(data, trailers, max_size) do
    case status_code(trailers) do
      0 -> unframe_response(data, max_size)
      status -> {:error, Error.from(status, grpc_message(trailers))}
    end
  end

  def close(%{conn_pid: pid}) do
    Connection.close(pid)
  end

  defp parse_target(target) do
    target =
      target
      |> String.replace_prefix("dns://", "")
      |> String.replace_prefix("ipv4:", "")
      |> String.replace_prefix("ipv6:", "")

    case String.split(target, ":", parts: 2) do
      [host, port] when host != "" and port != "" ->
        case Integer.parse(port) do
          {port, ""} when port > 0 -> {:ok, host, port}
          _ -> {:error, :invalid_target}
        end

      _ ->
        {:error, :invalid_target}
    end
  end

  defp request_headers(host, scheme, metadata) do
    headers = [
      {":method", "POST"},
      {":scheme", scheme},
      {":authority", host},
      {"content-type", @content_type},
      {"te", "trailers"},
      {"grpc-encoding", @grpc_encoding},
      {"grpc-accept-encoding", @grpc_encoding},
      {"user-agent", "temporal-elixir-community/#{Temporal.SDKMetadata.version()} grpc-elixir"}
    ]

    Enum.reduce(metadata, headers, fn {key, value}, acc ->
      [{String.downcase(to_string(key)), to_string(value)} | acc]
    end)
  end

  defp frame(data) do
    <<0::8, byte_size(data)::32, data::binary>>
  end

  defp unframe(<<0::8, length::32, data::binary>> = _frame) do
    <<message::binary-size(^length), rest::binary>> = data
    {:ok, message, rest}
  end

  defp unframe(_data), do: {:error, :invalid_grpc_frame}

  defp unframe_response(data, max_size) when byte_size(data) <= max_size, do: unframe(data)
  defp unframe_response(_data, _max_size), do: {:error, :response_too_large}

  defp status_code(trailers) do
    case header_value(trailers, "grpc-status") do
      nil -> 0
      code when is_binary(code) -> String.to_integer(code)
      code when is_integer(code) -> code
    end
  end

  defp grpc_message(trailers), do: header_value(trailers, "grpc-message")

  defp header_value(trailers, name) do
    Enum.find_value(trailers || %{}, fn {key, value} ->
      if to_string(key) == name, do: value
    end)
  end

  defp within_limit(bytes, limit, _direction) when byte_size(bytes) <= limit, do: :ok

  defp within_limit(_bytes, _limit, direction),
    do:
      {:error,
       %Error{status: :resource_exhausted, message: "#{direction} exceeds message size limit"}}
end
