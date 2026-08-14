defmodule Temporal.Connection.Options do
  @moduledoc """
  Validated immutable connection configuration.

  Credential provider functions are intentionally evaluated for each RPC so
  short-lived API keys can be refreshed without recreating a connection.

  Supported options:

    * `:target` - host and port, e.g. `"localhost:7233"` (default)
    * `:namespace` - Temporal namespace (default `"default"`)
    * `:identity` - worker/client identity (defaults to `pid@node`)
    * `:tls` - enable TLS (defaults to `true` for non-local targets)
    * `:tls_options` - extra `:ssl` options merged last
    * `:ca_cert` / `:ca_certs` - PEM-encoded CA certificate(s) to trust
      instead of the system store
    * `:server_name` - override the SNI and hostname-verification name
    * `:verify` - `:verify_peer` (default) or `:verify_none` (opt-in, insecure)
    * `:client_cert` / `:client_key` - PEM-encoded mTLS credentials (both
      required together)
    * `:api_key` - string or zero-arity provider; sent as `authorization: Bearer`
    * `:metadata` - map or zero-arity provider of extra headers
    * `:default_deadline` - per-RPC timeout in ms (default 10_000)
    * `:connect_timeout` - connection establishment timeout in ms (default 15_000)
    * `:keepalive` - `[interval: ms, timeout: ms]` HTTP/2 PING keepalive
    * `:max_message_size` - request/response size limit in bytes
      (default 128 MiB)
    * `:retry_policy` - optional `Temporal.RPC.RetryPolicy`
  """

  @enforce_keys [:target, :namespace, :identity]
  defstruct target: nil,
            namespace: "default",
            identity: nil,
            tls: false,
            tls_options: [],
            api_key: nil,
            metadata: %{},
            default_deadline: 10_000,
            connect_timeout: 15_000,
            keepalive: nil,
            max_message_size: 128 * 1024 * 1024,
            retry_policy: nil

  @type provider(value) :: value | (-> value)
  @type keepalive :: %{interval: pos_integer(), timeout: pos_integer()} | nil
  @type t :: %__MODULE__{}

  @spec new(keyword()) :: {:ok, t()} | {:error, {:invalid_options, atom()}}
  def new(options) when is_list(options) do
    target = Keyword.get(options, :target, "localhost:7233")
    namespace = Keyword.get(options, :namespace, "default")
    identity = Keyword.get_lazy(options, :identity, &default_identity/0)
    max_message_size = Keyword.get(options, :max_message_size, 128 * 1024 * 1024)
    deadline = Keyword.get(options, :default_deadline, 10_000)
    connect_timeout = Keyword.get(options, :connect_timeout, 15_000)
    keepalive = normalize_keepalive(Keyword.get(options, :keepalive))

    with :ok <- validate_string(target, :target),
         :ok <- validate_string(namespace, :namespace),
         :ok <- validate_string(identity, :identity),
         :ok <- validate_positive(max_message_size, :max_message_size),
         :ok <- validate_positive(deadline, :default_deadline),
         :ok <- validate_positive(connect_timeout, :connect_timeout),
         :ok <- validate_mtls(options),
         :ok <- validate_verify(options) do
      tls = Keyword.get(options, :tls, tls_default?(target))

      {:ok,
       %__MODULE__{
         target: target,
         namespace: namespace,
         identity: identity,
         tls: tls,
         tls_options: tls_options(tls, options),
         api_key: Keyword.get(options, :api_key),
         metadata: Keyword.get(options, :metadata, %{}),
         default_deadline: deadline,
         connect_timeout: connect_timeout,
         keepalive: keepalive,
         max_message_size: max_message_size,
         retry_policy: Keyword.get(options, :retry_policy)
       }}
    end
  end

  @spec metadata(t()) :: map()
  def metadata(%__MODULE__{} = options) do
    metadata =
      options.metadata
      |> resolve()
      |> Map.merge(Temporal.SDKMetadata.headers())

    case resolve(options.api_key) do
      nil -> metadata
      key when is_binary(key) -> Map.put(metadata, "authorization", "Bearer " <> key)
    end
  end

  defp resolve(provider) when is_function(provider, 0), do: provider.()
  defp resolve(value), do: value

  defp validate_string(value, _key) when is_binary(value) and byte_size(value) > 0, do: :ok
  defp validate_string(_value, key), do: {:error, {:invalid_options, key}}
  defp validate_positive(value, _key) when is_integer(value) and value > 0, do: :ok
  defp validate_positive(_value, key), do: {:error, {:invalid_options, key}}

  defp validate_mtls(options) do
    case {Keyword.has_key?(options, :client_cert), Keyword.has_key?(options, :client_key)} do
      {false, false} -> :ok
      {true, true} -> :ok
      _ -> {:error, {:invalid_options, :mtls_requires_cert_and_key}}
    end
  end

  defp validate_verify(options) do
    case Keyword.get(options, :verify, :verify_peer) do
      verify when verify in [:verify_peer, :verify_none] -> :ok
      _ -> {:error, {:invalid_options, :verify}}
    end
  end

  defp normalize_keepalive(nil), do: nil

  defp normalize_keepalive(interval: interval, timeout: timeout)
       when is_integer(interval) and interval > 0 and is_integer(timeout) and timeout > 0 do
    %{interval: interval, timeout: timeout}
  end

  defp normalize_keepalive(%{interval: interval, timeout: timeout})
       when is_integer(interval) and interval > 0 and is_integer(timeout) and timeout > 0 do
    %{interval: interval, timeout: timeout}
  end

  defp normalize_keepalive(_other), do: %{}

  defp tls_default?(target) do
    host = target |> String.replace_prefix("dns://", "") |> String.split(":") |> hd()
    host not in ["localhost", "127.0.0.1", "::1"]
  end

  defp tls_options(false, _options), do: []

  defp tls_options(true, options) do
    verify = Keyword.get(options, :verify, :verify_peer)
    server_name = Keyword.get(options, :server_name)
    cacerts = ca_certs(options, verify)

    base = [verify: verify] |> maybe_put_cacerts(cacerts) |> maybe_put_server_name(server_name)

    base
    |> maybe_put_client_cert(options)
    |> maybe_put_client_key(options)
    |> Keyword.merge(Keyword.get(options, :tls_options, []))
  end

  defp ca_certs(options, :verify_none) do
    if Keyword.has_key?(options, :ca_cert) or Keyword.has_key?(options, :ca_certs) do
      ca_certs(options, :verify_peer)
    else
      []
    end
  end

  defp ca_certs(options, :verify_peer) do
    cond do
      Keyword.has_key?(options, :ca_certs) ->
        options[:ca_certs]
        |> List.wrap()
        |> Enum.map(&decode_cert_pem/1)

      Keyword.has_key?(options, :ca_cert) ->
        [decode_cert_pem(options[:ca_cert])]

      true ->
        :public_key.cacerts_get()
    end
  end

  defp maybe_put_cacerts(opts, []), do: opts
  defp maybe_put_cacerts(opts, cacerts), do: Keyword.put(opts, :cacerts, cacerts)

  defp maybe_put_server_name(opts, nil), do: opts

  defp maybe_put_server_name(opts, name),
    do: Keyword.put(opts, :server_name_indication, to_charlist(name))

  defp maybe_put_client_cert(opts, options) do
    case options[:client_cert] do
      nil -> opts
      pem -> Keyword.put(opts, :cert, decode_cert_pem(pem))
    end
  end

  defp maybe_put_client_key(opts, options) do
    case options[:client_key] do
      nil -> opts
      pem -> Keyword.put(opts, :key, decode_key_pem(pem))
    end
  end

  defp decode_cert_pem(pem) when is_binary(pem) do
    case :public_key.pem_decode(pem) do
      [{_type, der, _}] -> der
      _ -> raise ArgumentError, "invalid certificate PEM"
    end
  end

  defp decode_key_pem(pem) when is_binary(pem) do
    case :public_key.pem_decode(pem) do
      [{type, der, _}] when type in [:RSAPrivateKey, :DSAPrivateKey, :ECPrivateKey] ->
        {type, der}

      [{:PrivateKeyInfo, der, _}] ->
        {:PrivateKeyInfo, der}

      _ ->
        raise ArgumentError, "invalid client_key PEM"
    end
  end

  defp default_identity do
    "#{System.pid()}@#{node()}"
  end
end
