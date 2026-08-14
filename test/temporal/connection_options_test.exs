defmodule Temporal.ConnectionOptionsTest do
  use ExUnit.Case, async: true

  alias Temporal.Connection.Options

  test "defaults local targets to plaintext and supplies namespace and identity" do
    assert {:ok, options} = Options.new(target: "localhost:7233")
    refute options.tls
    assert options.namespace == "default"
    assert is_binary(options.identity)
    assert options.default_deadline == 10_000
  end

  test "defaults non-local targets to verified TLS using CAStore" do
    assert {:ok, options} = Options.new(target: "example.tmprl.cloud:7233")
    assert options.tls
    assert options.tls_options[:verify] == :verify_peer
    assert is_list(options.tls_options[:cacerts])
  end

  test "defaults max_message_size to 128 MiB and connect_timeout to 15s" do
    assert {:ok, options} = Options.new(target: "localhost:7233")
    assert options.max_message_size == 128 * 1024 * 1024
    assert options.connect_timeout == 15_000
  end

  test "overrides max_message_size and connect_timeout" do
    assert {:ok, options} =
             Options.new(target: "localhost:7233", max_message_size: 1024, connect_timeout: 5_000)

    assert options.max_message_size == 1024
    assert options.connect_timeout == 5_000

    assert {:error, {:invalid_options, :connect_timeout}} =
             Options.new(target: "localhost:7233", connect_timeout: 0)
  end

  test "normalizes keepalive to a map and validates fields" do
    assert {:ok, options} =
             Options.new(target: "localhost:7233", keepalive: [interval: 60_000, timeout: 20_000])

    assert options.keepalive == %{interval: 60_000, timeout: 20_000}

    assert {:ok, options} =
             Options.new(
               target: "localhost:7233",
               keepalive: %{interval: 30_000, timeout: 10_000}
             )

    assert options.keepalive == %{interval: 30_000, timeout: 10_000}

    assert {:ok, options} = Options.new(target: "localhost:7233", keepalive: %{})
    assert options.keepalive == %{}
  end

  test "rejects incomplete mTLS and invalid limits" do
    assert {:error, {:invalid_options, :mtls_requires_cert_and_key}} =
             Options.new(target: "cloud.example:7233", client_cert: "cert")

    assert {:error, {:invalid_options, :max_message_size}} =
             Options.new(target: "localhost:7233", max_message_size: 0)
  end

  test "accepts PEM-bytes mTLS and decodes cert and key to DER" do
    {cert_pem, key_pem} = self_signed_cert()
    {cert_der, key_der} = pem_to_der(cert_pem, key_pem)

    assert {:ok, options} =
             Options.new(
               target: "localhost:7233",
               tls: true,
               client_cert: cert_pem,
               client_key: key_pem
             )

    assert options.tls_options[:cert] == cert_der
    assert {:RSAPrivateKey, ^key_der} = options.tls_options[:key]
  end

  test "uses custom CA cert and server name for verification" do
    {cert_pem, _key_pem} = self_signed_cert()
    cert_der = :public_key.pem_decode(cert_pem) |> hd() |> elem(1)

    assert {:ok, options} =
             Options.new(
               target: "localhost:7233",
               tls: true,
               ca_cert: cert_pem,
               server_name: "temporal.example"
             )

    assert options.tls_options[:cacerts] == [cert_der]
    assert options.tls_options[:server_name_indication] == ~c"temporal.example"
  end

  test "verify_none disables peer verification without CA roots" do
    assert {:ok, options} =
             Options.new(target: "localhost:7233", tls: true, verify: :verify_none)

    assert options.tls_options[:verify] == :verify_none
    refute Keyword.has_key?(options.tls_options, :cacerts)

    assert {:error, {:invalid_options, :verify}} =
             Options.new(target: "localhost:7233", tls: true, verify: :verify_bogus)
  end

  test "refreshes API key and metadata providers for every call" do
    Process.put(:credential_version, 1)

    assert {:ok, options} =
             Options.new(
               target: "localhost:7233",
               api_key: fn -> "key-#{Process.get(:credential_version)}" end,
               metadata: fn ->
                 %{"trace-id" => Integer.to_string(Process.get(:credential_version))}
               end
             )

    assert Options.metadata(options) ==
             %{
               "authorization" => "Bearer key-1",
               "client-name" => "temporal-elixir-community",
               "client-version" => Mix.Project.config()[:version],
               "trace-id" => "1"
             }

    Process.put(:credential_version, 2)

    assert Options.metadata(options) ==
             %{
               "authorization" => "Bearer key-2",
               "client-name" => "temporal-elixir-community",
               "client-version" => Mix.Project.config()[:version],
               "trace-id" => "2"
             }
  end

  defp self_signed_cert do
    data = :public_key.pkix_test_data(%{root: [], peer: [{:key, {:rsa, 2048, 65_537}}]})
    cert_pem = :public_key.pem_encode([{:Certificate, data[:cert], :not_encrypted}])
    {type, der} = data[:key]
    key_pem = :public_key.pem_encode([{type, der, :not_encrypted}])
    {cert_pem, key_pem}
  end

  defp pem_to_der(cert_pem, key_pem) do
    cert_der = :public_key.pem_decode(cert_pem) |> hd() |> elem(1)
    key_der = :public_key.pem_decode(key_pem) |> hd() |> elem(1)
    {cert_der, key_der}
  end
end
