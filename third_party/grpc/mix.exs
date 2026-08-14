defmodule GRPC.MixProject do
  use Mix.Project

  @source_url "https://github.com/elixir-grpc/grpc/tree/master/grpc"
  @version "1.0.3"

  def project do
    [
      app: :grpc,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "gRPC client implementation for Elixir",
      package: package(),
      docs: docs(),
      name: "gRPC Client",
      source_url: @source_url
    ]
  end

  def application do
    [
      mod: {GRPC.Client.Application, []},
      extra_applications: [:logger]
    ]
  end

  # Vendored in-repo copy of the pinned grpc 1.0.3 hex package with the
  # Temporal SDK's HTTP/2 PING keepalive patch applied. Only runtime deps are
  # retained; test and dev-only dependencies are omitted.
  defp deps do
    [
      {:grpc_core, "~> 1.0.3"},
      {:gun, "~> 2.4.0", optional: true},
      {:mint, "~> 1.9", optional: true},
      {:castore, "~> 1.0", optional: true}
    ]
  end

  defp package do
    %{
      maintainers: ["Adriano Santos", "Dave Lucia", "Bing Han", "Paulo Valente"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(mix.exs README.md lib config LICENSE .formatter.exs)
    }
  end

  defp docs do
    [
      main: "GRPC.Stub",
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end