defmodule Temporal.MixProject do
  use Mix.Project

  def project do
    [
      app: :temporal,
      version: "0.0.1-dev.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      aliases: aliases(),
      dialyzer: [plt_add_apps: [:mix]]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Temporal.Application, []}
    ]
  end

  def cli do
    [preferred_envs: [quality: :test]]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:protobuf, "== 0.17.0"},
      {:mint, "== 1.9.3"},
      {:castore, "== 1.0.21"},
      {:jason, "== 1.4.5"},
      {:telemetry, "~> 1.0"},
      {:credo, "== 1.7.19", only: [:dev, :test], runtime: false},
      {:dialyxir, "== 1.4.7", only: [:dev], runtime: false},
      {:ex_doc, "== 0.40.3", only: :dev, runtime: false}
    ]
  end

  defp description do
    """
    Independent, unofficial community Temporal SDK for Elixir. This package is not
    developed, maintained, supported, or endorsed by Temporal Technologies.
    """
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"Source" => "https://github.com/vincentzhangz/temporal-elixir-sdk"},
      files:
        ~w(lib proto scripts docs README.md LICENSE NOTICE CHANGELOG.md CONTRIBUTING.md GOVERNANCE.md SECURITY.md THIRD_PARTY_NOTICES.md mix.exs)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md", "docs/compatibility.md", "docs/features.md"],
      skip_undefined_reference_warnings_on: [
        "Temporal.Api.Command.V1.Command",
        "Temporal.Api.Command.V1.Command.t/0",
        "Temporal.Api.Common.V1.Payloads",
        "Temporal.Api.Common.V1.Payloads.t/0",
        "Temporal.Api.History.V1.History",
        "Temporal.Api.History.V1.History.t/0",
        "Temporal.Api.History.V1.HistoryEvent",
        "Temporal.Api.History.V1.HistoryEvent.t/0",
        "Temporal.Api.Workflowservice.V1.CreateScheduleResponse",
        "Temporal.Api.Workflowservice.V1.CreateScheduleResponse.t/0",
        "Temporal.Api.Workflowservice.V1.DeleteScheduleResponse",
        "Temporal.Api.Workflowservice.V1.DeleteScheduleResponse.t/0",
        "Temporal.Api.Workflowservice.V1.DescribeScheduleResponse",
        "Temporal.Api.Workflowservice.V1.DescribeScheduleResponse.t/0",
        "Temporal.Api.Workflowservice.V1.PollActivityTaskQueueResponse",
        "Temporal.Api.Workflowservice.V1.PollActivityTaskQueueResponse.t/0",
        "Temporal.RPC"
      ],
      before_closing_body_tag: &docs_disclaimer/1
    ]
  end

  defp docs_disclaimer(:html) do
    """
    <aside><strong>Unofficial community SDK:</strong> Not developed, maintained,
    supported, or endorsed by Temporal Technologies.</aside>
    """
  end

  defp docs_disclaimer(_format), do: ""

  defp aliases do
    [
      quality: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "test",
        "credo --strict"
      ]
    ]
  end
end
