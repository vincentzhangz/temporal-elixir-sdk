# Temporal Elixir SDK

> [!WARNING]
> **Independent, unofficial community SDK.** This project is not developed,
> maintained, supported, or endorsed by Temporal Technologies. It is incomplete
> pre-release software and is not ready for production workloads.

An Elixir-first, pure-BEAM Temporal SDK under active development.

## Current scope

An Elixir-first, pure-BEAM Temporal SDK covering the core Workflow and Activity
surface: durable timers, signals and Signal-With-Start, queries, updates and
Update-With-Start, child workflows, external-workflow signals, cancellation and
failure, local/eager/asynchronous Activities, Continue-As-New, sticky queues,
worker versioning, schedules, and side effects — on a native gRPC/Mint
transport with TLS/mTLS, keepalive, and pluggable payload codecs.

See [the feature-compatibility matrix](docs/features.md) for a per-capability
comparison with the official Go and Rust SDKs, and
[the compatibility policy](docs/compatibility.md) for the deliberately limited
claim. Unsupported history is rejected explicitly.

## Basic synchronous workflow

With a Temporal server listening at `localhost:7233`:

```sh
mix run examples/basic_workflow.exs
```

Set `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, and (when needed)
`TEMPORAL_API_KEY` for another target. The opt-in live test uses the same
environment:

```sh
TEMPORAL_ADDRESS=localhost:7233 mix test test/integration/live_workflow_test.exs
```

## Offline history replay

`Temporal.Workflow.Replay.replay/3` accepts either an official History protobuf
or its protobuf-JSON representation:

```elixir
Temporal.Workflow.Replay.replay(history_json, &MyWorkflows.greeting/1,
  workflow_id: "hello-workflow",
  run_id: "run-id"
)
```

Successful replay returns a `Temporal.Workflow.HistoryCursor`. Invalid event
IDs/correlation attributes and command mismatches return structured,
event-specific diagnostics. See `test/fixtures/PROVENANCE.md` for the live
official-server history retained by this milestone.

## Workflow signals

Register handlers from deterministic Workflow code. Named handlers take
precedence over the dynamic fallback when both are present at delivery time:

```elixir
workflow = fn _input ->
  :ok =
    Temporal.Workflow.set_signal_handler("deposit", fn amount, context, state ->
      balance = Map.get(state, "balance", 0) + amount
      {:ok, Map.put(state, "balance", balance)}
    end)

  state = Temporal.Workflow.await_signal_state(&(Map.get(&1, "balance", 0) >= 100))
  state["balance"]
end
```

`await_signal_state/1` is the deterministic BEAM receive equivalent; it never
uses a process mailbox. Handler state and delivery are reconstructed from
ordered history in live execution and replay. A handler may call the supported
timer and Activity APIs. Workflow completion and Continue-As-New reject
unfinished buffered, scheduled, or running signal work; call
`wait_for_all_signal_handlers/0` when a workflow must explicitly drain it.

Temporal does **not** automatically transfer buffered signals to a
Continue-As-New run. A safe pattern is to wait for handlers, encode the
application-level state or messages in the new run input, then register the
same handlers in the new run:

```elixir
:ok = Temporal.Workflow.wait_for_all_signal_handlers()
Temporal.Workflow.continue_as_new(%{"carried" => Temporal.Workflow.signal_state()})
```

Signals sent after Continue-As-New should target the new run ID, or omit
`:run_id` to target the current run by Workflow ID. Carry an application
idempotency key in signal input when business-level exactly-once handling is
required; RPC request IDs only deduplicate transport requests.

## Requirements

- Elixir 1.17 or later
- a maintained compatible Erlang/OTP release
- `protoc` and `protoc-gen-elixir` 0.17.0 only when regenerating protobufs

There are no NIFs, ports, sidecars, Rust components, or runtime native
artifacts.

## Development

```sh
mix deps.get
mix quality
```

See `proto/PROVENANCE.md` for the pinned upstream source and reproducible
generation command. See [the feature-compatibility matrix](docs/features.md)
and [the compatibility policy](docs/compatibility.md) for the supported
surface and its intentionally limited claim.

## License

Original SDK code is licensed under Apache-2.0. Vendored Temporal API protobuf
inputs retain their upstream MIT license; see `NOTICE` and
`THIRD_PARTY_NOTICES.md`.
