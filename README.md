# Temporal Elixir SDK

> [!WARNING]
> **Independent, unofficial community SDK.** This project is not developed,
> maintained, supported, or endorsed by Temporal Technologies. It is incomplete
> pre-release software and is not ready for production workloads.

An Elixir-first, pure-BEAM Temporal SDK under active development.

## Current scope

The comparison baseline is the behavior exposed by the
[official Temporal Go SDK](https://github.com/temporalio/sdk-go) and the
[official Temporal Rust SDK](https://github.com/temporalio/sdk-rust), not the
presence of generated protobuf modules. The Rust SDK is in Public Preview; its
documented gaps (side effects, memos, enhanced stack traces) are marked per row.

**Status definitions:** In the baseline columns, **Supported** means the
official SDK exposes the behavior, **Partial** means only a documented subset
is exposed, and **Unsupported** means the behavior is absent. In the Elixir
column, **Supported** means the stated narrow behavior has passing unit and
official-server integration evidence; **Partial** means only a documented
subset works; **In progress** means isolated implementation/tests exist but the
feature is not wired end to end; **Experimental** means evidence is narrow and
the API or behavior may change; **Unsupported** means no usable SDK behavior is
provided.

| Capability compared with the official Go and Rust SDKs | Go SDK | Rust SDK | Elixir status | Current evidence and limitations |
| --- | --- | --- | --- | --- |
| Transport, TLS, mTLS, metadata, and API-key auth | Supported | Supported | Supported | Pure-BEAM gRPC/Mint transport with verified TLS defaults, custom CA (`ca_cert`/`ca_certs`) and SNI (`server_name`) override, PEM-bytes mTLS, opt-in `verify: :verify_none`, HTTP/2 PING keepalive, connect timeout, 128 MiB message limit, per-call metadata/API-key providers, deadlines, status mapping, and opt-in idempotent retries. Keepalive and connection health are live-verified against an official server ([keepalive tests](test/temporal/grpc_keepalive_test.exs), [transport tests](test/temporal/grpc_transport_test.exs), [option tests](test/temporal/connection_options_test.exs)). HTTP CONNECT proxies and DNS load balancing are not supported by the Mint/grpc stack. |
| Client start, result, and Continue-As-New | Supported | Supported | Experimental | Start/result and following a Continue-As-New run chain are live-tested only for the narrow synchronous runtime ([live tests](test/integration/live_workflow_test.exs), [Continue-As-New tests](test/temporal/continue_as_new_test.exs)). |
| Workflow execution and history replay | Supported | Supported | Experimental | Named synchronous zero- or one-argument workflows, paginated Workflow Task history retrieval, serial history validation, task-token fencing, immediate completion, and offline replay share one typed event reducer and machine registry. Differential and page-boundary tests cover the shared path ([kernel tests](test/temporal/workflow_kernel_v2_test.exs), [runtime tests](test/temporal/workflow_runtime_test.exs), [replay tests](test/temporal/workflow_replay_test.exs)). A controlled live nondeterminism regression verifies that the worker fails the Workflow Task instead of dropping it ([live tests](test/integration/live_workflow_test.exs)). This is not a general deterministic workflow runtime. |
| Activities, heartbeats, cancellation, retries, and failures | Supported | Supported | Partial | The live-tested slice covers sequential synchronous Activities, retry exhaustion, non-retryable application failures, and heartbeat checkpoint resume ([live tests](test/integration/live_workflow_test.exs)). Typed failures and cancellation acknowledgement have narrower unit evidence ([Activity tests](test/temporal/activity_ga_test.exs)); concurrent and advanced Activity semantics are absent. |
| Durable Workflow timers | Supported | Supported | Partial | `Temporal.Workflow.sleep/1,2`, `new_timer/1,2`, `await/1`, `cancel_timer/1`, logical `now/0`, `TimerOptions`, and timer cancellation scopes use deterministic IDs and history-backed `StartTimer`/`CancelTimer` machines. Unit/replay/live evidence covers zero and invalid durations, sub-millisecond rounding, command batching, multiple wakeups, Activity interleaving, cancellation races, and Continue-As-New ([API tests](test/temporal/workflow_timer_api_test.exs), [machine tests](test/temporal/workflow_timer_machine_test.exs), [replay tests](test/temporal/workflow_timer_replay_test.exs), [live tests](test/integration/live_workflow_test.exs)). There is no general selector/channel API: the minimal scheduler supports timer futures and their tested interleavings with the existing synchronous Activity API only. |
| Signals and Signal-With-Start | Supported | Supported | Partial | `Temporal.Client.signal_workflow/3-5` and `signal_with_start/6` support workflow/run targeting, stable request IDs, headers, identity, structured conversion/RPC errors, and the generated unary transports. Workflow code can install/replace/remove named and dynamic handlers, access signal headers, mutate deterministic handler state, wait on that state, and wait for all handlers. History reduction buffers before registration, preserves FIFO event order, deduplicates request/event IDs, and keeps blocking handlers replay-safe across Workflow Tasks. Unit/property/replay/live coverage includes unknown signals, Signal-With-Start, timers, Activities, and Continue-As-New routing ([client tests](test/temporal/client_signal_test.exs), [dispatcher tests](test/temporal/workflow_signal_dispatcher_test.exs), [replay tests](test/temporal/workflow_signal_replay_test.exs), [live tests](test/integration/live_signal_test.exs)). This remains a sequential handler scheduler, not the full Go/Rust SDK concurrency surface. |
| Queries | Supported | Supported | Unsupported | Generated wire types do not provide query APIs or worker dispatch. |
| Updates and Update-With-Start | Supported | Supported | Unsupported | Generated wire types do not provide update APIs, validators, handlers, or lifecycle semantics. |
| Child workflows | Supported | Supported | Unsupported | No child-workflow command/runtime APIs. |
| External-workflow signals from Workflow code | Supported | Supported | Unsupported | No `SignalExternalWorkflowExecution` command/event state machine is exposed; client-originated signals are supported by the separate Signals row. |
| Workflow cancellation and workflow failure | Supported | Supported | Unsupported | No complete workflow cancellation scope or workflow-failure command/runtime path. |
| Local, eager, and asynchronously completed Activities | Supported | Supported | Unsupported | Only the bounded synchronous remote Activity slice exists. |
| Sticky cache | Supported | Supported | Unsupported | No sticky task queues or workflow cache. |
| Worker concurrency, shutdown, and versioning | Supported | Supported | Partial | Bounded polling and graceful stop have unit evidence ([worker tests](test/temporal/worker_test.exs)); execution is intentionally sequential, and worker/build-ID/deployment versioning is absent. |
| Data converters, payload codecs, and encryption | Supported | Supported | Partial | Built-in payload encoding/decoding covers the current JSON-oriented slice; there is no Go/Rust SDK-equivalent converter stack, payload codec chain, or encryption integration. |
| Client terminate, list, and describe workflows | Supported | Supported | Unsupported | No `TerminateWorkflowExecution`, `ListWorkflowExecutions`, or `DescribeWorkflowExecution` client calls are exposed; only start/result/cancel/signal/signal-with-start exist. |
| Side effects | Supported | Unsupported | Unsupported | Go exposes `workflow.SideEffect`/`MutableSideEffect`; the Rust SDK intentionally runs non-deterministic work in Activities instead. Elixir has neither. |
| Search attributes and memos | Supported | Partial | Unsupported | Both official SDKs upsert search attributes; Rust's documented surface covers search attributes but not memos. Elixir exposes neither at start or from Workflow code. |
| Schedules | Supported | Supported | Unsupported | No schedule client APIs or behavior. |
| Nexus | Supported | Supported | Unsupported | No Nexus client, operation, or worker support. |
| Interceptors and telemetry | Supported | Supported | Unsupported | No interceptor chain, metrics exporter, or tracing integration. |
| Enhanced stack traces | Supported | Unsupported | Unsupported | Go exposes the enhanced stack trace workflow API; the Rust SDK does not. Elixir only carries the generated wire type, with no stack-trace RPC registered. |
| Testing, time skipping, and features harness | Supported | Supported | Partial | Unit tests, replay fixtures, and opt-in official-server integration tests exist; there is no time-skipping test environment or cross-SDK features harness. |
| Temporal Cloud compatibility | Supported | Supported | Experimental | TLS, mTLS (PEM or file), API-key configuration, and custom CA/SNI primitives exist and are live-tested only against a plaintext local server; there is no verified Cloud conformance or compatibility claim. |

Last verified inputs: Temporal API **v1.63.3** and Temporal Server **1.31.2**.
See [the compatibility policy](docs/compatibility.md) for the deliberately
limited claim. Unsupported history is rejected explicitly.

Every RPC carries Temporal's standard `client-name` and `client-version`
headers. The truthful community identifier is `temporal-elixir-community`; its
version is derived from the application version. These SDK headers are separate
from configurable client/worker `identity`.

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
generation command. See `docs/compatibility.md` for the intentionally limited
compatibility policy.

## License

Original SDK code is licensed under Apache-2.0. Vendored Temporal API protobuf
inputs retain their upstream MIT license; see `NOTICE` and
`THIRD_PARTY_NOTICES.md`.
