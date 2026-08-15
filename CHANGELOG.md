# Changelog

All notable changes will be documented here. The project follows Semantic
Versioning for released public Elixir APIs, subject to the pre-1.0 policy in
`docs/compatibility.md`.

## Unreleased

### Transport and connection

- Native pure-BEAM gRPC unary transport: `Temporal.RPC.MintTransport` on
  `mint` + `castore` replaces the previously vendored `grpc` library (no
  `grpc`/`grpc_core`/`googleapis` dependency, no path deps — the package is
  publishable to Hex). It implements gRPC framing, `grpc-status`/`grpc-message`
  trailer mapping, HTTP/2 PING keepalive with PONG timeout, TLS/mTLS (PEM or
  file, custom CA, SNI override, opt-in `verify: :verify_none`), connect
  timeout, 128 MiB message-size limit, per-call metadata/API-key providers,
  deadlines, and caller-monitor cleanup for timed-out calls. Generated protos
  are message-only (`--elixir_out` without `plugins=grpc`); the transport
  routes by method path.
- Supervised connection ownership with a unary transport behaviour
  (`Temporal.RPC.Transport`), refreshable API-key auth, and `GetSystemInfo`
  support. Verified TLS defaults, custom CA/SNI, mTLS, keepalive, and
  message-size limits have unit coverage.
- Opt-in idempotent retries (`Temporal.RPC.RetryPolicy`).

### Workflow execution

- Named synchronous zero- or one-argument workflows with paginated Workflow
  Task history retrieval, serial history validation, task-token fencing,
  immediate completion, and offline replay sharing one typed event reducer and
  machine registry (`Temporal.Workflow.TaskKernel.*`).
  `Temporal.Workflow.Replay.replay/3` replays official-server history JSON.
- Durable timers: `Temporal.Workflow.sleep/1,2`, `new_timer/1,2`, `await/1`,
  `cancel_timer/1`, logical `now/0`, `TimerOptions`, and cancellation scopes
  via `StartTimer`/`CancelTimer` machines.
- Signals and Signal-With-Start: `Temporal.Client.signal_workflow/3-5` and
  `signal_with_start/6`, with `set_signal_handler/2`, dynamic handlers, signal
  state, FIFO history ordering, and deduplication.
- Queries: `Temporal.Client.query_workflow/3-5` with `set_query_handler/2`,
  answering queries on Workflow Tasks and legacy query-only tasks.
- Updates and Update-With-Start: `Temporal.Client.update_workflow/3-5` and
  `update_with_start/6,7` (atomic `ExecuteMultiOperation`), with
  `set_update_handler/2` and `Acceptance`/`Response` protocol messages.
- Continue-As-New: `Temporal.Workflow.continue_as_new/1,2` with run-chain
  following (`Temporal.Client.result_with_run_chain/2`) and run-cycle
  detection.
- Child workflows and external-workflow signals:
  `execute_child_workflow/3,4`, `await_child_workflow/1`,
  `signal_external_workflow/4,5`, with dedicated machines resolving the
  initiated/started/signaled/failed/canceled history events.
- Workflow cancellation and failure: `fail_workflow/1`, `cancel_workflow/0`,
  `cancel_scope/1` (including child-workflow cancellation), with replay support
  for the corresponding close and cancel-request events.
- Deterministic utilities: `get_version/3`, `get_version_default/0`,
  `side_effect/1`, `mutable_side_effect/3`, `info/0`, `deterministic_keys/1`,
  and search-attribute/memo upserts.
- Workflow-side Nexus operations: `new_nexus_client/2` +
  `execute_nexus_operation/3,4` via `COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION`
  with replay support. The Nexus worker is not implemented (the pinned
  `temporalio/api` v1.63.3 vendors `nexusservices/workerservice` message-only).

### Activities

- Synchronous Activities with heartbeats and `cancel_requested` detection,
  typed failures, retry policy, retry exhaustion, non-retryable failures, and
  heartbeat-checkpoint resume.
- Async completion (`Temporal.Activity.Async.complete_async/3`,
  `fail_async/3`), local Activities (`execute_local_activity/3` via
  `LocalActivity` markers), and eager Activity tasks (`submit_eager/2`,
  forwarded from Workflow Task completions). The Activity worker executes tasks
  concurrently with per-task heartbeat/cancellation contexts.

### Client surface

- `Temporal.Client`: start/execute/result, terminate (handle-based and by-ID),
  list, describe, query, signal, update, update-with-start, and
  `execute_multi_operation/4`. Start options include workflow ID reuse/
  conflict policies, retry policy, cron schedule, memo, search attributes, and
  headers.
- `Temporal.ScheduleClient`: create/describe/delete/list Schedules via the
  generated schedule RPCs.

### Worker

- `Temporal.Worker` with concurrent Workflow Task processing (independent runs
  in parallel, per-run serialized), sticky task queues (`:sticky` option),
  opt-in build-ID versioning (`:build_id`/`:use_versioning`), and graceful
  stop (`Temporal.Worker.stop/2` with a drain timeout).

### Converters, codecs, and observability

- `Temporal.Converter` (pluggable encode/decode behaviour) and
  `Temporal.Codec` payload-codec chain (with `Temporal.Codec.Base64` example),
  applied at client start and Activity boundaries via the `:payload_codecs`
  connection option.
- `Temporal.Interceptor` (`on_call`/`on_response`) and `Temporal.Telemetry`
  (`[:temporal, :client, :call]`, `[:temporal, :worker, :poll]`).

### Testing and documentation

- `Temporal.TestEnvironment.run_workflow/3` (offline Workflow harness) and
  `advance_time/3` (time-skipping that synthesizes timer history and
  re-reduces).
- Opt-in official-server integration tests and a Temporal Cloud conformance
  suite (`test/integration/`, `:cloud_live` tag).
- Feature-compatibility matrix moved to `docs/features.md`; README, third-party
  notices, and proto provenance updated to the native-transport dependency set.

No complete Temporal SDK release has been made.
