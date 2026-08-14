# Third-party notices

Audit basis: resolved `mix.lock` and dependency package metadata on 2026-08-13.
This is a license inventory, not legal advice.

## Runtime dependencies

- `protobuf` 0.17.0 — MIT
- `grpc` 1.0.3 — Apache-2.0
- `grpc_core` 1.0.3 — Apache-2.0
- `googleapis` 0.1.0 — Apache-2.0
- `mint` 1.9.3 — Apache-2.0
- `castore` 1.0.21 — Apache-2.0
- `hpax` 1.0.4 — Apache-2.0
- `jason` 1.4.5 — Apache-2.0
- `telemetry` 1.4.2 — Apache-2.0

## Development and test dependencies

- `credo` 1.7.19 — MIT
- `bunt` 1.0.0 — MIT
- `file_system` 1.1.1 — Apache-2.0
- `dialyxir` 1.4.7 — Apache-2.0
- `erlex` 0.2.9 — Apache-2.0
- `ex_doc` 0.40.3 — Apache-2.0
- `earmark_parser` 1.4.46 — Apache-2.0
- `makeup` 1.2.2 — BSD-2-Clause
- `makeup_elixir` 1.0.1 — BSD-2-Clause
- `makeup_erlang` 1.1.0 — BSD-2-Clause
- `nimble_parsec` 1.4.2 — Apache-2.0

## Vendored inputs

- `temporalio/api` v1.63.3 protobuf definitions — MIT. The upstream license is
  retained at `proto/upstream/temporal-api-v1.63.3/LICENSE`.
- `grpc` 1.0.3 source vendored at `third_party/grpc` — Apache-2.0. The upstream
  license is retained at `third_party/grpc/LICENSE`. It carries a Temporal SDK
  patch for HTTP/2 PING keepalive on the Mint adapter; no new code or
  dependencies were added.
