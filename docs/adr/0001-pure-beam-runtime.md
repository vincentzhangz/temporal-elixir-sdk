# ADR 0001: Pure-BEAM runtime

- Status: accepted
- Date: 2026-08-13

## Decision

The SDK runtime will use Elixir/Erlang processes and pure-BEAM dependencies.
Rust, NIFs, ports, helper executables, and sidecars are excluded. `protoc` is a
build-time regeneration tool only; generated `.pb.ex` files are committed.

## Consequences

The SDK owns gRPC transport policy, polling, task lifecycle, replay, and
deterministic scheduling in OTP. Each capability requires direct tests and
cannot be inferred from generated service definitions. Dependency review must
reject runtime native artifacts.
