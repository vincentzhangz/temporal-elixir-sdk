# Changelog

All notable changes will be documented here. The project follows Semantic
Versioning for released public Elixir APIs, subject to the pre-1.0 policy in
`docs/compatibility.md`.

## Unreleased

- Initialized the pure-BEAM Mix application and governance baseline.
- Pinned official Temporal API v1.63.3 protobuf inputs and generated modules.
- Added supervised connection ownership, a unary transport behaviour, and the
  initial `GetSystemInfo` wire slice.
- Completed the transport, TLS, mTLS, metadata, and API-key auth capability:
  verified TLS defaults, custom CA and SNI override, PEM-bytes mTLS,
  opt-in `verify: :verify_none`, HTTP/2 PING keepalive, connect timeout, and a
  128 MiB default message-size limit.
- Vendored `grpc` 1.0.3 into `third_party/grpc` (path dependency) to carry the
  HTTP/2 PING keepalive patch on the Mint adapter; `grpc_core` remains a Hex
  dependency.

No complete Temporal SDK release has been made.
