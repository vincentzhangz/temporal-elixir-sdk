# Temporal API protobuf provenance

Only official `temporalio/api` protobufs are vendored.

- Repository: https://github.com/temporalio/api
- Release: `v1.63.3`
- Release URL: https://github.com/temporalio/api/releases/tag/v1.63.3
- Git commit: `bd41196044da1791481b552616e8a8cec28b506f`
- Published: 2026-07-10T18:54:49Z
- Downloaded archive SHA-256:
  `2076a67344a4c610c6d6880a511f14ba4a5d8135b0aeaba79922a4e012decd20`
- Upstream license: MIT, retained beside the inputs
- Input checksums: `TEMPORAL_API_V1.63.3_SHA256SUMS`

The vendored tree contains the 64 `.proto` files from that release. Files are
unmodified. Generated modules carry `proto_source` metadata and are checksummed
in `GENERATED_V1.63.3_SHA256SUMS`.

## Generator

- `protoc`: 35.1
- `protoc-gen-elixir`: 0.17.0
- runtime `protobuf`: 0.17.0

Install the exact generator and regenerate:

```sh
mix escript.install hex protobuf 0.17.0
PATH="$HOME/.mix/escripts:$PATH" scripts/generate_protos.sh
```

The script generates Temporal and Nexus annotation modules. Imported Google API
annotations are resolved from the official Temporal archive but are not
generated: the SDK has no `grpc`/`grpc_core` dependency, so only message
modules are produced (`--elixir_out` without the `plugins=grpc` flag) and the
standard Google protobuf types are supplied by the pinned `protobuf` runtime
and `protoc` installation.

Any API upgrade requires review of the official tag diff, refreshed input and
output checksums, strict compilation, tests, and compatibility documentation.
