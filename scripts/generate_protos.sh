#!/bin/sh
set -eu

api_version="v1.63.3"
generator_version="0.17.0"
protoc_version="libprotoc 35.1"
root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
source_dir="$root/proto/upstream/temporal-api-$api_version"
output_dir="$root/lib/temporal_api"

test "$(protoc --version)" = "$protoc_version"
test "$(protoc-gen-elixir --version)" = "$generator_version"

rm -rf "$output_dir"
mkdir -p "$output_dir"

cd "$source_dir"
# google/api inputs are resolved but not generated: the SDK has no
# grpc/grpc_core dependency, so only message modules are produced.
#
# Message-only generation: the SDK's transport routes by method path
# (@rpc_types) and does not use generated GRPC.Service/GRPC.Stub modules,
# so the `plugins=grpc` flag is omitted. This keeps the SDK free of the
# grpc/grpc_core dependencies.
protoc \
  -I . \
  --elixir_out="gen_proto_source=true:$output_dir" \
  $(rg --files temporal nexusannotations -g '*.proto' | LC_ALL=C sort)
