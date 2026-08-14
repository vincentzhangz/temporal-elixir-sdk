# Compatibility policy

## Project status

The package is an unofficial, incomplete pre-1.0 community SDK. Generated
protobuf modules expose wire types only; they do not imply supported Temporal
behavior. Feature comparisons in the README use the official Go SDK and the
Public-Preview Rust SDK as the baseline; the Rust SDK differs from Go by not
exposing side effects, memos, or enhanced stack traces.

## Language runtime

The declared minimum is Elixir 1.17. CI exercises representative maintained
Elixir/OTP combinations. Pure-BEAM runtime operation is mandatory.

## Transport dependencies

The gRPC client (`grpc` 1.0.3) is vendored in `third_party/grpc` so the SDK can
carry an HTTP/2 PING keepalive patch on the Mint adapter without depending on an
unreleased upstream. `grpc_core` remains a normal Hex dependency. The vendored
code is tracked as a distinct directory so upstream changes can be diffed and
re-applied.

Connection configuration covers verified TLS with custom CA roots and SNI
override, PEM-bytes mTLS client credentials, opt-in `verify: :verify_none`,
HTTP/2 PING keepalive (`interval`/`timeout`), a connect timeout, a 128 MiB
default message-size limit, per-call metadata, and refreshable API-key
providers. HTTP CONNECT proxies and DNS load balancing are not supported by the
Mint/grpc stack.

## Temporal API and Server

Protobuf inputs are pinned to official `temporalio/api` v1.63.3. The synchronous
completion slice has been exercised against official Temporal Server 1.31.2
using Temporal CLI 1.8.2. This is integration evidence, not a general Server or
Temporal Cloud compatibility claim. A server version enters the support matrix
only after broader integration and conformance tests pass.

## Versioning and deprecation

Semantic Versioning applies to released public Elixir APIs. Before 1.0,
incompatible public API changes may occur in minor releases and must be
documented. After 1.0, removals require prior deprecation in a minor release.

Stored histories are replayable only for the documented synchronous Workflow,
Activity, timer, Continue-As-New, and signal subsets. Signal handlers are
reconstructed from ordered history; buffered signals are not transferred to a
Continue-As-New run by Temporal and must be carried explicitly in application
input when required. The repository retains an official-server-generated
fixture for the initial completion slice. Before any workflow runtime release,
CI must retain histories from every SDK release and verify replay before
broader compatibility can be claimed.
