# Contributing

This repository is an independent, unofficial community SDK.

Before proposing behavior, open an issue describing the relevant official
Temporal repository, documentation, or protobuf reference and the conformance
evidence. Do not use third-party SDK reimplementations as behavioral authority.

For code changes:

1. Add a failing behavior test and record the expected failure.
2. Implement the smallest change that makes it pass.
3. Run `mix quality`.
4. Update compatibility, provenance, and changelog records when applicable.

Generated files must only change through the pinned process in
`proto/PROVENANCE.md`. Contributions introducing NIFs, ports, sidecars, Rust,
or runtime native artifacts are outside the project architecture.
