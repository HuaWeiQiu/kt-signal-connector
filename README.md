# KT Signal Connector

`kt-signal-connector` is the local open-source boundary between KT Desktop and the unofficial
[`signal-cli`](https://github.com/AsamK/signal-cli) runtime.

The connector is designed to run as an independent process. It supervises an unmodified
`signal-cli` child process, translates its JSON-RPC interface into a stable host contract, and
keeps Signal account keys outside the KT renderer and feature runtime.

## Status

Phases 1–3 are implemented for local development:

- Phase 1: process/protocol PoC
- Phase 2: link, accounts, text channel, SQLite
- Phase 3: unsigned local manifests, LKG stage/activate/rollback, resource smoke harness

The connector is not ready for production use. Local bundles are unsigned, Windows runtime
acceptance is pending a Windows host, and real Signal account acceptance requires separate
authorization.

The complete implementation and acceptance plan is in
[`docs/implementation-plan.md`](docs/implementation-plan.md).
Local evidence:

- [`docs/phase-1-validation.md`](docs/phase-1-validation.md)
- [`docs/phase-2-validation.md`](docs/phase-2-validation.md)
- [`docs/phase-3-validation.md`](docs/phase-3-validation.md)

Local packaging helpers:

```bash
./packaging/scripts/build-local-bundle.sh
./packaging/scripts/measure-local-resources.sh
```

## Non-goals

- This project is not an official Signal client or an official Signal integration.
- It does not embed or modify Signal Desktop.
- It does not link `libsignal` into KT Desktop or this connector.
- It does not provide bulk messaging or an unrestricted automation API.

## Development

Prerequisites:

- Rust 1.85 or newer.
- Python 3 for the Unix fake-engine integration fixture.
- A pinned, compatible `signal-cli` and JRE 25 bundle for real integration tests.

Local checks:

```bash
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
```

Real Signal account tests are intentionally separate from mocked tests and must never use production
customer accounts or commit account data.

## License

Copyright (C) 2026 KTAIorg contributors.

Licensed under the GNU Affero General Public License version 3 only. See [`LICENSE`](LICENSE).
`signal-cli`, `libsignal`, and the selected JRE remain separate components under their own licenses.
