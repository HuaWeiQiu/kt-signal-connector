# KT Signal Connector

`kt-signal-connector` is the local open-source boundary between KT Desktop and the unofficial
[`signal-cli`](https://github.com/AsamK/signal-cli) runtime.

The connector is designed to run as an independent process. It supervises an unmodified
`signal-cli` child process, translates its JSON-RPC interface into a stable host contract, and
keeps Signal account keys outside the KT renderer and feature runtime.

## Status

Connector Phases 1–3 are implemented for local development:

- Phase 1: process/protocol PoC
- Phase 2: link, accounts, text channel, SQLite
- Phase 3: unsigned local manifests, LKG stage/activate/rollback, resource smoke harness

The Phase 4 KT Desktop integration is locally merged in the separate `kt-desktop` repository:

- connector: `main` @ `6656f70` (`link.finish` uses an independent bounded wait lane)
- desktop integration: `codex/signal-test-main-latest` @ merge `5e18793c`
- source boundary: the Desktop launches this independent executable over authenticated local IPC;
  connector source is not copied or linked into the Desktop repository

The connector is not ready for production use. Local bundles are unsigned, Windows runtime
acceptance is pending a Windows host, no remote repository is configured, and the 24/72-hour and
real multi-account capacity gates are still pending.

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

## Local generated files

The repository audit on 2026-08-09 found no unreferenced tracked source modules. The following
ignored paths are generated locally and are not release source:

- `target/debug/` is rebuildable Rust debug output and can be removed when reclaiming disk space.
- `packaging/out/local-bundle/` must be rebuilt before use; an older local bundle must not be treated
  as the current `6656f70` acceptance artifact.
- `target/release/kt-signal-connector` is currently used by the local KT Desktop integration. Keep it
  until a replacement bundle or binary is built; recreate it with `cargo build --release` if removed.

Phase validation documents and test fixtures remain intentional acceptance evidence and should not
be removed as generated output.

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
