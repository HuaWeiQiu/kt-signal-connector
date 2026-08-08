# Phase 3 Local Validation

## Status

- Validation date: 2026-08-05
- Host: macOS arm64
- Result: local packaging/LKG/resource-smoke acceptance passed for development bundles
- Production signing, Windows runtime acceptance, and 24-hour soak: **not claimed**

## Implemented

- Runtime manifest schema (`schemas/runtime-manifest-v1.schema.json`) with connector,
  signal-cli, and JRE artifact hashes.
- Local unsigned manifest generation and hash verification (`package manifest|verify`).
- LKG stage / activate / rollback pointer protocol under a runtime root
  (`package stage|activate|rollback`).
- Local bundle builder script: `packaging/scripts/build-local-bundle.sh`.
- Resource smoke harness: idle connector RSS + fixture active-text sample
  (`packaging/scripts/measure-local-resources.sh`).
- Windows named-pipe transport implementation behind `cfg(windows)` with
  `reject_remote_clients(true)`, a current-user SID DACL, first-instance protection, and strict
  pipe-name normalization. Windows bootstrap uses inherited stdin rather than a secret file;
  profile directories receive a protected current-user DACL, and a parent-process monitor shuts
  down signal-cli if Electron exits. Runtime acceptance on Windows hardware was not executed in
  this environment.

## Commands

```bash
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
jq empty schemas/connector-api-v1.schema.json
jq empty schemas/runtime-manifest-v1.schema.json
./packaging/scripts/build-local-bundle.sh
./packaging/scripts/measure-local-resources.sh
```

## Explicit non-claims

- Manifests produced locally use `signature.scheme = unsigned-local`.
- Placeholder signal-cli/JRE files may be used when real distribution inputs are absent.
- Resource reports are short smoke samples, not 24-hour or multi-account capacity gates.
- Windows named-pipe code is present but not runtime-tested on Windows here.
- The Windows-only IPC and parent-monitor modules pass an isolated
  `x86_64-pc-windows-msvc` Rust target check. A full repository cross-check stops in bundled SQLite's
  C build without a Windows C toolchain, so neither result substitutes for a native Windows build.

No push, release publication, or external Signal action was performed.
