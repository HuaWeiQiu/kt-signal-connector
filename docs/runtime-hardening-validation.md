# Runtime Hardening Validation

Date: 2026-08-09

## Implemented

- Ed25519 manifest signing and verification with an explicit compiled trust-key ID.
- Exact bundle file inventory with streaming SHA-256 verification and symlink/path rejection.
- Required target platform, Connector, signal-cli and JRE identity, corresponding-source archive,
  SBOM, build record, and license records for Connector, signal-cli, libsignal and the selected JRE.
- Immutable version directories, verify-before-publish staging, atomic active/LKG pointer replacement,
  and validation again at activation and rollback.
- Optional fixed `JAVA_HOME`; signal-cli still runs with bounded `-Xms16m -Xmx384m` and ignores
  ambient Java option injection variables.
- Production CLI flow: `package manifest`, `package sign`, and
  `package verify --require-signature`. Local unsigned package/LKG commands remain development-only.

## Evidence

- Rust unit coverage includes valid signatures, wrong keys, tampering, incomplete licenses, unknown
  manifest fields, immutable staging, failed-stage cleanup, and pointer traversal rejection.
- CLI integration generates a complete manifest, signs it with an Ed25519 test key, then verifies it
  with the independent public key through the production command path.
- The LKG/manifest Windows-only code compiles in an isolated `x86_64-pc-windows-msvc` target crate.
  Full repository cross-check still stops in bundled SQLite C compilation because this macOS host has
  no Windows C toolchain.
- Desktop focused coverage verifies active resolution, tamper fallback to LKG, packaged environment
  bypass rejection, unknown fields, incomplete license delivery, bounded negative caching, and fixed
  JRE completeness.

## Not Claimed

- No production private key was generated or committed. Release key custody and rotation are an
  external release-process decision; only approved public keys belong in Desktop source.
- No signed macOS/Windows target bundle has been produced by this local test.
- No native Windows runtime, installer, code-signing, real multi-account soak, or 24/72-hour resource
  gate has passed yet.
- The existing macOS client can exercise development binaries through absolute environment paths,
  but that does not substitute for a signed packaged-runtime acceptance test.

## Release Gate

Before any production package enables Signal:

1. Build a pinned Connector + unmodified signal-cli + pinned JRE bundle for the exact target.
2. Include exact corresponding source, complete license/notice files, SBOM and build record.
3. Generate the manifest with explicit `--platform`, `--source-archive-path`, and one `--license`
   record per covered component, then sign it outside the source tree.
4. Compile the approved raw Ed25519 public key and key ID into Desktop and run
   `npm run verify:signal-runtime` before packaging.
5. Pass native macOS/Windows install, start/stop, linking, send/receive, rollback, upgrade, long-soak,
   memory, CPU, disk-pressure, network-loss and multi-account acceptance gates.
