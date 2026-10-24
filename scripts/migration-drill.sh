#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

# D2 migration/rollback drill (contract 1.197, §4.197).
#
# Runs the realistic-size store test: builds a v0 plaintext SQLite store with
# 20k messages over 200 conversations (~14 MB), opens it through the current
# binary (every additive migration + the v29 pin rebuild + the SQLCipher
# export + the plaintext rollback backup), verifies integrity and row counts,
# then restores the pre-migration file over the encrypted store and reopens it
# — the documented rollback route. Prints the drill's measured evidence.
#
# Usage: scripts/migration-drill.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

cargo test --lib realistic_size_schema_migration_and_plaintext_backup_rollback \
  -- --nocapture
