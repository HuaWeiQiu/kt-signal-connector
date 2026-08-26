#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Trim non-target-architecture native resource globs from signal-cli's GraalVM
reachability metadata before `nativeCompile`.

Phase 5 (native-image spike) size cut, measured locally 2026-08-25: removing the
three globs below shrinks the macOS arm64 binary from 340 MB to 112.8 MiB by
excluding the amd64 libsignal JNI library and the Linux x86_64 SQLite JDBC native.
The remaining `*signal_jni.*` / `*signal_jni_aarch64.*` globs keep the macOS
aarch64 natives reachable, so daemon/link/send functionality is unaffected.

Usage: trim-native-reachability-metadata.py <signal-cli-source-root>

Fails loudly when an expected glob is missing or duplicated, so an upstream
metadata change turns into a build error here instead of a silently un-trimmed
(or broken) binary.
"""

import json
import sys
from pathlib import Path

METADATA_REL = Path(
    "src/main/resources/META-INF/native-image/org.asamk/signal-cli/reachability-metadata.json"
)

# Exact glob values removed from the upstream v0.14.7 metadata. Each must be
# present exactly once, otherwise the pin no longer matches upstream and a human
# must re-derive the trim.
REMOVE_GLOBS = (
    "*signal_jni_amd64.*",
    "libsignal_jni_amd64.so",
    "org/sqlite/native/Linux/x86_64/libsqlitejdbc.so",
)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <signal-cli-source-root>", file=sys.stderr)
        return 2

    metadata_path = Path(sys.argv[1]) / METADATA_REL
    if not metadata_path.is_file():
        print(f"FAIL: metadata not found: {metadata_path}", file=sys.stderr)
        return 1

    data = json.loads(metadata_path.read_text(encoding="utf-8"))
    resources = data.get("resources")
    if not isinstance(resources, list):
        print("FAIL: unexpected metadata shape: 'resources' is not a list", file=sys.stderr)
        return 1

    for glob_value in REMOVE_GLOBS:
        matches = [e for e in resources if isinstance(e, dict) and e.get("glob") == glob_value]
        if len(matches) != 1:
            print(
                f"FAIL: expected exactly 1 entry with glob {glob_value!r}, "
                f"found {len(matches)} — upstream metadata drifted, re-derive the trim",
                file=sys.stderr,
            )
            return 1
        resources.remove(matches[0])
        print(f"removed resource glob: {glob_value}")

    metadata_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(f"trimmed: {metadata_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
