# Agent Rules

## Scope

This repository owns only the local Signal connector boundary. It does not own KT Desktop UI,
tenant rollout decisions, AI prompts, translation behavior, or the Signal protocol implementation.

## Required Reading

Before implementation changes, read:

1. `README.md`
2. `docs/implementation-plan.md`
3. `schemas/connector-api-v1.schema.json` when it exists
4. the relevant tests

## Non-negotiable Boundaries

- Keep KT Desktop, this connector, and `signal-cli` in separate processes and address spaces.
- Do not copy, link, or modify `signal-cli` or `libsignal` source in this repository.
- Use only the pinned public `signal-cli` JSON-RPC interface.
- Never expose raw JSON-RPC methods, account keys, the signal-cli data directory, raw envelopes, or
  arbitrary file paths to a caller.
- Multiple connector instances may share a host only under the ADR 0002 namespace
  rules: their `--endpoint`, `--state-dir`, and `--signal-data-dir` trees (the root
  and every `proxy-groups/<groupId>/` subdirectory) must be pairwise disjoint, and
  each instance owns its own state-dir store (the store has no cross-process lock).
  Data-dir occupancy is enforced by a kernel lock at startup; endpoint collisions
  and over-limit endpoint paths fail closed at bind.
- Inside one connector process, one signal-cli engine (JVM or native) per proxy
  group. Proxy groups are bounded (hard ceiling 8, product default 4), are defined
  by the trusted launcher at spawn, and are never created, reconfigured, or
  reassigned over IPC (ADR 0001).
- An account's proxy group is fixed when the account is linked and immutable for the life of
  that link; moving an account means deleting its local data and linking it again.
- Never spawn one engine per account, conversation, or feature: accounts that share a proxy
  group share its engine, data directory, watchdog, and RSS budget.
- Do not use fixed HTTP/TCP ports. Prefer a private Unix socket or Windows named pipe.
- Do not log message bodies, phone numbers, contacts, QR payloads, secrets, tokens, or key paths.
- An unknown send outcome must not be retried automatically.
- Keep all queues, request maps, caches, payloads, and attachment handling bounded.

## Delivery

- Update the implementation plan or API schema before changing a contract.
- Add failure-path and boundary tests for IPC, process lifecycle, authentication, idempotency, and
  resource limits.
- Run fmt, tests, clippy, and release build before a local commit.
- Review the diff after tests and fix every actionable finding before committing.
- Do not push, publish, or deploy unless explicitly authorized.
