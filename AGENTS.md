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
- One connector and one signal-cli JVM serve all accounts and conversations in one local profile.
- Never spawn one JVM per account, conversation, or feature.
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
