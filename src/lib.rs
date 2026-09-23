// SPDX-License-Identifier: AGPL-3.0-only

pub mod auth;
pub mod datalock;
pub mod engine;
pub mod groups;
pub mod host;
pub mod ids;
pub mod ipc;
pub mod link;
pub mod lkg;
pub mod manifest;
pub mod media;
pub mod methods;
pub mod metrics;
pub mod parent;
pub mod protocol;
pub mod registry;
pub mod resource;
pub mod service;
pub mod store;
pub mod supervisor;

pub const API_VERSION: &str = "1.0";
/// One ≤16 MiB inbound line per host connection (attachment base64 payloads,
/// implementation-plan §4.12). Matches the desktop client's connector frame
/// budget; the desktop is the only local peer on a private, handshake-
/// authenticated socket.
pub const DEFAULT_HOST_FRAME_LIMIT: usize = 160 * 1024 * 1024;
pub const DEFAULT_UPSTREAM_LINE_LIMIT: usize = 160 * 1024 * 1024;
pub const MAX_PENDING_UPSTREAM_REQUESTS: usize = 128;
/// The reserved, always-present proxy group (ADR 0001 R2). The legacy global
/// `--socks-proxy` / `KT_SIGNAL_SOCKS_PROXY` configures this group; the group
/// list must not redefine it. Accounts linked before Phase 4 read as members
/// of this group.
pub const DEFAULT_PROXY_GROUP_ID: &str = "default";

/// Advertised capability list, derived from the single method table
/// (`methods::METHOD_NAMES`) and never hand-written (optimization-plan §5.2
/// B1).
pub const PHASE2_CAPABILITIES: &[&str] = &methods::METHOD_NAMES;
