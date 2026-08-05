// SPDX-License-Identifier: AGPL-3.0-only

pub mod auth;
pub mod engine;
pub mod host;
pub mod ids;
pub mod ipc;
pub mod link;
pub mod lkg;
pub mod manifest;
pub mod protocol;
pub mod resource;
pub mod service;
pub mod store;
pub mod supervisor;

pub const API_VERSION: &str = "1.0";
pub const DEFAULT_HOST_FRAME_LIMIT: usize = 1024 * 1024;
pub const DEFAULT_UPSTREAM_LINE_LIMIT: usize = 8 * 1024 * 1024;
pub const MAX_PENDING_UPSTREAM_REQUESTS: usize = 128;

pub const PHASE2_CAPABILITIES: &[&str] = &[
    "runtime.status",
    "runtime.start",
    "runtime.stop",
    "accounts.list",
    "link.start",
    "link.finish",
    "link.cancel",
    "conversations.list",
    "messages.list",
    "messages.sendText",
];
