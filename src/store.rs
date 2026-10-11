// SPDX-License-Identifier: AGPL-3.0-only

use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

use crate::ids::{mask_address, random_id, stable_hash_id};

const SCHEMA_VERSION: i64 = 21;
/// Contract 1.42 (§4.41): at most this many dead rows leave the store in one
/// sweep transaction — the same bounded-batch family as retention and the
/// delete-operation ledger, so a large backlog converges over several passes
/// without ever holding the store lock long.
pub const EXPIRE_SWEEP_BATCH: u32 = 256;
const MAX_COMPLETED_ACCOUNT_DELETE_OPERATIONS: i64 = 256;
/// Storage engine: SQLCipher (rusqlite `bundled-sqlcipher`), keyed per profile.
const DATABASE_FILE_NAME: &str = "connector.sqlite3";
/// Phase 3 migration remnant: one consistent plaintext copy, kept next to the
/// store for rollback, then pruned after the retention window.
const PLAINTEXT_BACKUP_SUFFIX: &str = ".plaintext-backup";
/// Staging file a plaintext→encrypted migration builds before the atomic swap.
const MIGRATION_STAGING_SUFFIX: &str = ".encrypting";
/// Phase 3 retention: how long the plaintext migration backup is kept.
pub const PLAINTEXT_BACKUP_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
/// A readable plaintext SQLite store starts with these 16 bytes; an encrypted
/// (SQLCipher) store is indistinguishable from random bytes.
const SQLITE_PLAINTEXT_HEADER: &[u8; 16] = b"SQLite format 3\0";
/// Dev/test key override (optimization-plan Phase 3 contract): production keys
/// arrive over the bootstrap secret channel, never through the environment.
pub const STORE_KEY_ENV: &str = "KT_SIGNAL_STORE_KEY";

/// The full schema DDL, shared by `Store::open` and test fixtures so a test
/// store can never drift from the production schema.
const SCHEMA_DDL: &str = "
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS accounts (
  id TEXT PRIMARY KEY,
  signal_account TEXT NOT NULL UNIQUE,
  masked_address TEXT NOT NULL,
  display_name TEXT,
  state TEXT NOT NULL,
  linked_at INTEGER,
  last_message_at INTEGER,
  unread_count INTEGER NOT NULL DEFAULT 0,
  proxy_group TEXT NOT NULL DEFAULT 'default'
);
CREATE TABLE IF NOT EXISTS conversations (
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  peer_key TEXT NOT NULL,
  title TEXT NOT NULL,
  last_message_preview TEXT,
  last_message_at INTEGER,
  unread_count INTEGER NOT NULL DEFAULT 0,
  unread_mentions INTEGER NOT NULL DEFAULT 0,
  muted INTEGER NOT NULL DEFAULT 0,
  pinned INTEGER NOT NULL DEFAULT 0,
  expire_timer_seconds INTEGER,
  mrr_factors_json TEXT,
  UNIQUE(account_id, kind, peer_key),
  FOREIGN KEY(account_id) REFERENCES accounts(id)
);
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  direction TEXT NOT NULL,
  sender_id TEXT NOT NULL,
  sent_at INTEGER NOT NULL,
  received_at INTEGER,
  stored_at INTEGER,
  body TEXT,
  body_bytes INTEGER,
  body_truncated INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  client_request_id TEXT,
  quote_message_id TEXT,
  quote_snapshot TEXT,
  attachments_json TEXT,
  attachment_locators TEXT,
  live_attachment_locators TEXT,
  rich_json TEXT,
  sticker_json TEXT,
  expire_seconds INTEGER,
  expire_start_at INTEGER,
  expire_at INTEGER,
  server_guid TEXT,
  edited_at INTEGER,
  sender_name TEXT,
  mentions_self INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(account_id) REFERENCES accounts(id),
  FOREIGN KEY(conversation_id) REFERENCES conversations(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS messages_account_client_request
  ON messages(account_id, client_request_id)
  WHERE client_request_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS messages_conversation_sent_at
  ON messages(conversation_id, sent_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS messages_signal_identity_v2
  ON messages(account_id, conversation_id, direction, sent_at, sender_id);
CREATE INDEX IF NOT EXISTS conversations_account_last_message
  ON conversations(account_id, last_message_at DESC, id DESC);
CREATE TABLE IF NOT EXISTS account_delete_operations (
  operation_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('started', 'unknown', 'completed')),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS account_delete_one_pending_per_account
  ON account_delete_operations(account_id)
  WHERE state != 'completed';
CREATE TABLE IF NOT EXISTS contacts (
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('contact', 'group')),
  peer_key TEXT NOT NULL,
  title TEXT NOT NULL,
  extra TEXT,
  synced_at INTEGER NOT NULL,
  UNIQUE(account_id, kind, peer_key),
  FOREIGN KEY(account_id) REFERENCES accounts(id)
);
CREATE INDEX IF NOT EXISTS contacts_account_peer
  ON contacts(account_id, kind, peer_key);
CREATE TABLE IF NOT EXISTS message_edits (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  body TEXT NOT NULL,
  body_bytes INTEGER NOT NULL,
  edited_at INTEGER NOT NULL,
  FOREIGN KEY(account_id) REFERENCES accounts(id),
  FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS message_edits_message
  ON message_edits(account_id, message_id, seq);
CREATE TABLE IF NOT EXISTS message_events (
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('reaction')),
  emoji TEXT NOT NULL,
  target_timestamp INTEGER NOT NULL,
  actor_id TEXT NOT NULL,
  removed INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  actor_name TEXT,
  UNIQUE(account_id, conversation_id, kind, target_timestamp, actor_id),
  FOREIGN KEY(account_id) REFERENCES accounts(id),
  FOREIGN KEY(conversation_id) REFERENCES conversations(id)
);
CREATE INDEX IF NOT EXISTS message_events_conversation
  ON message_events(account_id, conversation_id, target_timestamp);
CREATE TABLE IF NOT EXISTS conversation_pins (
  account_id TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  target_author TEXT NOT NULL,
  target_sent_at INTEGER NOT NULL,
  pinned_at INTEGER NOT NULL,
  expires_at INTEGER,
  PRIMARY KEY(account_id, conversation_id),
  FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE,
  FOREIGN KEY(conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS peer_identities (
  account_id TEXT NOT NULL,
  aci TEXT NOT NULL,
  peer_key TEXT NOT NULL,
  PRIMARY KEY(account_id, aci),
  FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS history_imports (
  account_id TEXT PRIMARY KEY,
  state TEXT NOT NULL CHECK(state IN ('running', 'completed', 'failed')),
  attempts INTEGER NOT NULL DEFAULT 0,
  imported_messages INTEGER NOT NULL DEFAULT 0,
  skipped_lines INTEGER NOT NULL DEFAULT 0,
  skipped_chats INTEGER NOT NULL DEFAULT 0,
  skipped_messages INTEGER NOT NULL DEFAULT 0,
  error_class TEXT,
  started_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS history_import_stage_recipients (
  account_id TEXT NOT NULL,
  id INTEGER NOT NULL,
  kind TEXT NOT NULL,
  name TEXT NOT NULL,
  aci TEXT,
  e164 TEXT,
  master_key TEXT,
  PRIMARY KEY(account_id, id),
  FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS history_import_stage_chats (
  account_id TEXT NOT NULL,
  id INTEGER NOT NULL,
  recipient_id INTEGER NOT NULL,
  expire_timer_ms INTEGER,
  PRIMARY KEY(account_id, id),
  FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
);
";
/// DDL that must run after [`migrate_schema`], not in [`SCHEMA_DDL`], because
/// it references columns that older stores only gain through a migration.
const POST_MIGRATION_SCHEMA_DDL: &str = "
CREATE INDEX IF NOT EXISTS messages_held_since
  ON messages(COALESCE(stored_at, received_at, sent_at));
CREATE INDEX IF NOT EXISTS messages_expire_due
  ON messages(expire_at) WHERE expire_at IS NOT NULL;
";
/// Retention: newest rows a conversation keeps regardless of age.
const MAX_MESSAGES_PER_CONVERSATION: i64 = 2_000;
/// Retention: age past which a message is no longer kept.
const MESSAGE_RETENTION_MS: i64 = 90 * 24 * 60 * 60 * 1_000;
/// Preview length ingest stores for a conversation's newest message.
const PREVIEW_CHARS: i64 = 120;
/// Marks a message cursor that carries its own sort key.
const MESSAGE_CURSOR_PREFIX: &str = "m1:";
/// Marks a full-text search cursor (contract 1.30): account-scoped, carries
/// its own sort key like a message cursor.
const SEARCH_MESSAGE_CURSOR_PREFIX: &str = "s1:";
/// Retention floor. Receive dedupe and send idempotency both answer from stored
/// rows, so recent history is never pruned no matter which rule selected it:
/// signal-cli may still replay an envelope, and a resend may still arrive.
const RETENTION_SAFETY_WINDOW_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
pub const MAX_PAGE_LIMIT: u32 = 200;
/// Adjacent visual-media probe (contract 1.93, §4.76): at most this many
/// media-bearing rows per call. The desktop lightbox probes one row per
/// navigation step like the official `limit: 1`; the wider bound exists so a
/// future strip prefetch never pages unbounded.
pub const MAX_ADJACENT_MEDIA_LIMIT: u32 = 20;
/// One keyset chunk of the adjacent-media scan: rows are inspected in
/// bounded reads until enough media-bearing rows match (or the range is
/// exhausted), so a media-sparse conversation never loads its whole history
/// into memory at once.
const ADJACENT_MEDIA_SCAN_CHUNK: i64 = 128;
/// Contract 1.32 edit-history bounds: at most this many prior-body snapshots
/// are kept per message (oldest pruned) and one snapshot body is at most this
/// many bytes (UTF-8-safe truncation, the same budget as the host text
/// preview). Real edit chains stay far below both.
const MAX_EDIT_HISTORY_ENTRIES: usize = 20;
const MAX_EDIT_BODY_BYTES: usize = 4 * 1024;
/// Contract 1.27 read bounds for the per-actor reaction detail: at most this
/// many active reaction rows are fetched per conversation read (newest
/// reaction first, deterministic truncation), and one emoji pill lists at
/// most this many actors. Real conversations stay far below both; the caps
/// keep a pathological store from inflating a read response.
const MAX_REACTION_DETAIL_ROWS: usize = 1024;
const MAX_REACTION_ACTORS: usize = 64;
/// Contract 1.27: at most this many distinct reaction emoji are attached to a
/// conversation summary's last-message field (official previews render a
/// short emoji prefix, not the full pill set).
const MAX_SUMMARY_REACTIONS: usize = 8;

#[derive(Debug, Error)]
pub enum StoreError {
    /// The source io error is preserved for the chain but never logged: log
    /// the classification (Display), not the cause, per AGENTS.md.
    #[error("connector state directory is invalid")]
    InvalidStateDir(#[source] Option<io::Error>),
    /// Same discipline for the database cause: rusqlite errors may carry
    /// statement detail, so they stay in the source chain only.
    #[error("connector store is unavailable")]
    Unavailable(#[source] Option<rusqlite::Error>),
    #[error("account was not found")]
    AccountNotFound,
    #[error("conversation was not found")]
    ConversationNotFound,
    #[error("message was not found")]
    MessageNotFound,
    #[error("account delete operation conflicts with existing state")]
    OperationConflict,
    #[error("pagination cursor is invalid")]
    InvalidCursor,
    /// Fail closed (optimization-plan Phase 3): existing plaintext history is
    /// never opened for serving without a store key to encrypt it with.
    #[error(
        "connector store key is required: existing plaintext history is never opened without one"
    )]
    PlaintextStoreRequiresKey,
    /// Fail closed: without a store key there is no store at all; the desktop
    /// generates and delivers the key at spawn time (first run included).
    #[error("connector store key is required; the desktop generates and delivers it at spawn time")]
    StoreKeyRequired,
    /// The existing store did not accept the delivered key (wrong key or a
    /// damaged file) — fail closed rather than guessing.
    #[error("connector store key was rejected by the existing store")]
    StoreKeyRejected,
    /// Fail closed on migration error, never silently stay plaintext. The
    /// classified message is log-safe; the cause stays in the source chain.
    #[error("plaintext store migration to encrypted storage failed")]
    MigrationFailed(#[source] Option<Box<dyn std::error::Error + Send + Sync + 'static>>),
}

impl StoreError {
    /// Log-safe variant classification: the Display text is already
    /// content-free, and the preserved source is never logged, so logs carry
    /// this class only.
    pub fn class(&self) -> &'static str {
        match self {
            StoreError::InvalidStateDir(_) => "invalid_state_dir",
            StoreError::Unavailable(_) => "unavailable",
            StoreError::AccountNotFound => "account_not_found",
            StoreError::ConversationNotFound => "conversation_not_found",
            StoreError::MessageNotFound => "message_not_found",
            StoreError::OperationConflict => "operation_conflict",
            StoreError::InvalidCursor => "invalid_cursor",
            StoreError::PlaintextStoreRequiresKey => "plaintext_store_requires_key",
            StoreError::StoreKeyRequired => "store_key_required",
            StoreError::StoreKeyRejected => "store_key_rejected",
            StoreError::MigrationFailed(_) => "migration_failed",
        }
    }
}

#[derive(Debug, Error)]
pub enum StoreKeyError {
    /// The offending value is never echoed into the message: a rejected key
    /// candidate is still secret-shaped.
    #[error("store key must be exactly 64 lowercase hexadecimal characters")]
    Invalid,
}

const STORE_KEY_BYTES: usize = 32;

/// The 32-byte SQLCipher store key (optimization-plan Phase 3 contract). Owned
/// by the desktop, delivered as line 2 of the bootstrap payload, held only in
/// zeroizing memory, and never logged, persisted, or sent over the socket.
#[derive(Zeroize)]
#[zeroize(drop)]
pub struct StoreKey([u8; STORE_KEY_BYTES]);

impl StoreKey {
    pub fn from_bytes(bytes: [u8; STORE_KEY_BYTES]) -> Self {
        Self(bytes)
    }

    /// Canonical encoding: exactly 64 lowercase hexadecimal characters, the
    /// same shape the bootstrap secret already uses.
    pub fn from_hex(encoded: &str) -> Result<Self, StoreKeyError> {
        if encoded.len() != STORE_KEY_BYTES * 2
            || !encoded
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(StoreKeyError::Invalid);
        }
        let bytes = Zeroizing::new(hex::decode(encoded).map_err(|_| StoreKeyError::Invalid)?);
        let mut key = [0_u8; STORE_KEY_BYTES];
        key.copy_from_slice(&bytes);
        Ok(Self(key))
    }

    /// SQLCipher raw-key form (`x'<64 hex>'`), which skips passphrase KDF
    /// derivation. The rendered string is key material: zeroized on drop.
    fn raw_key_spec(&self) -> Zeroizing<String> {
        Zeroizing::new(format!("x'{}'", hex::encode(self.0)))
    }
}

/// Dev/test override only: read the store key from `KT_SIGNAL_STORE_KEY`.
/// Returns `Ok(None)` when unset so the bootstrap payload line 2 is used.
pub fn store_key_from_env() -> Result<Option<StoreKey>, StoreKeyError> {
    let value = match std::env::var(STORE_KEY_ENV) {
        Ok(value) => Zeroizing::new(value),
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => return Err(StoreKeyError::Invalid),
    };
    Ok(Some(StoreKey::from_hex(&value)?))
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountSummary {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub masked_address: String,
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub linked_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_at: Option<u64>,
    pub unread_count: u32,
    /// Proxy group the account is bound to (ADR 0001 R3). Always present on
    /// the wire; accounts linked before Phase 4 read as `default`. The binding
    /// is fixed when link.finish succeeds and never changes afterwards.
    pub proxy_group: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationSummary {
    pub id: String,
    pub account_id: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_preview: Option<String>,
    /// Contract 1.23: attachment category noun for the desktop list preview —
    /// present only when the newest message is an attachment-only row (no
    /// caption text). Text/caption and system endings stay `None`; the desktop
    /// then falls back to `last_message_preview`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_kind: Option<&'static str>,
    /// Contract 1.27: the newest row's direction — `outgoing` or `incoming`.
    /// Absent when the conversation has no rows or ends on a system row, the
    /// same endings the official list preview shows without send state or
    /// author prefix. `outgoing` marks the linked account as the author (the
    /// client renders its own localized self label).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_direction: Option<&'static str>,
    /// Contract 1.28: the newest outgoing row's send state — the exact
    /// vocabulary the message rows carry (`pending` / `sent` / `delivered` /
    /// `read` / `failed`), so the list icon and the bubble icon agree.
    /// Present only on outgoing endings with a genuine send tier: incoming
    /// and system endings, empty conversations, and outgoing rows that ended
    /// `remote-deleted`/`unknown` expose no send state (the official icon set
    /// has no representation for those).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_status: Option<&'static str>,
    /// Contract 1.27: newest incoming author's display name, captured from the
    /// envelope `sourceName` at receive and stored on the message row. Absent
    /// for outgoing (self), system endings, and rows received before 1.27 or
    /// without a captured name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_author_name: Option<String>,
    /// Contract 1.27: distinct active reaction emoji on the newest row, oldest
    /// reaction first (the same order the message pills use). Absent when the
    /// last message carries no active reactions.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub last_message_reactions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_at: Option<u64>,
    pub unread_count: u32,
    /// Contract 1.29: how many unread incoming rows @mention the linked
    /// account. Moves in lockstep with `unread_count` — incremented by the
    /// same receive transaction when the row mentions self, zeroed by the
    /// same open-chat clear — so the @ badge and the unread badge can never
    /// disagree. No account-level aggregate exists (the official list badge
    /// is per-conversation too). Absent when zero.
    #[serde(skip_serializing_if = "u32_is_zero")]
    pub unread_mentions: u32,
    pub muted: bool,
    pub pinned: bool,
    /// Contract 1.33: the conversation's current pinned message, projected
    /// from `conversation_pins` so a reloaded window renders the pin bar
    /// without replaying events. Absent when nothing is pinned, the pin
    /// expired (connector-clock computed), or the pinned row is no longer in
    /// the local history (retention) — an unrenderable pin is not surfaced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pinned_message: Option<ConversationPinnedMessage>,
    /// Contract 1.42 (§4.41): the conversation's current disappearing-message
    /// timer in seconds as the last timer-change event set it — `0` is the
    /// timer explicitly off, `N` counts down every message from its own
    /// send/start time. Absent while nothing has stated a timer yet (pre-1.42
    /// rows and conversations the connector never saw a timer event for), so
    /// an unknown state stays unknown instead of masquerading as off.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expire_timer_seconds: Option<u64>,
    /// Contract 1.43 (§4.42): true exactly for the linked account's own
    /// Note to Self chat — a direct conversation whose peer key equals the
    /// account's number. Computed at read time from the row plus the account
    /// record (zero storage), never persisted. The match is number-based by
    /// the §4.24 recorded boundary: the own ACI is not queryable upstream, so
    /// a direct row keyed only by a UUID cannot be marked. Groups are never
    /// self. The row's title stays raw contact material (the account's own
    /// profile name); the desktop localizes the presentation from this flag.
    pub is_self: bool,
    /// Contract 1.60 (§4.54): the group row's termination flag as the last
    /// contacts sync captured it (the §4.50 appendix `group_extra_json`
    /// marker). True only for a `kind='group'` conversation whose contacts
    /// row stores the flag — the flag lives in the cache row, never on the
    /// conversation itself, so a direct conversation and every pre-1.53 sync
    /// row answer false. Collapsed to an absent key when false (the same
    /// informative-keys discipline as the roster `admin` mark), so pre-1.60
    /// hosts read byte-identical summaries.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub is_terminated: bool,
    /// Contract 1.65 (§4.58): the conversation's message-request accepted
    /// state, derived at read time with the official
    /// `isConversationAccepted` factor weights over the material this chain
    /// can honestly see: the row's stored engine factors
    /// (`mrr_factors_json` — the {hidden, profileSharing, blocked} trio the
    /// throttled listContacts enrich stamps, the authoritative
    /// `acceptResponseType` bit the §4.58 receive face writes on ingest
    /// (contract 1.70), or the probed-absent `{}`), the message window, and
    /// the row's own is_self. Always serialized (bare bool, the
    /// muted/pinned/isSelf discipline): both values are meaningful — the
    /// desktop consumes the key as authoritative and keeps its local window
    /// derivation only for pre-1.65 connectors where the key is absent.
    /// Honest per-factor holes, each collapsed to inactive (documented in
    /// §4.58): `messageCountBeforeMessageRequests` (upstream never projects
    /// it), and for groups `profileSharing`/`addedBy`/`pendingAdminApprovalV2`
    /// (no engine read face) — a group row therefore derives accepted from
    /// its own outgoing window alone.
    pub message_request_accepted: bool,
    /// Contract 1.71 (§4.63): the desktop evidence-gate field the official
    /// `hasUserInitiatedMessages` faces need and no existing read face
    /// decomposes (the desktop matrix §18.4 data-plane gap, 2026-10-06
    /// record): true exactly when the conversation's **whole history**
    /// contains at least one outgoing data row — the official
    /// `sentMessageCount > 0` factor
    /// (`ts/util/isConversationAccepted.preload.ts:58,75`, AGPL read-only
    /// anchor) read as a standalone bit, window- and newest-row-independent.
    /// Derived at read time over the same indexed EXISTS the §4.58
    /// derivation runs (one source, no second truth); the factor row itself
    /// is read, never written — no counting logic changes. Always serialized
    /// (bare bool, the muted/pinned/isSelf discipline): both values are
    /// meaningful — true is the suppression signal, false the decisive
    /// negative the banner gate hangs on. A row with no store evidence at
    /// all (fresh row, link-time import gap, any history the connector does
    /// not hold) projects false — honest absence, never invented.
    pub has_user_initiated_outbound: bool,
}

/// One conversation's pinned state (contract 1.33): the local row the pin
/// resolves to, the protocol identity it was pinned under, and the pin
/// timing. `expires_at` is absent on a forever pin; a timed pin carries the
/// connector-clock expiry computed from `pinDurationSeconds` at pin time.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationPinnedMessage {
    pub message_id: String,
    pub target_author: String,
    pub target_sent_timestamp: u64,
    pub pinned_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

/// Serde gate for optional-count fields (contract 1.29): zero collapses to an
/// absent key so pre-1.29 hosts read byte-identical summaries.
fn u32_is_zero(value: &u32) -> bool {
    *value == 0
}

/// The number-only self match both wire faces share (§4.42/§4.43): the peer
/// key equals the linked account's own number. Number-based by the §4.24
/// recorded boundary — the own ACI is not queryable on either engine's
/// jsonRpc surface, so a UUID-shaped peer key cannot be marked.
fn peer_key_is_self(peer_key: &str, signal_account: Option<&str>) -> bool {
    signal_account == Some(peer_key)
}

/// Contract 1.43 (§4.42): the Note-to-Self predicate for a conversation row —
/// the direct chat whose peer key is the linked account's own number. Pure
/// read-time derivation over (kind, peer_key) plus the account record; no
/// column exists because the answer follows the keys it derives from. The
/// number-only match is the shared [`peer_key_is_self`] boundary, and groups
/// are never self.
fn summary_is_self(kind: &str, peer_key: &str, signal_account: Option<&str>) -> bool {
    kind == "direct" && peer_key_is_self(peer_key, signal_account)
}

/// Contract 1.44 (§4.43): the Note-to-Self predicate for a contacts-cache row
/// — the `kind='contact'` entry whose peer key is the linked account's own
/// number. Same read-time derivation and the same shared [`peer_key_is_self`]
/// match as the conversation summaries, so the compose list and the
/// conversation list cannot disagree about the same entry; the kind gate
/// keeps a group row unmarked no matter what its group id collides with.
fn contact_is_self(kind: &str, peer_key: &str, signal_account: Option<&str>) -> bool {
    kind == "contact" && peer_key_is_self(peer_key, signal_account)
}

/// Contract 1.60 (§4.54): the termination flag a contacts-cache row carries —
/// read out of the §4.50 appendix `group_extra_json` marker (stored only when
/// true) at projection time. Direct rows have no group marker and every
/// pre-1.53 sync row predates the key: both answer false, the same default
/// the §4.50 appendix `groups.get` projection already answers. No local
/// invention — the flag exists only because the upstream `JsonGroup` exposed
/// it to the sync.
fn extra_is_terminated(extra: Option<&str>) -> bool {
    extra
        .and_then(|extra| serde_json::from_str::<Value>(extra).ok())
        .and_then(|extra| extra.get("isTerminated").and_then(Value::as_bool))
        == Some(true)
}

/// Contract 1.65 (§4.58): the engine-supplied message-request factor trio a
/// conversation row's `mrr_factors_json` stores. Every member is optional:
/// the stamp carries exactly what the engine answer exposed (the pinned
/// signal-cli `listContacts` row always carries all three as booleans; an
/// engine without the flags — the kt-engine read face today — stamps the
/// empty object, every member absent).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MrrFactors {
    /// The upstream contact's hidden bit (`JsonContact.isHidden`): true =
    /// discovered but not in the phone's system contacts. Inverted it is the
    /// official `isInSystemContacts` trusted-contact factor (direct rows
    /// only).
    hidden: Option<bool>,
    /// The upstream contact's profile-sharing bit (`JsonContact.profileSharing`):
    /// the official `profileSharing` whitelisted factor, the same bit the
    /// official accept write flips upstream.
    profile_sharing: Option<bool>,
    /// The upstream contact's blocked bit (`JsonContact.isBlocked`). Carried
    /// for the request-row family faces; the official
    /// `isConversationAccepted` does not read it (blocked state rides the
    /// §4.45/§4.47 blocked faces), so it never factors the derivation here.
    blocked: Option<bool>,
    /// The authoritative `messageRequestResponseType` stamp (contract 1.70,
    /// §4.58 consume batch): true exactly when a linked device's
    /// `syncMessage.messageRequestResponse` named this conversation ACCEPT —
    /// the official factor's first weight, previously recorded as having no
    /// honest engine source. Absent = never received (the collapsed default);
    /// every other known member (DELETE / BLOCK / BLOCK_AND_DELETE / SPAM /
    /// BLOCK_AND_SPAM) stamps false.
    accept_response: Option<bool>,
    /// The raw member value behind [`MrrFactors::accept_response`]
    /// (UNKNOWN=0 … BLOCK_AND_SPAM=6), absent on the never-received default.
    /// Carried for diagnostics and future consumers; the derivation reads
    /// only the ACCEPT presence bit.
    accept_response_type: Option<u8>,
}

/// Parse a row's stored factor stamp. NULL (never probed) and any malformed
/// payload degrade to the all-absent default — a hostile or partial stamp can
/// never fail a summary read, the §4.50 appendix discipline.
fn mrr_factors_of(extra: Option<&str>) -> MrrFactors {
    let Some(object) = extra
        .and_then(|extra| serde_json::from_str::<Value>(extra).ok())
        .and_then(|value| value.as_object().cloned())
    else {
        return MrrFactors::default();
    };
    MrrFactors {
        hidden: object.get("hidden").and_then(Value::as_bool),
        profile_sharing: object.get("profileSharing").and_then(Value::as_bool),
        blocked: object.get("blocked").and_then(Value::as_bool),
        accept_response: object.get("acceptResponseType").and_then(Value::as_bool),
        accept_response_type: object
            .get("acceptResponseTypeMember")
            .and_then(Value::as_u64)
            .and_then(|value| u8::try_from(value).ok()),
    }
}

/// Contract 1.65 (§4.58): the message window a summary row derives its
/// accepted state from — read over the caller's already-held connection
/// guard. `has_data` mirrors the official `messageCount` factor (data rows
/// only: system rows are not conversation content the request flow gates on,
/// the same exclusion the wire `lastMessageDirection` makes); `has_outgoing`
/// mirrors `sentMessageCount`. Both read as EXISTS so a bounded row scan
/// answers, the same per-row lookup family the 1.27/1.60 projections run.
/// Contract 1.65 (§4.58): the official `isConversationAccepted` factor
/// weights (upstream `ts/util/isConversationAccepted.preload.ts:21-82`,
/// AGPL read-only anchor) collapsed onto the material this chain can see.
/// Weights kept 1:1 where a factor exists; absent factors collapse to
/// inactive exactly as the official `(attr || 0)`-style defaults do:
///
/// - `isMe` → the row's is_self (always accepted).
/// - `isEmptyPrivateConvo` → a direct row with an empty data window
///   (official: an empty conversation never shows the request UI; groups
///   need the whitelisted bit we cannot see, so an empty group stays
///   pending — the honest factor hole).
/// - `sentMessageCount > 0` → `has_outgoing`.
/// - `profileSharing` / `isInSystemContacts` → the engine-supplied
///   `profileSharing` / inverted `hidden` bits (direct rows only; groups
///   are not contacts, no engine read face exists for either).
/// - `messageRequestResponseType == ACCEPT` → the stamped
///   `acceptResponseType` bit (contract 1.70, §4.58 consume batch): the
///   authoritative face a linked device's
///   `syncMessage.messageRequestResponse` mirror writes on ingest. The
///   official factor reads the ACCEPT presence first (:33-39), so this
///   weight short-circuits ahead of the window weights: a stamped ACCEPT is
///   accepted even while the local window is incoming-only, and a stamped
///   non-ACCEPT terminal member keeps the row's window-derived answer (the
///   official factor treats every non-ACCEPT member as not-accepted — the
///   request stays pending for the desktop's banner, the §4.56 faces'
///   vocabulary). No stamp (the pre-1.70 shape) reads inactive here and the
///   derivation keeps its local fallback.
/// - `messageCountBeforeMessageRequests > 0` → inactive: upstream never
///   projects the attr (it is official-desktop migration state).
/// - `removalStage` → inactive by construction: the official factor only
///   fires when the attr is DEFINED, and ours is never stored.
/// - `addedBy` / `pendingAdminApprovalV2` (group trusted-contact factors) →
///   inactive: no engine read face.
fn derive_message_request_accepted(
    kind: &str,
    is_self: bool,
    has_data: bool,
    has_outgoing: bool,
    factors: MrrFactors,
) -> bool {
    if is_self {
        return true;
    }
    // The authoritative response stamp outranks every derived weight: the
    // user's own answer is the truth the desktop consumes (upstream :33-39).
    // Direct rows only — the sync face never names a group (groups carry no
    // request).
    if kind == "direct"
        && let Some(accept) = factors.accept_response
        && accept
    {
        return true;
    }
    if kind == "direct" && !has_data {
        return true;
    }
    has_outgoing
        || factors.profile_sharing == Some(true)
        || (kind == "direct" && factors.hidden == Some(false))
}

/// Contract 1.65 (§4.58): the accepted state for one conversation row — the
/// stored factor stamp plus the message window in one guarded lookup, called
/// from the post-map loops the 1.27/1.60 projections run (the row closures
/// answer rusqlite, so the fallible derivation rides the already-held guard
/// after the rows collect). Shared by the list page and the single-row
/// summary so a reloaded window and an event-driven refresh can never
/// disagree (the 1.60 discipline).
fn conversation_message_request_accepted(
    conn: &Connection,
    account_id: &str,
    conversation_id: &str,
    kind: &str,
    is_self: bool,
) -> Result<bool, StoreError> {
    let (factors_json, has_data, has_outgoing): (Option<String>, i64, i64) = conn
        .query_row(
            "SELECT mrr_factors_json,
               EXISTS(SELECT 1 FROM messages
                      WHERE account_id=?2 AND conversation_id=?1
                        AND direction IN ('incoming','outgoing')),
               EXISTS(SELECT 1 FROM messages
                      WHERE account_id=?2 AND conversation_id=?1 AND direction='outgoing')
             FROM conversations WHERE id=?1 AND account_id=?2",
            params![conversation_id, account_id],
            |row| {
                let factors_json: Option<String> = row.get(0)?;
                let has_data: i64 = row.get(1)?;
                let has_outgoing: i64 = row.get(2)?;
                Ok((factors_json, has_data, has_outgoing))
            },
        )
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
    Ok(derive_message_request_accepted(
        kind,
        is_self,
        has_data != 0,
        has_outgoing != 0,
        mrr_factors_of(factors_json.as_deref()),
    ))
}

/// Contract 1.71 (§4.63): the whole-history outbound row presence for one
/// conversation — the official `hasUserInitiatedMessages` evidence bit
/// (desktop matrix §18.4 data-plane gap) the summary wire now carries. Reads
/// the same indexed EXISTS the §4.58 accepted derivation runs, shares the
/// same held-connection-guard discipline, and answers false when the row is
/// gone mid-page (retention deleted between the page query and the post-map
/// loop — the same tolerance the meta lookups next door practice). No
/// counting logic is touched: the EXISTS reads the rows, it writes nothing.
fn conversation_has_user_initiated_outbound(
    conn: &Connection,
    account_id: &str,
    conversation_id: &str,
) -> bool {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM messages
             WHERE account_id=?2 AND conversation_id=?1 AND direction='outgoing')
         FROM conversations WHERE id=?1 AND account_id=?2",
        params![conversation_id, account_id],
        |row| row.get::<_, i64>(0),
    )
    .map(|present| present != 0)
    .unwrap_or(false)
}

/// Contract 1.60 (§4.54): the ids of the contacts-cache group rows whose last
/// sync captured the terminated flag. Read at projection time over the
/// caller's already-held connection guard (the store mutex is never
/// re-entrant); the scanned set is the whole group-kind contacts cache of the
/// account — one row per group, the scale the §6.5 sync already maintains.
fn group_terminated_ids(
    conn: &Connection,
    account_id: &str,
) -> Result<std::collections::HashSet<String>, StoreError> {
    let mut stmt = conn
        .prepare("SELECT id, extra FROM contacts WHERE account_id=?1 AND kind='group'")
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
    let rows = stmt
        .query_map(params![account_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
    let mut terminated = std::collections::HashSet::new();
    for row in rows {
        let (id, extra) = row.map_err(|error| StoreError::Unavailable(Some(error)))?;
        if extra_is_terminated(extra.as_deref()) {
            terminated.insert(id);
        }
    }
    Ok(terminated)
}

/// One aggregated reaction pill projected onto a message row (contract
/// 1.16, per-author detail added in 1.27): the emoji, how many distinct
/// actors reacted, whether this account is one of them, and who they are.
/// Derived from `message_events` at read time — never persisted on the
/// message row itself.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageReactionSummary {
    pub emoji: String,
    pub count: u32,
    pub mine: bool,
    /// Contract 1.27 per-actor detail: every active actor of this emoji,
    /// newest reaction first (the official ReactionViewer order). Always
    /// serialized — empty means no active actor — so host-side pill merges
    /// stay total and a removal clears the actor it removed. `count` and
    /// `actors.len()` agree: one `message_events` row per actor per target.
    pub actors: Vec<MessageReactionActor>,
}

/// One reacting actor (contract 1.27): the linked account itself
/// (`self: true`, the client renders its own localized label) or a peer with
/// the display name captured from the reaction envelope's `sourceName`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageReactionActor {
    #[serde(rename = "self")]
    pub is_self: bool,
    /// Peer display name at reaction time; absent for self and when the
    /// envelope carried no name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Local wall-clock ms the reaction was last recorded (receive time for
    /// inbound, echo time for own multi-device reactions).
    pub reacted_at: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageRecord {
    pub id: String,
    pub account_id: String,
    pub conversation_id: String,
    pub direction: &'static str,
    pub sender_id: String,
    /// Contract 1.27: incoming author display name captured from the envelope
    /// `sourceName` at receive (bounded by the engine to 64 chars). Absent on
    /// outgoing (sender is self), system rows, and pre-1.27 rows. Backs the
    /// conversation-summary author field and per-actor reaction names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_name: Option<String>,
    /// Contract 1.29: the incoming row @mentions the linked account — one of
    /// the normalized mention authors resolved to the account's own identity.
    /// Captured at receive and persisted on the row; the desktop cannot make
    /// this call itself (its account address is masked on the wire). Absent
    /// (false) on outgoing/system rows, non-mention rows, and pre-1.29 rows.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub mentions_self: bool,
    pub sent_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_bytes: Option<u32>,
    pub text_truncated: bool,
    pub text_retrievable: bool,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_message_id: Option<String>,
    /// Inbound quote snapshot (contract 1.15): upstream timestamp, author,
    /// bounded preview. Absent on rows without a quote.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_snapshot: Option<MessageQuoteSnapshot>,
    /// Attachment descriptors (metadata only, never bytes): inbound via
    /// engine normalization, outbound via send-attachment descriptors. Absent
    /// on rows without attachments.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<MessageAttachmentInfo>,
    /// Bounded rich-body payload (contract 1.25): link previews, @mentions,
    /// text-style ranges and the view-once marker, flattened onto the wire
    /// row so absent keys stay absent. None on plain and pre-1.25 rows.
    #[serde(flatten)]
    pub rich: Option<crate::engine::NormalizedRich>,
    /// Local wall-clock time the body was last edited upstream (contract
    /// 1.15). Absent on rows never edited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edited_at: Option<u64>,
    /// Aggregated active reaction pills (contract 1.16), derived from
    /// `message_events` at read time. Always serialized so host-side row
    /// merges stay total (an absent key would never clear stale pills).
    pub reactions: Vec<MessageReactionSummary>,
    /// Prior-body snapshots (contract 1.32), ascending by replacement time —
    /// the desktop renders newest-first like the official edit history
    /// unshift. Empty (absent on the wire) on rows never edited; attached at
    /// read time by `attach_edits`, never stored on the row itself.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub edits: Vec<MessageEditEntry>,
    /// Connector wall-clock ms the first delivery receipt stamped the row
    /// (contract 1.32). Absent on undelivered and pre-1.32 rows; never
    /// re-stamped by a later receipt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered_at: Option<u64>,
    /// Connector wall-clock ms the first read/viewed receipt stamped the row
    /// (contract 1.32). Same first-stamp-wins discipline as `delivered_at`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_at: Option<u64>,
    /// The viewed face (contract 1.94, §4.77): on outgoing rows, the receipt
    /// envelope timestamp of the first peer VIEWED receipt that hit the row
    /// (the official `sendState.isViewed`); on incoming rows, the connector
    /// wall-clock ms the host's `messages.markViewed` first selected the row
    /// (the official `readStatus === Viewed` — this device played it). Absent
    /// on rows never viewed and on every pre-1.94 row (never backfilled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub viewed_at: Option<u64>,
    /// Group admin-delete tombstone (contract 1.33): an inbound adminDelete
    /// envelope marked this row "deleted by admin". The body stays for the
    /// audit window and retention prunes the row normally. Absent (false) on
    /// rows no admin removed.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub admin_deleted: bool,
    /// Inbound sticker metadata (contract revision 1.35, §4.31): the pack
    /// identity beside the message. The sticker's byte pointer rides
    /// `attachments`, so absent keys stay absent on plain and pre-1.35 rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sticker: Option<crate::engine::NormalizedSticker>,
    /// Disappearing-message timer as the envelope stated it (contract
    /// revision 1.42, §4.41): `Some(n)` with `n > 0` is a live timer,
    /// `Some(0)` is the sender's timer explicitly off (every signal-cli
    /// `JsonDataMessage` carries the key), `None` is no stated value (the
    /// engine face omits the key, including its timer-off close form).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expire_seconds: Option<u64>,
    /// Engine-reported countdown start (§4.41): wall-clock ms the sender's
    /// timer began ticking, present only on engine surfaces that state it and
    /// only for a positive stamp. The sweep deadline falls back to the row's
    /// receive time when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expire_start_at: Option<u64>,
    /// The server-assigned message GUID the receive envelope carried
    /// (contract revision 1.49, §4.48): the upstream report API's path
    /// parameter, persisted per row at receive so a later
    /// `messages.reportSpam` resolves it without the caller ever seeing it.
    /// Connector-internal — never serialized onto the wire. NULL on
    /// pre-1.49 rows and envelopes without the key (never backfilled).
    #[serde(skip)]
    pub server_guid: Option<String>,
}

/// One prior body of an edited message (contract 1.32): the text the edit
/// replaced, its byte count, and the replacement time. Bounded at insert
/// (≤ 4096 bytes per entry, ≤ 20 entries per message, oldest pruned).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageEditEntry {
    pub body: String,
    pub body_bytes: u32,
    pub edited_at: u64,
}

/// Inbound quote snapshot stored on the quoted-by message row (same wire
/// shape the engine normalization produces).
pub type MessageQuoteSnapshot = crate::engine::NormalizedQuote;

/// The outcome of `upgrade_outgoing_receipts` (contract 1.94, §4.77): rows
/// whose status actually moved, plus — for a VIEWED receipt only — rows that
/// were already `read` but had never been viewed-stamped and took their
/// first `viewed_at` now. A row appears in at most one face; a replayed
/// receipt that changes nothing appears in neither.
#[derive(Debug, Default)]
pub struct UpgradedReceipts {
    pub moved: Vec<MessageRecord>,
    pub viewed_confirmed: Vec<MessageRecord>,
}

/// One attachment descriptor (metadata only, §4.13; same wire shape the
/// engine normalization produces — inbound and outbound sends alike).
pub type MessageAttachmentInfo = crate::engine::NormalizedAttachment;

/// Contacts-cache row for the §4.46 backup export build (store columns
/// verbatim; the builder decides what projects and what counts as skipped).
pub struct BackupContactRow {
    pub kind: &'static str,
    pub peer_key: String,
    pub title: String,
    pub extra: Option<String>,
}

/// Conversation row the export chats/chat items over.
pub struct BackupConversationRow {
    pub id: String,
    pub kind: &'static str,
    pub peer_key: String,
    pub title: String,
}

/// Message row flattened to the columns the v1 artifact (and its skip
/// counters) can see. Raw JSON markers stay strings; the builder interprets
/// them.
pub struct BackupMessageRow {
    pub direction: &'static str,
    pub sender_id: String,
    pub sent_at: u64,
    pub received_at: Option<u64>,
    pub body: Option<String>,
    pub body_truncated: bool,
    pub status: &'static str,
    pub admin_deleted: bool,
    pub has_quote: bool,
    pub attachments_json: Option<String>,
    pub rich_json: Option<String>,
    pub sticker_json: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactSummary {
    pub id: String,
    pub kind: &'static str,
    pub peer_key: String,
    pub title: String,
    /// Contract 1.44 (§4.43): true exactly for the linked account's own entry
    /// — the `kind='contact'` row whose peer key equals the account's number,
    /// the Note-to-Self entry the 1.43 sync admission keeps in the cache.
    /// Computed at read time from the row plus the account record (zero
    /// storage), never persisted, through the same number match the
    /// conversation summaries run. The match is number-based by the §4.24
    /// recorded boundary: the own ACI is not queryable upstream, so a
    /// UUID-keyed contact row cannot be marked. Groups are never self. The
    /// row's title stays raw contact material (the account's own profile
    /// name); the desktop localizes the presentation from this flag.
    pub is_self: bool,
    /// Contract 1.60 (§4.54): the row's own termination flag for
    /// `kind='group'` rows — the §4.50 appendix `group_extra_json` marker the
    /// sync captured, projected at read time. Direct contact rows and every
    /// pre-1.53 sync row answer false. Collapsed to an absent key when false
    /// (the same informative-keys discipline as the roster `admin` mark), so
    /// pre-1.60 hosts read byte-identical rows.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub is_terminated: bool,
}

/// One recorded reaction (contract 1.15): the actor's emoji state on a target
/// message, keyed by the target's upstream timestamp.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReactionEvent {
    pub emoji: String,
    pub target_timestamp: u64,
    pub actor_id: String,
    pub removed: bool,
    pub updated_at: u64,
}

/// One entry of a contacts sync batch: `kind` is 'contact' or 'group', `extra`
/// is an optional opaque JSON marker (e.g. member count).
#[derive(Clone, Copy, Debug)]
pub struct SyncedContact<'a> {
    pub kind: &'a str,
    pub peer_key: &'a str,
    pub title: &'a str,
    pub extra: Option<&'a str>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Page<T: Serialize> {
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Direction of the adjacent visual-media probe (contract 1.93, §4.76):
/// `Older` walks toward the conversation's past, `Newer` toward its present.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdjacentMediaDirection {
    Older,
    Newer,
}

#[derive(Clone, Debug)]
pub struct AccountRow {
    pub id: String,
    pub signal_account: String,
    pub proxy_group: String,
}

#[derive(Clone, Debug)]
pub struct ConversationRow {
    pub id: String,
    pub account_id: String,
    pub kind: String,
    pub peer_key: String,
}

/// What one retention pass removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HistoryPruneOutcome {
    pub messages_deleted: u64,
    pub conversations_repaired: u64,
}

/// One conversation's rows removed by an expire sweep (contract 1.42,
/// §4.41): the addressing the `message.expired` host event needs, message
/// ids oldest-deadline first. Deterministic `BTreeMap` grouping keeps a
/// single sweep's events stable across runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpiredMessageGroup {
    pub account_id: String,
    pub conversation_id: String,
    pub message_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountDeletePlan {
    Completed,
    Dispatch {
        signal_account: String,
        reconcile_first: bool,
    },
}

pub struct Store {
    path: PathBuf,
    conn: Mutex<Connection>,
}

impl Store {
    /// Open the per-profile store. Encryption at rest (optimization-plan Phase
    /// 3) is decided entirely here, keeping the blast radius in this function:
    ///
    /// - no key + existing plaintext store → fail closed (never serve plaintext)
    /// - no key + no store → fail closed (the desktop generates the key)
    /// - key + plaintext store → backup, migrate to SQLCipher, then open
    /// - key + encrypted store → open; a rejected key fails closed
    /// - key + no store → create an encrypted store
    pub fn open(state_dir: &Path, store_key: Option<StoreKey>) -> Result<Self, StoreError> {
        prepare_state_dir(state_dir)?;
        let path = state_dir.join(DATABASE_FILE_NAME);
        let state = db_file_state(&path)?;
        let (key, preexisting) = match (store_key, state) {
            (None, DbFileState::Plaintext) => return Err(StoreError::PlaintextStoreRequiresKey),
            (None, _) => return Err(StoreError::StoreKeyRequired),
            (Some(key), DbFileState::Plaintext) => {
                migrate_plaintext_store(&path, &key)?;
                (key, true)
            }
            (Some(key), DbFileState::Encrypted) => (key, true),
            (Some(key), DbFileState::Absent) => (key, false),
        };
        let conn = open_encrypted(&path, &key, preexisting)?;
        conn.busy_timeout(Duration::from_millis(250))
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        conn.execute_batch("PRAGMA journal_mode=WAL;\nPRAGMA foreign_keys=ON;")
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        conn.execute_batch(SCHEMA_DDL)
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        migrate_schema(&conn)?;
        // The held-since expression index (retention cost fix, prune_history)
        // references stored_at, which stores older than schema 6 only gain
        // inside migrate_schema; creating it after the migrations keeps the
        // opening DDL valid against every schema version from v0 up.
        conn.execute_batch(POST_MIGRATION_SCHEMA_DDL)
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(Self {
            path,
            conn: Mutex::new(conn),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The store is one `Arc` shared by every proxy-group runtime, so all
    /// access takes a short-lived lock on the single connection (rusqlite
    /// connections are not `Sync`). A poisoned lock means a panic mid-statement:
    /// report it with the same unavailable classification as any other
    /// storage failure.
    fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>, StoreError> {
        self.conn.lock().map_err(|_| StoreError::Unavailable(None))
    }

    /// Direct connection access for tests that install fault-injection
    /// triggers or assert on raw rows.
    #[cfg(test)]
    pub(crate) fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap()
    }

    /// Phase 3 retention: the plaintext migration backup is kept for
    /// `PLAINTEXT_BACKUP_RETENTION_MS`, then removed through the same
    /// once-per-start retention pass that prunes history. Anything that is not
    /// the exact backup file we created is left alone. Returns `true` when the
    /// backup was removed.
    pub fn prune_expired_plaintext_backup(&self, now_ms: u64) -> Result<bool, StoreError> {
        let backup = plaintext_backup_path(&self.path);
        let metadata = match std::fs::metadata(&backup) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(StoreError::InvalidStateDir(Some(error))),
        };
        if !metadata.is_file() {
            return Ok(false);
        }
        let modified_ms = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|age| age.as_millis().min(u64::MAX as u128) as u64)
            .unwrap_or(0);
        if now_ms.saturating_sub(modified_ms) < PLAINTEXT_BACKUP_RETENTION_MS {
            return Ok(false);
        }
        std::fs::remove_file(&backup).map_err(|error| StoreError::InvalidStateDir(Some(error)))?;
        Ok(true)
    }

    pub fn upsert_account_from_signal(
        &self,
        signal_account: &str,
        linked_at: Option<u64>,
        proxy_group: &str,
    ) -> Result<AccountSummary, StoreError> {
        if let Some(existing) = self.account_by_signal(signal_account)? {
            // The proxy-group binding is fixed at link time and immutable for
            // the life of the link (ADR 0001 R3): a re-sync of a known number
            // never moves it between groups.
            self.lock_conn()?
                .execute(
                    "UPDATE accounts SET state='ready', linked_at=COALESCE(linked_at, ?2)
                     WHERE id=?1",
                    params![existing.id, linked_at.map(|v| v as i64)],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            return self
                .account_summary(&existing.id)?
                .ok_or(StoreError::AccountNotFound);
        }
        let id = random_id();
        let masked = mask_address(signal_account);
        self.lock_conn()?
            .execute(
                "INSERT INTO accounts(id, signal_account, masked_address, display_name, state, linked_at, unread_count, proxy_group)
                 VALUES(?1, ?2, ?3, NULL, 'ready', ?4, 0, ?5)",
                params![
                    id,
                    signal_account,
                    masked,
                    linked_at.map(|v| v as i64),
                    proxy_group
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        self.account_summary(&id)?
            .ok_or(StoreError::AccountNotFound)
    }

    pub fn set_account_display_name(
        &self,
        account_id: &str,
        display_name: Option<&str>,
    ) -> Result<AccountSummary, StoreError> {
        let name = display_name
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        self.lock_conn()?
            .execute(
                "UPDATE accounts SET display_name=?2 WHERE id=?1",
                params![account_id, name],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        self.account_summary(account_id)?
            .ok_or(StoreError::AccountNotFound)
    }

    /// The Signal server rejected this device's credentials: the account
    /// holder unlinked this device on their phone (contract 1.21). The local
    /// credential is permanently dead — no engine restart can revive it — so
    /// the row carries `device_unlinked` until a fresh link (which re-keys the
    /// same unique-number row) or an explicit local-data delete removes it.
    /// A logout already in flight (`unlinking`) is not overwritten.
    /// `None` when no account row matches the signal-cli number.
    pub fn mark_account_device_unlinked(
        &self,
        signal_account: &str,
    ) -> Result<Option<AccountSummary>, StoreError> {
        let Some(existing) = self.account_by_signal(signal_account)? else {
            return Ok(None);
        };
        self.lock_conn()?
            .execute(
                "UPDATE accounts SET state='device_unlinked' WHERE id=?1 AND state != 'unlinking'",
                params![existing.id],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        self.account_summary(&existing.id)?
            .map(Some)
            .ok_or(StoreError::AccountNotFound)
    }

    pub fn list_accounts(&self) -> Result<Vec<AccountSummary>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, masked_address, display_name, state, linked_at, last_message_at, unread_count, proxy_group
                 FROM accounts ORDER BY linked_at IS NULL, linked_at DESC, id ASC",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(AccountSummary {
                    id: row.get(0)?,
                    masked_address: row.get(1)?,
                    display_name: row.get(2)?,
                    state: static_state(row.get::<_, String>(3)?),
                    linked_at: row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                    last_message_at: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                    unread_count: row.get::<_, i64>(6)? as u32,
                    proxy_group: row.get(7)?,
                })
            })
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// Accounts bound to one proxy group, in the same order as
    /// [`Store::list_accounts`]. The per-group fallback view used when that
    /// group's engine cannot answer.
    pub fn list_accounts_in_group(
        &self,
        proxy_group: &str,
    ) -> Result<Vec<AccountSummary>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, masked_address, display_name, state, linked_at, last_message_at, unread_count, proxy_group
                 FROM accounts WHERE proxy_group=?1
                 ORDER BY linked_at IS NULL, linked_at DESC, id ASC",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(params![proxy_group], |row| {
                Ok(AccountSummary {
                    id: row.get(0)?,
                    masked_address: row.get(1)?,
                    display_name: row.get(2)?,
                    state: static_state(row.get::<_, String>(3)?),
                    linked_at: row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                    last_message_at: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                    unread_count: row.get::<_, i64>(6)? as u32,
                    proxy_group: row.get(7)?,
                })
            })
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// Number of accounts bound to a proxy group; the `accountCount` field of
    /// the runtime `proxyGroups[]` entries (implementation-plan §4.4).
    pub fn count_accounts_in_group(&self, proxy_group: &str) -> Result<u64, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT COUNT(*) FROM accounts WHERE proxy_group=?1",
                params![proxy_group],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as u64)
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    pub fn account_by_id(&self, account_id: &str) -> Result<Option<AccountRow>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT id, signal_account, proxy_group FROM accounts WHERE id=?1",
                params![account_id],
                |row| {
                    Ok(AccountRow {
                        id: row.get(0)?,
                        signal_account: row.get(1)?,
                        proxy_group: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    pub fn account_by_signal(
        &self,
        signal_account: &str,
    ) -> Result<Option<AccountRow>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT id, signal_account, proxy_group FROM accounts WHERE signal_account=?1",
                params![signal_account],
                |row| {
                    Ok(AccountRow {
                        id: row.get(0)?,
                        signal_account: row.get(1)?,
                        proxy_group: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// Signal number of any linked account, used as a read-only liveness probe target.
    pub fn any_signal_account_number(&self) -> Result<Option<String>, StoreError> {
        self.lock_conn()?
            .query_row("SELECT signal_account FROM accounts LIMIT 1", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// Group-scoped watchdog ping target: only an account of this group's own
    /// engine can serve as its liveness probe.
    pub fn any_signal_account_number_in_group(
        &self,
        proxy_group: &str,
    ) -> Result<Option<String>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT signal_account FROM accounts WHERE proxy_group=?1 LIMIT 1",
                params![proxy_group],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    pub fn account_summary(&self, account_id: &str) -> Result<Option<AccountSummary>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT id, masked_address, display_name, state, linked_at, last_message_at, unread_count, proxy_group
                 FROM accounts WHERE id=?1",
                params![account_id],
                |row| {
                    Ok(AccountSummary {
                        id: row.get(0)?,
                        masked_address: row.get(1)?,
                        display_name: row.get(2)?,
                        state: static_state(row.get::<_, String>(3)?),
                        linked_at: row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                        last_message_at: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                        unread_count: row.get::<_, i64>(6)? as u32,
                        proxy_group: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    pub fn prepare_account_delete(
        &self,
        account_id: &str,
        operation_id: &str,
        now_ms: u64,
    ) -> Result<AccountDeletePlan, StoreError> {
        let mut conn = self.lock_conn()?;
        let transaction = conn
            .transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let existing = transaction
            .query_row(
                "SELECT account_id, state FROM account_delete_operations WHERE operation_id=?1",
                params![operation_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if let Some((existing_account_id, state)) = existing.as_ref() {
            if existing_account_id != account_id {
                return Err(StoreError::OperationConflict);
            }
            if state == "completed" {
                transaction
                    .commit()
                    .map_err(|error| StoreError::Unavailable(Some(error)))?;
                return Ok(AccountDeletePlan::Completed);
            }
        }

        let signal_account = transaction
            .query_row(
                "SELECT signal_account FROM accounts WHERE id=?1",
                params![account_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let Some(signal_account) = signal_account else {
            transaction
                .execute(
                    "INSERT INTO account_delete_operations(
                       operation_id, account_id, state, created_at, updated_at
                     ) VALUES(?1, ?2, 'completed', ?3, ?3)
                     ON CONFLICT(operation_id) DO UPDATE SET
                       state='completed', updated_at=excluded.updated_at",
                    params![operation_id, account_id, now_ms as i64],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            prune_completed_account_deletes(&transaction)?;
            transaction
                .commit()
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            return Ok(AccountDeletePlan::Completed);
        };

        let reconcile_first = existing.is_some();
        if existing.is_none() {
            transaction
                .execute(
                    "INSERT INTO account_delete_operations(
                       operation_id, account_id, state, created_at, updated_at
                     ) VALUES(?1, ?2, 'started', ?3, ?3)",
                    params![operation_id, account_id, now_ms as i64],
                )
                .map_err(|error| {
                    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
                        StoreError::OperationConflict
                    } else {
                        StoreError::Unavailable(Some(error))
                    }
                })?;
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(AccountDeletePlan::Dispatch {
            signal_account,
            reconcile_first,
        })
    }

    pub fn mark_account_delete_unknown(
        &self,
        operation_id: &str,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        let changed = self
            .lock_conn()?
            .execute(
                "UPDATE account_delete_operations
                 SET state='unknown', updated_at=?2
                 WHERE operation_id=?1 AND state!='completed'",
                params![operation_id, now_ms as i64],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if changed == 0 {
            return Err(StoreError::OperationConflict);
        }
        Ok(())
    }

    /// Remove account and dependent rows from the connector store (local exit).
    pub fn complete_account_delete(
        &self,
        account_id: &str,
        operation_id: Option<&str>,
        now_ms: u64,
    ) -> Result<bool, StoreError> {
        let mut conn = self.lock_conn()?;
        let transaction = conn
            .transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM accounts WHERE id=?1",
                params![account_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .is_some();
        if exists {
            transaction
                .execute(
                    // Reactions first: message_events carries a plain
                    // conversation_id FK (no cascade, unlike every other
                    // account-scoped table), so dropping conversations before
                    // it aborts the whole delete — the production stuck-logout
                    // on stores holding any reaction row.
                    "DELETE FROM message_events WHERE account_id=?1",
                    params![account_id],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            transaction
                .execute(
                    "DELETE FROM messages WHERE account_id=?1",
                    params![account_id],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            transaction
                .execute(
                    "DELETE FROM conversations WHERE account_id=?1",
                    params![account_id],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            transaction
                .execute(
                    "DELETE FROM contacts WHERE account_id=?1",
                    params![account_id],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            transaction
                .execute(
                    "DELETE FROM meta WHERE key=?1",
                    params![format!("contacts_synced_at:{account_id}")],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            transaction
                .execute("DELETE FROM accounts WHERE id=?1", params![account_id])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        if let Some(operation_id) = operation_id {
            let changed = transaction
                .execute(
                    "UPDATE account_delete_operations
                     SET state='completed', updated_at=?3
                     WHERE operation_id=?1 AND account_id=?2",
                    params![operation_id, account_id, now_ms as i64],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            if changed == 0 {
                return Err(StoreError::OperationConflict);
            }
            prune_completed_account_deletes(&transaction)?;
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(exists)
    }

    pub fn delete_account_cascade(&self, account_id: &str) -> Result<bool, StoreError> {
        self.complete_account_delete(account_id, None, 0)
    }

    pub fn ensure_conversation(
        &self,
        account_id: &str,
        kind: &str,
        peer_key: &str,
        title: &str,
    ) -> Result<ConversationRow, StoreError> {
        if let Some(existing) = self.conversation_by_peer(account_id, kind, peer_key)? {
            // Upgrade masked/placeholder titles when we learn a real display name.
            if let Some(current) = self.conversation_title(&existing.id)?
                && title_should_upgrade(&current, title)
            {
                let _ = self.set_conversation_title(&existing.id, title);
            }
            return Ok(existing);
        }
        let id = stable_hash_id(&[account_id, kind, peer_key]);
        self.lock_conn()?
            .execute(
                "INSERT INTO conversations(id, account_id, kind, peer_key, title, unread_count, muted, pinned)
                 VALUES(?1, ?2, ?3, ?4, ?5, 0, 0, 0)",
                params![id, account_id, kind, peer_key, title],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(ConversationRow {
            id,
            account_id: account_id.to_string(),
            kind: kind.to_string(),
            peer_key: peer_key.to_string(),
        })
    }

    pub fn conversation_title(&self, conversation_id: &str) -> Result<Option<String>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT title FROM conversations WHERE id=?1",
                params![conversation_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    pub fn set_conversation_title(
        &self,
        conversation_id: &str,
        title: &str,
    ) -> Result<(), StoreError> {
        let trimmed = title.trim();
        if trimmed.is_empty() {
            return Ok(());
        }
        self.lock_conn()?
            .execute(
                "UPDATE conversations SET title=?2 WHERE id=?1",
                params![conversation_id, trimmed],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    pub fn set_conversation_title_for_peer(
        &self,
        account_id: &str,
        kind: &str,
        peer_key: &str,
        title: &str,
    ) -> Result<bool, StoreError> {
        let Some(row) = self.conversation_by_peer(account_id, kind, peer_key)? else {
            return Ok(false);
        };
        let current = self.conversation_title(&row.id)?.unwrap_or_default();
        if !title_should_upgrade(&current, title) {
            return Ok(false);
        }
        self.set_conversation_title(&row.id, title)?;
        Ok(true)
    }

    /// Contract 1.65 (§4.58): stamp one direct conversation's engine factor
    /// trio (`mrr_factors_json`). The caller serializes exactly the flags the
    /// engine answer exposed — the pinned signal-cli row always carries all
    /// three booleans; an engine without the flags stamps the honest
    /// probed-absent `{}` marker so the probe never re-fires for the row.
    /// Direct rows only: groups are not contacts and carry no trio. Unstamped
    /// rows stay NULL and are never backfilled. The trio keys are this
    /// setter's truth (a re-stamp replaces them verbatim); the authoritative
    /// `acceptResponseType` pair the §4.58 receive face
    /// ([`Store::set_conversation_accept_response_factor`]) wrote survives a
    /// re-stamp — each key family has exactly one writer, so the throttled
    /// enrich can never clobber the response state and the response face can
    /// never clobber the trio.
    pub fn set_conversation_mrr_factors_for_peer(
        &self,
        account_id: &str,
        peer_key: &str,
        factors_json: &str,
    ) -> Result<bool, StoreError> {
        let conn = self.lock_conn()?;
        // The accept pair is not the enrich's to write: carry it over from
        // the row's current stamp so a re-stamp of the trio (every enrich
        // window re-stamps every direct row the engine answer names) keeps
        // the response state.
        let accept = conn
            .query_row(
                "SELECT mrr_factors_json FROM conversations
                 WHERE account_id=?1 AND kind='direct' AND peer_key=?2",
                params![account_id, peer_key],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .flatten()
            .and_then(|current| serde_json::from_str::<Value>(&current).ok())
            .and_then(|value| value.as_object().cloned());
        let mut object = serde_json::from_str::<Value>(factors_json)
            .ok()
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        if let Some(current) = accept {
            for key in ["acceptResponseType", "acceptResponseTypeMember"] {
                if let Some(value) = current.get(key)
                    && !object.contains_key(key)
                {
                    object.insert(key.to_string(), value.clone());
                }
            }
        }
        let merged = Value::Object(object).to_string();
        let updated = conn
            .execute(
                "UPDATE conversations SET mrr_factors_json=?1
                 WHERE account_id=?2 AND kind='direct' AND peer_key=?3",
                params![merged, account_id, peer_key],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(updated > 0)
    }

    /// Contract 1.70 (§4.58 consume batch): merge the authoritative
    /// `messageRequestResponseType` face into one direct conversation's
    /// factor stamp. The stamp is read-modify-written so the sync face never
    /// clobbers the enrich trio (and vice versa): the accept keys land on
    /// top of whatever the row already stores, and a row with no stamp at
    /// all gets exactly the accept pair. Direct rows only, matching the
    /// sync-face grammar (the response never names a group thread with
    /// request semantics).
    pub fn set_conversation_accept_response_factor(
        &self,
        account_id: &str,
        conversation_id: &str,
        accepted: bool,
        response_type: u8,
    ) -> Result<bool, StoreError> {
        let conn = self.lock_conn()?;
        // Outer None = no such direct row (or the wrong kind); inner None =
        // the row exists but was never enriched (a NULL stamp) — both inner
        // shapes merge to an accept-only stamp.
        let current: Option<Option<String>> = conn
            .query_row(
                "SELECT mrr_factors_json FROM conversations
                 WHERE id=?1 AND account_id=?2 AND kind='direct'",
                params![conversation_id, account_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let Some(current) = current else {
            // No such direct row (or the wrong kind): nothing to stamp.
            return Ok(false);
        };
        let mut object = current
            .as_deref()
            .and_then(|json| serde_json::from_str::<Value>(json).ok())
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        object.insert("acceptResponseType".to_string(), Value::Bool(accepted));
        object.insert(
            "acceptResponseTypeMember".to_string(),
            Value::from(response_type),
        );
        let merged = Value::Object(object).to_string();
        let updated = conn
            .execute(
                "UPDATE conversations SET mrr_factors_json=?1
                 WHERE id=?2 AND account_id=?3 AND kind='direct'",
                params![merged, conversation_id, account_id],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(updated > 0)
    }

    /// Contract 1.65 (§4.58): how many direct conversation rows owe their
    /// factor probe — never stamped (`mrr_factors_json IS NULL`) **and** the
    /// only rows whose derivation can still flip on a stamp (a data window
    /// with no outgoing row: empty windows are accepted by the official
    /// empty-private-conversation weight and outgoing-bearing windows by
    /// `sentMessageCount`, regardless of the factors). The throttled enrich
    /// widens its trigger with this count so a derivability-critical fresh
    /// row is probed on the next window even when every title is already
    /// resolved; once every such row carries a stamp (values, the
    /// probed-absent `{}` marker, or the §4.58 receive face's authoritative
    /// accept stamp) the probe goes quiet and the enrich keeps its
    /// no-engine-call fast path.
    pub fn count_direct_conversations_without_mrr_factors(
        &self,
        account_id: &str,
    ) -> Result<i64, StoreError> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT count(*) FROM conversations c
             WHERE c.account_id=?1 AND c.kind='direct' AND c.mrr_factors_json IS NULL
               AND EXISTS(SELECT 1 FROM messages m
                          WHERE m.account_id=c.account_id
                            AND m.conversation_id=c.id
                            AND m.direction IN ('incoming','outgoing'))
               AND NOT EXISTS(SELECT 1 FROM messages m
                              WHERE m.account_id=c.account_id
                                AND m.conversation_id=c.id
                                AND m.direction='outgoing')",
            params![account_id],
            |row| row.get(0),
        )
        .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    pub fn conversation_by_id(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<Option<ConversationRow>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT id, account_id, kind, peer_key FROM conversations
                 WHERE id=?1 AND account_id=?2",
                params![conversation_id, account_id],
                |row| {
                    Ok(ConversationRow {
                        id: row.get(0)?,
                        account_id: row.get(1)?,
                        kind: row.get(2)?,
                        peer_key: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    pub fn conversation_by_peer(
        &self,
        account_id: &str,
        kind: &str,
        peer_key: &str,
    ) -> Result<Option<ConversationRow>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT id, account_id, kind, peer_key FROM conversations
                 WHERE account_id=?1 AND kind=?2 AND peer_key=?3",
                params![account_id, kind, peer_key],
                |row| {
                    Ok(ConversationRow {
                        id: row.get(0)?,
                        account_id: row.get(1)?,
                        kind: row.get(2)?,
                        peer_key: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// Direct chats that may still show a masked peer id as title.
    pub fn list_direct_peers_needing_title(
        &self,
        account_id: &str,
    ) -> Result<Vec<(String /* peer_key */, String /* title */)>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT peer_key, title FROM conversations
                 WHERE account_id=?1 AND kind='direct'",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(params![account_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut out = Vec::new();
        for row in rows {
            let (peer, title) = row.map_err(|error| StoreError::Unavailable(Some(error)))?;
            if title.contains("***") || title.trim().is_empty() {
                out.push((peer, title));
            }
        }
        Ok(out)
    }

    pub fn list_conversations(
        &self,
        account_id: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<ConversationSummary>, StoreError> {
        let limit = limit.clamp(1, MAX_PAGE_LIMIT);
        let fetch = limit + 1;
        let decoded = cursor
            .map(|value| decode_conversation_cursor(value, account_id))
            .transpose()?;
        let cursor_present = i64::from(decoded.is_some());
        let cursor_null = i64::from(decoded.as_ref().is_some_and(|value| value.0));
        let cursor_sent_at = decoded.as_ref().and_then(|value| value.1);
        let cursor_id = decoded.as_ref().map(|value| value.2.as_str());
        let conn = self.lock_conn()?;
        // Contract 1.43 (§4.42): is_self derives the row from the account's
        // own number at read time — one lookup per page, through the already
        // held connection guard.
        let signal_account: Option<String> = conn
            .query_row(
                "SELECT signal_account FROM accounts WHERE id=?1",
                params![account_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, account_id, kind, title, last_message_preview, last_message_at,
                        unread_count, unread_mentions, muted, pinned, expire_timer_seconds, peer_key,
                        mrr_factors_json
                 FROM conversations
                 WHERE account_id=?1
                   AND (
                     ?2=0
                     OR (
                       ?3=0 AND (
                         last_message_at IS NULL
                         OR last_message_at < ?4
                         OR (last_message_at = ?4 AND id < ?5)
                       )
                     )
                     OR (?3=1 AND last_message_at IS NULL AND id < ?5)
                   )
                 ORDER BY last_message_at IS NULL, last_message_at DESC, id DESC
                 LIMIT ?6",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(
                params![
                    account_id,
                    cursor_present,
                    cursor_null,
                    cursor_sent_at,
                    cursor_id,
                    fetch as i64,
                ],
                |row| {
                    let kind = static_kind(row.get::<_, String>(2)?);
                    let peer_key: String = row.get(11)?;
                    Ok(ConversationSummary {
                        id: row.get(0)?,
                        account_id: row.get(1)?,
                        kind,
                        title: row.get(3)?,
                        last_message_preview: row.get(4)?,
                        last_message_kind: None,
                        last_message_direction: None,
                        last_message_status: None,
                        last_message_author_name: None,
                        last_message_reactions: Vec::new(),
                        last_message_at: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                        unread_count: row.get::<_, i64>(6)? as u32,
                        unread_mentions: row.get::<_, i64>(7)? as u32,
                        muted: row.get::<_, i64>(8)? != 0,
                        pinned: row.get::<_, i64>(9)? != 0,
                        pinned_message: None,
                        expire_timer_seconds: row
                            .get::<_, Option<i64>>(10)?
                            .map(|value| value.max(0) as u64),
                        is_self: summary_is_self(kind, &peer_key, signal_account.as_deref()),
                        // Contract 1.60 (§4.54): stamped from the contacts
                        // cache after the row maps (page lookup / single-row
                        // query below).
                        is_terminated: false,
                        // Contract 1.65 (§4.58): derived after the row maps
                        // (page lookup below), the same post-map loop.
                        message_request_accepted: false,
                        // Contract 1.71 (§4.63): derived in the same post-map
                        // loop (below), the same read-time family.
                        has_user_initiated_outbound: false,
                    })
                },
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut items = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        for item in &mut items {
            let meta = conversation_last_message_meta(&conn, account_id, &item.id)?;
            item.last_message_kind = meta.kind;
            item.last_message_direction = meta.direction;
            item.last_message_status = meta.status;
            item.last_message_author_name = meta.author_name;
            item.last_message_reactions = meta.reactions;
            item.pinned_message = pinned_message_for_conversation(&conn, account_id, &item.id)?;
            // Contract 1.65 (§4.58): the same read-time accepted derivation
            // the single-row summary applies, over the same held guard — a
            // reloaded page and an event-driven window cannot disagree.
            item.message_request_accepted = conversation_message_request_accepted(
                &conn,
                account_id,
                &item.id,
                item.kind,
                item.is_self,
            )?;
            // Contract 1.71 (§4.63): the whole-history outbound bit, the same
            // post-map loop, the same guard.
            item.has_user_initiated_outbound =
                conversation_has_user_initiated_outbound(&conn, account_id, &item.id);
        }
        let next_cursor = if items.len() as u32 > limit {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_conversation_cursor(account_id, item))
        } else {
            None
        };
        // Contract 1.60 (§4.54): group termination flags ride the summaries —
        // one bounded lookup against the contacts cache over the already-held
        // connection guard (the flag lives in the `group_extra_json` marker
        // the sync captured; §4.50 appendix). For a group the conversation
        // row and the contacts row share the same
        // `stable_hash_id([account, 'group', peer])` id, so the summary is
        // stamped through its own id; direct conversations and groups without
        // a cache row stay false.
        let terminated = group_terminated_ids(&conn, account_id)?;
        for item in &mut items {
            if item.kind == "group" {
                item.is_terminated = terminated.contains(&item.id);
            }
        }
        Ok(Page { items, next_cursor })
    }

    pub fn list_messages(
        &self,
        account_id: &str,
        conversation_id: &str,
        limit: u32,
        before: Option<&str>,
    ) -> Result<Page<MessageRecord>, StoreError> {
        let limit = limit.clamp(1, MAX_PAGE_LIMIT);
        let fetch = limit + 1;
        // A cursor carries its own sort key so a page still resolves after the
        // row it pointed at is gone — retention and account deletes both remove
        // history a caller may still be paging through.
        let anchor: Option<(i64, String)> = match before {
            Some(cursor) if cursor.starts_with(MESSAGE_CURSOR_PREFIX) => {
                Some(decode_message_cursor(cursor, account_id, conversation_id)?)
            }
            // Cursors handed out before this format, still held by a live host.
            Some(message_id) => {
                let sent_at: Option<i64> = self
                    .lock_conn()?
                    .query_row(
                        "SELECT sent_at FROM messages
                          WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                        params![message_id, account_id, conversation_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|error| StoreError::Unavailable(Some(error)))?;
                Some((
                    sent_at.ok_or(StoreError::InvalidCursor)?,
                    message_id.to_string(),
                ))
            }
            None => None,
        };
        let before_sent_at = anchor.as_ref().map(|value| value.0);
        let before_id = anchor.as_ref().map(|value| value.1.as_str());
        // Scoped guard: the page query's connection lock must be released
        // before the reaction aggregation re-enters the store (a std Mutex
        // is not reentrant — attach_reactions takes the lock itself).
        let mut items = {
            let conn = self.lock_conn()?;
            let mut stmt = conn
                .prepare(
                    "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                            body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                            quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                     FROM messages
                     WHERE account_id=?1 AND conversation_id=?2
                       AND (?3 IS NULL OR sent_at < ?3 OR (sent_at = ?3 AND id < ?4))
                     ORDER BY sent_at DESC, id DESC
                     LIMIT ?5",
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            let rows = stmt
                .query_map(
                    params![
                        account_id,
                        conversation_id,
                        before_sent_at,
                        before_id,
                        fetch as i64
                    ],
                    message_record_from_row,
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| StoreError::Unavailable(Some(error)))?
        };
        let next_cursor = if items.len() as u32 > limit {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_message_cursor(account_id, conversation_id, item))
        } else {
            None
        };
        self.attach_reactions(account_id, &mut items)?;
        self.attach_edits(account_id, &mut items)?;
        Ok(Page { items, next_cursor })
    }

    /// Adjacent visual-media probe (contract 1.93, §4.76): the messages next
    /// to an anchor in one direction that carry at least one visual-media
    /// attachment — an `image/*` or `video/*` content type, the desktop
    /// lightbox strip's own predicate — with view-once rows always excluded
    /// (the §4.37 boundary: a tap-to-view row never joins a navigation
    /// strip). Ordered adjacency-first: the row closest to the anchor comes
    /// first, and the anchor itself is never returned (the official
    /// requireDifferentMessage posture). Rows are inspected in bounded
    /// keyset chunks until `limit` matches accumulate or the range is
    /// exhausted, so a media-sparse conversation never loads its whole
    /// history at once; a missing anchor answers `InvalidCursor` (the
    /// caller probes from a row it already holds — a vanished anchor is a
    /// programming error, never silently re-anchored). Story rows need no
    /// predicate: a story never lands in this table at all (§4.67). The
    /// result carries the same stored shape as `messages.list`, reactions
    /// and edits attached the same way, so every read face agrees.
    pub fn list_adjacent_media(
        &self,
        account_id: &str,
        conversation_id: &str,
        anchor_message_id: &str,
        direction: AdjacentMediaDirection,
        limit: u32,
    ) -> Result<Vec<MessageRecord>, StoreError> {
        let limit = limit.clamp(1, MAX_ADJACENT_MEDIA_LIMIT);
        let anchor_sent_at: i64 = self
            .lock_conn()?
            .query_row(
                "SELECT sent_at FROM messages
                  WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                params![anchor_message_id, account_id, conversation_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .ok_or(StoreError::InvalidCursor)?;
        let mut matches: Vec<MessageRecord> = Vec::new();
        let mut cursor_sent_at = anchor_sent_at;
        let mut cursor_id = anchor_message_id.to_string();
        // The scan walks the keyset in the probe direction one bounded chunk
        // at a time; only the retained (sent_at, id) pair moves, never the
        // anchor, so matched rows can never be re-read by a later chunk.
        while (matches.len() as u32) < limit {
            let chunk = {
                let conn = self.lock_conn()?;
                let sql = match direction {
                    AdjacentMediaDirection::Older => {
                        "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                                body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                                quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                         FROM messages
                         WHERE account_id=?1 AND conversation_id=?2
                           AND (sent_at < ?3 OR (sent_at = ?3 AND id < ?4))
                         ORDER BY sent_at DESC, id DESC
                         LIMIT ?5"
                    }
                    AdjacentMediaDirection::Newer => {
                        "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                                body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                                quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                         FROM messages
                         WHERE account_id=?1 AND conversation_id=?2
                           AND (sent_at > ?3 OR (sent_at = ?3 AND id > ?4))
                         ORDER BY sent_at ASC, id ASC
                         LIMIT ?5"
                    }
                };
                let mut stmt = conn
                    .prepare(sql)
                    .map_err(|error| StoreError::Unavailable(Some(error)))?;
                let rows = stmt
                    .query_map(
                        params![
                            account_id,
                            conversation_id,
                            cursor_sent_at,
                            cursor_id,
                            ADJACENT_MEDIA_SCAN_CHUNK
                        ],
                        message_record_from_row,
                    )
                    .map_err(|error| StoreError::Unavailable(Some(error)))?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|error| StoreError::Unavailable(Some(error)))?
            };
            let exhausted = chunk.len() < ADJACENT_MEDIA_SCAN_CHUNK as usize;
            if let Some(last) = chunk.last() {
                cursor_sent_at = last.sent_at as i64;
                cursor_id = last.id.clone();
            }
            matches.extend(chunk.into_iter().filter(message_has_visual_media));
            if exhausted {
                break;
            }
        }
        matches.truncate(limit as usize);
        self.attach_reactions(account_id, &mut matches)?;
        self.attach_edits(account_id, &mut matches)?;
        Ok(matches)
    }

    /// Account-wide substring search over stored message bodies (contract
    /// 1.30, local store only — upstream Signal offers no server-side
    /// search, and Signal-Desktop searches its own database the same way).
    /// Newest-first pages across every conversation; reactions are attached
    /// so a hit renders identically to a thread read. The caller normalizes
    /// the query (trim, empty → no-op); this method only defends against an
    /// empty pattern matching every row.
    pub fn search_messages(
        &self,
        account_id: &str,
        query: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<MessageRecord>, StoreError> {
        if query.is_empty() {
            return Ok(Page {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let limit = limit.clamp(1, MAX_PAGE_LIMIT);
        let fetch = limit + 1;
        // The search cursor carries its own sort key like a message cursor,
        // so a page still resolves after retention removes a row it pointed
        // at — but it is bound to the account alone, since results span
        // conversations.
        let anchor = cursor
            .map(|value| decode_search_message_cursor(value, account_id))
            .transpose()?;
        let before_sent_at = anchor.as_ref().map(|value| value.0);
        let before_id = anchor.as_ref().map(|value| value.1.as_str());
        // SQLite LIKE folds case only for ASCII; non-ASCII terms match
        // byte-exactly. That is a recorded contract boundary (§4.25), not
        // something to paper over with an unindexable full fold.
        let like = escape_like(query);
        // Scoped guard, same as `list_messages`: release the connection lock
        // before `attach_reactions` re-enters the store.
        let mut items = {
            let conn = self.lock_conn()?;
            let mut stmt = conn
                .prepare(
                    "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                            body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                            quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                     FROM messages
                     WHERE account_id=?1
                       AND body LIKE '%'||?2||'%' ESCAPE '\\'
                       AND (?3 IS NULL OR sent_at < ?3 OR (sent_at = ?3 AND id < ?4))
                     ORDER BY sent_at DESC, id DESC
                     LIMIT ?5",
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            let rows = stmt
                .query_map(
                    params![account_id, like, before_sent_at, before_id, fetch as i64],
                    message_record_from_row,
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| StoreError::Unavailable(Some(error)))?
        };
        let next_cursor = if items.len() as u32 > limit {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_search_message_cursor(account_id, item))
        } else {
            None
        };
        self.attach_reactions(account_id, &mut items)?;
        self.attach_edits(account_id, &mut items)?;
        Ok(Page { items, next_cursor })
    }

    pub fn message_by_client_request(
        &self,
        account_id: &str,
        client_request_id: &str,
    ) -> Result<Option<MessageRecord>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages WHERE account_id=?1 AND client_request_id=?2",
                params![account_id, client_request_id],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    pub fn message_by_id(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<Option<MessageRecord>, StoreError> {
        let mut record = self
            .lock_conn()?
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                params![message_id, account_id, conversation_id],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if let Some(record) = record.as_mut() {
            self.attach_reactions(account_id, std::slice::from_mut(record))?;
            self.attach_edits(account_id, std::slice::from_mut(record))?;
        }
        Ok(record)
    }

    pub fn message_by_signal_identity(
        &self,
        account_id: &str,
        conversation_id: &str,
        direction: &str,
        sent_at: u64,
        sender_id: &str,
        legacy_sender_id: &str,
    ) -> Result<Option<MessageRecord>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND direction=?3 AND sent_at=?4
                   AND sender_id IN (?5, ?6)
                 ORDER BY id ASC LIMIT 2",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut rows = stmt
            .query_map(
                params![
                    account_id,
                    conversation_id,
                    direction,
                    sent_at as i64,
                    sender_id,
                    legacy_sender_id,
                ],
                message_record_from_row,
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let first = rows
            .next()
            .transpose()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if rows.next().is_some() {
            return Ok(None);
        }
        Ok(first)
    }

    pub fn insert_message(
        &self,
        message: &MessageRecord,
        client_request_id: Option<&str>,
        preview: Option<&str>,
        increment_unread: bool,
    ) -> Result<bool, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        // Retention counts how long this machine has kept a row, so the
        // stored-at stamp reads our clock here and never a peer's claimed
        // time; the sweep deadline derives from the same clock when the
        // envelope's own start stamp is absent (§4.41).
        let stored_at = crate::link::now_ms();
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO messages(
                    id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                    stored_at, body, body_bytes, body_truncated, status, client_request_id,
                    quote_message_id, quote_snapshot, attachments_json, live_attachment_locators,
                    rich_json, edited_at, sender_name,
                    mentions_self, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?14, ?8, ?9, ?10, ?11, ?12, ?13, ?15, ?16, ?17, ?19, ?18, ?20, ?21, ?22, ?23, ?24, ?25, ?26)",
                params![
                    message.id,
                    message.account_id,
                    message.conversation_id,
                    message.direction,
                    message.sender_id,
                    message.sent_at as i64,
                    message.received_at.map(|v| v as i64),
                    message.text,
                    message.text_bytes.map(i64::from),
                    i64::from(message.text_truncated && !message.text_retrievable),
                    message.status,
                    client_request_id,
                    message.quote_message_id,
                    stored_at as i64,
                    message
                        .quote_snapshot
                        .as_ref()
                        .map(|quote| serde_json::to_string(quote).expect("quote snapshot json")),
                    (!message.attachments.is_empty()).then(|| {
                        serde_json::to_string(&message.attachments).expect("attachments json")
                    }),
                    // The receive envelope's transit pointer faces (contract
                    // 1.61), index-aligned with `attachments_json`; the
                    // host-facing descriptor stays byte-identical (v1 shape).
                    // An engine without the projection stores all-None faces
                    // (indistinguishable from NULL on the read face).
                    (!message.attachments.is_empty()).then(|| {
                        serde_json::to_string(
                            &message
                                .attachments
                                .iter()
                                .map(|descriptor| descriptor.locator.clone().unwrap_or_default())
                                .collect::<Vec<_>>(),
                        )
                        .expect("live attachment locators json")
                    }),
                    message.edited_at.map(|v| v as i64),
                    message
                        .rich
                        .as_ref()
                        .map(|rich| serde_json::to_string(rich).expect("rich json")),
                    message.sender_name,
                    i64::from(message.mentions_self),
                    message
                        .sticker
                        .as_ref()
                        .map(|sticker| serde_json::to_string(sticker).expect("sticker json")),
                    message.expire_seconds.map(|value| value as i64),
                    message.expire_start_at.map(|value| value as i64),
                    expire_deadline(message, stored_at).map(|value| value as i64),
                    message.server_guid,
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if inserted == 0 {
            transaction
                .commit()
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            return Ok(false);
        }
        // The @ badge rides the same transaction as the unread badge (contract
        // 1.29): an incoming row that mentions self bumps both counters or
        // neither, so a crash between them can never split the pair.
        let conversation_updated = transaction
            .execute(
                "UPDATE conversations
                 SET last_message_preview=?2,
                     last_message_at=?3,
                     unread_count = unread_count + ?4,
                     unread_mentions = unread_mentions + ?5
                 WHERE id=?1",
                params![
                    message.conversation_id,
                    preview,
                    message.sent_at as i64,
                    if increment_unread { 1 } else { 0 },
                    i64::from(increment_unread && message.mentions_self)
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if conversation_updated != 1 {
            return Err(StoreError::ConversationNotFound);
        }
        let account_updated = transaction
            .execute(
                "UPDATE accounts
                 SET last_message_at=?2,
                     unread_count = unread_count + ?3
                 WHERE id=?1",
                params![
                    message.account_id,
                    message.sent_at as i64,
                    if increment_unread { 1 } else { 0 }
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if account_updated != 1 {
            return Err(StoreError::AccountNotFound);
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(true)
    }

    /// Opening a chat: zero conversation unread and subtract from account total.
    pub fn clear_conversation_unread(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<u32, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let prev: i64 = transaction
            .query_row(
                "SELECT unread_count FROM conversations WHERE id=?1 AND account_id=?2",
                params![conversation_id, account_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .unwrap_or(0);
        if prev <= 0 {
            transaction
                .commit()
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            return Ok(0);
        }
        let conversation_updated = transaction
            .execute(
                // Contract 1.29: the @ badge clears with the unread badge —
                // opening the chat is reading it, exactly like the official
                // conversation open.
                "UPDATE conversations SET unread_count=0, unread_mentions=0
                 WHERE id=?1 AND account_id=?2",
                params![conversation_id, account_id],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if conversation_updated != 1 {
            return Err(StoreError::ConversationNotFound);
        }
        let account_updated = transaction
            .execute(
                "UPDATE accounts
                 SET unread_count = CASE
                     WHEN unread_count > ?2 THEN unread_count - ?2
                     ELSE 0
                 END
                 WHERE id=?1",
                params![account_id, prev],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if account_updated != 1 {
            return Err(StoreError::AccountNotFound);
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(prev as u32)
    }

    /// Update one message's status. Returns the row after the write together
    /// with whether the status actually transitioned: an idempotent replay of
    /// the same terminal write reports `false`, so the service layer does not
    /// re-emit a status event for a no-op.
    pub fn update_message_status(
        &self,
        message_id: &str,
        status: &str,
        sent_at: Option<u64>,
    ) -> Result<Option<(MessageRecord, bool)>, StoreError> {
        // One transaction for the write and the read-back: two separate
        // lock/execute rounds let another writer flip the status in between,
        // so the returned (record, changed) pair could describe two different
        // writes — and the status event built from it would carry a status
        // this call never set. The transaction keeps the pair atomic.
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let changed = transaction
            .execute(
                "UPDATE messages SET status=?2, sent_at=COALESCE(?3, sent_at)
                 WHERE id=?1 AND status<>?2",
                params![message_id, status, sent_at.map(|v| v as i64)],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let record = transaction
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages WHERE id=?1",
                params![message_id],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(record.map(|record| (record, changed == 1)))
    }

    /// Rearm exactly one failed outgoing row for a send retry (contract 1.31):
    /// the transition `failed -> pending` is the single guarded write — a row
    /// in any other state (or already rearmed by a concurrent retry) is left
    /// untouched and reported as `changed=false`, so a double-click retry or a
    /// status push that settled the row between guard and rearm can never arm
    /// two dispatches of the same message. Same transaction shape as
    /// `update_message_status`: one lock round for write + read-back.
    pub fn rearm_failed_message(
        &self,
        message_id: &str,
    ) -> Result<Option<(MessageRecord, bool)>, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let changed = transaction
            .execute(
                "UPDATE messages SET status='pending' WHERE id=?1 AND status='failed'",
                params![message_id],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let record = transaction
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages WHERE id=?1",
                params![message_id],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(record.map(|record| (record, changed == 1)))
    }

    /// Apply an inbound edit (contract 1.15): replace the body of the row the
    /// target upstream timestamp addresses, only when the editor matches the
    /// row's sender identity, and stamp `edited_at`. Returns the updated row,
    /// or None when no matching row exists (missing target / editor mismatch /
    /// already remote-deleted) — the caller drops the edit in that case.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_inbound_edit(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_sent_at: u64,
        sender_id: &str,
        legacy_sender_id: &str,
        new_text: &str,
        new_text_bytes: u32,
    ) -> Result<Option<MessageRecord>, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        // The sender match uses the same identity pair as receive dedupe: the
        // sender hash for incoming rows differs between store generations.
        // The prior body is read before the overwrite so the contract-1.32
        // snapshot can record what this edit replaced.
        let prior: Option<(String, String, i64)> = transaction
            .query_row(
                "SELECT id, body, body_bytes FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                   AND sender_id IN (?4, ?5) AND direction='incoming'
                   AND status NOT IN ('remote-deleted', 'system')
                 ORDER BY id ASC LIMIT 1",
                params![
                    account_id,
                    conversation_id,
                    target_sent_at as i64,
                    sender_id,
                    legacy_sender_id,
                ],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        row.get::<_, Option<i64>>(2)?.unwrap_or(0),
                    ))
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let changed = transaction
            .execute(
                "UPDATE messages
                 SET body=?4, body_bytes=?5, body_truncated=0, edited_at=?6
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                   AND sender_id IN (?7, ?8) AND direction='incoming'
                   AND status NOT IN ('remote-deleted', 'system')",
                params![
                    account_id,
                    conversation_id,
                    target_sent_at as i64,
                    new_text,
                    new_text_bytes,
                    crate::link::now_ms() as i64,
                    sender_id,
                    legacy_sender_id,
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if changed == 0 {
            transaction
                .commit()
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            return Ok(None);
        }
        if let Some((message_id, prior_body, _)) = prior {
            Self::snapshot_prior_body(&transaction, account_id, &message_id, &prior_body)?;
        }
        let record = transaction
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                   AND sender_id IN (?4, ?5)
                 ORDER BY id ASC LIMIT 1",
                params![
                    account_id,
                    conversation_id,
                    target_sent_at as i64,
                    sender_id,
                    legacy_sender_id,
                ],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(record)
    }

    /// Replace an outgoing row's body after a confirmed upstream `send` with
    /// `editTimestamp` (contract 1.15). The status is untouched — an edited
    /// message stays `sent` — and `edited_at` marks the row.
    pub fn apply_outgoing_edit(
        &self,
        message_id: &str,
        account_id: &str,
        _conversation_id: &str,
        new_text: &str,
        new_text_bytes: u32,
    ) -> Result<Option<MessageRecord>, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        // Contract-1.32 snapshot: the body this host-initiated edit replaces.
        let prior: Option<(String, String)> = transaction
            .query_row(
                "SELECT body, body_bytes FROM messages
                 WHERE id=?1 AND account_id=?2 AND direction='outgoing'",
                params![message_id, account_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                        row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                    ))
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .map(|(body, _)| (message_id.to_string(), body));
        transaction
            .execute(
                "UPDATE messages
                 SET body=?3, body_bytes=?4, body_truncated=0, edited_at=?5
                 WHERE id=?1 AND account_id=?2
                   AND direction='outgoing'",
                params![
                    message_id,
                    account_id,
                    new_text,
                    new_text_bytes,
                    crate::link::now_ms() as i64,
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if let Some((snapshot_id, prior_body)) = prior {
            Self::snapshot_prior_body(&transaction, account_id, &snapshot_id, &prior_body)?;
        }
        let record = transaction
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages WHERE id=?1",
                params![message_id],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(record)
    }

    /// Our own multi-device edit mirror (contract 1.15): locate the outgoing
    /// row by its upstream timestamp — the phone's edit echoes
    /// `targetSentTimestamp`, which equals the row's `sent_at` after
    /// `complete_outgoing_send` overwrote it — and apply the same body
    /// replacement as a host-initiated edit.
    pub fn apply_outgoing_edit_by_signal(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_sent_at: u64,
        new_text: &str,
        new_text_bytes: u32,
    ) -> Result<Option<MessageRecord>, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        // Contract-1.32 snapshot: the body this multi-device mirror edit
        // replaces, read under the same predicate the UPDATE below guards.
        let prior: Option<(String, String)> = transaction
            .query_row(
                "SELECT id, body FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                   AND direction='outgoing'
                   AND status NOT IN ('remote-deleted', 'system')
                 ORDER BY id ASC LIMIT 1",
                params![account_id, conversation_id, target_sent_at as i64],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                    ))
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .execute(
                "UPDATE messages
                 SET body=?4, body_bytes=?5, body_truncated=0, edited_at=?6
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                   AND direction='outgoing'
                   AND status NOT IN ('remote-deleted', 'system')",
                params![
                    account_id,
                    conversation_id,
                    target_sent_at as i64,
                    new_text,
                    new_text_bytes,
                    crate::link::now_ms() as i64,
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if let Some((snapshot_id, prior_body)) = prior {
            Self::snapshot_prior_body(&transaction, account_id, &snapshot_id, &prior_body)?;
        }
        let record = transaction
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                   AND direction='outgoing'
                 ORDER BY id ASC LIMIT 1",
                params![account_id, conversation_id, target_sent_at as i64],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(record)
    }

    /// Mark a message remote-deleted (contract 1.15): a peer's or our own
    /// multi-device remoteDelete pointing at this row's upstream timestamp.
    /// Idempotent; returns the updated row when the status actually
    /// transitioned so a replayed delete does not re-emit events.
    pub fn mark_remote_deleted(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_sent_at: u64,
    ) -> Result<Option<(MessageRecord, bool)>, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let changed = transaction
            .execute(
                "UPDATE messages
                 SET status='remote-deleted'
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                   AND status NOT IN ('remote-deleted', 'system')",
                params![account_id, conversation_id, target_sent_at as i64],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let record = transaction
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                 ORDER BY id ASC LIMIT 1",
                params![account_id, conversation_id, target_sent_at as i64],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(record.map(|record| (record, changed > 0)))
    }

    /// Record the conversation's pinned state (contract 1.33). The official
    /// model keeps at most one pinned message per conversation, so the
    /// (account, conversation) primary key turns a newer pin into a
    /// replacement of the older one. `pinned_at` is the connector clock at
    /// pin time; `expires_at` is that clock plus `pinDurationSeconds`
    /// (absent duration = forever pin, no expiry). A missing target row is
    /// not an error: the state is still derived (the summary projection only
    /// surfaces pins whose row is locally renderable).
    pub fn upsert_conversation_pin(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_author: &str,
        target_sent_at: u64,
        duration_seconds: Option<u32>,
    ) -> Result<(), StoreError> {
        let pinned_at = crate::link::now_ms();
        let expires_at =
            duration_seconds.map(|seconds| pinned_at.saturating_add(u64::from(seconds) * 1_000));
        self.lock_conn()?
            .execute(
                "INSERT INTO conversation_pins(
                    account_id, conversation_id, target_author, target_sent_at,
                    pinned_at, expires_at
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(account_id, conversation_id) DO UPDATE SET
                   target_author=excluded.target_author,
                   target_sent_at=excluded.target_sent_at,
                   pinned_at=excluded.pinned_at,
                   expires_at=excluded.expires_at",
                params![
                    account_id,
                    conversation_id,
                    target_author,
                    target_sent_at as i64,
                    pinned_at as i64,
                    expires_at.map(|value| value as i64),
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// Clear the conversation's pinned state, but only when it still points
    /// at the (author, timestamp) the unpin / admin-delete names — the
    /// official unpinMessage carries a target identity, and a stale or
    /// mismatched unpin leaves a newer pin alone. Returns whether a row was
    /// removed so replays stay silent.
    pub fn clear_conversation_pin(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_author: &str,
        target_sent_at: u64,
    ) -> Result<bool, StoreError> {
        let changed = self
            .lock_conn()?
            .execute(
                "DELETE FROM conversation_pins
             WHERE account_id=?1 AND conversation_id=?2
               AND target_author=?3 AND target_sent_at=?4",
                params![
                    account_id,
                    conversation_id,
                    target_author,
                    target_sent_at as i64
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(changed > 0)
    }

    /// Mark a message admin-deleted (contract 1.33): a group admin removed
    /// this row. The status ladder is untouched — the tombstone is the
    /// additive `admin_deleted` flag and the body stays for the audit window;
    /// retention prunes the row like any other. Idempotent; returns the
    /// updated row when the flag actually flipped so a replayed adminDelete
    /// does not re-emit events.
    pub fn mark_admin_deleted(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_sent_at: u64,
    ) -> Result<Option<(MessageRecord, bool)>, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let changed = transaction
            .execute(
                "UPDATE messages
                 SET admin_deleted=1
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                   AND admin_deleted=0 AND status!='system'",
                params![account_id, conversation_id, target_sent_at as i64],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let record = transaction
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                 ORDER BY id ASC LIMIT 1",
                params![account_id, conversation_id, target_sent_at as i64],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(record.map(|record| (record, changed > 0)))
    }

    /// Record the ACI→peer-key identity an inbound envelope revealed
    /// (contract revision 1.38, §4.37): the engine carries `sourceUuid` (the
    /// sender's ACI) on every envelope while conversations key on the peer
    /// identity the source resolved to (a phone number on a contacts hit, the
    /// ACI itself otherwise). One row per (account, ACI) — the mapping is
    /// bounded by the contacts scale and `INSERT OR REPLACE` keeps the newest
    /// observation. Written by ingest; an older store simply lacks the row
    /// and the §4.37 ladder falls back.
    pub fn upsert_peer_identity(
        &self,
        account_id: &str,
        aci: &str,
        peer_key: &str,
    ) -> Result<(), StoreError> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT OR REPLACE INTO peer_identities (account_id, aci, peer_key)
             VALUES (?1, ?2, ?3)",
            params![account_id, aci, peer_key],
        )
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// The peer keys one ACI has been observed as (contract revision 1.38):
    /// the §4.37 ladder's second rung. Empty when the account has no recorded
    /// mapping — the ACI itself stays the first rung.
    pub fn peer_keys_for_aci(
        &self,
        account_id: &str,
        aci: &str,
    ) -> Result<Vec<String>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare("SELECT peer_key FROM peer_identities WHERE account_id=?1 AND aci=?2")
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(params![account_id, aci], |row| row.get::<_, String>(0))
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(rows)
    }

    /// The inverse of `peer_keys_for_aci` (contract revision 1.49, §4.48):
    /// the most recent ACI a conversation's peer key was observed as. A
    /// direct conversation keyed on a phone number maps through the row the
    /// ingest wrote from the envelope's `sourceUuid`; no row (never seen
    /// live, or history-imported) answers None and the caller refuses.
    pub fn aci_for_peer_key(
        &self,
        account_id: &str,
        peer_key: &str,
    ) -> Result<Option<String>, StoreError> {
        let conn = self.lock_conn()?;
        let aci = conn
            .query_row(
                "SELECT aci FROM peer_identities WHERE account_id=?1 AND peer_key=?2 LIMIT 1",
                params![account_id, peer_key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(aci)
    }

    // -------------------------------------------------------------------------
    // Android local-backup export raw reads (contract revision 1.47, §4.46).
    // The artifact builder walks the account's rows in bulk on the blocking
    // pool; the read projections here return the stored columns verbatim
    // (no per-row enrichment), because the builder — not the store — decides
    // what projects into the artifact and what counts as skipped. The row
    // structs live at module scope beside `MessageRecord`.
    // -------------------------------------------------------------------------

    /// One contacts-cache row: kind (`contact`/`group`), peer key, title, and
    /// the opaque sync marker (`members` roster for groups).
    pub fn backup_contact_rows(
        &self,
        account_id: &str,
    ) -> Result<Vec<BackupContactRow>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT kind, peer_key, title, extra FROM contacts
                 WHERE account_id=?1 ORDER BY kind, peer_key",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(params![account_id], |row| {
                Ok(BackupContactRow {
                    kind: static_contact_kind(row.get::<_, String>(0)?),
                    peer_key: row.get(1)?,
                    title: row.get(2)?,
                    extra: row.get(3)?,
                })
            })
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(rows)
    }

    /// All conversation rows for the account (id/kind/peer/title only).
    pub fn backup_conversation_rows(
        &self,
        account_id: &str,
    ) -> Result<Vec<BackupConversationRow>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, kind, peer_key, title FROM conversations
                 WHERE account_id=?1 ORDER BY id",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(params![account_id], |row| {
                Ok(BackupConversationRow {
                    id: row.get(0)?,
                    kind: static_kind(row.get::<_, String>(1)?),
                    peer_key: row.get(2)?,
                    title: row.get(3)?,
                })
            })
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(rows)
    }

    /// All message rows of one conversation, oldest first — the artifact's
    /// per-chat emission order. Bounded by the retention cap
    /// (`MAX_MESSAGES_PER_CONVERSATION`); the builder's own caps fail closed
    /// before any runaway can reach disk.
    pub fn backup_message_rows(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<Vec<BackupMessageRow>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT direction, sender_id, sent_at, received_at, body, body_truncated,
                        status, admin_deleted, quote_message_id, attachments_json,
                        rich_json, sticker_json
                 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2
                 ORDER BY sent_at ASC, id ASC",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(params![account_id, conversation_id], |row| {
                Ok(BackupMessageRow {
                    direction: static_direction(row.get::<_, String>(0)?),
                    sender_id: row.get(1)?,
                    sent_at: row.get::<_, i64>(2)?.max(0) as u64,
                    received_at: row.get::<_, Option<i64>>(3)?.map(|v| v.max(0) as u64),
                    body: row.get(4)?,
                    body_truncated: row.get::<_, i64>(5)? != 0,
                    status: static_status(row.get::<_, String>(6)?),
                    admin_deleted: row.get::<_, i64>(7)? != 0,
                    has_quote: row.get::<_, Option<String>>(8)?.is_some(),
                    attachments_json: row.get(9)?,
                    rich_json: row.get(10)?,
                    sticker_json: row.get(11)?,
                })
            })
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(rows)
    }

    /// Every `message_events` row of the account — v1 artifacts carry no
    /// reaction face, so the builder counts them all as skipped.
    pub fn count_reaction_events(&self, account_id: &str) -> Result<u64, StoreError> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT COUNT(*) FROM message_events WHERE account_id=?1",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as u64)
        .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// Every `message_edits` row of the account — v1 artifacts carry no edit
    /// face (the row body is the latest body), so the builder counts them all
    /// as skipped.
    pub fn count_message_edits(&self, account_id: &str) -> Result<u64, StoreError> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT COUNT(*) FROM message_edits WHERE account_id=?1",
            params![account_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as u64)
        .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// The observed aci ↔ peer-key pairs of the account: the builder hashes
    /// both identity forms of a peer so an incoming row's `sender_id` resolves
    /// no matter which spelling the receive envelope used.
    pub fn backup_peer_identity_rows(
        &self,
        account_id: &str,
    ) -> Result<Vec<(String, String)>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare("SELECT aci, peer_key FROM peer_identities WHERE account_id=?1")
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(params![account_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(rows)
    }

    // -------------------------------------------------------------------------
    // Link-time history import (contract revision 1.39, §4.38): the status row
    // plus the bounded batch writer and per-run staging scratch. The writer
    // keeps the live-receive identity discipline — the same signal-message-v2
    // stable id over (account, conversation, direction, sent_at, sender) — so
    // an import, a re-run, and live envelopes of the same Signal message all
    // collapse onto one row. The record types live at module scope beside
    // `MessageRecord`.
    // -------------------------------------------------------------------------

    /// Start (or restart) one import run: resets the counters, bumps the
    /// attempt counter, and flips the state to `running`.
    pub fn begin_history_import(
        &self,
        account_id: &str,
        now_ms: u64,
    ) -> Result<HistoryImportRow, StoreError> {
        self.lock_conn()?
            .execute(
                "INSERT INTO history_imports(
                    account_id, state, attempts, imported_messages, skipped_lines,
                    skipped_chats, skipped_messages, error_class, started_at, updated_at
                 ) VALUES(?1, 'running', 1, 0, 0, 0, 0, NULL, ?2, ?2)
                 ON CONFLICT(account_id) DO UPDATE SET
                   state='running',
                   attempts=history_imports.attempts + 1,
                   imported_messages=0,
                   skipped_lines=0,
                   skipped_chats=0,
                   skipped_messages=0,
                   error_class=NULL,
                   started_at=excluded.started_at,
                   updated_at=excluded.updated_at",
                params![account_id, now_ms as i64],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        self.history_import_row(account_id)?
            .ok_or(StoreError::AccountNotFound)
    }

    /// Land the terminal state of one import run with the final counters.
    pub fn complete_history_import(
        &self,
        account_id: &str,
        state: &str,
        counters: &ImportCounters,
        error_class: Option<&str>,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        debug_assert!(matches!(state, "completed" | "failed"));
        let changed = self
            .lock_conn()?
            .execute(
                "UPDATE history_imports
                 SET state=?2, imported_messages=?3, skipped_lines=?4, skipped_chats=?5,
                     skipped_messages=?6, error_class=?7, updated_at=?8
                 WHERE account_id=?1",
                params![
                    account_id,
                    state,
                    counters.imported as i64,
                    counters.skipped_lines as i64,
                    counters.skipped_chats as i64,
                    counters.skipped_messages as i64,
                    error_class,
                    now_ms as i64,
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if changed == 0 {
            return Err(StoreError::AccountNotFound);
        }
        Ok(())
    }

    pub fn history_import_row(
        &self,
        account_id: &str,
    ) -> Result<Option<HistoryImportRow>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT account_id, state, attempts, imported_messages, skipped_lines, \
                        skipped_chats, skipped_messages, error_class, started_at, updated_at
                 FROM history_imports WHERE account_id=?1",
                params![account_id],
                history_import_row_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// Clear one account's `failed` import row (contract revision 1.53,
    /// §4.38 retry face). Only a `failed` row qualifies — the caller owns the
    /// state gate; the delete returning 0 changed rows is the same-account
    /// race answer, not an error. Clearing re-arms the orchestration: the
    /// sweep (next start) or the link watcher picks the archive up and
    /// `begin_history_import` re-inserts the row with attempts+1.
    pub fn clear_failed_history_import(&self, account_id: &str) -> Result<(), StoreError> {
        self.lock_conn()?
            .execute(
                "DELETE FROM history_imports WHERE account_id=?1 AND state='failed'",
                params![account_id],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// The stored attachment pointer faces of one imported message
    /// (contract 1.55, §4.38 attachv2) — the download wiring's future input
    /// (§4.38 step 3, not wired this batch). NULL column or pre-1.55 row
    /// answers an empty vector; a malformed JSON cell is storage corruption
    /// of an internal column and surfaces as `Unavailable`, never silently
    /// swallowed (the faces are the download's key material — failing closed
    /// beats failing wrong).
    pub fn history_attachment_locators(
        &self,
        account_id: &str,
        message_id: &str,
    ) -> Result<Vec<HistoryAttachmentLocator>, StoreError> {
        self.message_attachment_locators_column(account_id, message_id, "attachment_locators")
    }

    /// The live-receive locator faces of one message row (contract revision
    /// 1.61, §4.54): the schema-19 `live_attachment_locators` column, the
    /// receive-envelope twin of [`Self::history_attachment_locators`]. Same
    /// read discipline: NULL / pre-19 row answers empty; a malformed cell is
    /// `Unavailable` (failing closed beats failing wrong on key material).
    pub fn live_attachment_locators(
        &self,
        account_id: &str,
        message_id: &str,
    ) -> Result<Vec<HistoryAttachmentLocator>, StoreError> {
        self.message_attachment_locators_column(account_id, message_id, "live_attachment_locators")
    }

    /// Shared single-column reader behind both locator faces.
    fn message_attachment_locators_column(
        &self,
        account_id: &str,
        message_id: &str,
        column: &str,
    ) -> Result<Vec<HistoryAttachmentLocator>, StoreError> {
        // The column name is a call-site literal, never caller input.
        let sql = format!("SELECT {column} FROM messages WHERE account_id=?1 AND id=?2");
        let cell: Option<String> = self
            .lock_conn()?
            .query_row(&sql, params![account_id, message_id], |row| {
                row.get::<_, Option<String>>(0)
            })
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .flatten();
        cell.map(|json| {
            serde_json::from_str(&json).map_err(|error| {
                StoreError::Unavailable(Some(rusqlite::Error::ToSqlConversionFailure(error.into())))
            })
        })
        .unwrap_or_else(|| Ok(Vec::new()))
    }

    /// Append one batch of pass-1 recipient staging rows (bounded: the caller
    /// streams the archive and flushes one batch at a time; nothing
    /// proportional to the archive is held in memory). The caller clears the
    /// account's scratch at the start of every run via [`Self::clear_history_stage`]
    /// so a crashed predecessor never leaves stale mappings behind.
    pub fn stage_history_recipients(
        &self,
        account_id: &str,
        recipients: &[HistoryStageRecipient],
    ) -> Result<(), StoreError> {
        if recipients.is_empty() {
            return Ok(());
        }
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        for recipient in recipients {
            transaction
                .execute(
                    "INSERT OR REPLACE INTO history_import_stage_recipients(
                        account_id, id, kind, name, aci, e164, master_key
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        account_id,
                        recipient.id as i64,
                        recipient.kind,
                        recipient.name,
                        recipient.aci,
                        recipient.e164,
                        recipient.master_key,
                    ],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// Append one batch of pass-1 chat→recipient staging rows.
    pub fn stage_history_chats(
        &self,
        account_id: &str,
        chats: &[HistoryStageChat],
    ) -> Result<(), StoreError> {
        if chats.is_empty() {
            return Ok(());
        }
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        for chat in chats {
            transaction
                .execute(
                    "INSERT OR REPLACE INTO history_import_stage_chats(
                        account_id, id, recipient_id, expire_timer_ms
                     ) VALUES(?1, ?2, ?3, ?4)",
                    params![
                        account_id,
                        chat.id as i64,
                        chat.recipient_id as i64,
                        chat.expire_timer_ms.map(|value| value as i64),
                    ],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// Drop the account's staging scratch (called on every exit path; the
    /// tables stay empty between runs).
    pub fn clear_history_stage(&self, account_id: &str) -> Result<(), StoreError> {
        let conn = self.lock_conn()?;
        for table in [
            "history_import_stage_recipients",
            "history_import_stage_chats",
        ] {
            conn.execute(
                &format!("DELETE FROM {table} WHERE account_id=?1"),
                params![account_id],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        Ok(())
    }

    /// One staged conversation identity (pass-2 point lookup): the chat's
    /// recipient row joined through the staging scratch, plus the chat
    /// frame's timer in ms (§4.41) — `None` when the frame stated none.
    pub fn history_chat_recipient(
        &self,
        account_id: &str,
        chat_id: u64,
    ) -> Result<Option<(HistoryStageRecipient, Option<u64>)>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT r.id, r.kind, r.name, r.aci, r.e164, r.master_key, c.expire_timer_ms
                 FROM history_import_stage_chats c
                 JOIN history_import_stage_recipients r
                   ON r.account_id = c.account_id AND r.id = c.recipient_id
                 WHERE c.account_id=?1 AND c.id=?2",
                params![account_id, chat_id as i64],
                |row| {
                    Ok((
                        HistoryStageRecipient {
                            id: row.get::<_, i64>(0)? as u64,
                            kind: row.get(1)?,
                            name: row.get(2)?,
                            aci: row.get(3)?,
                            e164: row.get(4)?,
                            master_key: row.get(5)?,
                        },
                        row.get::<_, Option<i64>>(6)?
                            .map(|value| value.max(0) as u64),
                    ))
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// One staged recipient identity (pass-2 author lookup).
    pub fn history_recipient(
        &self,
        account_id: &str,
        recipient_id: u64,
    ) -> Result<Option<HistoryStageRecipient>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT id, kind, name, aci, e164, master_key
                 FROM history_import_stage_recipients
                 WHERE account_id=?1 AND id=?2",
                params![account_id, recipient_id as i64],
                history_stage_recipient_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// Write one bounded import batch in a single transaction
    /// (§4.38 bounded discipline). Rows carry their final identity; the
    /// writer deduplicates on both the stable id (`INSERT OR IGNORE`) and the
    /// live identity tuple (sender id or its number/ACI alternate), then
    /// advances the touched conversations' summaries monotonically (a backfill
    /// must never regress `last_message_at`) and recomputes the account
    /// summary from the conversation maxima in the same transaction.
    pub fn insert_history_batch(
        &self,
        rows: &[HistoryImportInsert],
    ) -> Result<HistoryBatchOutcome, StoreError> {
        let mut outcome = HistoryBatchOutcome::default();
        if rows.is_empty() {
            return Ok(outcome);
        }
        let stored_at = crate::link::now_ms() as i64;
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        for row in rows {
            let record = &row.record;
            // Identity dedupe across the number/ACI duality: live receive may
            // have stored this Signal message under the number-derived sender
            // hash while the import resolved the ACI form (or the reverse), so
            // both candidate hashes qualify as "already stored".
            let sender_clause = match row.alt_sender_id {
                Some(_) => "IN (?5, ?6)",
                None => "IN (?5)",
            };
            let sql = format!(
                "SELECT 1 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND direction=?3
                   AND sent_at=?4 AND sender_id {sender_clause}
                 LIMIT 1"
            );
            let mut stmt = transaction
                .prepare(&sql)
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            let exists = {
                let sent_at = record.sent_at as i64;
                let mut all_params: Vec<&dyn rusqlite::ToSql> = vec![
                    &record.account_id,
                    &record.conversation_id,
                    &record.direction,
                    &sent_at,
                    &record.sender_id,
                ];
                if let Some(alt) = &row.alt_sender_id {
                    all_params.push(alt);
                }
                stmt.query_row(all_params.as_slice(), |_| Ok(()))
                    .optional()
                    .map_err(|error| StoreError::Unavailable(Some(error)))?
                    .is_some()
            };
            if exists {
                outcome.duplicates += 1;
                continue;
            }
            let inserted = transaction
                .execute(
                    "INSERT OR IGNORE INTO messages(
                        id, account_id, conversation_id, direction, sender_id, sent_at,
                        received_at, stored_at, body, body_bytes, body_truncated, status,
                        client_request_id, quote_message_id, quote_snapshot, attachments_json,
                        attachment_locators, rich_json, edited_at, sender_name, mentions_self,
                        sticker_json, expire_seconds, expire_start_at, expire_at, server_guid,
                        admin_deleted
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8, ?9, ?10, ?11, NULL, ?12,
                              ?13, ?14, ?21, ?20, NULL, ?15, ?16, ?22, ?17, ?18, ?19, NULL, ?23)",
                    params![
                            record.id,
                            record.account_id,
                            record.conversation_id,
                            record.direction,
                            record.sender_id,
                            record.sent_at as i64,
                            stored_at,
                            record.text,
                            record.text_bytes.map(i64::from),
                            i64::from(record.text_truncated && !record.text_retrievable),
                            record.status,
                            record.quote_message_id,
                            record
                                .quote_snapshot
                                .as_ref()
                                .map(|quote| serde_json::to_string(quote)
                                    .expect("quote snapshot json")),
                            (!record.attachments.is_empty()).then(|| {
                                serde_json::to_string(&record.attachments)
                                    .expect("attachments json")
                            }),
                            record.sender_name,
                            i64::from(record.mentions_self),
                            record.expire_seconds.map(|value| value as i64),
                            record.expire_start_at.map(|value| value as i64),
                            expire_deadline(record, stored_at as u64).map(|value| value as i64),
                            record
                                .rich
                                .as_ref()
                                .map(|rich| serde_json::to_string(rich).expect("rich json")),
                            (!row.locators.is_empty()).then(|| {
                                serde_json::to_string(&row.locators)
                                    .expect("attachment locators json")
                            }),
                            // The 1.35 sticker identity (contract 1.76 §4.38):
                            // the batch writer lands what the importer
                            // projected — absent stays NULL like plain rows.
                            record.sticker.as_ref().map(
                                |sticker| serde_json::to_string(sticker).expect("sticker json")
                            ),
                            // The 1.33 tombstone (contract 1.81 §4.38): the
                            // batch writer lands the importer's flag beside
                            // the intact row, exactly the live posture.
                            i64::from(record.admin_deleted),
                        ],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            if inserted == 0 {
                outcome.duplicates += 1;
                continue;
            }
            outcome.inserted += 1;
            // Backfill never regresses a summary: only advance when the
            // imported row is newer than what the conversation already shows.
            transaction
                .execute(
                    "UPDATE conversations
                     SET last_message_preview=?2, last_message_at=?3
                     WHERE id=?1 AND (last_message_at IS NULL OR last_message_at < ?3)",
                    params![record.conversation_id, row.preview, record.sent_at as i64],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        transaction
            .execute(
                "UPDATE accounts
                 SET last_message_at=(
                     SELECT MAX(last_message_at) FROM conversations WHERE account_id=?1
                 )
                 WHERE id=?1",
                params![rows[0].record.account_id],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(outcome)
    }

    /// Bounded view-once burn candidates for one open-sync identity set
    /// (contract revision 1.38, §4.37): rows at the synced timestamp whose
    /// conversation matches one of the candidate peer keys (incoming — the
    /// sync names the author), plus outgoing rows at the same timestamp (the
    /// sender's own devices burn their sent copy). Incoming first, `LIMIT 16`
    /// caps one sync's scan; the caller picks the first candidate whose rich
    /// record carries the view-once marker.
    pub fn view_once_open_candidates(
        &self,
        account_id: &str,
        sent_at: u64,
        peer_keys: &[String],
    ) -> Result<Vec<MessageRecord>, StoreError> {
        if peer_keys.is_empty() {
            return Ok(Vec::new());
        }
        let peer_keys = &peer_keys[..peer_keys.len().min(16)];
        let placeholders = peer_keys.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT m.id, m.account_id, m.conversation_id, m.direction, m.sender_id, m.sent_at, m.received_at,
                    m.body, m.body_bytes, m.body_truncated, m.status, m.quote_message_id, m.client_request_id,
                    m.quote_snapshot, m.attachments_json, m.rich_json, m.edited_at, m.sender_name, m.mentions_self,
                    m.delivered_at, m.read_at, m.admin_deleted, m.sticker_json,
                    m.expire_seconds, m.expire_start_at, m.expire_at, m.server_guid, m.viewed_at
             FROM messages m
             JOIN conversations c ON c.id = m.conversation_id AND c.account_id = m.account_id
             WHERE m.account_id = ?1 AND m.sent_at = ?2 AND m.rich_json IS NOT NULL
               AND (
                 (m.direction = 'incoming' AND c.peer_key IN ({placeholders}))
                 OR m.direction = 'outgoing'
               )
             ORDER BY CASE m.direction WHEN 'incoming' THEN 0 ELSE 1 END, m.id ASC
             LIMIT 16"
        );
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut bind: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(peer_keys.len() + 2);
        bind.push(&account_id);
        let sent_at_bound = sent_at as i64;
        bind.push(&sent_at_bound);
        for key in peer_keys {
            bind.push(key);
        }
        let rows = stmt
            .query_map(bind.as_slice(), message_record_from_row)
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(rows)
    }

    /// Burn one view-once row's bytes (contract revision 1.38, §4.37): clear
    /// the body and rewrite the rich record with the opened stamp — the
    /// metadata (attachment descriptors, quote snapshot, sticker identity)
    /// stays so the row still renders as the viewed placeholder. The first
    /// call performs the transition (`changed=true`); replays rewrite nothing
    /// and report the already-burned row (`changed=false`), so exactly one
    /// host event and one upstream fan-out escape per message. The caller
    /// validated the view-once marker before calling (the service lock makes
    /// that check race-free).
    pub fn burn_view_once_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        opened_at: u64,
    ) -> Result<Option<(MessageRecord, bool)>, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rich_json: Option<String> = transaction
            .query_row(
                "SELECT rich_json FROM messages
                 WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                params![message_id, account_id, conversation_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let Some(rich_json) = rich_json else {
            return Ok(None);
        };
        let mut rich: crate::engine::NormalizedRich =
            serde_json::from_str(&rich_json).unwrap_or_default();
        let changed = rich.view_once_opened_at.is_none();
        if changed {
            rich.view_once_opened_at = Some(opened_at);
            let burned = serde_json::to_string(&rich).map_err(|_| StoreError::Unavailable(None))?;
            transaction
                .execute(
                    "UPDATE messages
                     SET body=NULL, body_bytes=NULL, rich_json=?4
                     WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                    params![message_id, account_id, conversation_id, burned],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        let record = transaction
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self,
                        delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages
                 WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                params![message_id, account_id, conversation_id],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(record.map(|record| (record, changed)))
    }

    /// Which of the given upstream timestamps exist as outgoing rows in one
    /// conversation (contract 1.85, §4.70): the story-viewed split's hit
    /// face. A timestamp that hits any outgoing row — whatever its status —
    /// is a message receipt, so a replayed `read`-already receipt never
    /// leaks onto the story-viewed event; the leftovers are the story
    /// candidates. Input order preserved, bounded at the same 256 the
    /// receipt upgrade takes.
    pub fn existing_outgoing_sent_at(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_sent_at: &[u64],
    ) -> Result<Vec<u64>, StoreError> {
        if target_sent_at.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT 1 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                   AND direction='outgoing'
                 LIMIT 1",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut hits = Vec::new();
        for timestamp in target_sent_at.iter().copied().take(256) {
            let found = stmt
                .query_row(
                    params![account_id, conversation_id, timestamp as i64],
                    |_| Ok(()),
                )
                .optional()
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            if found.is_some() {
                hits.push(timestamp);
            }
        }
        Ok(hits)
    }

    /// Upgrade earlier outgoing rows on an inbound peer receipt (delivery /
    /// read / viewed). The status ladder is monotonic — `pending|sent` →
    /// `delivered` → `read` — and a receipt never downgrades a row (`read`
    /// over `delivered`), never touches terminal or non-outgoing rows. `when`
    /// is the receipt's arrival instant (envelope timestamp, contract 1.32)
    /// and stamps `delivered_at` / `read_at` on the rows the receipt actually
    /// moves — first stamp wins, a replayed or later receipt never re-stamps.
    /// A VIEWED receipt (contract 1.94, §4.77) rides the same ladder to
    /// `read` and additionally first-stamps `viewed_at` on every hit row in
    /// the read-upgrade eligibility set (`pending|sent|delivered|read`;
    /// failed rows never take the stamp): rows the receipt moves carry the
    /// stamp inside `moved`, rows already `read` that take their first stamp
    /// surface as `viewed_confirmed`, and a pure replay (already read,
    /// already stamped) produces neither. The two faces let the service emit
    /// one `message.statusChanged` per real transition, viewed included.
    pub fn upgrade_outgoing_receipts(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_sent_at: &[u64],
        receipt: crate::engine::ReceiptKind,
        when: u64,
    ) -> Result<UpgradedReceipts, StoreError> {
        if target_sent_at.is_empty() {
            return Ok(UpgradedReceipts::default());
        }
        // Ranked per row: the target tier only applies when every earlier
        // tier is already past (or the row is still below it).
        let target = receipt.as_str();
        let stamp_viewed = matches!(receipt, crate::engine::ReceiptKind::Viewed);
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut moved = Vec::new();
        let mut viewed_confirmed = Vec::new();
        {
            let mut stmt = transaction
                .prepare(
                    "SELECT id, account_id, conversation_id, direction, sender_id, sent_at,
                            received_at, body, body_bytes, body_truncated, status,
                            quote_message_id, client_request_id, quote_snapshot,
                            attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                     FROM messages
                     WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
                       AND direction='outgoing'
                     ORDER BY id ASC LIMIT 8",
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            for timestamp in target_sent_at.iter().copied().take(256) {
                let rows: Vec<MessageRecord> = stmt
                    .query_map(
                        params![account_id, conversation_id, timestamp as i64],
                        message_record_from_row,
                    )
                    .map_err(|error| StoreError::Unavailable(Some(error)))?
                    .collect::<Result<_, _>>()
                    .map_err(|error| StoreError::Unavailable(Some(error)))?;
                for record in rows {
                    let current = record.status;
                    let next = match (target, current) {
                        ("delivered", "pending" | "sent") => Some("delivered"),
                        ("read", "pending" | "sent" | "delivered") => Some("read"),
                        _ => None,
                    };
                    // A VIEWED receipt first-stamps `viewed_at` on every hit
                    // row inside the read-upgrade eligibility set, whether or
                    // not the status moves this time (§4.77).
                    let first_view = stamp_viewed
                        && record.viewed_at.is_none()
                        && matches!(current, "pending" | "sent" | "delivered" | "read");
                    let Some(next) = next else {
                        if first_view {
                            transaction
                                .execute(
                                    "UPDATE messages SET viewed_at=?4
                                     WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3",
                                    params![
                                        account_id,
                                        conversation_id,
                                        timestamp as i64,
                                        when as i64
                                    ],
                                )
                                .map_err(|error| StoreError::Unavailable(Some(error)))?;
                            viewed_confirmed.push(MessageRecord {
                                viewed_at: Some(when),
                                server_guid: None,
                                ..record
                            });
                        }
                        continue;
                    };
                    transaction
                        .execute(
                            "UPDATE messages SET status=?4,
                               delivered_at=CASE WHEN ?4='delivered'
                                 THEN COALESCE(delivered_at, ?5) ELSE delivered_at END,
                               read_at=CASE WHEN ?4='read'
                                 THEN COALESCE(read_at, ?5) ELSE read_at END,
                               viewed_at=CASE WHEN ?6=1
                                 THEN COALESCE(viewed_at, ?5) ELSE viewed_at END
                             WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3",
                            params![
                                account_id,
                                conversation_id,
                                timestamp as i64,
                                next,
                                when as i64,
                                i64::from(first_view)
                            ],
                        )
                        .map_err(|error| StoreError::Unavailable(Some(error)))?;
                    let delivered_at = if next == "delivered" {
                        Some(when)
                    } else {
                        record.delivered_at
                    };
                    let read_at = if next == "read" {
                        Some(when)
                    } else {
                        record.read_at
                    };
                    let viewed_at = if first_view {
                        Some(when)
                    } else {
                        record.viewed_at
                    };
                    moved.push(MessageRecord {
                        status: next,
                        delivered_at,
                        read_at,
                        viewed_at,
                        server_guid: None,
                        ..record
                    });
                }
            }
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(UpgradedReceipts {
            moved,
            viewed_confirmed,
        })
    }

    /// First-stamp `viewed_at` on the given incoming rows (contract 1.94,
    /// §4.77): `messages.markViewed` marks the rows locally before the
    /// upstream viewed-receipt fan-out, so the projection carries the played
    /// instant (the official `readStatus === Viewed`) even when the send
    /// later fails. First stamp wins — a repeated markViewed never
    /// re-stamps. Only incoming rows qualify: the outbound viewed face rides
    /// the receipt ladder (`upgrade_outgoing_receipts`), never this stamp.
    /// Returns how many rows took their first stamp.
    pub fn stamp_incoming_viewed(
        &self,
        account_id: &str,
        conversation_id: &str,
        row_ids: &[String],
        when: u64,
    ) -> Result<u64, StoreError> {
        if row_ids.is_empty() {
            return Ok(0);
        }
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "UPDATE messages SET viewed_at=?4
                 WHERE account_id=?1 AND conversation_id=?2 AND id=?3
                   AND direction='incoming' AND viewed_at IS NULL",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut stamped = 0u64;
        for row_id in row_ids.iter().take(256) {
            let changes = stmt
                .execute(params![account_id, conversation_id, row_id, when as i64])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            stamped += changes as u64;
        }
        Ok(stamped)
    }

    /// First-stamp `read_at` on the given incoming rows (contract 1.100,
    /// §4.81): the cross-device read sync's local projection — the account
    /// read these rows on another device, so this store carries the same
    /// stamp the desktop's own open path implies. Same first-stamp-wins
    /// discipline as `stamp_incoming_viewed` — a replayed sync never
    /// re-stamps. Only incoming rows qualify: the sync entries name messages
    /// the account received, never its own sends or system rows.
    pub fn stamp_incoming_read(
        &self,
        account_id: &str,
        conversation_id: &str,
        row_ids: &[String],
        when: u64,
    ) -> Result<u64, StoreError> {
        if row_ids.is_empty() {
            return Ok(0);
        }
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "UPDATE messages SET read_at=?4
                 WHERE account_id=?1 AND conversation_id=?2 AND id=?3
                   AND direction='incoming' AND read_at IS NULL",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut stamped = 0u64;
        for row_id in row_ids.iter().take(256) {
            let changes = stmt
                .execute(params![account_id, conversation_id, row_id, when as i64])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            stamped += changes as u64;
        }
        Ok(stamped)
    }

    /// The incoming rows one read/viewed sync names (contract 1.100, §4.81):
    /// rows of one conversation whose upstream sent timestamp is in the
    /// list — the sync's (sender, timestamp) entries resolved against this
    /// conversation. The result follows the caller's timestamp order;
    /// a timestamp with no incoming row (pruned, or never landed here)
    /// is simply absent — the sync never invents rows.
    pub fn incoming_rows_by_sent_at(
        &self,
        account_id: &str,
        conversation_id: &str,
        sent_at: &[u64],
    ) -> Result<Vec<MessageRecord>, StoreError> {
        if sent_at.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND direction='incoming' AND sent_at=?3",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut rows = Vec::with_capacity(sent_at.len());
        let mut seen = std::collections::HashSet::with_capacity(sent_at.len());
        for at in sent_at.iter().take(256) {
            if !seen.insert(*at) {
                continue;
            }
            if let Some(record) = stmt
                .query_row(
                    params![account_id, conversation_id, *at as i64],
                    message_record_from_row,
                )
                .optional()
                .map_err(|error| StoreError::Unavailable(Some(error)))?
            {
                rows.push(record);
            }
        }
        Ok(rows)
    }

    /// The incoming rows of one conversation in receipt order (`sent_at ASC,
    /// id ASC`), bounded — the absent-`messageIds` mode of
    /// `messages.markRead` / `messages.markViewed` (contract 1.34). Only
    /// incoming rows qualify: an outbound receipt references messages the
    /// peers authored, never our own sends or system rows.
    pub fn incoming_rows_for_receipts(
        &self,
        account_id: &str,
        conversation_id: &str,
        limit: u32,
    ) -> Result<Vec<MessageRecord>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND direction='incoming'
                 ORDER BY sent_at ASC, id ASC
                 LIMIT ?3",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(
                params![account_id, conversation_id, i64::from(limit)],
                message_record_from_row,
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(rows)
    }

    /// The explicit-`messageIds` mode: only rows that exist in this
    /// conversation and are incoming — outgoing and system rows are skipped,
    /// duplicates collapse, and the result comes back in receipt order
    /// (`sent_at ASC, id ASC`) regardless of caller order. One lock, one
    /// prepared statement, one round of per-id lookups bounded by the caller.
    pub fn incoming_rows_by_ids(
        &self,
        account_id: &str,
        conversation_id: &str,
        ids: &[String],
    ) -> Result<Vec<MessageRecord>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id,
                        quote_snapshot, attachments_json, rich_json, edited_at, sender_name, mentions_self, delivered_at, read_at, admin_deleted, sticker_json, expire_seconds, expire_start_at, expire_at, server_guid, viewed_at
                 FROM messages
                 WHERE id=?1 AND account_id=?2 AND conversation_id=?3 AND direction='incoming'",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut rows = Vec::with_capacity(ids.len());
        let mut seen = std::collections::HashSet::with_capacity(ids.len());
        for id in ids {
            if !seen.insert(id) {
                continue;
            }
            if let Some(record) = stmt
                .query_row(
                    params![id, account_id, conversation_id],
                    message_record_from_row,
                )
                .optional()
                .map_err(|error| StoreError::Unavailable(Some(error)))?
            {
                rows.push(record);
            }
        }
        rows.sort_by(|left, right| {
            (left.sent_at, left.id.as_str()).cmp(&(right.sent_at, right.id.as_str()))
        });
        Ok(rows)
    }

    /// The reportable server GUIDs for one conversation row set (contract
    /// revision 1.49, §4.48): per requested message id, the row's persisted
    /// `server_guid` — the result aligns 1:1 with the caller's id list. An id
    /// with no row in this conversation, or a row whose GUID is NULL
    /// (pre-1.49 rows, envelopes without the key) reports `None` — the caller
    /// turns each miss into a per-id failure instead of failing the whole
    /// request. Bounded by the caller's id list.
    pub fn server_guids_by_ids(
        &self,
        account_id: &str,
        conversation_id: &str,
        ids: &[String],
    ) -> Result<Vec<Option<String>>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT server_guid FROM messages
                 WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        ids.iter()
            .map(|id| {
                stmt.query_row(params![id, account_id, conversation_id], |row| row.get(0))
                    .optional()
                    .map_err(|error| StoreError::Unavailable(Some(error)))
                    .map(|guid: Option<Option<String>>| guid.flatten())
            })
            .collect()
    }

    /// The account's cached contact addresses (kind='contact' peer keys) for
    /// the mention-number resolution (contract 1.34). The caller matches
    /// digit suffixes in Rust — the same resolution rule the engine applies —
    /// so a mention number that resolves to nothing local is dropped before
    /// any upstream call (the official behavior for unresolvable addresses).
    pub fn contact_peer_keys(&self, account_id: &str) -> Result<Vec<String>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare("SELECT peer_key FROM contacts WHERE account_id=?1 AND kind='contact'")
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(params![account_id], |row| row.get::<_, String>(0))
            .map_err(|error| StoreError::Unavailable(Some(error)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(rows)
    }

    /// Record one inbound reaction (contract 1.15; actor name captured since
    /// 1.27). The protocol shape — one emoji state per (conversation, target
    /// message, actor) — maps to an upsert: a repeated reaction replaces the
    /// emoji, `isRemove` marks the row removed instead of deleting it
    /// (history stays auditable), and a later envelope refreshes the captured
    /// display name.
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_reaction_event(
        &self,
        account_id: &str,
        conversation_id: &str,
        emoji: &str,
        target_sent_at: u64,
        actor_id: &str,
        removed: bool,
        actor_name: Option<&str>,
    ) -> Result<(), StoreError> {
        self.lock_conn()?
            .execute(
                "INSERT INTO message_events(
                    id, account_id, conversation_id, kind, emoji,
                    target_timestamp, actor_id, removed, updated_at, actor_name
                 ) VALUES(?1, ?2, ?3, 'reaction', ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(account_id, conversation_id, kind, target_timestamp, actor_id)
                 DO UPDATE SET emoji=excluded.emoji,
                               removed=excluded.removed,
                               updated_at=excluded.updated_at,
                               actor_name=excluded.actor_name",
                params![
                    crate::ids::stable_hash_id(&[
                        account_id,
                        conversation_id,
                        "reaction",
                        &target_sent_at.to_string(),
                        actor_id,
                    ]),
                    account_id,
                    conversation_id,
                    emoji,
                    target_sent_at as i64,
                    actor_id,
                    i64::from(removed),
                    crate::link::now_ms() as i64,
                    actor_name,
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// Test-only projection of the raw reaction rows; production serves the
    /// aggregated `attach_reactions` pill list instead.
    #[cfg(test)]
    pub fn list_reaction_events(
        &self,
        account_id: &str,
        conversation_id: &str,
        limit: u32,
    ) -> Result<Vec<ReactionEvent>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT emoji, target_timestamp, actor_id, removed, updated_at
                 FROM message_events
                 WHERE account_id=?1 AND conversation_id=?2 AND kind='reaction'
                 ORDER BY updated_at DESC, id DESC
                 LIMIT ?3",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(
                params![
                    account_id,
                    conversation_id,
                    i64::from(limit.clamp(1, MAX_PAGE_LIMIT))
                ],
                |row| {
                    Ok(ReactionEvent {
                        emoji: row.get(0)?,
                        target_timestamp: row.get::<_, i64>(1)? as u64,
                        actor_id: row.get(2)?,
                        removed: row.get::<_, i64>(3)? != 0,
                        updated_at: row.get::<_, i64>(4)? as u64,
                    })
                },
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// Active reaction pills for one conversation, keyed by the target
    /// message's upstream timestamp (contract 1.16, per-author detail in
    /// 1.27): per emoji the distinct active actor count, whether this account
    /// reacted, and every actor newest-first (the official ReactionViewer
    /// order). Rows flagged `removed` are excluded — removal is the actor's
    /// disappearance from `actors` and from `count`. One read, bounded by
    /// `MAX_REACTION_DETAIL_ROWS` newest rows and `MAX_REACTION_ACTORS` per
    /// emoji; `count` and `actors.len()` always agree because an actor owns
    /// exactly one event row per target. Ordered by first reaction time so
    /// pill order is stable across refreshes.
    pub fn reaction_aggregates_for_conversation(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<std::collections::HashMap<u64, Vec<MessageReactionSummary>>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT target_timestamp, emoji, actor_id, actor_name, updated_at
                 FROM message_events
                 WHERE account_id=?1 AND conversation_id=?2
                   AND kind='reaction' AND removed=0
                 ORDER BY updated_at DESC, id DESC
                 LIMIT ?3",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(
                params![account_id, conversation_id, MAX_REACTION_DETAIL_ROWS as i64],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)? as u64,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)? as u64,
                    ))
                },
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        // Assemble pills newest-first per emoji, then re-order the pills by
        // their oldest member (first reaction wins, then emoji) so the order
        // matches the pre-1.27 aggregate exactly.
        struct Pill {
            actors: Vec<MessageReactionActor>,
            first_reacted_at: u64,
            mine: bool,
        }
        let mut order: Vec<(u64, String)> = Vec::new();
        let mut pills: std::collections::HashMap<(u64, String), Pill> =
            std::collections::HashMap::new();
        for row in rows {
            let (target, emoji, actor_id, actor_name, updated_at) =
                row.map_err(|error| StoreError::Unavailable(Some(error)))?;
            let key = (target, emoji.clone());
            let pill = pills.entry(key.clone()).or_insert_with(|| {
                order.push(key);
                Pill {
                    actors: Vec::new(),
                    first_reacted_at: updated_at,
                    mine: false,
                }
            });
            if pill.actors.len() < MAX_REACTION_ACTORS {
                pill.actors.push(MessageReactionActor {
                    is_self: actor_id == account_id,
                    name: actor_name.filter(|name| !name.trim().is_empty()),
                    reacted_at: updated_at,
                });
                if actor_id == account_id {
                    pill.mine = true;
                }
            }
        }
        order.sort_by(|(left_target, left_emoji), (right_target, right_emoji)| {
            let left = pills
                .get(&(*left_target, left_emoji.clone()))
                .map(|pill| pill.first_reacted_at)
                .unwrap_or(u64::MAX);
            let right = pills
                .get(&(*right_target, right_emoji.clone()))
                .map(|pill| pill.first_reacted_at)
                .unwrap_or(u64::MAX);
            left.cmp(&right).then(left_emoji.cmp(right_emoji))
        });
        let mut grouped: std::collections::HashMap<u64, Vec<MessageReactionSummary>> =
            std::collections::HashMap::new();
        for (target, emoji) in order {
            let pill = pills
                .remove(&(target, emoji.clone()))
                .expect("pill tracked");
            let count = pill.actors.len().min(u32::MAX as usize) as u32;
            grouped
                .entry(target)
                .or_default()
                .push(MessageReactionSummary {
                    emoji,
                    count,
                    mine: pill.mine,
                    actors: pill.actors,
                });
        }
        Ok(grouped)
    }

    /// Attach reaction aggregates onto message rows by their `sent_at`
    /// (message_events key the target by the upstream timestamp, which is
    /// exactly `messages.sent_at`). Read paths and event paths both call
    /// this so every projected row carries current pills.
    pub fn attach_reactions(
        &self,
        account_id: &str,
        records: &mut [MessageRecord],
    ) -> Result<(), StoreError> {
        if records.is_empty() {
            return Ok(());
        }
        let conversation_id = records[0].conversation_id.clone();
        let grouped = self.reaction_aggregates_for_conversation(account_id, &conversation_id)?;
        if grouped.is_empty() {
            return Ok(());
        }
        for record in records.iter_mut() {
            if let Some(reactions) = grouped.get(&record.sent_at) {
                record.reactions = reactions.clone();
            }
        }
        Ok(())
    }

    /// Attach prior-body edit history (contract 1.32) to the records that
    /// carry `edited_at`: one batched read over `message_edits`, ascending by
    /// replacement time (the desktop renders newest-first like the official
    /// unshift). Rows never edited are untouched — the wire key stays absent,
    /// mirroring the `attach_reactions` read-side discipline. The table is
    /// capped per message at insert, so the read is bounded by page size.
    pub fn attach_edits(
        &self,
        account_id: &str,
        records: &mut [MessageRecord],
    ) -> Result<(), StoreError> {
        let ids: Vec<&str> = records
            .iter()
            .filter(|record| record.edited_at.is_some())
            .map(|record| record.id.as_str())
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        let conn = self.lock_conn()?;
        let mut grouped: std::collections::HashMap<String, Vec<MessageEditEntry>> =
            std::collections::HashMap::new();
        {
            let placeholders = std::iter::repeat_n("?", ids.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT message_id, body, body_bytes, edited_at
                 FROM message_edits
                 WHERE account_id=?1 AND message_id IN ({placeholders})
                 ORDER BY seq ASC"
            );
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            let mut bind: Vec<&dyn rusqlite::ToSql> = vec![&account_id];
            for id in &ids {
                bind.push(id);
            }
            let rows = stmt
                .query_map(bind.as_slice(), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?.max(0).min(u32::MAX as i64) as u32,
                        row.get::<_, i64>(3)?.max(0) as u64,
                    ))
                })
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            for row in rows {
                let (message_id, body, body_bytes, edited_at) =
                    row.map_err(|error| StoreError::Unavailable(Some(error)))?;
                grouped
                    .entry(message_id)
                    .or_default()
                    .push(MessageEditEntry {
                        body,
                        body_bytes,
                        edited_at,
                    });
            }
        }
        for record in records.iter_mut() {
            if let Some(edits) = grouped.get(&record.id) {
                record.edits = edits.clone();
            }
        }
        Ok(())
    }

    /// Snapshot the prior body of one message row (contract 1.32) inside the
    /// caller's transaction, then prune past the per-message cap. The
    /// snapshot rides the same transaction as the overwriting UPDATE, so a
    /// rolled-back edit never loses the prior body. An empty prior body
    /// snapshots nothing — there is no text worth showing in a history list.
    fn snapshot_prior_body(
        transaction: &rusqlite::Transaction<'_>,
        account_id: &str,
        message_id: &str,
        prior_body: &str,
    ) -> Result<(), StoreError> {
        if prior_body.is_empty() {
            return Ok(());
        }
        let body = crate::engine::truncate_utf8_bytes(prior_body, MAX_EDIT_BODY_BYTES);
        let body_bytes = body.len().min(u32::MAX as usize) as i64;
        transaction
            .execute(
                "INSERT INTO message_edits (account_id, message_id, body, body_bytes, edited_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    account_id,
                    message_id,
                    body,
                    body_bytes,
                    crate::link::now_ms() as i64
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .execute(
                "DELETE FROM message_edits
                 WHERE message_id=?1
                   AND seq NOT IN (
                     SELECT seq FROM message_edits WHERE message_id=?1
                     ORDER BY seq DESC LIMIT ?2
                   )",
                params![message_id, MAX_EDIT_HISTORY_ENTRIES as i64],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// Mark a pending outgoing row sent. Returns the updated row and whether
    /// the status transitioned to 'sent' (a replayed completion reports
    /// `false`, so no duplicate status event is emitted for it).
    pub fn complete_outgoing_send(
        &self,
        message_id: &str,
        account_id: &str,
        conversation_id: &str,
        sent_at: u64,
    ) -> Result<Option<(MessageRecord, bool)>, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let previous_status: Option<String> = transaction
            .query_row(
                "SELECT status FROM messages
                 WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                params![message_id, account_id, conversation_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let duplicate_ids = {
            let mut stmt = transaction
                .prepare(
                    "SELECT id FROM messages
                     WHERE account_id=?1 AND conversation_id=?2 AND direction='outgoing'
                       AND sent_at=?3 AND id<>?4 AND sender_id=?5
                       AND client_request_id IS NULL
                     ORDER BY id ASC LIMIT 2",
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            let rows = stmt
                .query_map(
                    params![
                        account_id,
                        conversation_id,
                        sent_at as i64,
                        message_id,
                        account_id,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| StoreError::Unavailable(Some(error)))?
        };
        if duplicate_ids.len() == 1 {
            transaction
                .execute(
                    "DELETE FROM messages WHERE id=?1",
                    params![duplicate_ids[0]],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        let updated = transaction
            .execute(
                "UPDATE messages SET status='sent', sent_at=?2
                 WHERE id=?1 AND account_id=?3 AND conversation_id=?4",
                params![message_id, sent_at as i64, account_id, conversation_id],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if updated != 1 {
            return Ok(None);
        }
        let status_transitioned = previous_status.as_deref() != Some("sent");
        transaction
            .execute(
                "UPDATE conversations SET last_message_at=?2 WHERE id=?1 AND account_id=?3",
                params![conversation_id, sent_at as i64, account_id],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .execute(
                "UPDATE accounts SET last_message_at=?2 WHERE id=?1",
                params![account_id, sent_at as i64],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        // The connection guard must be released before any other store method
        // runs: every one of them takes the same mutex.
        drop(conn);
        Ok(self
            .message_by_id(account_id, conversation_id, message_id)?
            .map(|record| (record, status_transitioned)))
    }

    pub fn conversation_summary(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<Option<ConversationSummary>, StoreError> {
        let conn = self.lock_conn()?;
        // Contract 1.43 (§4.42): the same read-time is_self derivation the
        // list applies, through the already held connection guard.
        let signal_account: Option<String> = conn
            .query_row(
                "SELECT signal_account FROM accounts WHERE id=?1",
                params![account_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let summary = conn
            .query_row(
                "SELECT id, account_id, kind, title, last_message_preview, last_message_at,
                        unread_count, unread_mentions, muted, pinned, expire_timer_seconds, peer_key
                 FROM conversations WHERE id=?1 AND account_id=?2",
                params![conversation_id, account_id],
                |row| {
                    let kind = static_kind(row.get::<_, String>(2)?);
                    let peer_key: String = row.get(11)?;
                    Ok(ConversationSummary {
                        id: row.get(0)?,
                        account_id: row.get(1)?,
                        kind,
                        title: row.get(3)?,
                        last_message_preview: row.get(4)?,
                        last_message_kind: None,
                        last_message_direction: None,
                        last_message_status: None,
                        last_message_author_name: None,
                        last_message_reactions: Vec::new(),
                        last_message_at: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                        unread_count: row.get::<_, i64>(6)? as u32,
                        unread_mentions: row.get::<_, i64>(7)? as u32,
                        muted: row.get::<_, i64>(8)? != 0,
                        pinned: row.get::<_, i64>(9)? != 0,
                        pinned_message: None,
                        expire_timer_seconds: row
                            .get::<_, Option<i64>>(10)?
                            .map(|value| value.max(0) as u64),
                        is_self: summary_is_self(kind, &peer_key, signal_account.as_deref()),
                        // Contract 1.60 (§4.54): stamped from the contacts
                        // cache after the row maps (page lookup / single-row
                        // query below).
                        is_terminated: false,
                        // Contract 1.65 (§4.58): derived after the row maps
                        // (single-row match below), the same post-map loop.
                        message_request_accepted: false,
                        // Contract 1.71 (§4.63): derived in the same post-map
                        // match (below), the same read-time family.
                        has_user_initiated_outbound: false,
                    })
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        match summary {
            Some(mut summary) => {
                let meta =
                    conversation_last_message_meta(&conn, &summary.account_id, conversation_id)?;
                summary.last_message_kind = meta.kind;
                summary.last_message_direction = meta.direction;
                summary.last_message_status = meta.status;
                summary.last_message_author_name = meta.author_name;
                summary.last_message_reactions = meta.reactions;
                summary.pinned_message =
                    pinned_message_for_conversation(&conn, account_id, conversation_id)?;
                // Contract 1.60 (§4.54): the same read-time terminated
                // projection the list applies — the summary that rides
                // conversation.changed carries the flag, so an event-driven
                // window and a full reload cannot disagree. For a group the
                // conversation row and the contacts row share the same stable
                // id; a group without a cache row stays false.
                if summary.kind == "group" {
                    summary.is_terminated = conn
                        .query_row(
                            "SELECT c.extra FROM contacts c
                             WHERE c.id = ?1 AND c.account_id = ?2 AND c.kind = 'group'",
                            params![conversation_id, account_id],
                            |row| row.get::<_, Option<String>>(0),
                        )
                        .optional()
                        .map_err(|error| StoreError::Unavailable(Some(error)))?
                        .flatten()
                        .map(|extra| extra_is_terminated(Some(&extra)))
                        .unwrap_or(false);
                }
                // Contract 1.65 (§4.58): the same read-time accepted
                // derivation the list applies — the summary that rides
                // conversation.changed carries the key, so an event-driven
                // window and a full reload cannot disagree.
                summary.message_request_accepted = conversation_message_request_accepted(
                    &conn,
                    account_id,
                    conversation_id,
                    summary.kind,
                    summary.is_self,
                )?;
                // Contract 1.71 (§4.63): the whole-history outbound bit —
                // the summary that rides conversation.changed carries it so
                // an event-driven window and a full reload cannot disagree.
                summary.has_user_initiated_outbound =
                    conversation_has_user_initiated_outbound(&conn, account_id, conversation_id);
                Ok(Some(summary))
            }
            None => Ok(None),
        }
    }

    /// Cache one full contacts sync in a single transaction: every entry is
    /// upserted with the same columns and conflict semantics as
    /// [`Store::upsert_contact`], and the sync marker advances in the same
    /// commit. A failure on any entry rolls the whole batch back, so a partial
    /// sync is never visible and the 60s short-circuit marker can never move
    /// ahead of the rows it summarizes.
    pub fn upsert_synced_contacts(
        &self,
        account_id: &str,
        entries: &[SyncedContact<'_>],
        synced_at: u64,
    ) -> Result<(), StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        {
            let mut stmt = transaction
                .prepare(
                    "INSERT INTO contacts(id, account_id, kind, peer_key, title, extra, synced_at)
                     VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(account_id, kind, peer_key) DO UPDATE SET
                       title=excluded.title,
                       extra=excluded.extra,
                       synced_at=excluded.synced_at",
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            for entry in entries {
                let id = stable_hash_id(&[account_id, entry.kind, entry.peer_key]);
                stmt.execute(params![
                    id,
                    account_id,
                    entry.kind,
                    entry.peer_key,
                    entry.title,
                    entry.extra,
                    synced_at as i64
                ])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            }
        }
        // Contract revision 1.14 (§6.5): every synced direct contact and member
        // group also materializes its conversation skeleton, so the linked
        // account sees its existing chats right after the sync instead of one
        // message at a time. The insert is idempotent on the same
        // stable_hash_id key ensure_conversation uses; existing rows keep
        // their state (unread/muted/pinned), and a masked/placeholder title
        // upgrades through the normal path when a real message later lands.
        {
            let mut stmt = transaction
                .prepare(
                    "INSERT INTO conversations(id, account_id, kind, peer_key, title, unread_count, muted, pinned)
                     VALUES(?1, ?2, ?3, ?4, ?5, 0, 0, 0)
                     ON CONFLICT(id) DO NOTHING",
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            for entry in entries {
                let conversation_kind = match entry.kind {
                    "contact" => "direct",
                    other => other,
                };
                let id = stable_hash_id(&[account_id, conversation_kind, entry.peer_key]);
                let inserted = stmt
                    .execute(params![
                        id,
                        account_id,
                        conversation_kind,
                        entry.peer_key,
                        entry.title,
                    ])
                    .map_err(|error| StoreError::Unavailable(Some(error)))?;
                if inserted == 0 {
                    // The row already exists (created by an earlier sync or a
                    // real message): leave its state alone, but still upgrade
                    // masked/placeholder titles — the sync cache may carry a
                    // display name the conversation has not learned yet.
                    let existing = transaction
                        .query_row(
                            "SELECT title FROM conversations WHERE id=?1",
                            params![id],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()
                        .map_err(|error| StoreError::Unavailable(Some(error)))?;
                    if let Some(current) = existing
                        && title_should_upgrade(&current, entry.title)
                    {
                        transaction
                            .execute(
                                "UPDATE conversations SET title=?2 WHERE id=?1",
                                params![id, entry.title],
                            )
                            .map_err(|error| StoreError::Unavailable(Some(error)))?;
                    }
                }
            }
        }
        transaction
            .execute(
                "INSERT INTO meta(key, value) VALUES(?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![
                    format!("contacts_synced_at:{account_id}"),
                    synced_at.to_string()
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// Cache one synced contact/group entry. `kind` is 'contact' or 'group';
    /// `extra` is an optional JSON marker (e.g. member count) kept opaque.
    pub fn upsert_contact(
        &self,
        account_id: &str,
        kind: &str,
        peer_key: &str,
        title: &str,
        extra: Option<&str>,
        synced_at: u64,
    ) -> Result<(), StoreError> {
        let id = stable_hash_id(&[account_id, kind, peer_key]);
        self.lock_conn()?
            .execute(
                "INSERT INTO contacts(id, account_id, kind, peer_key, title, extra, synced_at)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(account_id, kind, peer_key) DO UPDATE SET
                   title=excluded.title,
                   extra=excluded.extra,
                   synced_at=excluded.synced_at",
                params![
                    id,
                    account_id,
                    kind,
                    peer_key,
                    title,
                    extra,
                    synced_at as i64
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// Read one cached contact/group row by its natural key. `kind` is
    /// 'contact' or 'group'; `extra` is the opaque JSON marker stored by the
    /// sync batch (e.g. memberCount for groups), `synced_at` the row's last
    /// sync timestamp. `groups.get` (§4.8) projects the `kind='group'` rows.
    pub fn contact_by_peer(
        &self,
        account_id: &str,
        kind: &str,
        peer_key: &str,
    ) -> Result<Option<(String, Option<String>, u64)>, StoreError> {
        let conn = self.lock_conn()?;
        let row = conn
            .query_row(
                "SELECT title, extra, synced_at
                 FROM contacts
                 WHERE account_id=?1 AND kind=?2 AND peer_key=?3",
                params![account_id, kind, peer_key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)? as u64,
                    ))
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(row)
    }

    /// The cached `kind='contact'` display names of one account, keyed by
    /// peer_key (§4.39): the roster projection resolves member names from
    /// this map at read time instead of copying them into the group row, so
    /// the roster shows the same titles the next sync already improved. The
    /// map is bounded by the contacts cache itself — one entry per cached
    /// contact, the scale the §6.5 sync already maintains.
    pub fn contact_title_map(
        &self,
        account_id: &str,
    ) -> Result<std::collections::HashMap<String, String>, StoreError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT peer_key, title
                 FROM contacts
                 WHERE account_id=?1 AND kind='contact'",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(params![account_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut map = std::collections::HashMap::new();
        for row in rows {
            let (peer_key, title) = row.map_err(|error| StoreError::Unavailable(Some(error)))?;
            map.insert(peer_key, title);
        }
        Ok(map)
    }

    /// Read-only paged view of the contacts cache, ordered by (kind, peer_key).
    /// `query` is an optional case-insensitive substring filter over title/peer_key.
    pub fn list_contacts(
        &self,
        account_id: &str,
        query: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<ContactSummary>, StoreError> {
        let limit = limit.clamp(1, MAX_PAGE_LIMIT);
        let fetch = limit + 1;
        let decoded = cursor
            .map(|value| decode_contact_cursor(value, account_id))
            .transpose()?;
        let cursor_present = i64::from(decoded.is_some());
        let cursor_kind = decoded.as_ref().map(|value| value.0.as_str());
        let cursor_peer_key = decoded.as_ref().map(|value| value.1.as_str());
        let like = query.map(escape_like);
        let conn = self.lock_conn()?;
        // Contract 1.44 (§4.43): is_self derives the row from the account's
        // own number at read time — one lookup per page, through the already
        // held connection guard (the §4.42 summary-list discipline).
        let signal_account: Option<String> = conn
            .query_row(
                "SELECT signal_account FROM accounts WHERE id=?1",
                params![account_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut stmt = conn
            .prepare(
                "SELECT id, kind, peer_key, title, extra
                 FROM contacts
                 WHERE account_id=?1
                   AND (
                     ?2 IS NULL
                     OR title LIKE '%'||?2||'%' ESCAPE '\\'
                     OR peer_key LIKE '%'||?2||'%' ESCAPE '\\'
                   )
                   AND (
                     ?3=0
                     OR kind > ?4
                     OR (kind = ?4 AND peer_key > ?5)
                   )
                 ORDER BY kind ASC, peer_key ASC
                 LIMIT ?6",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(
                params![
                    account_id,
                    like,
                    cursor_present,
                    cursor_kind,
                    cursor_peer_key,
                    fetch as i64,
                ],
                |row| {
                    let kind = static_contact_kind(row.get::<_, String>(1)?);
                    let peer_key: String = row.get(2)?;
                    let extra: Option<String> = row.get(4)?;
                    Ok(ContactSummary {
                        id: row.get(0)?,
                        is_terminated: kind == "group" && extra_is_terminated(extra.as_deref()),
                        kind,
                        is_self: contact_is_self(kind, &peer_key, signal_account.as_deref()),
                        peer_key,
                        title: row.get(3)?,
                    })
                },
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut items = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let next_cursor = if items.len() as u32 > limit {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_contact_cursor(account_id, item))
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }

    /// (contact_count, group_count) currently cached for the account.
    pub fn count_contacts(&self, account_id: &str) -> Result<(u64, u64), StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT
                   COALESCE(SUM(CASE WHEN kind='contact' THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN kind='group' THEN 1 ELSE 0 END), 0)
                 FROM contacts WHERE account_id=?1",
                params![account_id],
                |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))
    }

    /// unix ms of the last successful contacts sync (meta-backed; survives restarts).
    pub fn contacts_synced_at(&self, account_id: &str) -> Result<Option<u64>, StoreError> {
        let value = self
            .lock_conn()?
            .query_row(
                "SELECT value FROM meta WHERE key=?1",
                params![format!("contacts_synced_at:{account_id}")],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(value.and_then(|value| value.parse::<u64>().ok()))
    }

    /// The account's stories-disabled flag (contract 1.76, §4.67, meta-backed;
    /// survives restarts). Absent = false — the official default posture
    /// (itemStorage `hasStoriesDisabled` defaults off, stories on,
    /// ts/util/stories.preload.ts:5-7, AGPL read-only anchor). The flag is
    /// connector-local: the pinned upstream faces carry no stories-disabled
    /// storage record, so the server keeps delivering and the ingest gate
    /// (the official getStoriesBlocked leg, MessageReceiver.preload.ts:
    /// 1550) drops story material on arrival instead.
    pub fn stories_disabled(&self, account_id: &str) -> Result<bool, StoreError> {
        let value = self
            .lock_conn()?
            .query_row(
                "SELECT value FROM meta WHERE key=?1",
                params![format!("stories_disabled:{account_id}")],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(value.is_some_and(|value| value == "1"))
    }

    /// Write the account's stories-disabled flag (contract 1.76, §4.67).
    pub fn set_stories_disabled(&self, account_id: &str, disabled: bool) -> Result<(), StoreError> {
        self.lock_conn()?
            .execute(
                "INSERT INTO meta(key, value) VALUES(?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![
                    format!("stories_disabled:{account_id}"),
                    if disabled { "1" } else { "0" }
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// Test-only wrapper; production writes the marker inside the
    /// `upsert_synced_contacts` transaction.
    #[cfg(test)]
    pub fn set_contacts_synced_at(
        &self,
        account_id: &str,
        synced_at: u64,
    ) -> Result<(), StoreError> {
        self.lock_conn()?
            .execute(
                "INSERT INTO meta(key, value) VALUES(?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![
                    format!("contacts_synced_at:{account_id}"),
                    synced_at.to_string()
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(())
    }

    /// Drop history past what the product promises to keep: every conversation
    /// keeps its newest [`MAX_MESSAGES_PER_CONVERSATION`] rows plus everything
    /// this machine has held for less than [`MESSAGE_RETENTION_MS`]. Age comes
    /// from `stored_at`, never from `sent_at`, so a peer with a wrong clock
    /// cannot decide when our history disappears. Rows inside the safety window
    /// and rows whose send has not resolved stay regardless, because dedupe and
    /// idempotency read them. Conversations, contacts and accounts survive an
    /// empty history so titles, pins and the link itself are never lost here.
    ///
    /// One call deletes at most `max_messages` rows and repairs the summaries it
    /// disturbed, so the caller keeps the store lock for a bounded time; call it
    /// again while it reports a full batch.
    ///
    /// Cost, not semantics (optimization-plan A9): a row qualifies either by
    /// age alone (`held_since < expired_before` — branch 1) or by falling past
    /// the per-conversation cap while out of the safety window (branch 2).
    /// Every branch-1 row is strictly older than every branch-2 row, so
    /// deleting branch 1 first and spending the batch budget there yields the
    /// exact rows the old single windowed scan picked, in the same
    /// oldest-first order. Branch 1 is an index range scan over
    /// `messages_held_since` (the expression index on the age key) instead of
    /// a full-table window pass; branch 2 runs the window only over messages
    /// of conversations that actually exceed the cap, so the steady state
    /// (no conversation over cap) never walks the table.
    pub fn prune_history(
        &self,
        now_ms: u64,
        max_messages: u32,
    ) -> Result<HistoryPruneOutcome, StoreError> {
        let now = now_ms as i64;
        let expired_before = now.saturating_sub(MESSAGE_RETENTION_MS);
        let keep_after = now.saturating_sub(RETENTION_SAFETY_WINDOW_MS);
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut touched_conversations = std::collections::BTreeSet::new();
        let mut messages_deleted = 0_u64;
        // Branch 1, age rule: no recency is involved, so the expression index
        // answers both the filter and the oldest-first order directly.
        {
            let mut delete = transaction
                .prepare(
                    "DELETE FROM messages WHERE id IN (
                       SELECT id FROM messages
                       WHERE COALESCE(stored_at, received_at, sent_at) < ?1
                         AND status NOT IN ('pending', 'unknown')
                       ORDER BY COALESCE(stored_at, received_at, sent_at)
                       LIMIT ?2
                     )
                     RETURNING conversation_id",
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            let deleted = delete
                .query_map(params![expired_before, max_messages], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            for conversation_id in deleted {
                touched_conversations
                    .insert(conversation_id.map_err(|error| StoreError::Unavailable(Some(error)))?);
                messages_deleted += 1;
            }
        }
        // Branch 2, per-conversation cap rule: only rows too young for branch 1
        // but out of the safety window can still qualify, and only in
        // conversations holding more than the cap — the window ranks every row
        // of exactly those conversations.
        if messages_deleted < max_messages as u64 {
            let remaining = (max_messages as u64 - messages_deleted) as i64;
            let mut delete = transaction
                .prepare(
                    "DELETE FROM messages WHERE id IN (
                       SELECT id FROM (
                         SELECT id, status,
                                COALESCE(stored_at, received_at, sent_at) AS held_since,
                                ROW_NUMBER() OVER (
                                  PARTITION BY conversation_id ORDER BY sent_at DESC, id DESC
                                ) AS recency
                         FROM messages
                         WHERE conversation_id IN (
                           SELECT conversation_id FROM messages
                           GROUP BY conversation_id
                           HAVING COUNT(*) > ?1
                         )
                       )
                       WHERE held_since >= ?2
                         AND held_since < ?3
                         AND status NOT IN ('pending', 'unknown')
                         AND recency > ?1
                       ORDER BY held_since
                       LIMIT ?4
                     )
                     RETURNING conversation_id",
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            let deleted = delete
                .query_map(
                    params![
                        MAX_MESSAGES_PER_CONVERSATION,
                        expired_before,
                        keep_after,
                        remaining
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            for conversation_id in deleted {
                touched_conversations
                    .insert(conversation_id.map_err(|error| StoreError::Unavailable(Some(error)))?);
                messages_deleted += 1;
            }
        }
        if touched_conversations.is_empty() {
            transaction
                .commit()
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            return Ok(HistoryPruneOutcome::default());
        }
        // A summary may outlive the row it was copied from: age is measured on
        // our clock while recency follows the sender's, so the newest-looking row
        // can be the one that expired. Recompute from whatever each conversation
        // still holds — including the unread badge, which may never promise more
        // mail than the history behind it. Only conversations that lost a row are
        // touched, so untouched previews keep exactly what ingest normalized.
        let mut repair = transaction
            .prepare(
                "UPDATE conversations
                    SET last_message_at = (
                          SELECT MAX(sent_at) FROM messages WHERE conversation_id=?1
                        ),
                        last_message_preview = (
                          SELECT substr(body, 1, ?2) FROM messages
                           WHERE conversation_id=?1
                           ORDER BY sent_at DESC, id DESC
                           LIMIT 1
                        ),
                        unread_count = MIN(unread_count, (
                          SELECT COUNT(*) FROM messages
                           WHERE conversation_id=?1 AND direction='incoming'
                        ))
                  WHERE id=?1",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        for conversation_id in &touched_conversations {
            repair
                .execute(params![conversation_id, PREVIEW_CHARS])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        drop(repair);
        transaction
            .execute(
                "UPDATE accounts
                    SET unread_count = COALESCE((
                          SELECT SUM(unread_count) FROM conversations
                           WHERE conversations.account_id = accounts.id
                        ), 0),
                        last_message_at = (
                          SELECT MAX(last_message_at) FROM conversations
                           WHERE conversations.account_id = accounts.id
                        )",
                [],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(HistoryPruneOutcome {
            messages_deleted,
            conversations_repaired: touched_conversations.len() as u64,
        })
    }

    /// Record one conversation's current disappearing-message timer
    /// (contract 1.42, §4.41): `0` off, `N` seconds. Written by the ingest of
    /// a timer-change event and by the `conversations.setExpireTimer` success
    /// path — the local mirror of upstream state, never a source of truth.
    pub fn set_conversation_expire_timer(
        &self,
        account_id: &str,
        conversation_id: &str,
        seconds: u64,
    ) -> Result<bool, StoreError> {
        let changed = self
            .lock_conn()?
            .execute(
                "UPDATE conversations SET expire_timer_seconds=?3
                 WHERE id=?1 AND account_id=?2 AND (expire_timer_seconds IS NULL OR expire_timer_seconds != ?3)",
                params![conversation_id, account_id, seconds as i64],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(changed > 0)
    }

    /// One bounded expire-sweep pass (contract 1.42, §4.41): delete at most
    /// [`EXPIRE_SWEEP_BATCH`] rows whose deadline has passed, oldest deadline
    /// first, then repair exactly the touched conversations' summaries — the
    /// same last-message/unread clamp [`Self::prune_history`] applies — and
    /// recompute the account aggregates in the same transaction, so a sweep
    /// never leaves a summary promising a row the timer ate. Returns the
    /// deletions grouped per conversation (empty = nothing was due; a full
    /// batch means the caller should run another pass). Attachments stay
    /// where retention leaves them: byte cleanup is the media governor's
    /// existing discipline, identical to a retention prune.
    pub fn sweep_expired_messages(
        &self,
        now_ms: u64,
        cap: u32,
    ) -> Result<Vec<ExpiredMessageGroup>, StoreError> {
        let conn = self.lock_conn()?;
        let transaction = conn
            .unchecked_transaction()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut deleted = transaction
            .prepare(
                "DELETE FROM messages WHERE id IN (
                   SELECT id FROM messages
                   WHERE expire_at IS NOT NULL AND expire_at <= ?1
                   ORDER BY expire_at, id
                   LIMIT ?2
                 )
                 RETURNING account_id, conversation_id, id",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = deleted
            .query_map(params![now_ms as i64, cap], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let mut groups: std::collections::BTreeMap<(String, String), Vec<String>> =
            std::collections::BTreeMap::new();
        for row in rows {
            let (account_id, conversation_id, id) =
                row.map_err(|error| StoreError::Unavailable(Some(error)))?;
            groups
                .entry((account_id, conversation_id))
                .or_default()
                .push(id);
        }
        drop(deleted);
        if groups.is_empty() {
            transaction
                .commit()
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            return Ok(Vec::new());
        }
        // The newest row may be the one that expired: recompute each touched
        // conversation's summary from what it still holds, then the account
        // aggregates from the conversation maxima (prune_history's repair).
        let mut repair = transaction
            .prepare(
                "UPDATE conversations
                    SET last_message_at = (
                          SELECT MAX(sent_at) FROM messages WHERE conversation_id=?1
                        ),
                        last_message_preview = (
                          SELECT substr(body, 1, ?2) FROM messages
                           WHERE conversation_id=?1
                           ORDER BY sent_at DESC, id DESC
                           LIMIT 1
                        ),
                        unread_count = MIN(unread_count, (
                          SELECT COUNT(*) FROM messages
                           WHERE conversation_id=?1 AND direction='incoming'
                        ))
                  WHERE id=?1",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        for (account_id, conversation_id) in groups.keys() {
            let touched = repair
                .execute(params![conversation_id, PREVIEW_CHARS])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            if touched == 0 {
                // The conversation row itself is gone (concurrent account
                // delete): its group simply carries no event target anymore,
                // but the ids were deleted, so the group stays for symmetry.
                tracing::debug!(account = %account_id, "expire sweep outlived conversation");
            }
        }
        drop(repair);
        transaction
            .execute(
                "UPDATE accounts
                    SET unread_count = COALESCE((
                          SELECT SUM(unread_count) FROM conversations
                           WHERE conversations.account_id = accounts.id
                        ), 0),
                        last_message_at = (
                          SELECT MAX(last_message_at) FROM conversations
                           WHERE conversations.account_id = accounts.id
                        )",
                [],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        transaction
            .commit()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        Ok(groups
            .into_iter()
            .map(
                |((account_id, conversation_id), message_ids)| ExpiredMessageGroup {
                    account_id,
                    conversation_id,
                    message_ids,
                },
            )
            .collect())
    }
}

/// The connector-computed sweep deadline for one row (contract 1.42, §4.41):
/// wall-clock ms when a live timer (`expire_seconds > 0`) runs out. The
/// countdown starts at the engine-reported start stamp when the envelope
/// stated one, else at the row's receive time, else at the store's own
/// insert-time clock (history rows carry the import stamp). Rows without a
/// live timer — timer-off, no stated value, system/timer-notice rows — sweep
/// never, so a conversation without disappearing messages costs no sweep work.
fn expire_deadline(message: &MessageRecord, stored_at: u64) -> Option<u64> {
    let seconds = message.expire_seconds.filter(|seconds| *seconds > 0)?;
    let start = message
        .expire_start_at
        .or(message.received_at)
        .unwrap_or(stored_at);
    Some(
        start
            .saturating_add(seconds.saturating_mul(1_000))
            .min(i64::MAX as u64),
    )
}

fn prune_completed_account_deletes(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), StoreError> {
    transaction
        .execute(
            "DELETE FROM account_delete_operations
             WHERE state='completed' AND operation_id NOT IN (
               SELECT operation_id FROM account_delete_operations
               WHERE state='completed'
               ORDER BY updated_at DESC, operation_id DESC
               LIMIT ?1
             )",
            params![MAX_COMPLETED_ACCOUNT_DELETE_OPERATIONS],
        )
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
    Ok(())
}

fn encode_message_cursor(account_id: &str, conversation_id: &str, item: &MessageRecord) -> String {
    format!(
        "{MESSAGE_CURSOR_PREFIX}{account_id}:{conversation_id}:{}:{}",
        item.sent_at, item.id
    )
}

/// Returns the `(sent_at, id)` sort key a page should resume below. The cursor
/// is bound to one account and conversation, so a cursor from elsewhere is
/// refused rather than silently paging the wrong thread.
fn decode_message_cursor(
    cursor: &str,
    expected_account_id: &str,
    expected_conversation_id: &str,
) -> Result<(i64, String), StoreError> {
    let mut parts = cursor
        .strip_prefix(MESSAGE_CURSOR_PREFIX)
        .ok_or(StoreError::InvalidCursor)?
        .splitn(4, ':');
    if parts.next() != Some(expected_account_id) {
        return Err(StoreError::InvalidCursor);
    }
    if parts.next() != Some(expected_conversation_id) {
        return Err(StoreError::InvalidCursor);
    }
    let sent_at = parts
        .next()
        .ok_or(StoreError::InvalidCursor)?
        .parse::<i64>()
        .map_err(|_| StoreError::InvalidCursor)?;
    let id = parts.next().ok_or(StoreError::InvalidCursor)?;
    if sent_at < 0 || id.is_empty() || id.len() > 128 {
        return Err(StoreError::InvalidCursor);
    }
    Ok((sent_at, id.to_string()))
}

fn encode_search_message_cursor(account_id: &str, item: &MessageRecord) -> String {
    format!(
        "{SEARCH_MESSAGE_CURSOR_PREFIX}{account_id}:{}:{}",
        item.sent_at, item.id
    )
}

/// Returns the `(sent_at, id)` sort key a search page should resume below.
/// Bound to the account only (results span conversations); a cursor from
/// another account is refused rather than silently paging the wrong data.
fn decode_search_message_cursor(
    cursor: &str,
    expected_account_id: &str,
) -> Result<(i64, String), StoreError> {
    let mut parts = cursor
        .strip_prefix(SEARCH_MESSAGE_CURSOR_PREFIX)
        .ok_or(StoreError::InvalidCursor)?
        .splitn(3, ':');
    if parts.next() != Some(expected_account_id) {
        return Err(StoreError::InvalidCursor);
    }
    let sent_at = parts
        .next()
        .ok_or(StoreError::InvalidCursor)?
        .parse::<i64>()
        .map_err(|_| StoreError::InvalidCursor)?;
    let id = parts.next().ok_or(StoreError::InvalidCursor)?;
    if sent_at < 0 || id.is_empty() || id.len() > 128 {
        return Err(StoreError::InvalidCursor);
    }
    Ok((sent_at, id.to_string()))
}

fn encode_conversation_cursor(account_id: &str, item: &ConversationSummary) -> String {
    match item.last_message_at {
        Some(last_message_at) => format!("v2:{account_id}:0:{last_message_at}:{}", item.id),
        None => format!("v2:{account_id}:1:0:{}", item.id),
    }
}

fn encode_contact_cursor(account_id: &str, item: &ContactSummary) -> String {
    format!("c1:{account_id}:{}:{}", item.kind, item.peer_key)
}

fn decode_contact_cursor(
    cursor: &str,
    expected_account_id: &str,
) -> Result<(String, String), StoreError> {
    let mut parts = cursor.splitn(4, ':');
    if parts.next() != Some("c1") {
        return Err(StoreError::InvalidCursor);
    }
    let account_id = parts.next().ok_or(StoreError::InvalidCursor)?;
    if account_id != expected_account_id {
        return Err(StoreError::InvalidCursor);
    }
    let kind = parts.next().ok_or(StoreError::InvalidCursor)?;
    if kind != "contact" && kind != "group" {
        return Err(StoreError::InvalidCursor);
    }
    // Peer keys (numbers, uuids, base64 group ids) never contain ':'; splitn
    // keeps any tail intact regardless.
    let peer_key = parts.next().ok_or(StoreError::InvalidCursor)?;
    if peer_key.is_empty() || peer_key.len() > 128 {
        return Err(StoreError::InvalidCursor);
    }
    Ok((kind.to_string(), peer_key.to_string()))
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn decode_conversation_cursor(
    cursor: &str,
    expected_account_id: &str,
) -> Result<(bool, Option<i64>, String), StoreError> {
    let mut parts = cursor.splitn(5, ':');
    if parts.next() != Some("v2") {
        return Err(StoreError::InvalidCursor);
    }
    let account_id = parts.next().ok_or(StoreError::InvalidCursor)?;
    if account_id != expected_account_id {
        return Err(StoreError::InvalidCursor);
    }
    let null_flag = parts.next().ok_or(StoreError::InvalidCursor)?;
    let timestamp = parts
        .next()
        .ok_or(StoreError::InvalidCursor)?
        .parse::<i64>()
        .map_err(|_| StoreError::InvalidCursor)?;
    let id = parts.next().ok_or(StoreError::InvalidCursor)?;
    if timestamp < 0 || id.is_empty() || id.len() > 128 {
        return Err(StoreError::InvalidCursor);
    }
    match null_flag {
        "0" => Ok((false, Some(timestamp), id.to_string())),
        "1" if timestamp == 0 => Ok((true, None, id.to_string())),
        _ => Err(StoreError::InvalidCursor),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DbFileState {
    Absent,
    Plaintext,
    Encrypted,
}

/// Classify the store file without opening it: a readable plaintext SQLite
/// header is definitive; anything else goes through the keyed open, which
/// fails closed on a file that is not a valid encrypted store.
fn db_file_state(path: &Path) -> Result<DbFileState, StoreError> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(DbFileState::Absent),
        Err(error) => return Err(StoreError::InvalidStateDir(Some(error))),
    };
    if metadata.len() == 0 {
        // A zero-length file is an empty database to SQLite: nothing to
        // protect or migrate yet.
        return Ok(DbFileState::Absent);
    }
    let mut header = [0_u8; 16];
    let mut file =
        std::fs::File::open(path).map_err(|error| StoreError::InvalidStateDir(Some(error)))?;
    match file.read_exact(&mut header) {
        Ok(()) if &header == SQLITE_PLAINTEXT_HEADER => Ok(DbFileState::Plaintext),
        _ => Ok(DbFileState::Encrypted),
    }
}

/// Open a store under SQLCipher: key first, then a forced first-page read so a
/// wrong key or a damaged file fails closed here instead of mid-request.
fn open_encrypted(
    path: &Path,
    key: &StoreKey,
    preexisting: bool,
) -> Result<Connection, StoreError> {
    let conn = Connection::open(path).map_err(unavailable)?;
    let key_spec = key.raw_key_spec();
    let pragma = Zeroizing::new(format!("PRAGMA key = \"{}\"", key_spec.as_str()));
    drop(key_spec);
    conn.execute_batch(&pragma).map_err(unavailable)?;
    match conn.query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(())) {
        Ok(_) => Ok(conn),
        Err(error)
            if preexisting
                && matches!(
                    error,
                    rusqlite::Error::SqliteFailure(ref failure, _)
                    if failure.code == rusqlite::ErrorCode::NotADatabase
                ) =>
        {
            Err(StoreError::StoreKeyRejected)
        }
        Err(error) => Err(StoreError::Unavailable(Some(error))),
    }
}

/// Migrate a plaintext store to encrypted storage (optimization-plan Phase 3):
/// checkpoint the plaintext WAL, take one consistent plaintext backup, export
/// online into a fresh encrypted staging file via `sqlcipher_export`, then
/// atomically swap it in. Fail closed: any error removes the staging debris,
/// keeps the original plaintext file exactly where it was, and never opens it
/// for serving. No plaintext copy lands anywhere but the single backup file.
fn migrate_plaintext_store(path: &Path, key: &StoreKey) -> Result<(), StoreError> {
    let backup = plaintext_backup_path(path);
    let staging = append_suffix(path, MIGRATION_STAGING_SUFFIX);
    let migrated = (|| -> Result<(), StoreError> {
        // Debris from a crashed earlier attempt only ever holds a partial
        // copy, never the only copy of anything.
        remove_if_exists(&staging)?;
        remove_sidecars(&staging)?;
        remove_if_exists(&backup)?;
        // Unkeyed SQLCipher connections read plaintext stores unchanged.
        let plaintext = Connection::open(path).map_err(unavailable)?;
        // Fold any committed WAL tail into the main file so it is complete on
        // its own before it is copied, exported, or replaced.
        plaintext
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .map_err(unavailable)?;
        plaintext
            .execute("VACUUM INTO ?1", params![path_str(&backup)?])
            .map_err(unavailable)?;
        // Path and raw-key spec are bound parameters, never interpolated, so
        // no part of the key or a path becomes SQL text.
        let key_spec = key.raw_key_spec();
        plaintext
            .execute(
                "ATTACH DATABASE ?1 AS encrypted KEY ?2",
                params![path_str(&staging)?, key_spec.as_str()],
            )
            .map_err(unavailable)?;
        drop(key_spec);
        let export = plaintext
            .execute_batch("SELECT sqlcipher_export('encrypted'); DETACH DATABASE encrypted");
        drop(plaintext);
        export.map_err(unavailable)?;
        // The plaintext `-wal`/`-shm` are empty after the checkpoint, and any
        // sidecar left next to the swapped-in encrypted file would corrupt its
        // next open, so removal is part of the swap.
        remove_sidecars(path)?;
        std::fs::rename(&staging, path)
            .map_err(|error| StoreError::InvalidStateDir(Some(error)))?;
        Ok(())
    })();
    match migrated {
        Ok(()) => {
            tracing::info!("plaintext store migrated to encrypted storage");
            Ok(())
        }
        Err(error) => {
            // Fail closed: best-effort debris cleanup; the original plaintext
            // file and its backup stay put for the next attempt or a rollback.
            let _ = remove_if_exists(&staging);
            let _ = remove_sidecars(&staging);
            Err(StoreError::MigrationFailed(Some(Box::new(error))))
        }
    }
}

fn unavailable(error: rusqlite::Error) -> StoreError {
    StoreError::Unavailable(Some(error))
}

fn path_str(path: &Path) -> Result<&str, StoreError> {
    path.to_str().ok_or(StoreError::InvalidStateDir(None))
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut owned = path.as_os_str().to_owned();
    owned.push(suffix);
    PathBuf::from(owned)
}

fn plaintext_backup_path(db_path: &Path) -> PathBuf {
    append_suffix(db_path, PLAINTEXT_BACKUP_SUFFIX)
}

fn remove_if_exists(path: &Path) -> Result<(), StoreError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StoreError::InvalidStateDir(Some(error))),
    }
}

fn remove_sidecars(path: &Path) -> Result<(), StoreError> {
    for suffix in ["-wal", "-shm", "-journal"] {
        remove_if_exists(&append_suffix(path, suffix))?;
    }
    Ok(())
}

fn prepare_state_dir(path: &Path) -> Result<(), StoreError> {
    if !path.is_absolute() {
        return Err(StoreError::InvalidStateDir(None));
    }
    std::fs::create_dir_all(path).map_err(|error| StoreError::InvalidStateDir(Some(error)))?;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| StoreError::InvalidStateDir(Some(error)))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(StoreError::InvalidStateDir(None));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != rustix::process::getuid().as_raw() {
            return Err(StoreError::InvalidStateDir(None));
        }
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| StoreError::InvalidStateDir(Some(error)))?;
    }
    Ok(())
}

/// Prefer real display names over masked peer ids / placeholders.
fn title_should_upgrade(current: &str, candidate: &str) -> bool {
    let cand = candidate.trim();
    if cand.is_empty() {
        return false;
    }
    let cur = current.trim();
    if cur.is_empty() || cur == "group" || cur == "contact" {
        return true;
    }
    // mask_address style: "8fc***e2" / "+86***50"
    if cur.contains("***") && !cand.contains("***") {
        return true;
    }
    false
}

fn migrate_schema(conn: &Connection) -> Result<(), StoreError> {
    let current: i64 = conn
        .query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    if current > SCHEMA_VERSION {
        return Err(StoreError::Unavailable(None));
    }
    if current < 2 {
        // Older DBs created before display_name column.
        if !table_has_column(conn, "accounts", "display_name")? {
            conn.execute("ALTER TABLE accounts ADD COLUMN display_name TEXT", [])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    if current < 4 {
        if !table_has_column(conn, "messages", "body_bytes")? {
            conn.execute("ALTER TABLE messages ADD COLUMN body_bytes INTEGER", [])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        if !table_has_column(conn, "messages", "body_truncated")? {
            conn.execute(
                "ALTER TABLE messages ADD COLUMN body_truncated INTEGER NOT NULL DEFAULT 0",
                [],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    // Schema 5 adds the contacts cache table; it is created via
    // CREATE TABLE IF NOT EXISTS above, so no data migration is needed.
    if current < 6 && !table_has_column(conn, "messages", "stored_at")? {
        // Metadata-only: history written before this column is left NULL rather
        // than backfilled, because rewriting a whole table here would delay the
        // startup handshake. Retention dates those rows by their receive time
        // instead, so nothing reads the missing value as the epoch.
        conn.execute("ALTER TABLE messages ADD COLUMN stored_at INTEGER", [])
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
    }
    if current < 7 && !table_has_column(conn, "accounts", "proxy_group")? {
        // Phase 4 (ADR 0001 R4): every pre-Phase-4 account belongs to the
        // reserved `default` group, so the additive column defaults to it and
        // no backfill pass is needed. The column carries a NOT NULL default so
        // rows written by older binaries (rollback scenario) still read.
        conn.execute(
            "ALTER TABLE accounts ADD COLUMN proxy_group TEXT NOT NULL DEFAULT 'default'",
            [],
        )
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
    }
    if current < 8 {
        // Contract revision 1.15 inbound control plane: quote snapshot and
        // attachment metadata (descriptors only) are JSON text; the edit
        // marker is a timestamp. All additive nullable columns — pre-1.15
        // rows legitimately have none. The message_events table is created
        // by CREATE TABLE IF NOT EXISTS above, so no data migration is needed.
        for (column, kind) in [
            ("quote_snapshot", "TEXT"),
            ("attachments_json", "TEXT"),
            ("edited_at", "INTEGER"),
        ] {
            if !table_has_column(conn, "messages", column)? {
                conn.execute(
                    &format!("ALTER TABLE messages ADD COLUMN {column} {kind}"),
                    [],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            }
        }
    }
    if current < 9 {
        // Contract revision 1.25 inbound rich bodies: one additive nullable
        // JSON column packs previews/mentions/textStyles/viewOnce (§4.20).
        // Pre-1.25 rows legitimately have none; bounds live in the engine, so
        // the column only ever holds what §4.20 bounded.
        if !table_has_column(conn, "messages", "rich_json")? {
            conn.execute("ALTER TABLE messages ADD COLUMN rich_json TEXT", [])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    if current < 10 {
        // Contract revision 1.27: per-row author display names captured from
        // the receive envelope's `sourceName` — on messages (backs the
        // conversation-summary author field) and on reaction events (backs the
        // per-actor reaction detail). Both additive nullable: pre-1.27 rows
        // legitimately have none and are never backfilled, because the name a
        // past envelope carried was never stored and must not be invented.
        if !table_has_column(conn, "messages", "sender_name")? {
            conn.execute("ALTER TABLE messages ADD COLUMN sender_name TEXT", [])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        if !table_has_column(conn, "message_events", "actor_name")? {
            conn.execute("ALTER TABLE message_events ADD COLUMN actor_name TEXT", [])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    if current < 11 {
        // Contract revision 1.29: unread @mentions. `messages.mentions_self`
        // is captured at receive from the normalized mention authors and the
        // account's own identity — never backfilled, because which past
        // envelopes addressed this account is only known from the envelopes
        // themselves. `conversations.unread_mentions` is a write-side counter
        // moving in lockstep with `unread_count`; history rows that predate
        // the column contributed to neither, so the counter starts at zero
        // and there is nothing to reconstruct.
        if !table_has_column(conn, "messages", "mentions_self")? {
            conn.execute(
                "ALTER TABLE messages ADD COLUMN mentions_self INTEGER NOT NULL DEFAULT 0",
                [],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
        if !table_has_column(conn, "conversations", "unread_mentions")? {
            conn.execute(
                "ALTER TABLE conversations ADD COLUMN unread_mentions INTEGER NOT NULL DEFAULT 0",
                [],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    if current < 12 {
        // Contract revision 1.32: peer-receipt timestamps and prior-body edit
        // history. `delivered_at` / `read_at` are additive nullable columns —
        // receipts only ever arrive live, so history rows are never backfilled
        // and pre-1.32 rows legitimately carry none. The `message_edits` table
        // is created by CREATE TABLE IF NOT EXISTS above, so no data migration
        // is needed; its rows cascade with their message row.
        for (column, kind) in [("delivered_at", "INTEGER"), ("read_at", "INTEGER")] {
            if !table_has_column(conn, "messages", column)? {
                conn.execute(
                    &format!("ALTER TABLE messages ADD COLUMN {column} {kind}"),
                    [],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            }
        }
    }
    if current < 13 {
        // Contract revision 1.33: message pin/unpin + group admin delete. The
        // `admin_deleted` marker is an additive flag — the tombstone is a
        // property of rows that receive an adminDelete envelope, so history
        // rows are never backfilled. The `conversation_pins` table is created
        // by CREATE TABLE IF NOT EXISTS above, so no data migration is needed;
        // its rows cascade with their conversation (account deletes clean up).
        if !table_has_column(conn, "messages", "admin_deleted")? {
            conn.execute(
                "ALTER TABLE messages ADD COLUMN admin_deleted INTEGER NOT NULL DEFAULT 0",
                [],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    if current < 14 {
        // Contract revision 1.35: inbound sticker metadata (§4.31). One
        // additive nullable JSON column beside `rich_json` — pre-1.35 rows
        // legitimately carry none and are never backfilled, because the
        // sticker identity a past envelope carried was never stored. The
        // sticker's byte pointer keeps riding `attachments_json`, so no
        // attachment data moves with this column.
        if !table_has_column(conn, "messages", "sticker_json")? {
            conn.execute("ALTER TABLE messages ADD COLUMN sticker_json TEXT", [])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    if current < 15 {
        // Contract revision 1.39 (§4.38): link-time history import. The
        // `history_imports` status row and the two per-run staging tables are
        // created by CREATE TABLE IF NOT EXISTS above — no data migration;
        // staging rows are per-account scratch that every import run clears,
        // and both cascade away with their account row.
    }
    if current < 16 {
        // Contract revision 1.42 (§4.41): disappearing-message metadata. All
        // additive nullable columns — pre-1.42 rows legitimately carry none
        // and are never backfilled, because the timer state a past envelope
        // carried was never stored. `messages.expire_seconds` keeps the
        // envelope-stated timer verbatim (0 = the sender's timer is off);
        // `messages.expire_start_at` is the engine-reported countdown start
        // (engine surfaces only); `messages.expire_at` is the connector's
        // sweep deadline, written at insert for rows with a live timer only.
        // `conversations.expire_timer_seconds` is the conversation's current
        // timer (NULL unknown, 0 off, N seconds). The staging column rides
        // the same run-scratch discipline as its table. The partial index is
        // created by POST_MIGRATION_SCHEMA_DDL above.
        for (table, column, kind) in [
            ("messages", "expire_seconds", "INTEGER"),
            ("messages", "expire_start_at", "INTEGER"),
            ("messages", "expire_at", "INTEGER"),
            ("conversations", "expire_timer_seconds", "INTEGER"),
            ("history_import_stage_chats", "expire_timer_ms", "INTEGER"),
        ] {
            if !table_has_column(conn, table, column)? {
                conn.execute(
                    &format!("ALTER TABLE {table} ADD COLUMN {column} {kind}"),
                    [],
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            }
        }
    }
    if current < 17 {
        // Contract revision 1.49 (§4.48): reportSpam persistence. One
        // additive nullable column — the server-assigned message GUID the
        // receive envelope carried (`serverGuid`, the upstream report API's
        // path parameter). Pre-1.49 rows and envelopes without the key keep
        // NULL and are never backfilled: a GUID a past envelope carried was
        // never stored, and inventing one would report the wrong upstream
        // message. Report requests resolve the GUID from these rows at call
        // time; NULL rows refuse individually (§4.48 failures semantics).
        if !table_has_column(conn, "messages", "server_guid")? {
            conn.execute("ALTER TABLE messages ADD COLUMN server_guid TEXT", [])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    if current < 18 {
        // Contract revision 1.55 (§4.38 attachv2): link-time import
        // attachment pointer faces. One additive nullable JSON column beside
        // `attachments_json` — a strict subset projection of the engine's
        // NDJSON locator object (the wire-facing attachment descriptor stays
        // byte-identical; §4.38). Key material rides only this internal
        // column and never the host wire. Pre-1.55 rows (and live receives,
        // which carry no pointer) keep NULL and are never backfilled. The
        // ingest-side caps live in the import parser, so the column only
        // ever holds what §4.38 bounded.
        if !table_has_column(conn, "messages", "attachment_locators")? {
            conn.execute(
                "ALTER TABLE messages ADD COLUMN attachment_locators TEXT",
                [],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    if current < 19 {
        // Contract revision 1.61 (§4.54): live-receive attachment pointer
        // faces. One additive nullable JSON column — the same ingest face as
        // the schema-18 import column, captured from the receive envelope
        // instead of the archive. Today's pinned engines (signal-cli 0.14.8,
        // the kt-engine receive projection) ship metadata-only envelopes, so
        // live rows keep NULL until an engine projects the receive locator —
        // the column then fills without a further migration. Never
        // backfilled: a past envelope's pointer face (if any) is gone, and
        // inventing one is impossible. Same internal-only discipline as
        // schema 18: key material never crosses the host wire.
        if !table_has_column(conn, "messages", "live_attachment_locators")? {
            conn.execute(
                "ALTER TABLE messages ADD COLUMN live_attachment_locators TEXT",
                [],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    if current < 20 {
        // Contract revision 1.65 (§4.58): message-request factor faces. One
        // additive nullable JSON column on the conversation row — the
        // engine-supplied {hidden, profileSharing, blocked} trio the
        // throttled listContacts enrich stamps for direct conversations, or
        // `{}` when the engine answered without any of the three flags (the
        // honest probed-absent marker an engine without the trio leaves).
        // NULL = never probed (fresh rows; the next enrich window
        // stamps them). Never backfilled: the factor state a past engine
        // answer carried is only known from that answer. The accepted value
        // itself is never stored — it stays a read-time derivation over this
        // column plus the message window (§4.58), so a factor refresh never
        // needs a second migration.
        if !table_has_column(conn, "conversations", "mrr_factors_json")? {
            conn.execute(
                "ALTER TABLE conversations ADD COLUMN mrr_factors_json TEXT",
                [],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    if current < 21 {
        // Contract revision 1.94 (§4.77): the viewed face. One additive
        // nullable column — outgoing rows stamp it from the first peer VIEWED
        // receipt that hits them (the official sendState.isViewed), incoming
        // rows stamp it when the host's `messages.markViewed` selects them
        // (the official readStatus=Viewed). Viewed facts only ever arrive
        // live, so pre-1.94 rows legitimately carry none and are never
        // backfilled: whether a past row was played/viewed was never stored,
        // and inventing it would lie to the desktop's played dot.
        if !table_has_column(conn, "messages", "viewed_at")? {
            conn.execute("ALTER TABLE messages ADD COLUMN viewed_at INTEGER", [])
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
        }
    }
    conn.execute(
        "INSERT INTO meta(key, value) VALUES('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![SCHEMA_VERSION.to_string()],
    )
    .map_err(|error| StoreError::Unavailable(Some(error)))?;
    Ok(())
}

fn table_has_column(
    conn: &Connection,
    table: &str,
    expected_column: &str,
) -> Result<bool, StoreError> {
    let sql = match table {
        "accounts" => "PRAGMA table_info(accounts)",
        "conversations" => "PRAGMA table_info(conversations)",
        "messages" => "PRAGMA table_info(messages)",
        "message_events" => "PRAGMA table_info(message_events)",
        "history_import_stage_chats" => "PRAGMA table_info(history_import_stage_chats)",
        _ => return Err(StoreError::Unavailable(None)),
    };
    let mut stmt = conn
        .prepare(sql)
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
    for column in columns {
        if column.map_err(|error| StoreError::Unavailable(Some(error)))? == expected_column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn static_state(value: String) -> &'static str {
    match value.as_str() {
        "linking" => "linking",
        "ready" => "ready",
        "reconnecting" => "reconnecting",
        "disabled" => "disabled",
        "unlinking" => "unlinking",
        "device_unlinked" => "device_unlinked",
        _ => "error",
    }
}

fn static_kind(value: String) -> &'static str {
    match value.as_str() {
        "group" => "group",
        _ => "direct",
    }
}

fn static_contact_kind(value: String) -> &'static str {
    match value.as_str() {
        "group" => "group",
        _ => "contact",
    }
}

fn static_direction(value: String) -> &'static str {
    match value.as_str() {
        "outgoing" => "outgoing",
        "system" => "system",
        _ => "incoming",
    }
}

fn static_status(value: String) -> &'static str {
    match value.as_str() {
        "pending" => "pending",
        "sent" => "sent",
        "delivered" => "delivered",
        "read" => "read",
        "failed" => "failed",
        "system" => "system",
        "remote-deleted" => "remote-deleted",
        _ => "unknown",
    }
}

/// Contract 1.23 (attachment noun) generalized by contracts 1.27/1.28: one
/// indexed read of the newest row decides every last-message projection on a
/// conversation summary. The desktop list renders the official preview line —
/// attachment noun ("Photo" / "Video" / "Voice message" / "File"), send-state
/// icon (outgoing only), group author prefix, reaction emoji prefix — so the
/// read returns `kind`, `direction`, the outgoing send state, the captured
/// author name, and the distinct active reaction emoji of that row.
///
/// Rules unchanged from 1.23 for `kind`: an attachment-only ending (no
/// caption text) yields its first attachment's kind; a text/caption ending, a
/// system row, or an empty conversation stays `None` — the caption wins over
/// the noun, exactly like the official list preview. `direction` follows the
/// same newest row but stays `None` for system rows and empty conversations;
/// the author name is surfaced only for incoming rows that carry one.
/// `status` is the send-state tier of an outgoing ending, in the message-row
/// vocabulary; every other ending exposes none.
struct ConversationLastMessageMeta {
    kind: Option<&'static str>,
    direction: Option<&'static str>,
    status: Option<&'static str>,
    author_name: Option<String>,
    reactions: Vec<String>,
}

/// Project one conversation's pinned state onto the summary wire shape
/// (contract 1.33). Expired pins stop rendering (connector-clock comparison
/// — a timed pin past its `expires_at` is no longer pinned); a pin whose
/// target row is no longer in the local history (retention pruned it, or it
/// never landed here) is not surfaced either, because the desktop can only
/// render a pin bar for a message it holds. Read-time resolution keeps the
/// stored state free of dangling row ids.
fn pinned_message_for_conversation(
    conn: &Connection,
    account_id: &str,
    conversation_id: &str,
) -> Result<Option<ConversationPinnedMessage>, StoreError> {
    let stored = conn
        .query_row(
            "SELECT target_author, target_sent_at, pinned_at, expires_at
             FROM conversation_pins
             WHERE account_id=?1 AND conversation_id=?2",
            params![account_id, conversation_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)? as u64,
                    row.get::<_, i64>(2)? as u64,
                    row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                ))
            },
        )
        .optional()
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
    let Some((target_author, target_sent_at, pinned_at, expires_at)) = stored else {
        return Ok(None);
    };
    if expires_at.is_some_and(|expires_at| expires_at <= crate::link::now_ms()) {
        return Ok(None);
    }
    let message_id: Option<String> = conn
        .query_row(
            "SELECT id FROM messages
             WHERE account_id=?1 AND conversation_id=?2 AND sent_at=?3
             ORDER BY id ASC LIMIT 1",
            params![account_id, conversation_id, target_sent_at as i64],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
    Ok(message_id.map(|message_id| ConversationPinnedMessage {
        message_id,
        target_author,
        target_sent_timestamp: target_sent_at,
        pinned_at,
        expires_at,
    }))
}

fn conversation_last_message_meta(
    conn: &Connection,
    account_id: &str,
    conversation_id: &str,
) -> Result<ConversationLastMessageMeta, StoreError> {
    let latest = conn
        .query_row(
            "SELECT direction, body, attachments_json, sender_name, status, sent_at FROM messages
             WHERE account_id=?1 AND conversation_id=?2
             ORDER BY sent_at DESC, id DESC
             LIMIT 1",
            params![account_id, conversation_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|error| StoreError::Unavailable(Some(error)))?;
    let Some((direction, body, attachments_json, sender_name, status, sent_at)) = latest else {
        return Ok(ConversationLastMessageMeta {
            kind: None,
            direction: None,
            status: None,
            author_name: None,
            reactions: Vec::new(),
        });
    };
    let has_caption = body.is_some_and(|text| !text.trim().is_empty());
    let kind = if direction == "system" || has_caption {
        None
    } else {
        let attachments: Vec<crate::engine::NormalizedAttachment> = attachments_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default();
        attachments.first().map(|first| {
            let content_type = first.content_type.as_deref().unwrap_or_default();
            if first.is_voice_note || content_type.starts_with("audio/") {
                "audio"
            } else if content_type.starts_with("image/") {
                "image"
            } else if content_type.starts_with("video/") {
                "video"
            } else {
                "file"
            }
        })
    };
    let wire_direction = match direction.as_str() {
        "system" => None,
        other => Some(static_direction(other.to_string())),
    };
    let author_name = match wire_direction {
        Some("incoming") => sender_name.filter(|name| !name.trim().is_empty()),
        _ => None,
    };
    // Contract 1.28: the official list shows the send-state icon only when
    // the conversation ends on an outgoing message. The value is that row's
    // own status — the same projection `MessageRecord.status` carries, so the
    // list icon and the bubble icon can never disagree — restricted to the
    // genuine send tiers: a `remote-deleted`/`unknown` outgoing ending has no
    // official icon and exposes no state.
    let wire_status = if direction == "outgoing" {
        match static_status(status) {
            tier @ ("pending" | "sent" | "delivered" | "read" | "failed") => Some(tier),
            _ => None,
        }
    } else {
        None
    };
    // Active reactions on exactly the newest row, keyed by its upstream
    // timestamp; pill order (oldest reaction first, then emoji) matches the
    // message projection so the summary prefix and the open conversation
    // agree.
    let mut reactions = Vec::new();
    if direction != "system" {
        let mut stmt = conn
            .prepare(
                "SELECT emoji FROM message_events
                 WHERE conversation_id=?1 AND kind='reaction' AND removed=0
                   AND target_timestamp=?2
                 GROUP BY emoji
                 ORDER BY MIN(updated_at) ASC, emoji ASC
                 LIMIT ?3",
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let rows = stmt
            .query_map(
                params![conversation_id, sent_at, MAX_SUMMARY_REACTIONS as i64],
                |row| row.get::<_, String>(0),
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        for emoji in rows {
            reactions.push(emoji.map_err(|error| StoreError::Unavailable(Some(error)))?);
        }
    }
    Ok(ConversationLastMessageMeta {
        kind,
        direction: wire_direction,
        status: wire_status,
        author_name,
        reactions,
    })
}

fn history_import_row_from_row(row: &Row<'_>) -> rusqlite::Result<HistoryImportRow> {
    Ok(HistoryImportRow {
        account_id: row.get(0)?,
        state: row.get(1)?,
        attempts: row.get::<_, i64>(2)?.max(0) as u32,
        imported_messages: row.get::<_, i64>(3)?.max(0) as u64,
        skipped_lines: row.get::<_, i64>(4)?.max(0) as u64,
        skipped_chats: row.get::<_, i64>(5)?.max(0) as u64,
        skipped_messages: row.get::<_, i64>(6)?.max(0) as u64,
        error_class: row.get(7)?,
        started_at: row.get::<_, i64>(8)?.max(0) as u64,
        updated_at: row.get::<_, i64>(9)?.max(0) as u64,
    })
}

/// Terminal per-run counters of one import (§4.38): what landed, and every
/// skip class that keeps the projection honest. The ledger row stores them
/// verbatim on completion.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ImportCounters {
    pub imported: u64,
    pub skipped_lines: u64,
    pub skipped_chats: u64,
    pub skipped_messages: u64,
}

/// Persisted import state for one account (§4.38): at most one row, the
/// honest minimal projection the desktop polls through
/// `history.importStatus`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryImportRow {
    pub account_id: String,
    /// running | completed | failed
    pub state: String,
    pub attempts: u32,
    pub imported_messages: u64,
    pub skipped_lines: u64,
    pub skipped_chats: u64,
    pub skipped_messages: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,
    pub started_at: u64,
    pub updated_at: u64,
}

/// One archive recipient's staged identity (pass 1 scratch): the fields
/// the engine's NDJSON face projects — `kind`/`name` always, and the
/// service-identity fields the engine's S3 task must add (§4.38). A
/// recipient without any resolvable identity yields no conversation and
/// its messages are skip-counted.
#[derive(Clone, Debug)]
pub struct HistoryStageRecipient {
    pub id: u64,
    pub kind: String,
    pub name: String,
    pub aci: Option<String>,
    pub e164: Option<String>,
    pub master_key: Option<String>,
}

/// One pass-1 staged chat row (§4.38/§4.41): the chat→recipient identity
/// plus the chat frame's disappearing-message timer in ms, if the frame
/// stated one (`None` = no stated timer — the conversation state stays
/// untouched).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryStageChat {
    pub id: u64,
    pub recipient_id: u64,
    pub expire_timer_ms: Option<u64>,
}

/// One import batch row: the message record carries the resolved
/// conversation/direction/sender identity, the preview is the
/// conversation-summary projection, and `alt_sender_id` covers the
/// number/ACI duality — a stored row matching either sender hash counts
/// as the same Signal message.
pub struct HistoryImportInsert {
    pub record: MessageRecord,
    pub preview: Option<String>,
    pub alt_sender_id: Option<String>,
    /// Contract 1.55 (§4.38 attachv2): the archive attachments' ingest-side
    /// pointer faces, aligned by index with `record.attachments`. Key
    /// material rides this field and the internal `attachment_locators`
    /// column only — never the host wire (`attachments_json` keeps its
    /// byte-identical v1 shape). Empty = no pointers on this message.
    pub locators: Vec<HistoryAttachmentLocator>,
}

/// The ingest-side attachment pointer face of one imported attachment
/// (contract 1.55, §4.38): the strict subset of the engine's NDJSON locator
/// object the connector can honestly consume today. Serialize-safe
/// (`attachment_locators` JSON) and wire-free — the host-facing attachment
/// descriptor never carries key material (§4.38 decision). Integrity is
/// one-of: the archive's `digest` (encrypted, standard base64) or
/// `plaintextHash` (lowercase hex), or neither when the locator was absent.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryAttachmentLocator {
    /// The attachment encryption key (base64). Required for any download;
    /// absent means the pointer face is unknown/invalid (kept only for
    /// index alignment).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdn_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdn_number: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_timestamp: Option<u64>,
    /// Encrypted-digest integrity face (base64) — the sender-original form.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Plaintext-hash integrity face (lowercase hex) — the downloaded form.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plaintext_hash: Option<String>,
}

#[derive(Debug, Default, PartialEq)]
pub struct HistoryBatchOutcome {
    pub inserted: u64,
    pub duplicates: u64,
}

fn history_stage_recipient_from_row(row: &Row<'_>) -> rusqlite::Result<HistoryStageRecipient> {
    Ok(HistoryStageRecipient {
        id: row.get::<_, i64>(0)?.max(0) as u64,
        kind: row.get(1)?,
        name: row.get(2)?,
        aci: row.get(3)?,
        e164: row.get(4)?,
        master_key: row.get(5)?,
    })
}

/// The adjacent-media probe's row predicate (contract 1.93, §4.76): at
/// least one attachment with an `image/*` or `video/*` content type, and
/// never a view-once row (the marker and its invalid twin both exclude —
/// the §4.37 boundary keeps tap-to-view media out of every navigation
/// strip). Voice notes are audio, not visual; an attachment without a
/// content type cannot be classified and is honestly skipped.
fn message_has_visual_media(record: &MessageRecord) -> bool {
    let view_once = record
        .rich
        .as_ref()
        .map(|rich| rich.view_once || rich.view_once_invalid)
        .unwrap_or(false);
    if view_once {
        return false;
    }
    record.attachments.iter().any(|attachment| {
        attachment
            .content_type
            .as_deref()
            .map(|content_type| {
                content_type.starts_with("image/") || content_type.starts_with("video/")
            })
            .unwrap_or(false)
    })
}

fn message_record_from_row(row: &Row<'_>) -> rusqlite::Result<MessageRecord> {
    let text: Option<String> = row.get(7)?;
    let text_bytes = row
        .get::<_, Option<i64>>(8)?
        .map(|value| value.max(0).min(u32::MAX as i64) as u32)
        .or_else(|| {
            text.as_ref()
                .map(|value| value.len().min(u32::MAX as usize) as u32)
        });
    let persisted_truncated = row.get::<_, i64>(9)? != 0;
    let quote_snapshot: Option<String> = row.get(13)?;
    let attachments_json: Option<String> = row.get(14)?;
    let rich_json: Option<String> = row.get(15)?;
    Ok(MessageRecord {
        id: row.get(0)?,
        account_id: row.get(1)?,
        conversation_id: row.get(2)?,
        direction: static_direction(row.get::<_, String>(3)?),
        sender_id: row.get(4)?,
        sent_at: row.get::<_, i64>(5)? as u64,
        received_at: row.get::<_, Option<i64>>(6)?.map(|value| value as u64),
        text_retrievable: text.is_some() && !persisted_truncated,
        text,
        text_bytes,
        text_truncated: persisted_truncated,
        status: static_status(row.get::<_, String>(10)?),
        client_request_id: row.get(12)?,
        quote_message_id: row.get(11)?,
        quote_snapshot: quote_snapshot
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
        attachments: attachments_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default(),
        rich: rich_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
        edited_at: row.get::<_, Option<i64>>(16)?.map(|value| value as u64),
        sender_name: row.get(17)?,
        mentions_self: row.get::<_, i64>(18)? != 0,
        reactions: Vec::new(),
        edits: Vec::new(),
        delivered_at: row.get::<_, Option<i64>>(19)?.map(|value| value as u64),
        read_at: row.get::<_, Option<i64>>(20)?.map(|value| value as u64),
        viewed_at: row.get::<_, Option<i64>>(27)?.map(|value| value as u64),
        admin_deleted: row.get::<_, i64>(21)? != 0,
        sticker: row
            .get::<_, Option<String>>(22)?
            .and_then(|json| serde_json::from_str(&json).ok()),
        expire_seconds: row
            .get::<_, Option<i64>>(23)?
            .map(|value| value.max(0) as u64),
        expire_start_at: row
            .get::<_, Option<i64>>(24)?
            .map(|value| value.max(0) as u64),
        server_guid: row.get(26)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_PROXY_GROUP_ID;
    use tempfile::TempDir;

    /// Every unit-test store is encrypted: the fixture key exercises the same
    /// keyed-open path production uses (optimization-plan Phase 3).
    const TEST_KEY_BYTES: [u8; 32] = [0x5A; 32];

    /// Contract 1.43 (§4.42): `isSelf` marks exactly the linked account's own
    /// direct chat — the number match — and never a sibling contact, a
    /// UUID-keyed row (the §4.24 boundary: the own ACI is not queryable), a
    /// group, or another account's conversation. The list and the single-row
    /// read derive the same answer.
    #[test]
    fn conversation_summary_is_self_marks_only_the_own_direct_chat() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let own = store
            .ensure_conversation(&account.id, "direct", "+15555550100", "Test User")
            .unwrap();
        let contact = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "Alice")
            .unwrap();
        let uuid_peer = store
            .ensure_conversation(
                &account.id,
                "direct",
                "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10",
                "Aci Peer",
            )
            .unwrap();
        let group = store
            .ensure_conversation(&account.id, "group", "Z3JvdXAtaWQ=", "Group")
            .unwrap();

        let summary = |conversation_id: &str| {
            store
                .conversation_summary(&account.id, conversation_id)
                .unwrap()
                .unwrap()
        };
        assert!(summary(&own.id).is_self);
        assert!(!summary(&contact.id).is_self);
        assert!(!summary(&uuid_peer.id).is_self);
        assert!(!summary(&group.id).is_self);

        let listed = store.list_conversations(&account.id, 10, None).unwrap();
        let marked: Vec<_> = listed.items.iter().filter(|row| row.is_self).collect();
        assert_eq!(marked.len(), 1);
        assert_eq!(marked[0].id, own.id);
        assert_eq!(marked[0].kind, "direct");
        for row in &listed.items {
            assert_eq!(row.is_self, summary(&row.id).is_self);
        }

        // The same peer key means different things per owning account: under
        // the second account, +15555550199 is its own Note to Self chat, while
        // under the first account it is just a contact.
        let other = store
            .upsert_account_from_signal("+15555550199", Some(2), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let other_own = store
            .ensure_conversation(&other.id, "direct", "+15555550199", "Other Self")
            .unwrap();
        assert!(
            store
                .conversation_summary(&other.id, &other_own.id)
                .unwrap()
                .unwrap()
                .is_self
        );
        let contact_view = store
            .ensure_conversation(&account.id, "direct", "+15555550199", "Bob")
            .unwrap();
        assert!(
            !store
                .conversation_summary(&account.id, &contact_view.id)
                .unwrap()
                .unwrap()
                .is_self
        );
        let other_listed = store.list_conversations(&other.id, 10, None).unwrap();
        let other_marked: Vec<_> = other_listed
            .items
            .iter()
            .filter(|row| row.is_self)
            .collect();
        assert_eq!(other_marked.len(), 1);
        assert_eq!(other_marked[0].id, other_own.id);
    }

    /// Contract 1.44 (§4.43): `contacts.list` marks exactly the linked
    /// account's own entry — the `kind='contact'` row whose peer key is the
    /// account's number — and never a sibling contact, a UUID-keyed row (the
    /// §4.24 boundary), a group, or another account's entry. The marker is a
    /// read-time projection: the cache row stores nothing, a resync that
    /// rewrites the row in place neither computes nor erases it, and the
    /// cursor path marks its page rows like the first page does.
    #[test]
    fn contact_list_is_self_marks_only_the_own_contact_entry() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        store
            .upsert_contact(
                &account.id,
                "contact",
                "+15555550100",
                "Test User",
                None,
                10,
            )
            .unwrap();
        store
            .upsert_contact(&account.id, "contact", "+15555550101", "Alice", None, 10)
            .unwrap();
        store
            .upsert_contact(
                &account.id,
                "contact",
                "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10",
                "Aci Peer",
                None,
                10,
            )
            .unwrap();
        store
            .upsert_contact(&account.id, "group", "Z3JvdXAtaWQ=", "Group", None, 10)
            .unwrap();

        let listed = store.list_contacts(&account.id, None, 10, None).unwrap();
        let marked: Vec<_> = listed.items.iter().filter(|row| row.is_self).collect();
        assert_eq!(marked.len(), 1);
        assert_eq!(marked[0].kind, "contact");
        assert_eq!(marked[0].peer_key, "+15555550100");
        // Title material is the account's own profile name (§4.42); the
        // desktop localizes the Note-to-Self presentation from the marker.
        assert_eq!(marked[0].title, "Test User");

        // A resync rewrites the row in place (upsert, same title) and the
        // marker is unchanged — nothing about it is stored.
        store
            .upsert_contact(&account.id, "contact", "+15555550101", "Alice A.", None, 11)
            .unwrap();
        let relisted = store.list_contacts(&account.id, None, 10, None).unwrap();
        let still_marked: Vec<_> = relisted.items.iter().filter(|row| row.is_self).collect();
        assert_eq!(still_marked.len(), 1);
        assert_eq!(still_marked[0].peer_key, "+15555550100");
        assert_eq!(relisted.items.len(), listed.items.len());

        // Cursor pagination derives the same marker on the second page: the
        // account lookup rides whichever page the guard is held for.
        let first = store.list_contacts(&account.id, None, 2, None).unwrap();
        assert_eq!(first.items.len(), 2);
        let cursor = first.next_cursor.clone().unwrap();
        let second = store
            .list_contacts(&account.id, None, 10, Some(&cursor))
            .unwrap();
        for row in second.items.iter().chain(first.items.iter()) {
            assert_eq!(row.is_self, row.peer_key == "+15555550100");
        }

        // The same peer key means different things per owning account: under
        // the second account, +15555550199 is its own entry, while under the
        // first account it is just another contact.
        let other = store
            .upsert_account_from_signal("+15555550199", Some(2), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        store
            .upsert_contact(&other.id, "contact", "+15555550199", "Other Self", None, 10)
            .unwrap();
        let other_listed = store.list_contacts(&other.id, None, 10, None).unwrap();
        let other_marked: Vec<_> = other_listed
            .items
            .iter()
            .filter(|row| row.is_self)
            .collect();
        assert_eq!(other_marked.len(), 1);
        assert_eq!(other_marked[0].peer_key, "+15555550199");

        store
            .upsert_contact(&account.id, "contact", "+15555550199", "Bob", None, 10)
            .unwrap();
        let account_view = store.list_contacts(&account.id, None, 10, None).unwrap();
        assert_eq!(
            account_view
                .items
                .iter()
                .filter(|row| row.is_self)
                .map(|row| row.peer_key.as_str())
                .collect::<Vec<_>>(),
            vec!["+15555550100"]
        );
    }

    /// Contract 1.60 (§4.54): the terminated flag rides the summary and the
    /// contacts-row projections from the §4.50 appendix capture — a group
    /// whose cache row stores the flag answers true on the list, the
    /// single-summary (conversation.changed) face, and the contacts row;
    /// a non-terminated group, a direct conversation, and a group with no
    /// cache row (pre-1.53 sync) all answer false, and false collapses to the
    /// absent wire key (pre-1.60 hosts read byte-identical rows).
    #[test]
    fn terminated_projection_marks_only_sync_captured_groups() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        // Conversation skeletons through the normal ensure path (the same
        // stable_hash_id([account, kind, peer]) key the sync materializes).
        let ended = store
            .ensure_conversation(&account.id, "group", "ZW5kZWQ=", "Ended Group")
            .unwrap();
        let alive = store
            .ensure_conversation(&account.id, "group", "YWxpdmU=", "Alive Group")
            .unwrap();
        let direct = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "Alice")
            .unwrap();

        // The §4.50 appendix capture discipline verbatim: terminated stored
        // only when true, the marker JSON mirrors group_extra_json's shape.
        store
            .upsert_contact(
                &account.id,
                "group",
                "ZW5kZWQ=",
                "Ended Group",
                Some(r#"{"memberCount":3,"isTerminated":true}"#),
                20,
            )
            .unwrap();
        store
            .upsert_contact(
                &account.id,
                "group",
                "YWxpdmU=",
                "Alive Group",
                Some(r#"{"memberCount":4}"#),
                20,
            )
            .unwrap();
        store
            .upsert_contact(&account.id, "contact", "+15555550101", "Alice", None, 20)
            .unwrap();

        // List face: exactly the flagged group marked, direct never.
        let listed = store.list_conversations(&account.id, 10, None).unwrap();
        let ended_row = listed.items.iter().find(|row| row.id == ended.id).unwrap();
        let alive_row = listed.items.iter().find(|row| row.id == alive.id).unwrap();
        let direct_row = listed.items.iter().find(|row| row.id == direct.id).unwrap();
        assert!(ended_row.is_terminated);
        assert!(!alive_row.is_terminated);
        assert!(!direct_row.is_terminated);

        // Single-summary face (the conversation.changed payload).
        assert!(
            store
                .conversation_summary(&account.id, &ended.id)
                .unwrap()
                .unwrap()
                .is_terminated
        );
        assert!(
            !store
                .conversation_summary(&account.id, &alive.id)
                .unwrap()
                .unwrap()
                .is_terminated
        );
        assert!(
            !store
                .conversation_summary(&account.id, &direct.id)
                .unwrap()
                .unwrap()
                .is_terminated
        );

        // Contacts-row face: the group carries its own marker, the direct
        // row never marks.
        let contacts = store.list_contacts(&account.id, None, 10, None).unwrap();
        let ended_contact = contacts
            .items
            .iter()
            .find(|row| row.kind == "group" && row.peer_key == "ZW5kZWQ=")
            .unwrap();
        let alive_contact = contacts
            .items
            .iter()
            .find(|row| row.kind == "group" && row.peer_key == "YWxpdmU=")
            .unwrap();
        assert!(ended_contact.is_terminated);
        assert!(!alive_contact.is_terminated);
        assert!(
            contacts
                .items
                .iter()
                .filter(|row| row.kind == "contact")
                .all(|row| !row.is_terminated)
        );

        // Wire shape: true serializes, false collapses to the absent key —
        // pre-1.60 hosts read byte-identical rows.
        use serde_json::json;
        let wire = serde_json::to_value(&*listed.items).unwrap();
        let rows = wire.as_array().unwrap();
        for row in rows {
            let flag = row.get("isTerminated");
            let expected = row["id"] == json!(ended.id);
            assert_eq!(flag.is_some(), expected, "row {}", row["id"]);
            if expected {
                assert_eq!(flag, Some(&json!(true)));
            }
        }
        let contact_wire = serde_json::to_value(&*contacts.items).unwrap();
        for row in contact_wire.as_array().unwrap() {
            let expected = row["peerKey"] == json!("ZW5kZWQ=");
            assert_eq!(row.get("isTerminated").is_some(), expected);
        }

        // A group with no contacts-cache row at all (never synced) stays
        // false on both faces.
        let orphan = store
            .ensure_conversation(&account.id, "group", "b3JwaGFu", "Orphan")
            .unwrap();
        assert!(
            !store
                .conversation_summary(&account.id, &orphan.id)
                .unwrap()
                .unwrap()
                .is_terminated
        );
        let orphan_listed = store.list_conversations(&account.id, 10, None).unwrap();
        assert!(
            !orphan_listed
                .items
                .iter()
                .find(|row| row.id == orphan.id)
                .unwrap()
                .is_terminated
        );
    }

    fn test_store_key() -> StoreKey {
        StoreKey::from_bytes(TEST_KEY_BYTES)
    }

    /// A second connection to a test store's file must present the same key,
    /// exactly like production's keyed open.
    fn keyed_observer(store: &Store) -> Connection {
        let conn = Connection::open(store.path()).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA key = \"x'{}'\"",
            hex::encode(TEST_KEY_BYTES)
        ))
        .unwrap();
        conn
    }

    #[test]
    fn account_message_idempotency_and_pagination() {
        let temp = TempDir::new().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let message = MessageRecord {
            id: "m1".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "outgoing",
            sender_id: "self".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 10,
            received_at: None,
            text: Some("hello".into()),
            text_bytes: Some(5),
            text_truncated: false,
            text_retrievable: true,

            status: "sent",
            client_request_id: Some("client-1".into()),
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        assert!(
            store
                .insert_message(&message, Some("client-1"), Some("hello"), false)
                .unwrap()
        );
        assert!(
            !store
                .insert_message(&message, Some("client-1"), Some("hello"), false)
                .unwrap()
        );
        let again = store
            .message_by_client_request(&account.id, "client-1")
            .unwrap()
            .unwrap();
        assert_eq!(again.id, "m1");
        assert_eq!(again.client_request_id.as_deref(), Some("client-1"));
        let page = store
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap();
        assert_eq!(page.items.len(), 1);
    }

    /// Contract 1.25: a rich-body payload persists in the rich_json column,
    /// round-trips through reads, and flattens onto the wire row as top-level
    /// previews/mentions/textStyles/viewOnce; a plain row stays key-absent.
    #[test]
    fn rich_payload_round_trips_and_flattens_on_the_wire() {
        use crate::engine::{NormalizedMention, NormalizedPreview, NormalizedRich};
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let base = |id: &str| MessageRecord {
            id: id.into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 10,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };

        let mut rich = base("rich-1");
        rich.rich = Some(NormalizedRich {
            previews: vec![NormalizedPreview {
                url: "https://example.com/a".into(),
                title: Some("Title".into()),
                description: None,
                image: None,
            }],
            mentions: vec![NormalizedMention {
                author: "+15555550101".into(),
                author_aci: None,
                name: Some("Peer".into()),
                start: 0,
                length: 4,
            }],
            text_styles: vec![crate::engine::NormalizedTextStyle {
                style: "BOLD".into(),
                start: 0,
                length: 4,
            }],
            view_once: true,
            ..crate::engine::NormalizedRich::default()
        });
        assert!(
            store
                .insert_message(&rich, None, Some("body"), true)
                .unwrap()
        );
        let read = store
            .message_by_id(&account.id, &conversation.id, "rich-1")
            .unwrap();
        let read = read.unwrap();
        let stored = read.rich.as_ref().expect("rich round-trips");
        assert_eq!(stored.previews[0].url, "https://example.com/a");
        assert_eq!(stored.mentions[0].author, "+15555550101");
        assert_eq!(stored.text_styles[0].style, "BOLD");
        assert!(stored.view_once);
        let wire = serde_json::to_value(&read).unwrap();
        assert_eq!(wire["previews"][0]["url"], "https://example.com/a");
        assert_eq!(wire["mentions"][0]["author"], "+15555550101");
        assert_eq!(wire["textStyles"][0]["style"], "BOLD");
        assert_eq!(wire["viewOnce"], true);

        // Contract 1.40 (§4.40): an authorAci on the mention round-trips and
        // lands on the wire row beside the unchanged author; a mention
        // without one stays byte-identical to the pre-1.40 wire shape.
        let mut aci = base("rich-aci");
        aci.rich = Some(NormalizedRich {
            mentions: vec![NormalizedMention {
                author: "+15555550101".into(),
                author_aci: Some("0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10".into()),
                name: None,
                start: 0,
                length: 4,
            }],
            ..crate::engine::NormalizedRich::default()
        });
        assert!(
            store
                .insert_message(&aci, None, Some("body"), true)
                .unwrap()
        );
        let read = store
            .message_by_id(&account.id, &conversation.id, "rich-aci")
            .unwrap()
            .unwrap();
        let wire = serde_json::to_value(&read).unwrap();
        assert_eq!(wire["mentions"][0]["author"], "+15555550101");
        assert_eq!(
            wire["mentions"][0]["authorAci"],
            "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10"
        );
        let bare = serde_json::to_value(&read.rich.as_ref().unwrap().mentions[0]).unwrap();
        assert_eq!(bare["author"], "+15555550101");
        assert_eq!(bare["authorAci"], "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10");

        // A pre-1.40 rich_json (no authorAci key) deserializes with the field
        // absent and projects nothing — old rows need no rewrite.
        let legacy: crate::engine::NormalizedRich = serde_json::from_str(
            r#"{"mentions":[{"author":"+15555550101","start":0,"length":4}]}"#,
        )
        .unwrap();
        assert_eq!(legacy.mentions[0].author, "+15555550101");
        assert_eq!(legacy.mentions[0].author_aci, None);
        let legacy_wire = serde_json::to_value(&legacy.mentions[0]).unwrap();
        assert!(legacy_wire.get("authorAci").is_none());

        assert!(
            store
                .insert_message(&base("plain-1"), None, Some("body"), true)
                .unwrap()
        );
        let plain = store
            .message_by_id(&account.id, &conversation.id, "plain-1")
            .unwrap()
            .unwrap();
        assert!(plain.rich.is_none());
        let plain_wire = serde_json::to_value(&plain).unwrap();
        assert!(plain_wire.get("previews").is_none());
        assert!(plain_wire.get("viewOnce").is_none());
    }

    /// Contract 1.93 (§4.76): the adjacent visual-media probe walks the
    /// keyset in the probe direction, adjacency-first with the anchor itself
    /// never returned; only rows with an image/* or video/* attachment match
    /// (audio/file/unclassified honestly skip), view-once rows never join,
    /// and a media message in another conversation stays invisible.
    #[test]
    fn list_adjacent_media_walks_both_directions_with_the_strip_predicate() {
        use crate::engine::{NormalizedAttachment, NormalizedRich};
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let other = store
            .ensure_conversation(&account.id, "direct", "+15555550102", "other")
            .unwrap();
        let attachment = |id: &str, content_type: Option<&str>| NormalizedAttachment {
            id: id.into(),
            content_type: content_type.map(str::to_string),
            filename: None,
            size: Some(1024),
            width: None,
            height: None,
            is_voice_note: false,
            locator: None,
        };
        let row = |id: &str,
                   sent_at: u64,
                   attachments: Vec<NormalizedAttachment>,
                   rich: Option<NormalizedRich>|
         -> MessageRecord {
            MessageRecord {
                id: id.into(),
                account_id: account.id.clone(),
                conversation_id: conversation.id.clone(),
                direction: "incoming",
                sender_id: "peer".into(),
                sender_name: None,
                mentions_self: false,
                sent_at,
                received_at: None,
                text: None,
                text_bytes: None,
                text_truncated: false,
                text_retrievable: false,
                status: "delivered",
                client_request_id: None,
                quote_message_id: None,
                quote_snapshot: None,
                attachments,
                rich,
                edited_at: None,
                reactions: Vec::new(),
                edits: Vec::new(),
                delivered_at: None,
                read_at: None,
                viewed_at: None,
                admin_deleted: false,
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                server_guid: None,
            }
        };
        let insert = |message: &MessageRecord| {
            store.insert_message(message, None, None, false).unwrap();
        };
        // Timeline (sent_at order): 10 image, 20 text-only, 30 audio,
        // 40 video, 50 anchor text, 60 pdf, 70 view-once image, 80 image.
        insert(&row(
            "m-image-10",
            10,
            vec![attachment("a10", Some("image/jpeg"))],
            None,
        ));
        insert(&row("m-text-20", 20, vec![], None));
        insert(&row(
            "m-audio-30",
            30,
            vec![attachment("a30", Some("audio/aac"))],
            None,
        ));
        insert(&row(
            "m-video-40",
            40,
            vec![attachment("a40", Some("video/mp4"))],
            None,
        ));
        insert(&row("m-anchor-50", 50, vec![], None));
        insert(&row(
            "m-pdf-60",
            60,
            vec![attachment("a60", Some("application/pdf"))],
            None,
        ));
        insert(&row(
            "m-viewonce-70",
            70,
            vec![attachment("a70", Some("image/png"))],
            Some(NormalizedRich {
                view_once: true,
                ..NormalizedRich::default()
            }),
        ));
        insert(&row(
            "m-image-80",
            80,
            vec![attachment("a80", Some("image/png"))],
            None,
        ));
        // An unclassifiable attachment (no content type) never matches.
        insert(&row(
            "m-mystery-90",
            90,
            vec![attachment("a90", None)],
            None,
        ));
        // Media in another conversation is invisible to the probe.
        let mut cross = row(
            "m-cross-15",
            15,
            vec![attachment("a15", Some("image/gif"))],
            None,
        );
        cross.conversation_id = other.id.clone();
        insert(&cross);

        let older = store
            .list_adjacent_media(
                &account.id,
                &conversation.id,
                "m-anchor-50",
                AdjacentMediaDirection::Older,
                20,
            )
            .unwrap();
        assert_eq!(
            older.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            vec!["m-video-40", "m-image-10"],
            "adjacency-first, non-media rows and the cross-conversation row skipped"
        );
        let newer = store
            .list_adjacent_media(
                &account.id,
                &conversation.id,
                "m-anchor-50",
                AdjacentMediaDirection::Newer,
                20,
            )
            .unwrap();
        assert_eq!(
            newer.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            vec!["m-image-80"],
            "pdf, view-once, and unclassified attachments never join the strip"
        );
        // The anchor itself is never returned even when it carries media
        // (and the view-once row between is skipped, not matched).
        let anchored = store
            .list_adjacent_media(
                &account.id,
                &conversation.id,
                "m-image-80",
                AdjacentMediaDirection::Older,
                1,
            )
            .unwrap();
        assert_eq!(anchored.len(), 1);
        assert_eq!(anchored[0].id, "m-video-40");
        // limit truncates after full matches, not scanned rows.
        let limited = store
            .list_adjacent_media(
                &account.id,
                &conversation.id,
                "m-anchor-50",
                AdjacentMediaDirection::Older,
                1,
            )
            .unwrap();
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].id, "m-video-40");
        // Ranges without media answer an empty vec, never an error.
        let empty = store
            .list_adjacent_media(
                &account.id,
                &conversation.id,
                "m-image-10",
                AdjacentMediaDirection::Older,
                20,
            )
            .unwrap();
        assert!(empty.is_empty());
        // A vanished anchor is a deterministic InvalidCursor, never a silent
        // re-anchor.
        let missing = store.list_adjacent_media(
            &account.id,
            &conversation.id,
            "m-gone",
            AdjacentMediaDirection::Older,
            20,
        );
        assert!(matches!(missing, Err(StoreError::InvalidCursor)));
    }

    /// Contract 1.93 (§4.76): rows sharing the anchor's sent_at order by id
    /// (the keyset tiebreak), and the scan crosses chunk boundaries — a
    /// media-bearing row further than one bounded chunk of non-media rows is
    /// still found without loading the whole range at once.
    #[test]
    fn list_adjacent_media_tiebreaks_by_id_and_crosses_scan_chunks() {
        use crate::engine::NormalizedAttachment;
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let attachment = |id: &str| NormalizedAttachment {
            id: id.into(),
            content_type: Some("image/jpeg".to_string()),
            filename: None,
            size: Some(1024),
            width: None,
            height: None,
            is_voice_note: false,
            locator: None,
        };
        let row = |id: &str, sent_at: u64, media: bool| MessageRecord {
            id: id.into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: None,
            text: None,
            text_bytes: None,
            text_truncated: false,
            text_retrievable: false,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: if media {
                vec![attachment(id)]
            } else {
                Vec::new()
            },
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            admin_deleted: false,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
        };
        // Same sent_at as the anchor: id below the anchor is older, id above
        // is newer — the tiebreak matches list_messages' keyset order.
        store
            .insert_message(&row("m-tie-a", 50, true), None, None, false)
            .unwrap();
        store
            .insert_message(&row("m-tie-z", 50, true), None, None, false)
            .unwrap();
        store
            .insert_message(&row("m-tie-m", 50, false), None, None, false)
            .unwrap();
        let older = store
            .list_adjacent_media(
                &account.id,
                &conversation.id,
                "m-tie-m",
                AdjacentMediaDirection::Older,
                20,
            )
            .unwrap();
        assert_eq!(
            older.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            vec!["m-tie-a"]
        );
        let newer = store
            .list_adjacent_media(
                &account.id,
                &conversation.id,
                "m-tie-m",
                AdjacentMediaDirection::Newer,
                20,
            )
            .unwrap();
        assert_eq!(
            newer.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            vec!["m-tie-z"]
        );

        // More than one bounded scan chunk (128) of non-media rows between
        // the anchor and the next media row: the scan crosses chunks.
        for index in 0..(ADJACENT_MEDIA_SCAN_CHUNK as u64 + 10) {
            let id = format!("m-fill-{index:04}");
            store
                .insert_message(&row(&id, 100 + index, false), None, None, false)
                .unwrap();
        }
        store
            .insert_message(&row("m-far-media", 1_000, true), None, None, false)
            .unwrap();
        let found = store
            .list_adjacent_media(
                &account.id,
                &conversation.id,
                "m-tie-z",
                AdjacentMediaDirection::Newer,
                20,
            )
            .unwrap();
        assert_eq!(
            found.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            vec!["m-far-media"]
        );
    }

    /// Contract 1.34: the receipt selection queries — absent-`messageIds`
    /// mode orders incoming rows by `sent_at ASC, id ASC` under the caller's
    /// limit and skips outgoing rows; explicit-`messageIds` mode additionally
    /// drops unknown ids and collapses duplicates; `contact_peer_keys` only
    /// surfaces kind='contact' rows for mention-number resolution.
    #[test]
    fn receipt_selection_queries_order_filter_and_bound() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let base = |id: &str, sent_at: u64, direction: &'static str| MessageRecord {
            id: id.into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction,
            sender_id: if direction == "incoming" {
                "peer".into()
            } else {
                "self".into()
            },
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: if direction == "incoming" {
                "delivered"
            } else {
                "sent"
            },
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        for (id, sent_at, direction) in [
            ("m3", 30, "incoming"),
            ("out-1", 40, "outgoing"),
            ("m1", 10, "incoming"),
            ("m2", 20, "incoming"),
        ] {
            assert!(
                store
                    .insert_message(&base(id, sent_at, direction), None, Some("body"), true)
                    .unwrap()
            );
        }

        let bounded = store
            .incoming_rows_for_receipts(&account.id, &conversation.id, 2)
            .unwrap();
        assert_eq!(
            bounded
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["m1", "m2"]
        );

        let picked = store
            .incoming_rows_by_ids(
                &account.id,
                &conversation.id,
                &[
                    "m3".into(),
                    "m1".into(),
                    "out-1".into(),
                    "m3".into(),
                    "missing".into(),
                ],
            )
            .unwrap();
        assert_eq!(
            picked.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["m1", "m3"]
        );
        assert_eq!(picked[0].sent_at, 10);
        assert_eq!(picked[1].sent_at, 30);

        store
            .upsert_synced_contacts(
                &account.id,
                &[
                    SyncedContact {
                        kind: "contact",
                        peer_key: "+15555550101",
                        title: "Peer",
                        extra: None,
                    },
                    SyncedContact {
                        kind: "group",
                        peer_key: "ZmFrZS1ncm91cC0x",
                        title: "Group",
                        extra: None,
                    },
                ],
                1,
            )
            .unwrap();
        assert_eq!(
            store.contact_peer_keys(&account.id).unwrap(),
            ["+15555550101"]
        );
    }

    /// Contract 1.25 schema step: a v8 database migrates in place — the
    /// additive rich_json column appears and the version stamp moves to 9.
    #[test]
    fn schema_v9_upgrade_adds_rich_json() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '8');
             CREATE TABLE accounts (
               id TEXT PRIMARY KEY,
               signal_account TEXT NOT NULL UNIQUE,
               masked_address TEXT NOT NULL,
               display_name TEXT,
               state TEXT NOT NULL,
               linked_at INTEGER,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0
             );",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "messages", "rich_json").unwrap());
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    /// Contract 1.27 schema step: a v9 database migrates in place — both
    /// additive author-name columns appear (messages.sender_name,
    /// message_events.actor_name) and the version stamp moves to 10.
    #[test]
    fn schema_v10_upgrade_adds_author_name_columns() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '9');
             CREATE TABLE accounts (
               id TEXT PRIMARY KEY,
               signal_account TEXT NOT NULL UNIQUE,
               masked_address TEXT NOT NULL,
               display_name TEXT,
               state TEXT NOT NULL,
               linked_at INTEGER,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE messages (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               sender_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL,
               received_at INTEGER,
               stored_at INTEGER,
               body TEXT,
               body_bytes INTEGER,
               body_truncated INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               client_request_id TEXT,
               quote_message_id TEXT,
               quote_snapshot TEXT,
               attachments_json TEXT,
               rich_json TEXT,
               edited_at INTEGER
             );
             CREATE TABLE message_events (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               kind TEXT NOT NULL,
               emoji TEXT NOT NULL,
               target_timestamp INTEGER NOT NULL,
               actor_id TEXT NOT NULL,
               removed INTEGER NOT NULL DEFAULT 0,
               updated_at INTEGER NOT NULL
             );",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "messages", "sender_name").unwrap());
        assert!(table_has_column(&store.conn(), "message_events", "actor_name").unwrap());
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn schema_v12_upgrade_adds_receipt_timestamp_columns() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '11');
             CREATE TABLE accounts (
               id TEXT PRIMARY KEY,
               signal_account TEXT NOT NULL UNIQUE,
               masked_address TEXT NOT NULL,
               display_name TEXT,
               state TEXT NOT NULL,
               linked_at INTEGER,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE messages (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               sender_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL,
               received_at INTEGER,
               stored_at INTEGER,
               body TEXT,
               body_bytes INTEGER,
               body_truncated INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               client_request_id TEXT,
               quote_message_id TEXT,
               quote_snapshot TEXT,
               attachments_json TEXT,
               rich_json TEXT,
               edited_at INTEGER,
               sender_name TEXT,
               mentions_self INTEGER NOT NULL DEFAULT 0
             );",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "messages", "delivered_at").unwrap());
        assert!(table_has_column(&store.conn(), "messages", "read_at").unwrap());
        let edits_table: i64 = store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='message_edits'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            edits_table, 1,
            "the edit-history table exists after migration"
        );
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    /// Contract 1.33 schema step: a v12 database migrates in place — the
    /// additive `admin_deleted` tombstone column and the `conversation_pins`
    /// table appear and the version stamp moves to 13.
    #[test]
    fn schema_v13_upgrade_adds_admin_deleted_and_conversation_pins() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '12');
             CREATE TABLE accounts (
               id TEXT PRIMARY KEY,
               signal_account TEXT NOT NULL UNIQUE,
               masked_address TEXT NOT NULL,
               display_name TEXT,
               state TEXT NOT NULL,
               linked_at INTEGER,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE messages (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               sender_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL,
               received_at INTEGER,
               stored_at INTEGER,
               body TEXT,
               body_bytes INTEGER,
               body_truncated INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               client_request_id TEXT,
               quote_message_id TEXT,
               quote_snapshot TEXT,
               attachments_json TEXT,
               rich_json TEXT,
               edited_at INTEGER,
               sender_name TEXT,
               mentions_self INTEGER NOT NULL DEFAULT 0,
               delivered_at INTEGER,
               read_at INTEGER
             );",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "messages", "admin_deleted").unwrap());
        let pins_table: i64 = store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='conversation_pins'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            pins_table, 1,
            "the per-conversation pin table exists after migration"
        );
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    /// Contract revision 1.35 receive projection: opening a v13 store adds the
    /// additive `sticker_json` column (schema 13 → 14) and stamps the new
    /// version; history rows stay null and are never backfilled.
    #[test]
    fn schema_v14_upgrade_adds_sticker_json_column() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '13');
             CREATE TABLE accounts (
               id TEXT PRIMARY KEY,
               signal_account TEXT NOT NULL UNIQUE,
               masked_address TEXT NOT NULL,
               display_name TEXT,
               state TEXT NOT NULL,
               linked_at INTEGER,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE messages (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               sender_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL,
               received_at INTEGER,
               stored_at INTEGER,
               body TEXT,
               body_bytes INTEGER,
               body_truncated INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               client_request_id TEXT,
               quote_message_id TEXT,
               quote_snapshot TEXT,
               attachments_json TEXT,
               rich_json TEXT,
               sticker_json TEXT,
               edited_at INTEGER,
               sender_name TEXT,
               mentions_self INTEGER NOT NULL DEFAULT 0,
               delivered_at INTEGER,
               read_at INTEGER,
               admin_deleted INTEGER NOT NULL DEFAULT 0
             );",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "messages", "sticker_json").unwrap());
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    /// Sticker persistence (contract revision 1.35, §4.31): the pack identity
    /// round-trips through `sticker_json` beside the metadata-only descriptor,
    /// and `MessageRecord` keeps absent sticker keys off the wire so pre-1.35
    /// rows stay byte-identical.
    #[test]
    fn sticker_row_round_trips_and_plain_rows_stay_key_absent() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let base = |id: &str, sent_at: u64| MessageRecord {
            id: id.into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "outgoing",
            sender_id: account.id.clone(),
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: None,
            text: None,
            text_bytes: None,
            text_truncated: false,
            text_retrievable: false,
            status: "sent",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            admin_deleted: false,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
        };

        let mut sticker_row = base("sticker-row", 60);
        sticker_row.attachments = vec![crate::engine::NormalizedAttachment {
            id: "att-sticker".into(),
            content_type: Some("image/webp".into()),
            filename: None,
            size: Some(8192),
            width: Some(512),
            height: Some(512),
            is_voice_note: false,
            locator: None,
        }];
        sticker_row.sticker = Some(crate::engine::NormalizedSticker {
            pack_id: "abcdef01".into(),
            pack_key: "AAAAAAAAAAAAAAAAAAAAAA==".into(),
            sticker_id: 7,
            emoji: Some("🎉".into()),
        });
        store
            .insert_message(&sticker_row, Some("req-sticker-store"), None, false)
            .unwrap();
        let row = store
            .message_by_client_request(&account.id, "req-sticker-store")
            .unwrap()
            .expect("the sticker row exists");
        let sticker = row
            .sticker
            .clone()
            .expect("the pack identity survives the store");
        assert_eq!(sticker.pack_id, "abcdef01");
        assert_eq!(sticker.pack_key, "AAAAAAAAAAAAAAAAAAAAAA==");
        assert_eq!(sticker.sticker_id, 7);
        assert_eq!(sticker.emoji.as_deref(), Some("🎉"));
        assert_eq!(row.attachments.len(), 1);
        assert_eq!(row.attachments[0].id, "att-sticker");
        assert_eq!(row.attachments[0].width, Some(512));
        let wire = serde_json::to_value(&row).unwrap();
        assert_eq!(wire["sticker"]["packId"], "abcdef01");
        assert_eq!(wire["sticker"]["stickerId"], 7);
        assert_eq!(wire["attachments"][0]["contentType"], "image/webp");
        assert!(wire["attachments"][0].get("filename").is_none());

        // A plain row keeps the key absent — pre-1.35 rows stay byte-identical
        // on the wire.
        store
            .insert_message(&base("plain-row", 61), None, None, false)
            .unwrap();
        let rows = store
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        let plain_row = rows
            .iter()
            .find(|row| row.id == "plain-row")
            .expect("the plain row exists");
        let wire = serde_json::to_value(plain_row).unwrap();
        assert!(wire.get("sticker").is_none(), "{wire}");
    }

    /// Conversation pin state (contract 1.33): a newer pin replaces the older
    /// one, a mismatched clear leaves the pin alone, a matching clear removes
    /// it, the summary projects the camelCase `pinnedMessage` with `expiresAt`
    /// only on a live timed pin — an expired timed pin (`duration 0` = already
    /// past) and a pin whose target row is gone stop rendering.
    #[test]
    fn conversation_pin_upsert_replaces_and_clears_with_expiry_projection() {
        use serde_json::json;
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let base = |id: &str, sent_at: u64| MessageRecord {
            id: id.into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        assert!(
            store
                .insert_message(&base("m1", 10), None, Some("body"), true)
                .unwrap()
        );
        assert!(
            store
                .insert_message(&base("m2", 20), None, Some("body"), true)
                .unwrap()
        );

        // Forever pin on the first row: projected with the resolved message
        // id and no expiresAt (the wire key stays absent).
        store
            .upsert_conversation_pin(&account.id, &conversation.id, "+15555550101", 10, None)
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        let pinned = summary.pinned_message.as_ref().unwrap();
        assert_eq!(pinned.message_id, "m1");
        assert_eq!(pinned.target_author, "+15555550101");
        assert_eq!(pinned.target_sent_timestamp, 10);
        assert!(pinned.pinned_at > 0);
        assert_eq!(pinned.expires_at, None);
        let wire = serde_json::to_value(&summary).unwrap();
        assert_eq!(wire["pinnedMessage"]["messageId"], json!("m1"));
        assert_eq!(wire["pinnedMessage"]["targetSentTimestamp"], json!(10));
        assert!(wire["pinnedMessage"].get("expiresAt").is_none());

        // A newer pin replaces the older one in place (one row per
        // conversation, new pin wins).
        store
            .upsert_conversation_pin(
                &account.id,
                &conversation.id,
                "+15555550101",
                20,
                Some(3600),
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        let pinned = summary.pinned_message.as_ref().unwrap();
        assert_eq!(pinned.message_id, "m2");
        assert!(pinned.expires_at.is_some(), "a timed pin carries expiry");
        let wire = serde_json::to_value(&summary).unwrap();
        assert!(wire["pinnedMessage"]["expiresAt"].is_u64());

        // A clear naming the replaced (author, timestamp) is a no-op.
        assert!(
            !store
                .clear_conversation_pin(&account.id, &conversation.id, "+15555550101", 10)
                .unwrap()
        );
        assert_eq!(
            store
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .pinned_message
                .as_ref()
                .unwrap()
                .target_sent_timestamp,
            20
        );

        // The matching clear removes the state.
        assert!(
            store
                .clear_conversation_pin(&account.id, &conversation.id, "+15555550101", 20)
                .unwrap()
        );
        assert!(
            store
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .pinned_message
                .is_none()
        );

        // An already-expired timed pin (duration 0) and a pin whose target
        // row is no longer in the history stop rendering.
        store
            .upsert_conversation_pin(&account.id, &conversation.id, "+15555550101", 10, Some(0))
            .unwrap();
        assert!(
            store
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .pinned_message
                .is_none(),
            "an expired pin must not render"
        );
        store
            .upsert_conversation_pin(&account.id, &conversation.id, "+15555550101", 999, None)
            .unwrap();
        assert!(
            store
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .pinned_message
                .is_none(),
            "a pin without a local row must not render"
        );
    }

    /// The admin-delete tombstone (contract 1.33): the flag flips exactly
    /// once per row (idempotent replays answer `false` and re-emit nothing),
    /// the status ladder and body are untouched, and system rows are exempt —
    /// a state change notice is not an admin-removable message.
    #[test]
    fn mark_admin_deleted_flips_once_and_skips_system_rows() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "group", "Z3JvdXAtaWQ=", "group")
            .unwrap();
        let base = |id: &str, sent_at: u64, direction: &'static str| MessageRecord {
            id: id.into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction,
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: if direction == "system" {
                "system"
            } else {
                "delivered"
            },
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        assert!(
            store
                .insert_message(&base("m1", 30, "incoming"), None, Some("body"), true)
                .unwrap()
        );
        assert!(
            store
                .insert_message(
                    &base("m2", 31, "system"),
                    None,
                    Some("left the group"),
                    true
                )
                .unwrap()
        );

        let (record, changed) = store
            .mark_admin_deleted(&account.id, &conversation.id, 30)
            .unwrap()
            .unwrap();
        assert!(changed, "the first flip must report changed");
        assert!(record.admin_deleted);
        assert_eq!(record.status, "delivered", "the status ladder is untouched");
        assert_eq!(record.text.as_deref(), Some("body"));

        let (record, changed) = store
            .mark_admin_deleted(&account.id, &conversation.id, 30)
            .unwrap()
            .unwrap();
        assert!(!changed, "a replayed admin delete must not re-emit");
        assert!(record.admin_deleted);

        // A system row resolves but never flips.
        let (record, changed) = store
            .mark_admin_deleted(&account.id, &conversation.id, 31)
            .unwrap()
            .unwrap();
        assert!(!changed, "system rows are exempt");
        assert!(!record.admin_deleted);

        // An absent timestamp answers None.
        assert!(
            store
                .mark_admin_deleted(&account.id, &conversation.id, 999)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn summary_last_message_kind_follows_official_noun_rules() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let mut clock = 0u64;
        let mut push =
            |direction: &'static str,
             text: Option<&str>,
             attachments: Vec<crate::engine::NormalizedAttachment>| {
                clock += 1;
                let message = MessageRecord {
                    id: format!("m{clock}"),
                    account_id: account.id.clone(),
                    conversation_id: conversation.id.clone(),
                    direction,
                    sender_id: "peer".into(),
                    sender_name: None,
                    mentions_self: false,
                    sent_at: clock * 10,
                    received_at: None,
                    text: text.map(Into::into),
                    text_bytes: None,
                    text_truncated: false,
                    text_retrievable: true,
                    status: if direction == "system" {
                        "system"
                    } else {
                        "sent"
                    },
                    client_request_id: None,
                    quote_message_id: None,
                    quote_snapshot: None,
                    attachments,
                    edited_at: None,
                    rich: None,
                    reactions: Vec::new(),
                    edits: Vec::new(),
                    delivered_at: None,
                    read_at: None,
                    viewed_at: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    admin_deleted: false,
                    server_guid: None,
                };
                store.insert_message(&message, None, text, false).unwrap();
            };
        let attachment =
            |content_type: &str, is_voice_note: bool| crate::engine::NormalizedAttachment {
                id: "a1".into(),
                content_type: Some(content_type.into()),
                filename: Some("file.bin".into()),
                size: Some(8),
                width: None,
                height: None,
                is_voice_note,
                locator: None,
            };
        let kind = || {
            store
                .conversation_summary(&conversation.account_id, &conversation.id)
                .unwrap()
                .unwrap()
                .last_message_kind
        };

        // Empty conversation: no messages, no kind.
        assert_eq!(kind(), None);
        // Text ending: None — the preview text is authoritative.
        push("incoming", Some("hello"), Vec::new());
        assert_eq!(kind(), None);
        // Image-only ending: the official photo noun.
        push("incoming", None, vec![attachment("image/jpeg", false)]);
        assert_eq!(kind(), Some("image"));
        // A caption wins over the noun, exactly like the official preview.
        push(
            "incoming",
            Some("看这张图"),
            vec![attachment("image/jpeg", false)],
        );
        assert_eq!(kind(), None);
        // Voice note: the voice-message noun.
        push("incoming", None, vec![attachment("audio/aac", true)]);
        assert_eq!(kind(), Some("audio"));
        // Plain audio without the voice-note flag stays the audio noun.
        push("incoming", None, vec![attachment("audio/mpeg", false)]);
        assert_eq!(kind(), Some("audio"));
        // Video ending.
        push("incoming", None, vec![attachment("video/mp4", false)]);
        assert_eq!(kind(), Some("video"));
        // Unrecognized content type: the generic file noun.
        push("incoming", None, vec![attachment("application/pdf", false)]);
        assert_eq!(kind(), Some("file"));
        // A system row never yields a noun.
        push("system", Some("identity changed"), Vec::new());
        assert_eq!(kind(), None);
        // A whitespace-only caption counts as caption-less.
        push(
            "incoming",
            Some("   "),
            vec![attachment("image/png", false)],
        );
        assert_eq!(kind(), Some("image"));

        // The list path carries the same derivation as the single read.
        let page = store.list_conversations(&account.id, 10, None).unwrap();
        let listed = page
            .items
            .iter()
            .find(|item| item.id == conversation.id)
            .unwrap();
        assert_eq!(listed.last_message_kind, Some("image"));
    }

    /// Contract 1.27: the summary's last-message fields — direction, incoming
    /// author name (captured from the envelope `sourceName`), and the newest
    /// row's active reaction emoji. Outgoing rows mark self via direction;
    /// system endings and empty conversations expose nothing; reactions key
    /// on the newest row's upstream timestamp and follow removal. Contract
    /// 1.28 adds the outgoing send state: present only on outgoing endings,
    /// always `None` for incoming/system endings and empty conversations.
    #[test]
    fn summary_carries_last_message_direction_author_and_reactions() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "group", "fake-group-1", "Group")
            .unwrap();
        let push = |id: &str,
                    direction: &'static str,
                    sender_name: Option<&str>,
                    sent_at: u64,
                    text: Option<&str>| {
            let message = MessageRecord {
                id: id.into(),
                account_id: account.id.clone(),
                conversation_id: conversation.id.clone(),
                direction,
                sender_id: if direction == "outgoing" {
                    account.id.clone()
                } else {
                    "peer-hash".into()
                },
                sender_name: sender_name.map(Into::into),
                mentions_self: false,
                sent_at,
                received_at: None,
                text: text.map(Into::into),
                text_bytes: None,
                text_truncated: false,
                text_retrievable: true,
                status: if direction == "system" {
                    "system"
                } else if direction == "outgoing" {
                    "sent"
                } else {
                    "delivered"
                },
                client_request_id: None,
                quote_message_id: None,
                quote_snapshot: None,
                attachments: Vec::new(),
                edited_at: None,
                rich: None,
                reactions: Vec::new(),
                edits: Vec::new(),
                delivered_at: None,
                read_at: None,
                viewed_at: None,
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                admin_deleted: false,
                server_guid: None,
            };
            store.insert_message(&message, None, text, false).unwrap();
        };
        let summary = || {
            store
                .conversation_summary(&conversation.account_id, &conversation.id)
                .unwrap()
                .unwrap()
        };

        // Empty conversation: no direction, no author, no reactions, no
        // send state.
        let current = summary();
        assert_eq!(current.last_message_direction, None);
        assert_eq!(current.last_message_status, None);
        assert_eq!(current.last_message_author_name, None);
        assert!(current.last_message_reactions.is_empty());

        // Incoming ending: direction incoming, captured author name, and no
        // send state — the official icon exists only on outgoing endings.
        push("m1", "incoming", Some("林菲菲"), 100, Some("你好"));
        let current = summary();
        assert_eq!(current.last_message_direction, Some("incoming"));
        assert_eq!(current.last_message_status, None);
        assert_eq!(current.last_message_author_name.as_deref(), Some("林菲菲"));
        assert!(current.last_message_reactions.is_empty());

        // Outgoing ending: the author is the account itself — no name is
        // surfaced, the client renders its own self label. The send state is
        // the row's own status.
        push("m2", "outgoing", None, 200, Some("好的"));
        let current = summary();
        assert_eq!(current.last_message_direction, Some("outgoing"));
        assert_eq!(current.last_message_status, Some("sent"));
        assert_eq!(current.last_message_author_name, None);

        // A peer receipt moves the row's status and the summary follows.
        store
            .update_message_status("m2", "delivered", None)
            .unwrap();
        assert_eq!(summary().last_message_status, Some("delivered"));
        store.update_message_status("m2", "read", None).unwrap();
        assert_eq!(summary().last_message_status, Some("read"));

        // Reactions key on the newest row only: they target m2's timestamp.
        store
            .upsert_reaction_event(
                &account.id,
                &conversation.id,
                "👍",
                200,
                "peer-hash-1",
                false,
                Some("林菲菲"),
            )
            .unwrap();
        // Reaction timestamps are the receive clock; a same-millisecond tie
        // orders by the random event id, so step the clock like
        // reaction_aggregates_attach_to_message_projections does.
        std::thread::sleep(std::time::Duration::from_millis(3));
        store
            .upsert_reaction_event(
                &account.id,
                &conversation.id,
                "❤️",
                200,
                "peer-hash-2",
                false,
                None,
            )
            .unwrap();
        let current = summary();
        assert_eq!(
            current.last_message_reactions,
            vec!["👍".to_string(), "❤️".to_string()]
        );
        // A reaction on the older row must not leak into the summary.
        store
            .upsert_reaction_event(
                &account.id,
                &conversation.id,
                "😂",
                100,
                "peer-hash-1",
                false,
                None,
            )
            .unwrap();
        let current = summary();
        assert_eq!(
            current.last_message_reactions,
            vec!["👍".to_string(), "❤️".to_string()]
        );

        // Removal drops the emoji from the summary.
        store
            .upsert_reaction_event(
                &account.id,
                &conversation.id,
                "👍",
                200,
                "peer-hash-1",
                true,
                None,
            )
            .unwrap();
        let current = summary();
        assert_eq!(current.last_message_reactions, vec!["❤️".to_string()]);

        // A system ending exposes neither direction nor author nor reactions
        // nor send state.
        push("m3", "system", None, 300, Some("identity changed"));
        let current = summary();
        assert_eq!(current.last_message_direction, None);
        assert_eq!(current.last_message_status, None);
        assert_eq!(current.last_message_author_name, None);
        assert!(current.last_message_reactions.is_empty());

        // The list path carries the same projection.
        let page = store.list_conversations(&account.id, 10, None).unwrap();
        let listed = page
            .items
            .iter()
            .find(|item| item.id == conversation.id)
            .unwrap();
        assert_eq!(listed.last_message_direction, None);
        assert!(listed.last_message_reactions.is_empty());
        // (back to an incoming ending for the list-path assertion)
        push("m4", "incoming", Some("阿强"), 400, Some("收到"));
        let page = store.list_conversations(&account.id, 10, None).unwrap();
        let listed = page
            .items
            .iter()
            .find(|item| item.id == conversation.id)
            .unwrap();
        assert_eq!(listed.last_message_direction, Some("incoming"));
        assert_eq!(listed.last_message_author_name.as_deref(), Some("阿强"));
    }

    /// Contract 1.28: the summary send state covers the whole outgoing ladder
    /// (pending → sent → delivered → read, plus failed) in the exact message
    /// row vocabulary, and stays absent on outgoing endings that carry no
    /// official icon (remote-deleted) — while the direction keeps marking the
    /// ending as self-authored.
    #[test]
    fn summary_last_message_status_covers_the_outgoing_ladder_only() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550111", "Peer")
            .unwrap();
        let push = |id: &str, status: &'static str, sent_at: u64| {
            let message = MessageRecord {
                id: id.into(),
                account_id: account.id.clone(),
                conversation_id: conversation.id.clone(),
                direction: "outgoing",
                sender_id: account.id.clone(),
                sender_name: None,
                mentions_self: false,
                sent_at,
                received_at: None,
                text: Some("hi".into()),
                text_bytes: None,
                text_truncated: false,
                text_retrievable: true,
                status,
                client_request_id: None,
                quote_message_id: None,
                quote_snapshot: None,
                attachments: Vec::new(),
                edited_at: None,
                rich: None,
                reactions: Vec::new(),
                edits: Vec::new(),
                delivered_at: None,
                read_at: None,
                viewed_at: None,
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                admin_deleted: false,
                server_guid: None,
            };
            store.insert_message(&message, None, None, false).unwrap();
        };
        let summary_status = || {
            store
                .conversation_summary(&conversation.account_id, &conversation.id)
                .unwrap()
                .unwrap()
                .last_message_status
        };

        // A queued outgoing row carries its pending state into the summary.
        push("m1", "pending", 100);
        assert_eq!(summary_status(), Some("pending"));

        // The failed tier surfaces like every other tier.
        push("m2", "pending", 200);
        store.update_message_status("m2", "failed", None).unwrap();
        assert_eq!(summary_status(), Some("failed"));

        // A newer outgoing row replaces the ending, then receipts walk the
        // ladder through the summary.
        push("m3", "sent", 300);
        assert_eq!(summary_status(), Some("sent"));
        store
            .update_message_status("m3", "delivered", None)
            .unwrap();
        assert_eq!(summary_status(), Some("delivered"));
        store.update_message_status("m3", "read", None).unwrap();
        assert_eq!(summary_status(), Some("read"));

        // A remote-deleted outgoing ending keeps the self-authored direction
        // but exposes no send state — the official icon set has no
        // representation for it.
        store
            .mark_remote_deleted(&account.id, &conversation.id, 300)
            .unwrap();
        let current = store
            .conversation_summary(&conversation.account_id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(current.last_message_direction, Some("outgoing"));
        assert_eq!(current.last_message_status, None);

        // The list path carries the same projection.
        let page = store.list_conversations(&account.id, 10, None).unwrap();
        let listed = page
            .items
            .iter()
            .find(|item| item.id == conversation.id)
            .unwrap();
        assert_eq!(listed.last_message_direction, Some("outgoing"));
        assert_eq!(listed.last_message_status, None);
    }

    /// Contract 1.29: the @ badge counts only incoming rows whose normalized
    /// mention authors resolve to the linked account's own number; opening
    /// the chat clears it together with the unread badge, and a remote
    /// delete leaves both counters untouched (the row stays unread).
    #[test]
    fn unread_mentions_counts_only_self_addressed_incoming() {
        use crate::engine::{NormalizedMention, NormalizedRich};
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "group", "grp-1", "Group")
            .unwrap();
        let push = |id: &str, authors: &[&str], increment_unread: bool| {
            let message = MessageRecord {
                id: id.into(),
                account_id: account.id.clone(),
                conversation_id: conversation.id.clone(),
                direction: "incoming",
                sender_id: "peer".into(),
                sender_name: None,
                mentions_self: authors.contains(&"+15555550100"),
                sent_at: id.len() as u64 * 10,
                received_at: None,
                text: Some("body".into()),
                text_bytes: Some(4),
                text_truncated: false,
                text_retrievable: true,
                status: "delivered",
                client_request_id: None,
                quote_message_id: None,
                quote_snapshot: None,
                attachments: Vec::new(),
                edited_at: None,
                rich: (!authors.is_empty()).then(|| NormalizedRich {
                    mentions: authors
                        .iter()
                        .map(|author| NormalizedMention {
                            author: (*author).into(),
                            author_aci: None,
                            name: None,
                            start: 0,
                            length: 4,
                        })
                        .collect(),
                    ..Default::default()
                }),
                reactions: Vec::new(),
                edits: Vec::new(),
                delivered_at: None,
                read_at: None,
                viewed_at: None,
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                admin_deleted: false,
                server_guid: None,
            };
            store
                .insert_message(&message, None, None, increment_unread)
                .unwrap();
        };
        let summary = || {
            store
                .conversation_summary(&conversation.account_id, &conversation.id)
                .unwrap()
                .unwrap()
        };

        // A mention of a peer never moves the badge, and the summary key
        // stays absent at zero.
        push("m1", &["+15555550101"], true);
        let current = summary();
        assert_eq!(current.unread_count, 1);
        assert_eq!(current.unread_mentions, 0);

        // A mention resolving to the linked account's number counts.
        push("m2", &["+15555550100"], true);
        let current = summary();
        assert_eq!(current.unread_count, 2);
        assert_eq!(current.unread_mentions, 1);

        // The row carries the flag on reads, so the desktop can highlight
        // the bubble without knowing the account number (it is masked on
        // the wire).
        let row = store
            .message_by_id(&account.id, &conversation.id, "m2")
            .unwrap()
            .unwrap();
        assert!(row.mentions_self);
        let plain = store
            .message_by_id(&account.id, &conversation.id, "m1")
            .unwrap()
            .unwrap();
        assert!(!plain.mentions_self);

        // A mention author carrying only a UUID cannot be attributed to
        // self: the pinned upstream jsonRpc surface exposes the account by
        // number only (`listAccounts` returns `{number}`), the recorded
        // boundary of this revision.
        push("m3", &["3f2504e0-4f89-11d3-9a0c-0305e82c3301"], true);
        assert_eq!(summary().unread_mentions, 1);

        // Opening the chat clears both badges together.
        store
            .clear_conversation_unread(&account.id, &conversation.id)
            .unwrap();
        let current = summary();
        assert_eq!(current.unread_count, 0);
        assert_eq!(current.unread_mentions, 0);

        // A remote-deleted row stays unread: neither counter moves, the
        // same boundary `unread_count` already has.
        push("m4", &["+15555550100"], true);
        store
            .mark_remote_deleted(&account.id, &conversation.id, 40)
            .unwrap();
        let current = summary();
        assert_eq!(current.unread_count, 1);
        assert_eq!(current.unread_mentions, 1);
    }

    /// Contract 1.29 byte-compat: `unreadMentions` is absent at zero so a
    /// pre-1.29 host reads byte-identical summaries, and present exactly
    /// when the count is positive; `mentionsSelf` on a row is absent when
    /// false.
    #[test]
    fn unread_mentions_wire_keys_collapse_to_absent_at_zero() {
        use crate::engine::{NormalizedMention, NormalizedRich};
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let plain = MessageRecord {
            id: "m1".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 10,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store.insert_message(&plain, None, None, true).unwrap();

        let row_json = serde_json::to_value(
            store
                .message_by_id(&account.id, &conversation.id, "m1")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(row_json.get("mentionsSelf").is_none());
        let summary_json = serde_json::to_value(
            store
                .conversation_summary(&conversation.account_id, &conversation.id)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(summary_json.get("unreadMentions").is_none());

        let mentioned = MessageRecord {
            id: "m2".into(),
            sent_at: 20,
            mentions_self: true,
            rich: Some(NormalizedRich {
                mentions: vec![NormalizedMention {
                    author: "+15555550100".into(),
                    author_aci: None,
                    name: None,
                    start: 0,
                    length: 4,
                }],
                ..Default::default()
            }),
            server_guid: None,
            ..plain.clone()
        };
        store.insert_message(&mentioned, None, None, true).unwrap();
        let summary_json = serde_json::to_value(
            store
                .conversation_summary(&conversation.account_id, &conversation.id)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(summary_json["unreadMentions"], 1);
        let row_json = serde_json::to_value(
            store
                .message_by_id(&account.id, &conversation.id, "m2")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(row_json["mentionsSelf"], true);
    }

    #[test]
    fn send_completion_removes_only_unclaimed_sync_duplicate() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let mut message = MessageRecord {
            id: "pending".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "outgoing",
            sender_id: account.id.clone(),
            sender_name: None,
            mentions_self: false,
            sent_at: 1,
            received_at: None,
            text: Some("same".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,

            status: "pending",
            client_request_id: Some("request-pending".into()),
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(
                &message,
                message.client_request_id.as_deref(),
                Some("same"),
                false,
            )
            .unwrap();

        message.id = "other-local-send".into();
        message.sent_at = 99;
        message.status = "sent";
        message.client_request_id = Some("request-other".into());
        store
            .insert_message(
                &message,
                message.client_request_id.as_deref(),
                Some("same"),
                false,
            )
            .unwrap();

        message.id = "sync-duplicate".into();
        message.client_request_id = None;
        store
            .insert_message(&message, None, Some("same"), false)
            .unwrap();

        let (completed, transitioned) = store
            .complete_outgoing_send("pending", &account.id, &conversation.id, 99)
            .unwrap()
            .unwrap();
        assert_eq!(completed.status, "sent");
        assert!(transitioned);
        // A replayed completion finds the row already sent and reports no
        // transition, so the service layer emits no duplicate status event.
        let (_, replayed_transition) = store
            .complete_outgoing_send("pending", &account.id, &conversation.id, 99)
            .unwrap()
            .unwrap();
        assert!(!replayed_transition);
        let ids = store
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert!(ids.contains(&"pending".to_string()));
        assert!(ids.contains(&"other-local-send".to_string()));
        assert!(!ids.contains(&"sync-duplicate".to_string()));
    }

    #[test]
    fn message_insert_rolls_back_when_summary_update_fails() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        store
            .conn()
            .execute_batch(
                "CREATE TRIGGER fail_conversation_summary
                 BEFORE UPDATE ON conversations
                 BEGIN SELECT RAISE(FAIL, 'controlled test failure'); END;",
            )
            .unwrap();
        let message = MessageRecord {
            id: "atomic-message".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 10,
            received_at: Some(10),
            text: Some("hello".into()),
            text_bytes: Some(5),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };

        assert!(matches!(
            store.insert_message(&message, None, Some("hello"), true),
            Err(StoreError::Unavailable(_)),
        ));
        assert!(
            store
                .message_by_id(&account.id, &conversation.id, &message.id)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .conversation_summary(&conversation.account_id, &conversation.id)
                .unwrap()
                .unwrap()
                .unread_count,
            0
        );
        assert_eq!(
            store
                .account_summary(&account.id)
                .unwrap()
                .unwrap()
                .unread_count,
            0
        );
    }

    #[test]
    fn unread_clear_rolls_back_when_account_update_fails() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let message = MessageRecord {
            id: "unread-message".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 10,
            received_at: Some(10),
            text: Some("hello".into()),
            text_bytes: Some(5),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&message, None, Some("hello"), true)
            .unwrap();
        store
            .conn()
            .execute_batch(
                "CREATE TRIGGER fail_account_unread
                 BEFORE UPDATE ON accounts
                 BEGIN SELECT RAISE(FAIL, 'controlled test failure'); END;",
            )
            .unwrap();

        assert!(matches!(
            store.clear_conversation_unread(&account.id, &conversation.id),
            Err(StoreError::Unavailable(_)),
        ));
        assert_eq!(
            store
                .conversation_summary(&conversation.account_id, &conversation.id)
                .unwrap()
                .unwrap()
                .unread_count,
            1
        );
        assert_eq!(
            store
                .account_summary(&account.id)
                .unwrap()
                .unwrap()
                .unread_count,
            1
        );
    }

    #[test]
    fn system_direction_and_status_round_trip() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let message = MessageRecord {
            id: "system-message".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "system",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 10,
            received_at: Some(10),
            text: Some("control notice".into()),
            text_bytes: Some(14),
            text_truncated: false,
            text_retrievable: true,

            status: "system",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&message, None, Some("control notice"), false)
            .unwrap();

        let reloaded = store
            .message_by_id(&account.id, &conversation.id, &message.id)
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.direction, "system");
        assert_eq!(reloaded.status, "system");
    }

    #[test]
    fn concurrent_status_updates_never_mismatch_record_and_transition() {
        // Regression (optimization-plan A6): update_message_status used to run
        // its UPDATE and its read-back SELECT in two separate lock rounds, so
        // a concurrent writer could flip the status in between and the call
        // would return the other writer's row together with changed=true — a
        // status event this call never set. The single transaction keeps the
        // pair atomic: the read-back must always report the status this call
        // wrote (a replayed write reports the same status with changed=false).
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let message = MessageRecord {
            id: "contended".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "outgoing",
            sender_id: account.id.clone(),
            sender_name: None,
            mentions_self: false,
            sent_at: 10,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: "pending",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&message, None, Some("body"), false)
            .unwrap();

        let store = std::sync::Arc::new(store);
        std::thread::scope(|scope| {
            for thread_index in 0..4_u32 {
                let store = std::sync::Arc::clone(&store);
                scope.spawn(move || {
                    for round in 0..750_u32 {
                        let status = if (thread_index + round) % 2 == 0 {
                            "sent"
                        } else {
                            "failed"
                        };
                        let (record, _changed) = store
                            .update_message_status("contended", status, None)
                            .unwrap()
                            .expect("contended row exists");
                        assert_eq!(
                            record.status, status,
                            "read-back must report the status this call wrote"
                        );
                    }
                });
            }
        });
    }

    #[test]
    fn conversation_cursor_follows_full_sort_key_without_skips() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let older = store
            .ensure_conversation(&account.id, "direct", "older", "Older")
            .unwrap();
        let newer = store
            .ensure_conversation(&account.id, "direct", "newer", "Newer")
            .unwrap();
        let empty = store
            .ensure_conversation(&account.id, "direct", "empty", "Empty")
            .unwrap();
        let same_time = store
            .ensure_conversation(&account.id, "direct", "same-time", "Same time")
            .unwrap();
        for (conversation, id, sent_at) in [
            (&older, "older-message", 10),
            (&newer, "newer-message", 20),
            (&same_time, "same-time-message", 20),
        ] {
            let message = MessageRecord {
                id: id.into(),
                account_id: account.id.clone(),
                conversation_id: conversation.id.clone(),
                direction: "incoming",
                sender_id: "peer".into(),
                sender_name: None,
                mentions_self: false,
                sent_at,
                received_at: Some(sent_at),
                text: Some(id.into()),
                text_bytes: Some(id.len() as u32),
                text_truncated: false,
                text_retrievable: true,
                status: "delivered",
                client_request_id: None,
                quote_message_id: None,
                quote_snapshot: None,
                attachments: Vec::new(),
                edited_at: None,
                rich: None,
                reactions: Vec::new(),
                edits: Vec::new(),
                delivered_at: None,
                read_at: None,
                viewed_at: None,
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                admin_deleted: false,
                server_guid: None,
            };
            store
                .insert_message(&message, None, Some(id), false)
                .unwrap();
        }

        let mut cursor = None;
        let mut ids = Vec::new();
        loop {
            let page = store
                .list_conversations(&account.id, 1, cursor.as_deref())
                .unwrap();
            ids.extend(page.items.into_iter().map(|item| item.id));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        let mut same_timestamp_ids = [newer.id.clone(), same_time.id.clone()];
        same_timestamp_ids.sort();
        same_timestamp_ids.reverse();
        assert_eq!(
            ids,
            [
                same_timestamp_ids[0].clone(),
                same_timestamp_ids[1].clone(),
                older.id.clone(),
                empty.id.clone(),
            ],
        );
        assert!(matches!(
            store.list_conversations(&account.id, 1, Some("bad-cursor")),
            Err(StoreError::InvalidCursor),
        ));
        let other_account = store
            .upsert_account_from_signal("+15555550101", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let (cursor_conversation, remaining_same_time) = if newer.id > same_time.id {
            (&newer, &same_time)
        } else {
            (&same_time, &newer)
        };
        let account_cursor = format!("v2:{}:0:20:{}", account.id, cursor_conversation.id);
        assert!(matches!(
            store.list_conversations(&other_account.id, 1, Some(&account_cursor)),
            Err(StoreError::InvalidCursor),
        ));

        let changed = MessageRecord {
            id: "newer-message-2".into(),
            account_id: account.id.clone(),
            conversation_id: cursor_conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 30,
            received_at: Some(30),
            text: Some("changed after cursor".into()),
            text_bytes: Some(20),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&changed, None, Some("newer-message-2"), false)
            .unwrap();
        let after_change = store
            .list_conversations(&account.id, 1, Some(&account_cursor))
            .unwrap();
        assert_eq!(after_change.items[0].id, remaining_same_time.id);
    }

    #[test]
    fn message_cursor_must_exist_in_the_same_conversation() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "peer", "Peer")
            .unwrap();
        let other_conversation = store
            .ensure_conversation(&account.id, "direct", "other-peer", "Other peer")
            .unwrap();
        let other_message = MessageRecord {
            id: "other-message".into(),
            account_id: account.id.clone(),
            conversation_id: other_conversation.id.clone(),
            direction: "incoming",
            sender_id: "other-peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 10,
            received_at: Some(10),
            text: Some("other".into()),
            text_bytes: Some(5),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&other_message, None, Some("other-message"), false)
            .unwrap();
        let other_account = store
            .upsert_account_from_signal("+15555550101", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        assert!(matches!(
            store.list_messages(&account.id, &conversation.id, 10, Some("unknown-message")),
            Err(StoreError::InvalidCursor),
        ));
        assert!(matches!(
            store.list_messages(&account.id, &conversation.id, 10, Some("other-message")),
            Err(StoreError::InvalidCursor),
        ));
        // Same rule for a self-describing cursor: it is bound to the thread and
        // account that issued it, whatever sort key it claims.
        let other_cursor = encode_message_cursor(&account.id, &other_conversation.id, &{
            let mut record = other_message.clone();
            record.direction = "incoming";
            record
        });
        assert!(matches!(
            store.list_messages(&account.id, &conversation.id, 10, Some(&other_cursor)),
            Err(StoreError::InvalidCursor),
        ));
        assert!(matches!(
            store.list_messages(
                &other_account.id,
                &other_conversation.id,
                10,
                Some(&other_cursor)
            ),
            Err(StoreError::InvalidCursor),
        ));
        assert!(matches!(
            store.list_messages(&account.id, &conversation.id, 10, Some("m1:broken")),
            Err(StoreError::InvalidCursor),
        ));
        assert!(matches!(
            store.list_messages(
                &other_account.id,
                &other_conversation.id,
                10,
                Some("other-message"),
            ),
            Err(StoreError::InvalidCursor),
        ));
    }

    #[test]
    fn search_matches_bodies_across_conversations_newest_first() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let one = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "One")
            .unwrap();
        let two = store
            .ensure_conversation(&account.id, "group", "group-id-1", "Two")
            .unwrap();
        let hit = |id: &str, conversation_id: &str, sent_at: u64, text: &str| {
            let mut message = MessageRecord {
                id: id.into(),
                account_id: account.id.clone(),
                conversation_id: conversation_id.into(),
                direction: "incoming",
                sender_id: "peer".into(),
                sender_name: None,
                mentions_self: false,
                sent_at,
                received_at: Some(sent_at),
                text: Some(text.into()),
                text_bytes: Some(text.len() as u32),
                text_truncated: false,
                text_retrievable: true,
                status: "delivered",
                client_request_id: None,
                quote_message_id: None,
                quote_snapshot: None,
                attachments: Vec::new(),
                rich: None,
                edited_at: None,
                reactions: Vec::new(),
                edits: Vec::new(),
                delivered_at: None,
                read_at: None,
                viewed_at: None,
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                admin_deleted: false,
                server_guid: None,
            };
            if id == "attachment-only" {
                message.text = None;
                message.text_bytes = None;
            }
            store.insert_message(&message, None, None, false).unwrap();
        };
        hit("old", &one.id, 10, "needle in the old thread");
        hit("new", &two.id, 20, "Needle in the new group");
        hit("miss", &one.id, 30, "nothing to see here");
        hit("attachment-only", &one.id, 40, "");
        let ids = store
            .search_messages(&account.id, "needle", 10, None)
            .unwrap()
            .items
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert_eq!(ids, ["new", "old"]);
        // Case folding covers ASCII; the empty query matches nothing rather
        // than every row.
        assert_eq!(
            store
                .search_messages(&account.id, "NEEDLE", 10, None)
                .unwrap()
                .items
                .len(),
            2
        );
        assert!(
            store
                .search_messages(&account.id, "", 10, None)
                .unwrap()
                .items
                .is_empty()
        );
        // Another account's store never leaks rows into the results.
        let other = store
            .upsert_account_from_signal("+15555550101", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert!(
            store
                .search_messages(&other.id, "needle", 10, None)
                .unwrap()
                .items
                .is_empty()
        );
    }

    #[test]
    fn search_like_metacharacters_are_literal_and_cursor_pages_are_stable() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        for (index, body) in [
            "100% done",
            "snake_case body",
            "plain body one",
            "plain body two",
        ]
        .into_iter()
        .enumerate()
        {
            let message = MessageRecord {
                id: format!("row-{index}"),
                account_id: account.id.clone(),
                conversation_id: conversation.id.clone(),
                direction: "incoming",
                sender_id: "peer".into(),
                sender_name: None,
                mentions_self: false,
                sent_at: 10 + index as u64,
                received_at: Some(10 + index as u64),
                text: Some(body.into()),
                text_bytes: Some(body.len() as u32),
                text_truncated: false,
                text_retrievable: true,
                status: "delivered",
                client_request_id: None,
                quote_message_id: None,
                quote_snapshot: None,
                attachments: Vec::new(),
                rich: None,
                edited_at: None,
                reactions: Vec::new(),
                edits: Vec::new(),
                delivered_at: None,
                read_at: None,
                viewed_at: None,
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                admin_deleted: false,
                server_guid: None,
            };
            store.insert_message(&message, None, None, false).unwrap();
        }
        // % and _ in the query match literally, not as wildcards.
        let ids = |query: &str| -> Vec<String> {
            store
                .search_messages(&account.id, query, 10, None)
                .unwrap()
                .items
                .into_iter()
                .map(|row| row.id)
                .collect()
        };
        assert_eq!(ids("0% d"), ["row-0"]);
        assert_eq!(ids("e_case"), ["row-1"]);
        // The query "%" matches the one row containing a percent sign —
        // literally, not as a wildcard over every row.
        assert_eq!(ids("%"), ["row-0"]);
        assert_eq!(ids("_"), ["row-1"]);

        // A cursor pages below its own sort key, so it stays resolvable no
        // matter what later happens to the row it was cut from; a cursor
        // from another account is refused.
        let first_page = store
            .search_messages(&account.id, "plain body", 1, None)
            .unwrap();
        assert_eq!(first_page.items.len(), 1);
        let cursor = first_page.next_cursor.unwrap();
        let second_page = store
            .search_messages(&account.id, "plain body", 10, Some(&cursor))
            .unwrap();
        assert_eq!(
            second_page
                .items
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["row-2"]
        );
        assert!(second_page.next_cursor.is_none());
        // The cursor carries its own sort key, so re-paging from it is
        // deterministic regardless of what happens to the anchor row.
        let repaged = store
            .search_messages(&account.id, "plain body", 10, Some(&cursor))
            .unwrap();
        assert_eq!(repaged.items[0].id, "row-2");
        let other = store
            .upsert_account_from_signal("+15555550101", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert!(matches!(
            store.search_messages(&other.id, "plain body", 10, Some(&cursor)),
            Err(StoreError::InvalidCursor),
        ));
        assert!(matches!(
            store.search_messages(&account.id, "plain body", 10, Some("m1:bogus")),
            Err(StoreError::InvalidCursor),
        ));
    }

    #[test]
    fn account_delete_is_atomic_cascading_and_idempotent() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let message = MessageRecord {
            id: "delete-message".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "+15555550101".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 10,
            received_at: Some(11),
            text: Some("delete me".into()),
            text_bytes: Some(9),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store.insert_message(&message, None, None, false).unwrap();

        assert!(store.delete_account_cascade(&account.id).unwrap());
        assert!(!store.delete_account_cascade(&account.id).unwrap());
        assert!(store.account_by_id(&account.id).unwrap().is_none());
        assert!(store.list_accounts().unwrap().is_empty());
    }

    #[test]
    fn account_delete_cascades_reaction_events_with_foreign_keys_on() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        store
            .upsert_reaction_event(
                &account.id,
                &conversation.id,
                "❤️",
                10,
                "+15555550101",
                false,
                Some("Peer"),
            )
            .unwrap();

        // complete_account_delete drops conversations before accounts; a
        // message_events row (FK conversation_id → conversations, no cascade)
        // must not abort the whole transaction — this is the production
        // stuck-logout failure (INTERNAL_ERROR on every retry, INTERNAL log
        // 2026-10-05).
        assert!(store.delete_account_cascade(&account.id).unwrap());
        assert!(store.account_by_id(&account.id).unwrap().is_none());
    }

    #[test]
    fn account_delete_operation_survives_unknown_and_completes_atomically() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        assert_eq!(
            store
                .prepare_account_delete(&account.id, "delete-op-1", 10)
                .unwrap(),
            AccountDeletePlan::Dispatch {
                signal_account: "+15555550100".into(),
                reconcile_first: false,
            },
        );
        store
            .mark_account_delete_unknown("delete-op-1", 11)
            .unwrap();
        assert_eq!(
            store
                .prepare_account_delete(&account.id, "delete-op-1", 12)
                .unwrap(),
            AccountDeletePlan::Dispatch {
                signal_account: "+15555550100".into(),
                reconcile_first: true,
            },
        );
        assert!(
            store
                .complete_account_delete(&account.id, Some("delete-op-1"), 13)
                .unwrap()
        );
        assert_eq!(
            store
                .prepare_account_delete(&account.id, "delete-op-1", 14)
                .unwrap(),
            AccountDeletePlan::Completed,
        );
        assert!(store.account_by_id(&account.id).unwrap().is_none());
    }

    #[test]
    fn account_delete_operation_cannot_change_target_or_compete() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let other = store
            .upsert_account_from_signal("+15555550101", Some(2), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        store
            .prepare_account_delete(&account.id, "delete-op-1", 10)
            .unwrap();

        assert!(matches!(
            store.prepare_account_delete(&other.id, "delete-op-1", 11),
            Err(StoreError::OperationConflict),
        ));
        assert!(matches!(
            store.prepare_account_delete(&account.id, "delete-op-2", 12),
            Err(StoreError::OperationConflict),
        ));
    }

    #[test]
    fn completed_account_delete_operations_are_bounded() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();

        for index in 0..300 {
            assert_eq!(
                store
                    .prepare_account_delete(
                        &format!("absent-account-{index}"),
                        &format!("completed-operation-{index}"),
                        index,
                    )
                    .unwrap(),
                AccountDeletePlan::Completed,
            );
        }

        let count: i64 = store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM account_delete_operations WHERE state='completed'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, MAX_COMPLETED_ACCOUNT_DELETE_OPERATIONS);
    }

    #[test]
    fn schema_v3_upgrade_adds_message_body_metadata_before_advancing_version() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '3');
             CREATE TABLE messages (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               sender_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL,
               received_at INTEGER,
               body TEXT,
               status TEXT NOT NULL,
               client_request_id TEXT,
               quote_message_id TEXT
             );",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "messages", "body_bytes").unwrap());
        assert!(table_has_column(&store.conn(), "messages", "body_truncated").unwrap());
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn future_schema_version_fails_closed() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        store
            .conn()
            .execute("UPDATE meta SET value='999' WHERE key='schema_version'", [])
            .unwrap();
        drop(store);

        assert!(matches!(
            Store::open(temp.path(), Some(test_store_key())),
            Err(StoreError::Unavailable(_))
        ));
    }

    /// Phase 4 (ADR 0001 R4): a pre-Phase-4 database (accounts without the
    /// proxy_group column) migrates in place; existing rows read as `default`
    /// and no data migration pass is needed.
    #[test]
    fn schema_v7_upgrade_adds_proxy_group_reading_old_rows_as_default() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '6');
             CREATE TABLE accounts (
               id TEXT PRIMARY KEY,
               signal_account TEXT NOT NULL UNIQUE,
               masked_address TEXT NOT NULL,
               display_name TEXT,
               state TEXT NOT NULL,
               linked_at INTEGER,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO accounts(id, signal_account, masked_address, state, linked_at)
               VALUES('legacy-1', '+15555550100', '8fc***e2', 'ready', 1);",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "accounts", "proxy_group").unwrap());
        let summary = store.account_summary("legacy-1").unwrap().unwrap();
        assert_eq!(summary.proxy_group, "default");

        // A fresh account binds to its configured group.
        let grouped = store
            .upsert_account_from_signal("+15555550101", Some(1), "team-a")
            .unwrap();
        assert_eq!(grouped.proxy_group, "team-a");
        assert_eq!(store.count_accounts_in_group("team-a").unwrap(), 1);
        assert_eq!(
            store
                .any_signal_account_number_in_group("team-a")
                .unwrap()
                .as_deref(),
            Some("+15555550101")
        );

        // Re-syncing a known number never moves it between groups (R3).
        let resynced = store
            .upsert_account_from_signal("+15555550101", None, DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(resynced.id, grouped.id);
        assert_eq!(resynced.proxy_group, "team-a");
    }

    /// Group-scoped views partition the account list exactly by binding;
    /// unknown groups report zero without error.
    #[test]
    fn group_scoped_account_views_partition_by_binding() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        store
            .upsert_account_from_signal("+15555550100", Some(2), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        store
            .upsert_account_from_signal("+15555550101", Some(1), "team-a")
            .unwrap();

        assert_eq!(store.list_accounts().unwrap().len(), 2);
        let default_accounts = store
            .list_accounts_in_group(DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(default_accounts.len(), 1);
        assert_eq!(default_accounts[0].proxy_group, "default");
        let team_a = store.list_accounts_in_group("team-a").unwrap();
        assert_eq!(team_a.len(), 1);
        // Newer linked_at first, mirroring the global order rule.
        assert_eq!(default_accounts[0].linked_at, Some(2));
        assert_eq!(store.count_accounts_in_group("missing").unwrap(), 0);
        assert_eq!(
            store.any_signal_account_number_in_group("missing").unwrap(),
            None
        );
    }

    fn open_store_with_contacts(temp: &TempDir) -> (Store, AccountSummary) {
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        store
            .upsert_contact(&account.id, "contact", "+15555550101", "Alice", None, 10)
            .unwrap();
        store
            .upsert_contact(&account.id, "contact", "+15555550102", "Bob", None, 10)
            .unwrap();
        store
            .upsert_contact(
                &account.id,
                "group",
                "ZmFrZS1ncm91cA==",
                "Fixture Group",
                Some("{\"memberCount\":3}"),
                10,
            )
            .unwrap();
        (store, account)
    }

    #[test]
    fn contacts_upsert_list_filter_and_paginate() {
        let temp = TempDir::new().unwrap();
        let (store, account) = open_store_with_contacts(&temp);

        // Upsert refreshes the title without duplicating the entry.
        store
            .upsert_contact(&account.id, "contact", "+15555550101", "Alice A.", None, 11)
            .unwrap();
        assert_eq!(store.count_contacts(&account.id).unwrap(), (2, 1));

        let page = store.list_contacts(&account.id, None, 10, None).unwrap();
        assert_eq!(page.items.len(), 3);
        assert!(page.next_cursor.is_none());
        assert_eq!(page.items[0].kind, "contact");
        assert_eq!(page.items[0].peer_key, "+15555550101");
        assert_eq!(page.items[0].title, "Alice A.");
        assert_eq!(page.items[2].kind, "group");
        assert_eq!(page.items[2].title, "Fixture Group");

        let filtered = store
            .list_contacts(&account.id, Some("alice"), 10, None)
            .unwrap();
        assert_eq!(filtered.items.len(), 1);
        assert_eq!(filtered.items[0].peer_key, "+15555550101");
        // LIKE metacharacters in the query are literal, not wildcards.
        assert!(
            store
                .list_contacts(&account.id, Some("%"), 10, None)
                .unwrap()
                .items
                .is_empty()
        );

        let first = store.list_contacts(&account.id, None, 1, None).unwrap();
        assert_eq!(first.items.len(), 1);
        let cursor = first.next_cursor.clone().unwrap();
        let rest = store
            .list_contacts(&account.id, None, 10, Some(&cursor))
            .unwrap();
        assert_eq!(rest.items.len(), 2);
        assert!(rest.next_cursor.is_none());
        assert!(rest.items.iter().all(|item| item.id != first.items[0].id));

        assert!(matches!(
            store.list_contacts(&account.id, None, 10, Some("bogus")),
            Err(StoreError::InvalidCursor)
        ));
        assert!(matches!(
            store.list_contacts(&account.id, None, 10, Some("c1:other-account:contact:+1")),
            Err(StoreError::InvalidCursor)
        ));
    }

    #[test]
    fn contacts_synced_at_marker_roundtrips() {
        let temp = TempDir::new().unwrap();
        let (store, account) = open_store_with_contacts(&temp);
        assert_eq!(store.contacts_synced_at(&account.id).unwrap(), None);
        store.set_contacts_synced_at(&account.id, 123).unwrap();
        assert_eq!(store.contacts_synced_at(&account.id).unwrap(), Some(123));
        store.set_contacts_synced_at(&account.id, 456).unwrap();
        assert_eq!(store.contacts_synced_at(&account.id).unwrap(), Some(456));
    }

    #[test]
    fn contacts_are_deleted_with_the_account() {
        let temp = TempDir::new().unwrap();
        let (store, account) = open_store_with_contacts(&temp);
        store.set_contacts_synced_at(&account.id, 10).unwrap();
        assert!(store.delete_account_cascade(&account.id).unwrap());
        assert_eq!(store.count_contacts(&account.id).unwrap(), (0, 0));
        assert_eq!(store.contacts_synced_at(&account.id).unwrap(), None);
        assert!(
            store
                .list_contacts(&account.id, None, 10, None)
                .unwrap()
                .items
                .is_empty()
        );
    }

    /// A contacts sync batch is one transaction: when an entry in the middle
    /// fails, none of the rows and not even the sync marker become visible.
    /// Fault injection uses a trigger on a second connection, the same pattern
    /// as the receive-persistence failure test in supervisor.rs.
    #[test]
    fn synced_contacts_batch_rolls_back_atomically_on_failure() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let observer = keyed_observer(&store);
        observer
            .execute_batch(
                "CREATE TRIGGER fail_contact_insert
                 BEFORE INSERT ON contacts
                 WHEN NEW.peer_key='+15555550102'
                 BEGIN SELECT RAISE(FAIL, 'controlled test failure'); END;",
            )
            .unwrap();

        let entries = [
            SyncedContact {
                kind: "contact",
                peer_key: "+15555550101",
                title: "Alice",
                extra: None,
            },
            SyncedContact {
                kind: "contact",
                peer_key: "+15555550102",
                title: "Bob",
                extra: None,
            },
            SyncedContact {
                kind: "group",
                peer_key: "Z3JvdXAtMQ==",
                title: "Group",
                extra: Some("{\"memberCount\":2}"),
            },
        ];
        assert!(matches!(
            store.upsert_synced_contacts(&account.id, &entries, 42),
            Err(StoreError::Unavailable(_))
        ));
        // Nothing from the failed batch is visible, and the sync marker did
        // not advance, so the next sync is not short-circuited.
        assert_eq!(store.count_contacts(&account.id).unwrap(), (0, 0));
        assert_eq!(store.contacts_synced_at(&account.id).unwrap(), None);

        observer
            .execute_batch("DROP TRIGGER fail_contact_insert;")
            .unwrap();
        store
            .upsert_synced_contacts(&account.id, &entries, 43)
            .unwrap();
        assert_eq!(store.count_contacts(&account.id).unwrap(), (2, 1));
        assert_eq!(store.contacts_synced_at(&account.id).unwrap(), Some(43));

        // Same upsert semantics as the single-row path: a re-sync updates the
        // stored title and extra in place.
        let renamed = [SyncedContact {
            kind: "contact",
            peer_key: "+15555550101",
            title: "Alice A.",
            extra: None,
        }];
        store
            .upsert_synced_contacts(&account.id, &renamed, 44)
            .unwrap();
        let page = store
            .list_contacts(&account.id, Some("alice"), 10, None)
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].title, "Alice A.");
        assert_eq!(store.count_contacts(&account.id).unwrap(), (2, 1));
    }

    /// Performance sanity, not a benchmark: a phone-scale contacts sync (a few
    /// thousand entries) must commit as one quick batch, not one transaction
    /// per row. Generous bound so slow CI machines still pass.
    #[test]
    fn synced_contacts_batch_handles_a_phone_scale_sync_quickly() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let keys: Vec<String> = (0..5_000)
            .map(|index| format!("+1555556{index:04}"))
            .collect();
        let entries: Vec<SyncedContact<'_>> = keys
            .iter()
            .map(|key| SyncedContact {
                kind: "contact",
                peer_key: key.as_str(),
                title: "Batch Contact",
                extra: None,
            })
            .collect();

        let started = std::time::Instant::now();
        store
            .upsert_synced_contacts(&account.id, &entries, 100)
            .unwrap();
        let elapsed = started.elapsed();

        assert_eq!(store.count_contacts(&account.id).unwrap(), (5_000, 0));
        assert_eq!(store.contacts_synced_at(&account.id).unwrap(), Some(100));
        assert!(
            elapsed < Duration::from_secs(5),
            "one 5000-row sync batch took {elapsed:?}, far above a single-transaction budget"
        );
    }

    #[test]
    fn schema_v4_upgrade_creates_contacts_table_before_advancing_version() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        store
            .conn()
            .execute_batch(
                "DROP TABLE contacts;
                 UPDATE meta SET value='4' WHERE key='schema_version';",
            )
            .unwrap();
        drop(store);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let count: i64 = store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='contacts'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn message_pages_resume_after_retention_removed_the_anchor() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let (account, conversation, ids) =
            seed_history(&store, now, &[100 * day, 200 * day, 300 * day]);

        let first = store
            .list_messages(&account.id, &conversation.id, 1, None)
            .unwrap();
        assert_eq!(first.items[0].id, ids[0]);
        let cursor = first.next_cursor.unwrap();
        // Everything the caller has seen and its anchor expire underneath it.
        assert_eq!(
            store.prune_history(now, u32::MAX).unwrap().messages_deleted,
            3
        );

        // The page still resolves; it is simply empty now that the tail is gone.
        let next = store
            .list_messages(&account.id, &conversation.id, 10, Some(&cursor))
            .unwrap();
        assert!(next.items.is_empty());
        assert!(next.next_cursor.is_none());
    }

    #[test]
    fn retention_repairs_a_preview_whose_anchor_shared_a_timestamp() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let mut message = MessageRecord {
            id: "kept".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: now - 10 * day,
            received_at: Some(now - 10 * day),
            text: Some("kept body".into()),
            text_bytes: Some(9),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&message, None, Some("kept body"), true)
            .unwrap();
        backdate(&store, "kept", now - 8 * day);
        // Same timestamp as the row that stays, so a timestamp-only check would
        // conclude the summary is still anchored and leave a deleted preview.
        message.id = "expired".into();
        message.text = Some("expired body".into());
        store
            .insert_message(&message, None, Some("expired body"), true)
            .unwrap();
        backdate(&store, "expired", now - 400 * day);

        assert_eq!(
            store
                .prune_history(now, u32::MAX)
                .unwrap()
                .conversations_repaired,
            1
        );

        let summary = store
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        assert_eq!(summary.last_message_at, Some(now - 10 * day));
        assert_eq!(summary.last_message_preview.as_deref(), Some("kept body"));
    }

    #[test]
    fn retention_dates_history_written_before_the_stored_at_column() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let message = MessageRecord {
            id: "before-upgrade".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            // Sender clock claims the epoch; we received it yesterday.
            sent_at: 5_000,
            received_at: Some(now - day),
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&message, None, Some("body"), true)
            .unwrap();
        store
            .conn()
            .execute_batch(
                // The held-since index is younger than schema 6, so a store
                // being reverted to its schema-5 shape cannot carry it — and
                // SQLite refuses to drop an indexed column.
                "DROP INDEX IF EXISTS messages_held_since;
                 ALTER TABLE messages DROP COLUMN stored_at;
                 UPDATE meta SET value='5' WHERE key='schema_version';",
            )
            .unwrap();
        drop(store);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();

        // The upgrade only adds the column; rewriting the table would delay the
        // startup handshake. Retention instead dates such a row by the time we
        // received it, so pre-upgrade history is not read as instantly expired.
        let stored_at: Option<i64> = store
            .conn()
            .query_row(
                "SELECT stored_at FROM messages WHERE id='before-upgrade'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored_at, None);
        assert_eq!(
            store.prune_history(now, u32::MAX).unwrap().messages_deleted,
            0
        );
        assert_eq!(stored_message_ids(&store, &conversation.id).len(), 1);
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    /// Seed one conversation with messages at the given ages, newest first in
    /// the returned ids. `now` is the wall clock the retention pass will see.
    fn seed_history(
        store: &Store,
        now: u64,
        ages_ms: &[u64],
    ) -> (AccountSummary, ConversationRow, Vec<String>) {
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let ids = ages_ms
            .iter()
            .enumerate()
            .map(|(index, age)| {
                let id = format!("message-{index}");
                let message = MessageRecord {
                    id: id.clone(),
                    account_id: account.id.clone(),
                    conversation_id: conversation.id.clone(),
                    direction: "incoming",
                    sender_id: "peer".into(),
                    sender_name: None,
                    mentions_self: false,
                    sent_at: now - age,
                    received_at: Some(now - age),
                    text: Some("body".into()),
                    text_bytes: Some(4),
                    text_truncated: false,
                    text_retrievable: true,
                    status: "delivered",
                    client_request_id: None,
                    quote_message_id: None,
                    quote_snapshot: None,
                    attachments: Vec::new(),
                    edited_at: None,
                    rich: None,
                    reactions: Vec::new(),
                    edits: Vec::new(),
                    delivered_at: None,
                    read_at: None,
                    viewed_at: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    admin_deleted: false,
                    server_guid: None,
                };
                store
                    .insert_message(&message, None, Some("body"), true)
                    .unwrap();
                backdate(store, &id, now - age);
                id
            })
            .collect();
        (account, conversation, ids)
    }

    /// Pretend the store has held a row since `stored_at`; inserts always stamp
    /// the real clock, which no test can wait out.
    fn backdate(store: &Store, message_id: &str, stored_at: u64) {
        let updated = store
            .conn()
            .execute(
                "UPDATE messages SET stored_at=?2 WHERE id=?1",
                params![message_id, stored_at as i64],
            )
            .unwrap();
        assert_eq!(updated, 1);
    }

    fn stored_message_ids(store: &Store, conversation_id: &str) -> Vec<String> {
        let conn = store.conn();
        let mut stmt = conn
            .prepare("SELECT id FROM messages WHERE conversation_id=?1 ORDER BY id")
            .unwrap();
        stmt.query_map(params![conversation_id], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn retention_drops_expired_history_and_keeps_the_rest() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let (account, conversation, ids) =
            seed_history(&store, now, &[day, 89 * day, 91 * day, 400 * day]);

        let outcome = store.prune_history(now, u32::MAX).unwrap();

        assert_eq!(outcome.messages_deleted, 2);
        let remaining = stored_message_ids(&store, &conversation.id);
        assert_eq!(remaining, vec![ids[0].clone(), ids[1].clone()]);
        // The seed inserted the oldest row last, so it owned the summary; the
        // repair moves the summary to the newest row still stored.
        assert_eq!(outcome.conversations_repaired, 1);
        let summary = store
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        assert_eq!(summary.last_message_at, Some(now - day));
    }

    #[test]
    fn retention_keeps_recent_history_beyond_the_per_conversation_cap() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        // More rows than the cap allows, all inside the safety window: replay
        // dedupe and send idempotency still need every one of them.
        let ages = (0..(MAX_MESSAGES_PER_CONVERSATION as u64 + 5))
            .map(|index| index + 1)
            .collect::<Vec<_>>();
        let (_account, conversation, _ids) = seed_history(&store, now, &ages);

        assert_eq!(
            store.prune_history(now, u32::MAX).unwrap().messages_deleted,
            0
        );
        assert_eq!(
            stored_message_ids(&store, &conversation.id).len(),
            ages.len()
        );
    }

    #[test]
    fn retention_enforces_the_per_conversation_cap_outside_the_safety_window() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let over_cap = 3;
        // Old enough to prune, young enough that only the cap can select them.
        let ages = (0..(MAX_MESSAGES_PER_CONVERSATION as u64 + over_cap))
            .map(|index| 8 * day + index)
            .collect::<Vec<_>>();
        let (_account, conversation, ids) = seed_history(&store, now, &ages);

        let outcome = store.prune_history(now, u32::MAX).unwrap();

        assert_eq!(outcome.messages_deleted, over_cap);
        let remaining = stored_message_ids(&store, &conversation.id);
        assert_eq!(remaining.len(), MAX_MESSAGES_PER_CONVERSATION as usize);
        // The oldest rows go; the newest are what the cap keeps.
        for id in ids.iter().take(MAX_MESSAGES_PER_CONVERSATION as usize) {
            assert!(remaining.contains(id));
        }
        for id in ids.iter().skip(MAX_MESSAGES_PER_CONVERSATION as usize) {
            assert!(!remaining.contains(id));
        }
    }

    #[test]
    fn retention_never_drops_a_send_that_has_not_resolved() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let mut message = MessageRecord {
            id: "in-flight".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "outgoing",
            sender_id: account.id.clone(),
            sender_name: None,
            mentions_self: false,
            sent_at: now - 400 * day,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,

            status: "pending",
            client_request_id: Some("request-pending".into()),
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&message, Some("request-pending"), Some("body"), false)
            .unwrap();
        message.id = "outcome-unknown".into();
        message.status = "unknown";
        message.client_request_id = Some("request-unknown".into());
        store
            .insert_message(&message, Some("request-unknown"), Some("body"), false)
            .unwrap();
        backdate(&store, "in-flight", now - 400 * day);
        backdate(&store, "outcome-unknown", now - 400 * day);

        assert_eq!(
            store.prune_history(now, u32::MAX).unwrap().messages_deleted,
            0
        );
        assert_eq!(
            store
                .message_by_client_request(&account.id, "request-pending")
                .unwrap()
                .map(|row| row.id),
            Some("in-flight".to_string())
        );
        assert_eq!(
            store
                .message_by_client_request(&account.id, "request-unknown")
                .unwrap()
                .map(|row| row.id),
            Some("outcome-unknown".to_string())
        );
    }

    #[test]
    fn retention_repairs_the_summaries_of_a_conversation_it_emptied() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let (account, conversation, _ids) = seed_history(&store, now, &[300 * day, 400 * day]);
        // Second conversation stays inside retention so the account keeps a
        // last-message time and an unread badge of its own.
        let kept = store
            .ensure_conversation(&account.id, "direct", "+15555550102", "contact")
            .unwrap();
        let recent = MessageRecord {
            id: "recent".into(),
            account_id: account.id.clone(),
            conversation_id: kept.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: now - day,
            received_at: Some(now - day),
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&recent, None, Some("body"), true)
            .unwrap();

        let outcome = store.prune_history(now, u32::MAX).unwrap();

        assert_eq!(outcome.messages_deleted, 2);
        assert_eq!(outcome.conversations_repaired, 1);
        let conversations = store
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items;
        let emptied = conversations
            .iter()
            .find(|item| item.id == conversation.id)
            .unwrap();
        // The conversation survives so its title and pins do, but it may not
        // advertise a preview, a time or unread mail that no longer exists.
        assert_eq!(emptied.last_message_at, None);
        assert_eq!(emptied.last_message_preview, None);
        assert_eq!(emptied.unread_count, 0);
        let accounts = store.list_accounts().unwrap();
        let summary = accounts.iter().find(|item| item.id == account.id).unwrap();
        assert_eq!(summary.last_message_at, Some(now - day));
        assert_eq!(summary.unread_count, 1);
    }

    #[test]
    fn retention_repoints_a_summary_whose_newest_row_expired() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let mut message = MessageRecord {
            id: "kept".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: now - 20 * day,
            received_at: Some(now - 20 * day),
            text: Some("kept body".into()),
            text_bytes: Some(9),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&message, None, Some("kept body"), true)
            .unwrap();
        backdate(&store, "kept", now - 8 * day);
        // A peer clock running ahead makes this the newest row by sent_at, so the
        // conversation summary points at it, yet we have held it long enough to
        // expire. Pruning it must not leave the summary on a deleted row.
        message.id = "skewed".into();
        message.sent_at = now - 10 * day;
        message.text = Some("skewed body".into());
        store
            .insert_message(&message, None, Some("skewed body"), true)
            .unwrap();
        backdate(&store, "skewed", now - 400 * day);

        let outcome = store.prune_history(now, u32::MAX).unwrap();

        assert_eq!(outcome.messages_deleted, 1);
        assert_eq!(outcome.conversations_repaired, 1);
        let summary = store
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        assert_eq!(summary.last_message_at, Some(now - 20 * day));
        assert_eq!(summary.last_message_preview.as_deref(), Some("kept body"));
        let accounts = store.list_accounts().unwrap();
        assert_eq!(accounts[0].last_message_at, Some(now - 20 * day));
    }

    #[test]
    fn retention_caps_unread_by_the_incoming_mail_it_kept() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let mut message = MessageRecord {
            id: "unread-incoming".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: now - 300 * day,
            received_at: Some(now - 300 * day),
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&message, None, Some("body"), true)
            .unwrap();
        backdate(&store, "unread-incoming", now - 300 * day);
        // Our own reply keeps the conversation anchored, so only the unread cap
        // is under test here, and replies were never unread mail.
        message.id = "own-reply".into();
        message.direction = "outgoing";
        message.sender_id = account.id.clone();
        message.received_at = None;
        message.sent_at = now - day;
        store
            .insert_message(&message, None, Some("body"), false)
            .unwrap();

        assert_eq!(
            store.prune_history(now, u32::MAX).unwrap().messages_deleted,
            1
        );

        let summary = store
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        assert_eq!(summary.unread_count, 0);
        assert_eq!(store.list_accounts().unwrap()[0].unread_count, 0);
    }

    #[test]
    fn retention_deletes_at_most_one_batch_per_call() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let (_account, conversation, _ids) =
            seed_history(&store, now, &[300 * day, 350 * day, 400 * day]);

        assert_eq!(store.prune_history(now, 2).unwrap().messages_deleted, 2);
        assert_eq!(stored_message_ids(&store, &conversation.id).len(), 1);
        assert_eq!(store.prune_history(now, 2).unwrap().messages_deleted, 1);
        assert!(stored_message_ids(&store, &conversation.id).is_empty());
    }

    #[test]
    fn retention_prune_plans_stay_index_driven() {
        // Guard for the A9 cost fix: the age branch must seek through the
        // messages_held_since expression index, and no step of either prune
        // branch may degenerate into an unindexed full walk of the messages
        // table. The statements below mirror the two prune_history DELETEs
        // verbatim; the plans are checked with data present so the planner
        // chooses the same shape it will choose in production.
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        seed_history(&store, now, &[day, 400 * day]);

        let query_plan = |sql: String| -> Vec<String> {
            let conn = store.conn();
            let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
            let rows = stmt.query_map([], |row| row.get::<_, String>(3)).unwrap();
            rows.map(|row| row.unwrap()).collect()
        };
        let age_branch = query_plan(
            "DELETE FROM messages WHERE id IN (
               SELECT id FROM messages
               WHERE COALESCE(stored_at, received_at, sent_at) < 5
                 AND status NOT IN ('pending', 'unknown')
               ORDER BY COALESCE(stored_at, received_at, sent_at)
               LIMIT 10
             ) RETURNING conversation_id"
                .to_string(),
        );
        assert!(
            age_branch
                .iter()
                .any(|line| line.contains("messages_held_since")),
            "age branch must use the held-since expression index: {age_branch:?}"
        );

        let cap_branch = query_plan(
            "DELETE FROM messages WHERE id IN (
               SELECT id FROM (
                 SELECT id, status,
                        COALESCE(stored_at, received_at, sent_at) AS held_since,
                        ROW_NUMBER() OVER (
                          PARTITION BY conversation_id ORDER BY sent_at DESC, id DESC
                        ) AS recency
                 FROM messages
                 WHERE conversation_id IN (
                   SELECT conversation_id FROM messages
                   GROUP BY conversation_id
                   HAVING COUNT(*) > 2000
                 )
               )
               WHERE held_since >= 5 AND held_since < 10
                 AND status NOT IN ('pending', 'unknown')
                 AND recency > 2000
               ORDER BY held_since
               LIMIT 10
             ) RETURNING conversation_id"
                .to_string(),
        );
        for line in age_branch.iter().chain(cap_branch.iter()) {
            assert!(
                !line.starts_with("SCAN messages") || line.contains("USING"),
                "prune step degenerated into a full table walk: {line}"
            );
        }
    }

    #[test]
    fn retention_is_idempotent_and_quiet_when_nothing_expired() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let now = 40 * 365 * 24 * 60 * 60 * 1_000;
        let day = 24 * 60 * 60 * 1_000;
        let (account, conversation, _ids) = seed_history(&store, now, &[day, 400 * day]);

        assert_eq!(
            store.prune_history(now, u32::MAX).unwrap().messages_deleted,
            1
        );
        let before = store
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items;
        assert_eq!(
            store.prune_history(now, u32::MAX).unwrap(),
            HistoryPruneOutcome::default()
        );
        let after = store
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items;
        assert_eq!(before.len(), after.len());
        assert_eq!(before[0].unread_count, after[0].unread_count);
        assert_eq!(before[0].last_message_at, after[0].last_message_at);
        assert_eq!(stored_message_ids(&store, &conversation.id).len(), 1);
    }

    // ---- Phase 3: encryption at rest ----

    /// Tables whose row counts must survive a plaintext→encrypted migration.
    const TRACKED_TABLES: [&str; 6] = [
        "meta",
        "accounts",
        "conversations",
        "messages",
        "account_delete_operations",
        "contacts",
    ];

    fn table_counts(conn: &Connection) -> Vec<(&'static str, i64)> {
        TRACKED_TABLES
            .iter()
            .map(|table| {
                let count = conn
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                (*table, count)
            })
            .collect()
    }

    /// Build a plaintext store through the same SQLCipher connection type,
    /// bypassing `Store::open` so the file stays plaintext, with rows in every
    /// table and a WAL tail that is never checkpointed here. Returns per-table
    /// row counts plus a content probe for post-migration comparison.
    fn seed_plaintext_store(temp: &TempDir) -> (Vec<(&'static str, i64)>, String) {
        let path = temp.path().join(DATABASE_FILE_NAME);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;\nPRAGMA foreign_keys=ON;")
            .unwrap();
        conn.execute_batch(SCHEMA_DDL).unwrap();
        conn.execute_batch(
            "INSERT INTO meta(key, value) VALUES('schema_version', '6');
             INSERT INTO accounts(id, signal_account, masked_address, state)
               VALUES ('a1', '+15555550100', '+15***00', 'ready'),
                      ('a2', '+15555550200', '+15***00', 'ready');
             INSERT INTO conversations(id, account_id, kind, peer_key, title)
               VALUES ('c1', 'a1', 'direct', '+15555550101', 'contact'),
                      ('c2', 'a2', 'direct', '+15555550201', 'contact');
             INSERT INTO messages(id, account_id, conversation_id, direction, sender_id, sent_at, body, status)
               VALUES ('m1', 'a1', 'c1', 'incoming', '+15555550101', 1, 'migration probe body', 'delivered'),
                      ('m2', 'a1', 'c1', 'outgoing', '+15555550100', 2, 'second', 'sent'),
                      ('m3', 'a1', 'c1', 'incoming', '+15555550101', 3, NULL, 'read'),
                      ('m4', 'a2', 'c2', 'incoming', '+15555550201', 4, 'other account', 'delivered'),
                      ('m5', 'a2', 'c2', 'outgoing', '+15555550200', 5, 'tail in wal', 'pending');
             INSERT INTO account_delete_operations(operation_id, account_id, state, created_at, updated_at)
               VALUES ('op1', 'a2', 'completed', 1, 1);
             INSERT INTO contacts(id, account_id, kind, peer_key, title, synced_at)
               VALUES ('k1', 'a1', 'contact', '+15555550101', 'Alice', 10),
                      ('k2', 'a1', 'group', 'ZmFrZS1ncm91cA==', 'Group', 10),
                      ('k3', 'a2', 'contact', '+15555550201', 'Bob', 10);",
        )
        .unwrap();
        let counts = table_counts(&conn);
        let probe: String = conn
            .query_row("SELECT body FROM messages WHERE id='m1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        // Deliberately no checkpoint: the committed rows may live only in the
        // WAL tail, which the migration must carry over.
        drop(conn);
        (counts, probe)
    }

    #[test]
    fn plaintext_store_migrates_to_encrypted_with_identical_rows() {
        let temp = TempDir::new().unwrap();
        let (before, probe_before) = seed_plaintext_store(&temp);
        let db_path = temp.path().join(DATABASE_FILE_NAME);
        assert_eq!(db_file_state(&db_path).unwrap(), DbFileState::Plaintext);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();

        assert_eq!(db_file_state(&db_path).unwrap(), DbFileState::Encrypted);
        assert_eq!(table_counts(&store.conn()), before);
        let probe_after: String = store
            .conn()
            .query_row("SELECT body FROM messages WHERE id='m1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(probe_after, probe_before);
        // The plaintext backup holds the same rows for the 7-day rollback
        // window, and no staging debris outlives the swap.
        let backup_conn = Connection::open(plaintext_backup_path(&db_path)).unwrap();
        assert_eq!(table_counts(&backup_conn), before);
        drop(backup_conn);
        assert!(!append_suffix(&db_path, MIGRATION_STAGING_SUFFIX).exists());

        // The migrated store is durable: a fresh keyed open sees every row.
        drop(store);
        let reopened = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert_eq!(table_counts(&reopened.conn()), before);
    }

    #[test]
    fn a_plaintext_store_without_a_key_fails_closed_and_is_untouched() {
        let temp = TempDir::new().unwrap();
        let (before, _) = seed_plaintext_store(&temp);
        let db_path = temp.path().join(DATABASE_FILE_NAME);

        assert!(matches!(
            Store::open(temp.path(), None),
            Err(StoreError::PlaintextStoreRequiresKey)
        ));
        // Fail closed means untouched: still plaintext, no backup, same rows.
        assert_eq!(db_file_state(&db_path).unwrap(), DbFileState::Plaintext);
        assert!(!plaintext_backup_path(&db_path).exists());
        let conn = Connection::open(&db_path).unwrap();
        assert_eq!(table_counts(&conn), before);
    }

    #[test]
    fn no_key_and_no_store_fails_closed_without_creating_anything() {
        let temp = TempDir::new().unwrap();
        assert!(matches!(
            Store::open(temp.path(), None),
            Err(StoreError::StoreKeyRequired)
        ));
        assert!(!temp.path().join(DATABASE_FILE_NAME).exists());
    }

    #[test]
    fn an_encrypted_store_without_a_key_fails_closed() {
        let temp = TempDir::new().unwrap();
        {
            let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
            store
                .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
                .unwrap();
        }
        assert!(matches!(
            Store::open(temp.path(), None),
            Err(StoreError::StoreKeyRequired)
        ));
    }

    #[test]
    fn an_encrypted_store_reopens_with_the_same_key() {
        let temp = TempDir::new().unwrap();
        {
            let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
            store
                .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
                .unwrap();
        }
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert_eq!(store.list_accounts().unwrap().len(), 1);
    }

    #[test]
    fn an_encrypted_store_rejects_a_wrong_key() {
        let temp = TempDir::new().unwrap();
        {
            let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
            store
                .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
                .unwrap();
        }
        let wrong = StoreKey::from_bytes([0x11; STORE_KEY_BYTES]);
        assert!(matches!(
            Store::open(temp.path(), Some(wrong)),
            Err(StoreError::StoreKeyRejected)
        ));
    }

    #[test]
    fn a_failed_migration_fails_closed_and_keeps_the_plaintext_store() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join(DATABASE_FILE_NAME);
        // A plaintext-looking header over garbage: opens as "plaintext",
        // breaks as soon as a page is actually read.
        let mut garbage = SQLITE_PLAINTEXT_HEADER.to_vec();
        garbage.extend_from_slice(&[0xAA; 4096]);
        std::fs::write(&db_path, &garbage).unwrap();

        assert!(matches!(
            Store::open(temp.path(), Some(test_store_key())),
            Err(StoreError::MigrationFailed(_))
        ));
        // The original file is byte-identical, no backup, no staging debris.
        assert_eq!(std::fs::read(&db_path).unwrap(), garbage);
        assert!(!plaintext_backup_path(&db_path).exists());
        assert!(!append_suffix(&db_path, MIGRATION_STAGING_SUFFIX).exists());
    }

    #[test]
    fn plaintext_backup_is_pruned_only_after_its_retention_window() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let backup = plaintext_backup_path(store.path());
        std::fs::write(&backup, b"plaintext-bytes").unwrap();
        let now = 1_800_000_000_000_u64;

        // Inside the window the backup stays.
        let file = std::fs::File::options().write(true).open(&backup).unwrap();
        file.set_modified(SystemTime::UNIX_EPOCH + Duration::from_millis(now))
            .unwrap();
        drop(file);
        assert!(!store.prune_expired_plaintext_backup(now).unwrap());
        assert!(backup.exists());

        // Past the window it is removed; afterwards pruning is a quiet no-op.
        let file = std::fs::File::options().write(true).open(&backup).unwrap();
        file.set_modified(
            SystemTime::UNIX_EPOCH + Duration::from_millis(now - PLAINTEXT_BACKUP_RETENTION_MS - 1),
        )
        .unwrap();
        drop(file);
        assert!(store.prune_expired_plaintext_backup(now).unwrap());
        assert!(!backup.exists());
        assert!(!store.prune_expired_plaintext_backup(now).unwrap());
    }

    #[test]
    fn store_key_hex_parsing_is_strict() {
        let valid = hex::encode([0xAB_u8; STORE_KEY_BYTES]);
        assert!(StoreKey::from_hex(&valid).is_ok());
        // Length, case, and trailing bytes are all rejected.
        assert!(StoreKey::from_hex(&valid[..62]).is_err());
        assert!(StoreKey::from_hex(&valid.to_uppercase()).is_err());
        assert!(StoreKey::from_hex(&format!("{valid}\n")).is_err());
    }

    #[test]
    fn store_key_env_override_parses_only_canonical_hex() {
        // SAFETY: this is the only test touching KT_SIGNAL_STORE_KEY, and it
        // restores the unset state before returning.
        unsafe { std::env::remove_var(STORE_KEY_ENV) };
        assert!(store_key_from_env().unwrap().is_none());
        let valid = hex::encode([0x0F_u8; STORE_KEY_BYTES]);
        unsafe { std::env::set_var(STORE_KEY_ENV, &valid) };
        assert!(store_key_from_env().unwrap().is_some());
        unsafe { std::env::set_var(STORE_KEY_ENV, "not-hex") };
        assert!(matches!(store_key_from_env(), Err(StoreKeyError::Invalid)));
        unsafe { std::env::set_var(STORE_KEY_ENV, valid.to_uppercase()) };
        assert!(matches!(store_key_from_env(), Err(StoreKeyError::Invalid)));
        unsafe { std::env::remove_var(STORE_KEY_ENV) };
    }

    fn seeded_outgoing_message(
        store: &Store,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        sent_at: u64,
    ) {
        let message = MessageRecord {
            id: message_id.into(),
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            direction: "outgoing",
            sender_id: "self".into(),
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: None,
            text: Some("hello".into()),
            text_bytes: Some(5),
            text_truncated: false,
            text_retrievable: true,
            status: "sent",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&message, None, Some("hello"), false)
            .unwrap();
    }

    /// Contract 1.32 edit history: every peer edit snapshots the body it
    /// replaced, the per-message history stays capped at twenty ascending
    /// snapshots, and a row nobody edited carries none.
    #[test]
    fn edit_history_caps_at_twenty_entries_and_rides_edited_rows_only() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let message = MessageRecord {
            id: "m-edited".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 1000,
            received_at: Some(1000),
            text: Some("v0".into()),
            text_bytes: Some(2),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&message, None, Some("v0"), true)
            .unwrap();

        // A fresh row projects an empty (but present) edit history.
        let fresh = store
            .message_by_id(&account.id, &conversation.id, "m-edited")
            .unwrap()
            .unwrap();
        assert!(fresh.edits.is_empty());

        for round in 1..=22 {
            let body = format!("v{round}");
            let updated = store
                .apply_inbound_edit(
                    &account.id,
                    &conversation.id,
                    1000,
                    "peer",
                    "legacy-peer",
                    &body,
                    body.len() as u32,
                )
                .unwrap()
                .expect("each edit rewrites the seeded incoming row");
            assert_eq!(updated.text.as_deref(), Some(body.as_str()));
        }

        let row = store
            .message_by_id(&account.id, &conversation.id, "m-edited")
            .unwrap()
            .unwrap();
        assert_eq!(row.text.as_deref(), Some("v22"));
        assert!(row.edited_at.is_some());
        assert_eq!(
            row.edits.len(),
            20,
            "history keeps the newest twenty snapshots"
        );
        assert_eq!(
            row.edits[0].body, "v2",
            "pruning drops the oldest snapshots first"
        );
        assert_eq!(
            row.edits.last().unwrap().body,
            "v21",
            "the current body lives on the row, not in its own history"
        );
        assert!(
            row.edits
                .windows(2)
                .all(|pair| pair[0].edited_at <= pair[1].edited_at),
            "history is ascending, oldest first"
        );
    }

    #[test]
    fn reaction_aggregates_attach_to_message_projections() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let other_conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550102", "contact")
            .unwrap();
        seeded_outgoing_message(&store, &account.id, &conversation.id, "m-target", 100);
        seeded_outgoing_message(&store, &account.id, &other_conversation.id, "m-other", 100);

        // A fresh row projects an empty (but present) reaction list.
        let record = store
            .message_by_id(&account.id, &conversation.id, "m-target")
            .unwrap()
            .unwrap();
        assert!(record.reactions.is_empty());
        let page = store
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap();
        assert!(page.items[0].reactions.is_empty());

        // Two peers plus the linked account react 👍; another peer adds ❤️.
        // One reaction per actor per target (official semantics): a second
        // emoji from the same actor replaces the first. Reaction timestamps
        // are the connector's receive clock, so the ordering steps sleep
        // briefly to keep the newest-first actor order deterministic.
        store
            .upsert_reaction_event(
                &account.id,
                &conversation.id,
                "👍",
                100,
                "peer-1",
                false,
                Some("林菲菲"),
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(3));
        store
            .upsert_reaction_event(
                &account.id,
                &conversation.id,
                "👍",
                100,
                "peer-2",
                false,
                None,
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(3));
        store
            .upsert_reaction_event(
                &account.id,
                &conversation.id,
                "👍",
                100,
                &account.id,
                false,
                None,
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(3));
        store
            .upsert_reaction_event(
                &account.id,
                &conversation.id,
                "❤️",
                100,
                "peer-3",
                false,
                None,
            )
            .unwrap();

        let record = store
            .message_by_id(&account.id, &conversation.id, "m-target")
            .unwrap()
            .unwrap();
        assert_eq!(record.reactions.len(), 2);
        let thumbs = record
            .reactions
            .iter()
            .find(|summary| summary.emoji == "👍")
            .unwrap();
        assert_eq!(thumbs.count, 3);
        assert!(thumbs.mine);
        // Per-actor detail (contract 1.27): newest first, self flagged, peer
        // names carried when the envelope captured one. count and actors stay
        // in lockstep.
        assert_eq!(thumbs.actors.len(), 3);
        assert!(thumbs.actors[0].is_self);
        assert!(thumbs.actors[0].name.is_none());
        assert!(!thumbs.actors[1].is_self);
        assert_eq!(thumbs.actors[1].name, None);
        assert!(!thumbs.actors[2].is_self);
        assert_eq!(thumbs.actors[2].name.as_deref(), Some("林菲菲"));
        assert!(thumbs.actors[0].reacted_at >= thumbs.actors[1].reacted_at);
        assert!(thumbs.actors[1].reacted_at >= thumbs.actors[2].reacted_at);
        assert_eq!(thumbs.count as usize, thumbs.actors.len());
        let heart = record
            .reactions
            .iter()
            .find(|summary| summary.emoji == "❤️")
            .unwrap();
        assert_eq!(heart.count, 1);
        assert!(!heart.mine);
        assert_eq!(heart.actors.len(), 1);
        assert!(!heart.actors[0].is_self);

        // The same-timestamp row in another conversation stays untouched.
        let other = store
            .message_by_id(&account.id, &other_conversation.id, "m-other")
            .unwrap()
            .unwrap();
        assert!(other.reactions.is_empty());

        // The linked account removes its 👍: count drops, `mine` clears, and
        // the actor disappears from the per-actor detail (removal semantics).
        store
            .upsert_reaction_event(
                &account.id,
                &conversation.id,
                "👍",
                100,
                &account.id,
                true,
                None,
            )
            .unwrap();
        let record = store
            .message_by_id(&account.id, &conversation.id, "m-target")
            .unwrap()
            .unwrap();
        let thumbs = record
            .reactions
            .iter()
            .find(|summary| summary.emoji == "👍")
            .unwrap();
        assert_eq!(thumbs.count, 2);
        assert!(!thumbs.mine);
        assert_eq!(thumbs.actors.len(), 2);
        assert!(thumbs.actors.iter().all(|actor| !actor.is_self));
    }

    /// 摘要与列表 meta 必须按账户过滤：FK 允许出现「account_id 与会话归属
    /// 不一致」的孤儿消息行（历史缺陷可留下的残行），只按 conversation_id
    /// 过滤的旧 meta 查询会让更晚 sent_at 的幽灵 pending 行顶掉真末条——
    /// 对应线上「会话列表 pending、消息窗口 delivered」互相矛盾的成因类别。
    #[test]
    fn summary_and_list_meta_ignore_rows_from_other_accounts() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let other = store
            .upsert_account_from_signal("+15555550101", Some(2), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550199", "peer")
            .unwrap();
        let message = MessageRecord {
            id: "m-own".into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "outgoing",
            sender_id: "self".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 100,
            received_at: None,
            text: Some("delivered row".into()),
            text_bytes: None,
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store.insert_message(&message, None, None, false).unwrap();
        // FK 合法的幽灵行：账户存在、会话存在，但 account 归属另一账户。
        store
            .lock_conn()
            .unwrap()
            .execute(
                "INSERT INTO messages(id, account_id, conversation_id, direction, sender_id,
                                      sent_at, status)
                 VALUES('m-ghost', ?1, ?2, 'outgoing', 'self', 200, 'pending')",
                params![other.id, conversation.id],
            )
            .unwrap();

        let summary = store
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_message_status, Some("delivered"));
        assert_eq!(summary.last_message_direction, Some("outgoing"));
        // 错账户查询：conversations 行本身按账户过滤，直接不可见。
        assert!(
            store
                .conversation_summary(&other.id, &conversation.id)
                .unwrap()
                .is_none()
        );

        let listed = store
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        assert_eq!(listed.last_message_status, Some("delivered"));
    }

    /// Contract 1.38 (§4.37): the ACI→peer-key identity table records one row
    /// per (account, ACI), replaces on re-observation, and never leaks across
    /// accounts.
    #[test]
    fn peer_identities_upsert_replace_and_stay_per_account() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let other = store
            .upsert_account_from_signal("+15555550200", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        assert!(
            store
                .peer_keys_for_aci(&account.id, "aci-1")
                .unwrap()
                .is_empty()
        );
        store
            .upsert_peer_identity(&account.id, "aci-1", "+15555550101")
            .unwrap();
        assert_eq!(
            store.peer_keys_for_aci(&account.id, "aci-1").unwrap(),
            vec!["+15555550101".to_string()]
        );
        // The newest observation wins; one row per (account, ACI).
        store
            .upsert_peer_identity(&account.id, "aci-1", "+15555550199")
            .unwrap();
        assert_eq!(
            store.peer_keys_for_aci(&account.id, "aci-1").unwrap(),
            vec!["+15555550199".to_string()]
        );
        // Another account's mapping of the same ACI is independent.
        store
            .upsert_peer_identity(&other.id, "aci-1", "+15555550300")
            .unwrap();
        assert_eq!(
            store.peer_keys_for_aci(&account.id, "aci-1").unwrap(),
            vec!["+15555550199".to_string()]
        );
        assert_eq!(
            store.peer_keys_for_aci(&other.id, "aci-1").unwrap(),
            vec!["+15555550300".to_string()]
        );
    }

    /// Contract 1.38 (§4.37): the candidate ladder ranks incoming rows whose
    /// conversation matches a candidate peer key ahead of outgoing rows at
    /// the same timestamp, skips rows without a rich record, and ignores
    /// conversations outside the candidate set. An empty candidate set is
    /// answered without touching the database.
    #[test]
    fn view_once_open_candidates_rank_incoming_first() {
        use crate::engine::NormalizedRich;
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let peer = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let stranger = store
            .ensure_conversation(&account.id, "direct", "+15555550999", "contact")
            .unwrap();
        let base =
            |id: &str, conversation_id: &str, direction: &'static str, rich: bool| MessageRecord {
                id: id.into(),
                account_id: account.id.clone(),
                conversation_id: conversation_id.into(),
                direction,
                sender_id: if direction == "outgoing" {
                    "self".into()
                } else {
                    "peer".into()
                },
                sender_name: None,
                mentions_self: false,
                sent_at: 99,
                received_at: None,
                text: Some("burn me".into()),
                text_bytes: Some(7),
                text_truncated: false,
                text_retrievable: true,
                status: "delivered",
                client_request_id: None,
                quote_message_id: None,
                quote_snapshot: None,
                attachments: Vec::new(),
                rich: rich.then_some(NormalizedRich {
                    view_once: true,
                    ..NormalizedRich::default()
                }),
                edited_at: None,
                reactions: Vec::new(),
                edits: Vec::new(),
                delivered_at: None,
                read_at: None,
                viewed_at: None,
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                admin_deleted: false,
                server_guid: None,
            };
        for record in [
            base("m-incoming", &peer.id, "incoming", true),
            // Gets a non-marker rich_json from the UPDATE below: the caller's
            // marker filter must skip it even though the query surfaces it.
            base("m-plain-rich", &peer.id, "incoming", true),
            base("m-no-rich", &peer.id, "incoming", false),
            base("m-outgoing", &peer.id, "outgoing", true),
            base("m-stranger", &stranger.id, "incoming", true),
        ] {
            store
                .insert_message(&record, None, Some("burn me"), true)
                .unwrap();
        }
        // The non-view-once rich row: same shape, no marker — the caller's
        // filter (first candidate carrying the marker) must not select it.
        store
            .lock_conn()
            .unwrap()
            .execute(
                "UPDATE messages SET rich_json='{\"previews\":[]}' WHERE id='m-plain-rich'",
                [],
            )
            .unwrap();

        let candidates = store
            .view_once_open_candidates(
                &account.id,
                99,
                &["+15555550101".to_string(), "aci-peer".to_string()],
            )
            .unwrap();
        // Incoming rows first (id order), then the outgoing row. The
        // non-marker rich row and the rich-less row both surface here —
        // the marker filter is the caller's job (`apply_view_once_open_sync`
        // takes the first candidate carrying `viewOnce`/`viewOnceInvalid`),
        // and the stranger conversation's row never enters the scan.
        let ids: Vec<&str> = candidates.iter().map(|record| record.id.as_str()).collect();
        assert_eq!(ids, vec!["m-incoming", "m-plain-rich", "m-outgoing"]);

        // A candidate set that matches nothing (wrong timestamp, wrong keys)
        // and the empty set both answer empty.
        assert!(
            store
                .view_once_open_candidates(&account.id, 98, &["+15555550101".to_string()])
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .view_once_open_candidates(&account.id, 99, &[])
                .unwrap()
                .is_empty()
        );
    }

    /// Contract 1.38 (§4.37): the first burn clears the body bytes, stamps
    /// `openedAt` onto the rich record, and keeps the renderable metadata;
    /// a replay rewrites nothing and reports `changed=false` — exactly-one
    /// event and fan-out are the caller's contract, this is its foundation.
    #[test]
    fn burn_view_once_message_transitions_once_and_keeps_metadata() {
        use crate::engine::{NormalizedAttachment, NormalizedRich};
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let peer = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let record = MessageRecord {
            id: "m-view".into(),
            account_id: account.id.clone(),
            conversation_id: peer.id.clone(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 99,
            received_at: None,
            text: Some("self-destructing".into()),
            text_bytes: Some(16),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: vec![NormalizedAttachment {
                id: "att-view-1".into(),
                content_type: Some("image/jpeg".into()),
                filename: Some("snap.jpg".into()),
                size: Some(2048),
                width: Some(64),
                height: Some(32),
                is_voice_note: false,
                locator: None,
            }],
            rich: Some(NormalizedRich {
                view_once: true,
                ..NormalizedRich::default()
            }),
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        store
            .insert_message(&record, None, Some("self-destructing"), true)
            .unwrap();

        let (burned, changed) = store
            .burn_view_once_message(&account.id, &peer.id, "m-view", 12345)
            .unwrap()
            .expect("the row exists");
        assert!(changed);
        assert!(burned.text.is_none());
        assert_eq!(burned.text_bytes, None);
        assert_eq!(burned.attachments.len(), 1);
        assert_eq!(burned.attachments[0].id, "att-view-1");
        let rich = burned.rich.expect("rich survives the burn");
        assert_eq!(rich.view_once_opened_at, Some(12345));
        assert!(rich.view_once);

        let (replayed, changed) = store
            .burn_view_once_message(&account.id, &peer.id, "m-view", 99999)
            .unwrap()
            .expect("the row still exists");
        assert!(!changed);
        let rich = replayed.rich.expect("rich survives the replay");
        assert_eq!(rich.view_once_opened_at, Some(12345));
        assert_eq!(replayed.text, None);

        // A missing row answers None both for the burn and for its replay.
        assert!(
            store
                .burn_view_once_message(&account.id, &peer.id, "m-absent", 1)
                .unwrap()
                .is_none()
        );
    }

    // ---- Contract 1.42 (§4.41): disappearing-message metadata ----

    fn expire_record(id: &str, account_id: &str, conversation_id: &str) -> MessageRecord {
        MessageRecord {
            id: id.into(),
            account_id: account_id.into(),
            conversation_id: conversation_id.into(),
            direction: "incoming",
            sender_id: "peer".into(),
            sender_name: None,
            mentions_self: false,
            sent_at: 10,
            received_at: None,
            text: Some("vanishing".into()),
            text_bytes: Some(9),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        }
    }

    /// A v15 store upgrades in place: every 1.42 column lands additively on
    /// its table and the version stamp moves to 16 — pre-1.42 rows carry no
    /// timer state and are never backfilled.
    #[test]
    fn schema_v16_upgrade_adds_expire_columns() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '15');
             CREATE TABLE accounts (
               id TEXT PRIMARY KEY,
               signal_account TEXT NOT NULL UNIQUE,
               masked_address TEXT NOT NULL,
               display_name TEXT,
               state TEXT NOT NULL,
               linked_at INTEGER,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0,
               proxy_group TEXT NOT NULL DEFAULT 'default'
             );
             CREATE TABLE conversations (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               kind TEXT NOT NULL,
               peer_key TEXT NOT NULL,
               title TEXT NOT NULL,
               last_message_preview TEXT,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0,
               unread_mentions INTEGER NOT NULL DEFAULT 0,
               muted INTEGER NOT NULL DEFAULT 0,
               pinned INTEGER NOT NULL DEFAULT 0,
               UNIQUE(account_id, kind, peer_key),
               FOREIGN KEY(account_id) REFERENCES accounts(id)
             );
             CREATE TABLE history_import_stage_chats (
               account_id TEXT NOT NULL,
               id INTEGER NOT NULL,
               recipient_id INTEGER NOT NULL,
               PRIMARY KEY(account_id, id),
               FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
             );
             CREATE TABLE messages (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               sender_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL,
               received_at INTEGER,
               stored_at INTEGER,
               body TEXT,
               body_bytes INTEGER,
               body_truncated INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               client_request_id TEXT,
               quote_message_id TEXT,
               quote_snapshot TEXT,
               attachments_json TEXT,
               rich_json TEXT,
               sticker_json TEXT,
               edited_at INTEGER,
               sender_name TEXT,
               mentions_self INTEGER NOT NULL DEFAULT 0,
               delivered_at INTEGER,
               read_at INTEGER,
               admin_deleted INTEGER NOT NULL DEFAULT 0
             );",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "messages", "expire_seconds").unwrap());
        assert!(table_has_column(&store.conn(), "messages", "expire_start_at").unwrap());
        assert!(table_has_column(&store.conn(), "messages", "expire_at").unwrap());
        assert!(table_has_column(&store.conn(), "conversations", "expire_timer_seconds").unwrap());
        assert!(
            table_has_column(
                &store.conn(),
                "history_import_stage_chats",
                "expire_timer_ms"
            )
            .unwrap()
        );
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    /// Schema v17 (contract revision 1.49, §4.48): the `server_guid` column
    /// exists after the migration ladder and survives the reopen.
    #[test]
    fn schema_v17_upgrade_adds_server_guid_column() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '16');
             CREATE TABLE accounts (
               id TEXT PRIMARY KEY,
               signal_account TEXT NOT NULL UNIQUE,
               masked_address TEXT NOT NULL,
               display_name TEXT,
               state TEXT NOT NULL,
               linked_at INTEGER,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0,
               proxy_group TEXT NOT NULL DEFAULT 'default'
             );
             CREATE TABLE conversations (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               kind TEXT NOT NULL,
               peer_key TEXT NOT NULL,
               title TEXT NOT NULL,
               last_message_preview TEXT,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0,
               unread_mentions INTEGER NOT NULL DEFAULT 0,
               muted INTEGER NOT NULL DEFAULT 0,
               pinned INTEGER NOT NULL DEFAULT 0,
               expire_timer_seconds INTEGER,
               UNIQUE(account_id, kind, peer_key),
               FOREIGN KEY(account_id) REFERENCES accounts(id)
             );
             CREATE TABLE history_import_stage_chats (
               account_id TEXT NOT NULL,
               id INTEGER NOT NULL,
               recipient_id INTEGER NOT NULL,
               expire_timer_ms INTEGER,
               PRIMARY KEY(account_id, id),
               FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
             );
             CREATE TABLE messages (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               sender_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL,
               received_at INTEGER,
               stored_at INTEGER,
               body TEXT,
               body_bytes INTEGER,
               body_truncated INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               client_request_id TEXT,
               quote_message_id TEXT,
               quote_snapshot TEXT,
               attachments_json TEXT,
               rich_json TEXT,
               sticker_json TEXT,
               edited_at INTEGER,
               sender_name TEXT,
               mentions_self INTEGER NOT NULL DEFAULT 0,
               delivered_at INTEGER,
               read_at INTEGER,
               admin_deleted INTEGER NOT NULL DEFAULT 0,
               expire_seconds INTEGER,
               expire_start_at INTEGER,
               expire_at INTEGER
             );",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "messages", "server_guid").unwrap());
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    /// The live locator column round-trips (contract revision 1.61, §4.54):
    /// a receive envelope's transit faces land index-aligned beside the
    /// descriptors, a metadata-only envelope lands the all-None faces, and
    /// both read back through `live_attachment_locators` — the wire
    /// descriptor stays byte-identical either way.
    #[test]
    fn live_locator_column_round_trips_index_aligned() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550105", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550106", "Peer")
            .unwrap();
        let descriptor = |id: &str, locator: Option<crate::store::HistoryAttachmentLocator>| {
            crate::engine::NormalizedAttachment {
                id: id.into(),
                content_type: Some("image/jpeg".into()),
                filename: None,
                size: Some(2048),
                width: None,
                height: None,
                is_voice_note: false,
                locator,
            }
        };
        let full_face = crate::store::HistoryAttachmentLocator {
            key: Some("a2F0LWtleQ==".into()),
            cdn_key: Some("live-key".into()),
            cdn_number: Some(3),
            upload_timestamp: Some(1700000000000),
            digest: Some("ZGl-nonsense".into()),
            plaintext_hash: Some("aabb".into()),
        };
        let mut with_face = expire_record("m-loc", &account.id, &conversation.id);
        with_face.attachments = vec![
            descriptor("att-a", Some(full_face.clone())),
            // The hole stays index-aligned: a metadata-only sibling keeps its
            // own all-None face instead of shifting the row.
            descriptor("att-b", None),
        ];
        store
            .insert_message(&with_face, None, Some("face"), true)
            .unwrap();
        let faces = store
            .live_attachment_locators(&account.id, "m-loc")
            .unwrap();
        assert_eq!(faces.len(), 2);
        assert_eq!(faces[0], full_face);
        assert_eq!(faces[1], crate::store::HistoryAttachmentLocator::default());

        let plain = expire_record("m-plain", &account.id, &conversation.id);
        store
            .insert_message(&plain, None, Some("plain"), true)
            .unwrap();
        assert!(
            store
                .live_attachment_locators(&account.id, "m-plain")
                .unwrap()
                .is_empty()
        );
    }

    /// serverGuid round-trip (contract revision 1.49, §4.48): a live receive
    /// row persists the envelope's serverGuid, and `server_guids_by_ids`
    /// answers a 1:1 list — the recorded guid for the row, None for missing
    /// rows and None for guid-less rows.
    #[test]
    fn server_guid_round_trip_and_lookup() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550101", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550102", "Peer")
            .unwrap();
        let mut with_guid = expire_record("m1", &account.id, &conversation.id);
        with_guid.server_guid = Some("guid-one".to_string());
        store.insert_message(&with_guid, None, None, true).unwrap();
        let without_guid = expire_record("m2", &account.id, &conversation.id);
        store
            .insert_message(&without_guid, None, None, true)
            .unwrap();

        let fetched = store
            .message_by_id(&account.id, &conversation.id, "m1")
            .unwrap()
            .unwrap();
        assert_eq!(fetched.server_guid.as_deref(), Some("guid-one"));
        let plain = store
            .message_by_id(&account.id, &conversation.id, "m2")
            .unwrap()
            .unwrap();
        assert_eq!(plain.server_guid, None);

        let guids = store
            .server_guids_by_ids(
                &account.id,
                &conversation.id,
                &["m1".into(), "missing".into(), "m2".into()],
            )
            .unwrap();
        assert_eq!(guids, vec![Some("guid-one".to_string()), None, None]);
    }

    /// The sweep deadline picks the honest countdown start — the engine's
    /// start stamp when stated, else the receive time, else the store's own
    /// insert clock — and rows without a live timer never get one.
    #[test]
    fn expire_deadline_forms() {
        let mut record = expire_record("m1", "a", "c");
        let due = |record: &MessageRecord| expire_deadline(record, 1_000);
        assert_eq!(due(&record), None);
        record.expire_seconds = Some(0);
        assert_eq!(due(&record), None);

        record.expire_seconds = Some(60);
        record.expire_start_at = Some(500);
        assert_eq!(due(&record), Some(60_500));

        record.expire_start_at = None;
        record.received_at = Some(2_000);
        assert_eq!(due(&record), Some(62_000));

        record.received_at = None;
        assert_eq!(due(&record), Some(61_000));
    }

    /// Expire fields round-trip through the row: the stored keys read back
    /// verbatim, the sweep deadline is written at insert from the store clock
    /// when the timer is live, and rows without a timer keep every expire
    /// column NULL so the sweeper never sees them.
    #[test]
    fn expire_fields_round_trip_and_stamp_the_deadline() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();

        let mut live = expire_record("m-live", &account.id, &conversation.id);
        live.expire_seconds = Some(60);
        live.expire_start_at = Some(500);
        assert!(
            store
                .insert_message(&live, None, Some("vanishing"), true)
                .unwrap()
        );

        let mut stated_off = expire_record("m-off", &account.id, &conversation.id);
        stated_off.expire_seconds = Some(0);
        assert!(
            store
                .insert_message(&stated_off, None, None, false)
                .unwrap()
        );

        let page = store
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap();
        assert_eq!(page.items.len(), 2);
        let read_live = page
            .items
            .iter()
            .find(|row| row.id == "m-live")
            .expect("live row");
        assert_eq!(read_live.expire_seconds, Some(60));
        assert_eq!(read_live.expire_start_at, Some(500));
        let read_off = page
            .items
            .iter()
            .find(|row| row.id == "m-off")
            .expect("off row");
        assert_eq!(read_off.expire_seconds, Some(0));
        assert_eq!(read_off.expire_start_at, None);

        let observer = keyed_observer(&store);
        // The deadline derives from the engine-stated start (500 + 60s), not
        // from the store clock.
        let live_expire_at: Option<i64> = observer
            .query_row(
                "SELECT expire_at FROM messages WHERE id='m-live'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(live_expire_at, Some(60_500));
        let (off_expire_at, off_stored_expire): (Option<i64>, Option<i64>) = observer
            .query_row(
                "SELECT expire_at, expire_start_at FROM messages WHERE id='m-off'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(off_expire_at, None);
        assert_eq!(off_stored_expire, None);
    }

    /// One sweep pass deletes exactly the due rows (deadline at or before the
    /// clock), groups them per conversation, repairs each touched summary's
    /// newest-row projection and the account aggregates, and leaves not-yet-due
    /// and timerless rows alone. A quiet sweep answers empty.
    #[test]
    fn sweep_expired_messages_deletes_due_and_repairs_summaries() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let other = store
            .ensure_conversation(&account.id, "direct", "+15555550102", "contact")
            .unwrap();

        // Due in conversation A, oldest deadline first.
        let mut due_old = expire_record("m-due-old", &account.id, &conversation.id);
        due_old.sent_at = 100;
        due_old.expire_seconds = Some(60);
        due_old.expire_start_at = Some(1_000);
        let mut due_new = expire_record("m-due-new", &account.id, &conversation.id);
        due_new.sent_at = 200;
        due_new.expire_seconds = Some(60);
        due_new.expire_start_at = Some(20_000);
        // Not due: deadline far in the future.
        let mut waiting = expire_record("m-waiting", &account.id, &conversation.id);
        waiting.sent_at = 300;
        waiting.expire_seconds = Some(3_600);
        waiting.expire_start_at = Some(1_000_000_000_000);
        // Timerless: never swept, stays the summary's newest row.
        let mut keeper = expire_record("m-keeper", &account.id, &conversation.id);
        keeper.sent_at = 400;
        keeper.expire_seconds = None;
        // Due in conversation B: one conversation's event stays its own.
        let mut due_b = expire_record("m-due-b", &account.id, &other.id);
        due_b.expire_seconds = Some(1);
        due_b.expire_start_at = Some(1);

        for record in [&due_old, &due_new, &waiting, &keeper, &due_b] {
            assert!(
                store
                    .insert_message(
                        record,
                        None,
                        Some(record.text.as_deref().unwrap_or("")),
                        true
                    )
                    .unwrap()
            );
        }
        // The due rows arrived unread: after the sweep the unread badge may
        // only count what still exists.
        store
            .set_conversation_expire_timer(&account.id, &conversation.id, 60)
            .unwrap();

        let groups = store
            .sweep_expired_messages(1_000_000, EXPIRE_SWEEP_BATCH)
            .unwrap();
        assert_eq!(groups.len(), 2, "one group per touched conversation");
        let group_a = groups
            .iter()
            .find(|group| group.conversation_id == conversation.id)
            .expect("conversation A group");
        assert_eq!(group_a.account_id, account.id);
        assert_eq!(group_a.message_ids, vec!["m-due-old", "m-due-new"]);
        let group_b = groups
            .iter()
            .find(|group| group.conversation_id == other.id)
            .expect("conversation B group");
        assert_eq!(group_b.message_ids, vec!["m-due-b"]);

        let summary = store
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .expect("summary");
        assert_eq!(summary.last_message_at, Some(400));
        assert_eq!(summary.last_message_preview.as_deref(), Some("vanishing"));
        assert_eq!(
            summary.unread_count, 2,
            "unread clamps to the surviving rows"
        );
        assert_eq!(summary.expire_timer_seconds, Some(60));

        let remaining = store
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap();
        let ids: Vec<&str> = remaining.items.iter().map(|row| row.id.as_str()).collect();
        assert_eq!(ids, vec!["m-keeper", "m-waiting"]);

        // The follow-up pass is quiet: nothing else was due.
        assert!(
            store
                .sweep_expired_messages(1_000_000, EXPIRE_SWEEP_BATCH)
                .unwrap()
                .is_empty()
        );
    }

    /// The conversation timer mirror writes only on real transitions: the
    /// first write reports a change, rewriting the same value reports none,
    /// and an unknown conversation never invents one.
    #[test]
    fn set_conversation_expire_timer_reports_transitions() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();

        assert!(
            store
                .set_conversation_expire_timer(&account.id, &conversation.id, 86400)
                .unwrap()
        );
        let summary = store
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.expire_timer_seconds, Some(86400));

        assert!(
            !store
                .set_conversation_expire_timer(&account.id, &conversation.id, 86400)
                .unwrap()
        );
        assert!(
            store
                .set_conversation_expire_timer(&account.id, &conversation.id, 0)
                .unwrap()
        );
        let summary = store
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.expire_timer_seconds, Some(0));

        assert!(
            !store
                .set_conversation_expire_timer(&account.id, "conv-absent", 60)
                .unwrap()
        );
    }

    // ---- Contract 1.65 (§4.58): message-request receive projection ----

    fn mrr_record(
        id: &str,
        account_id: &str,
        conversation_id: &str,
        direction: &'static str,
        sent_at: u64,
    ) -> MessageRecord {
        MessageRecord {
            id: id.into(),
            account_id: account_id.into(),
            conversation_id: conversation_id.into(),
            direction,
            sender_id: if direction == "outgoing" {
                "self".into()
            } else {
                "peer".into()
            },
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        }
    }

    /// A v19 store upgrades in place: the additive `mrr_factors_json` column
    /// lands on `conversations`, existing rows stay NULL (never backfilled —
    /// the factor state a past engine answer carried is only known from that
    /// answer) and the version stamp moves to 20.
    #[test]
    fn schema_v20_upgrade_adds_mrr_factors_column() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '19');
             CREATE TABLE accounts (
               id TEXT PRIMARY KEY,
               signal_account TEXT NOT NULL UNIQUE,
               masked_address TEXT NOT NULL,
               display_name TEXT,
               state TEXT NOT NULL,
               linked_at INTEGER,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE conversations (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               kind TEXT NOT NULL,
               peer_key TEXT NOT NULL,
               title TEXT NOT NULL,
               last_message_preview TEXT,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0,
               unread_mentions INTEGER NOT NULL DEFAULT 0,
               muted INTEGER NOT NULL DEFAULT 0,
               pinned INTEGER NOT NULL DEFAULT 0,
               expire_timer_seconds INTEGER,
               UNIQUE(account_id, kind, peer_key)
             );
             INSERT INTO conversations(id, account_id, kind, peer_key, title)
               VALUES('conv-1', 'acc-1', 'direct', '+15555550101', 'peer');
             CREATE TABLE messages (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               sender_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL,
               received_at INTEGER,
               stored_at INTEGER,
               body TEXT,
               body_bytes INTEGER,
               body_truncated INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               client_request_id TEXT,
               quote_message_id TEXT,
               quote_snapshot TEXT,
               attachments_json TEXT,
               attachment_locators TEXT,
               live_attachment_locators TEXT,
               rich_json TEXT,
               sticker_json TEXT,
               expire_seconds INTEGER,
               expire_start_at INTEGER,
               expire_at INTEGER,
               server_guid TEXT,
               edited_at INTEGER,
               sender_name TEXT,
               mentions_self INTEGER NOT NULL DEFAULT 0,
               delivered_at INTEGER,
               read_at INTEGER,
               admin_deleted INTEGER NOT NULL DEFAULT 0
             );",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "conversations", "mrr_factors_json").unwrap());
        let stamped: Option<String> = store
            .conn()
            .query_row(
                "SELECT mrr_factors_json FROM conversations WHERE id='conv-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(stamped.is_none(), "history rows are never backfilled");
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    /// A v20 store upgrades in place (contract 1.94, §4.77): the additive
    /// `viewed_at` column lands on `messages`, existing rows stay NULL
    /// (never backfilled — a past viewed state is only known from the
    /// receipt or the local markViewed that stated it) and the version
    /// stamp moves to 21.
    #[test]
    fn schema_v21_upgrade_adds_viewed_at_column() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("connector.sqlite3");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES('schema_version', '20');
             CREATE TABLE accounts (
               id TEXT PRIMARY KEY,
               signal_account TEXT NOT NULL UNIQUE,
               masked_address TEXT NOT NULL,
               display_name TEXT,
               state TEXT NOT NULL,
               linked_at INTEGER,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE conversations (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               kind TEXT NOT NULL,
               peer_key TEXT NOT NULL,
               title TEXT NOT NULL,
               last_message_preview TEXT,
               last_message_at INTEGER,
               unread_count INTEGER NOT NULL DEFAULT 0,
               unread_mentions INTEGER NOT NULL DEFAULT 0,
               muted INTEGER NOT NULL DEFAULT 0,
               pinned INTEGER NOT NULL DEFAULT 0,
               expire_timer_seconds INTEGER,
               mrr_factors_json TEXT,
               UNIQUE(account_id, kind, peer_key)
             );
             CREATE TABLE messages (
               id TEXT PRIMARY KEY,
               account_id TEXT NOT NULL,
               conversation_id TEXT NOT NULL,
               direction TEXT NOT NULL,
               sender_id TEXT NOT NULL,
               sent_at INTEGER NOT NULL,
               received_at INTEGER,
               stored_at INTEGER,
               body TEXT,
               body_bytes INTEGER,
               body_truncated INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL,
               client_request_id TEXT,
               quote_message_id TEXT,
               quote_snapshot TEXT,
               attachments_json TEXT,
               attachment_locators TEXT,
               live_attachment_locators TEXT,
               rich_json TEXT,
               sticker_json TEXT,
               expire_seconds INTEGER,
               expire_start_at INTEGER,
               expire_at INTEGER,
               server_guid TEXT,
               edited_at INTEGER,
               sender_name TEXT,
               mentions_self INTEGER NOT NULL DEFAULT 0,
               delivered_at INTEGER,
               read_at INTEGER,
               admin_deleted INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO messages(id, account_id, conversation_id, direction, sender_id, sent_at, status)
               VALUES('m-1', 'acc-1', 'conv-1', 'outgoing', 'self', 400, 'read');",
        )
        .unwrap();
        drop(conn);

        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        assert!(table_has_column(&store.conn(), "messages", "viewed_at").unwrap());
        let stamped: Option<i64> = store
            .conn()
            .query_row("SELECT viewed_at FROM messages WHERE id='m-1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(stamped.is_none(), "history rows are never backfilled");
        let version: i64 = store
            .conn()
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    /// The VIEWED receipt face (contract 1.94, §4.77): it rides the read
    /// ladder and first-stamps `viewed_at` on every eligible row it hits —
    /// moved rows carry the stamp inside `moved`, an already-read unstamped
    /// row surfaces as `viewed_confirmed`, a pure replay produces neither,
    /// and a failed row never takes the stamp. Delivery and read receipts
    /// never stamp `viewed_at`.
    #[test]
    fn viewed_receipt_stamps_viewed_at_through_the_ladder() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let record = |id: &str, sent_at: u64, status: &'static str| MessageRecord {
            id: id.into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "outgoing",
            sender_id: "self".into(),
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: None,
            text: Some("voice note".into()),
            text_bytes: Some(10),
            text_truncated: false,
            text_retrievable: true,
            status,
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        for (id, sent_at, status) in [
            ("m-400", 400, "sent"),
            ("m-401", 401, "delivered"),
            ("m-402", 402, "sent"),
            ("m-403", 403, "failed"),
        ] {
            let row = record(id, sent_at, status);
            store
                .insert_message(&row, None, row.text.as_deref(), true)
                .unwrap();
        }
        let upgrade = |sent_at: u64, kind, when: u64| {
            store
                .upgrade_outgoing_receipts(&account.id, &conversation.id, &[sent_at], kind, when)
                .unwrap()
        };
        let stored = |id: &str| {
            store
                .message_by_id(&account.id, &conversation.id, id)
                .unwrap()
                .unwrap()
        };

        // A read receipt moves and never stamps viewed_at.
        let upgraded = upgrade(400, crate::engine::ReceiptKind::Read, 1000);
        assert_eq!(upgraded.moved.len(), 1);
        assert!(upgraded.viewed_confirmed.is_empty());
        assert_eq!(upgraded.moved[0].status, "read");
        assert_eq!(upgraded.moved[0].viewed_at, None);
        assert_eq!(stored("m-400").viewed_at, None);

        // A delivery receipt that moves nothing stamps nothing either.
        let upgraded = upgrade(401, crate::engine::ReceiptKind::Delivered, 1001);
        assert!(upgraded.moved.is_empty());
        assert!(upgraded.viewed_confirmed.is_empty());
        assert_eq!(stored("m-401").viewed_at, None);

        // m-402 reaches read through a plain read receipt first.
        let upgraded = upgrade(402, crate::engine::ReceiptKind::Read, 1002);
        assert_eq!(upgraded.moved.len(), 1);
        assert_eq!(stored("m-402").status, "read");
        assert_eq!(stored("m-402").read_at, Some(1002));

        // VIEWED on the already-read unstamped row: no status move, the
        // first viewed stamp surfaces as viewed_confirmed.
        let upgraded = upgrade(402, crate::engine::ReceiptKind::Viewed, 2000);
        assert!(upgraded.moved.is_empty());
        assert_eq!(upgraded.viewed_confirmed.len(), 1);
        let confirmed = &upgraded.viewed_confirmed[0];
        assert_eq!(confirmed.status, "read");
        assert_eq!(confirmed.read_at, Some(1002));
        assert_eq!(confirmed.viewed_at, Some(2000));
        assert_eq!(stored("m-402").viewed_at, Some(2000));

        // A pure replay (already read, already stamped) produces neither
        // face and never re-stamps.
        let upgraded = upgrade(402, crate::engine::ReceiptKind::Viewed, 3000);
        assert!(upgraded.moved.is_empty());
        assert!(upgraded.viewed_confirmed.is_empty());
        assert_eq!(stored("m-402").viewed_at, Some(2000), "first stamp wins");

        // A failed row never takes the stamp.
        let upgraded = upgrade(403, crate::engine::ReceiptKind::Viewed, 2001);
        assert!(upgraded.moved.is_empty());
        assert!(upgraded.viewed_confirmed.is_empty());
        assert_eq!(stored("m-403").viewed_at, None);

        // VIEWED on a delivered row rides the ladder to read and stamps
        // inside the moved face.
        let upgraded = upgrade(401, crate::engine::ReceiptKind::Viewed, 2002);
        assert_eq!(upgraded.moved.len(), 1);
        assert!(upgraded.viewed_confirmed.is_empty());
        let moved = &upgraded.moved[0];
        assert_eq!(moved.status, "read");
        assert_eq!(moved.read_at, Some(2002));
        assert_eq!(moved.viewed_at, Some(2002));
        assert_eq!(stored("m-401").viewed_at, Some(2002));

        // And a late VIEWED on the row the plain read receipt moved earlier
        // confirms its viewed stamp too.
        let upgraded = upgrade(400, crate::engine::ReceiptKind::Viewed, 2003);
        assert!(upgraded.moved.is_empty());
        assert_eq!(upgraded.viewed_confirmed.len(), 1);
        assert_eq!(stored("m-400").viewed_at, Some(2003));
    }

    /// `stamp_incoming_viewed` (contract 1.94, §4.77): the local
    /// markViewed half — first stamp wins, only incoming rows qualify, the
    /// selection is exactly the given ids.
    #[test]
    fn stamp_incoming_viewed_first_stamp_wins_and_stays_incoming_only() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let record = |id: &str, sent_at: u64, direction: &'static str| MessageRecord {
            id: id.into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction,
            sender_id: if direction == "incoming" {
                "peer".into()
            } else {
                "self".into()
            },
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: None,
            text: Some("voice note".into()),
            text_bytes: Some(10),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        for (id, sent_at, direction) in [
            ("m-in-1", 500, "incoming"),
            ("m-in-2", 501, "incoming"),
            ("m-in-3", 502, "incoming"),
            ("m-out", 503, "outgoing"),
        ] {
            let row = record(id, sent_at, direction);
            store
                .insert_message(&row, None, row.text.as_deref(), true)
                .unwrap();
        }
        let stored = |id: &str| {
            store
                .message_by_id(&account.id, &conversation.id, id)
                .unwrap()
                .unwrap()
        };

        // The selection stamps exactly its incoming rows; the outgoing row
        // in the list is skipped, the unselected incoming row stays NULL.
        let stamped = store
            .stamp_incoming_viewed(
                &account.id,
                &conversation.id,
                &[
                    "m-in-1".to_string(),
                    "m-in-2".to_string(),
                    "m-out".to_string(),
                ],
                5000,
            )
            .unwrap();
        assert_eq!(stamped, 2);
        assert_eq!(stored("m-in-1").viewed_at, Some(5000));
        assert_eq!(stored("m-in-2").viewed_at, Some(5000));
        assert_eq!(stored("m-in-3").viewed_at, None);
        assert_eq!(stored("m-out").viewed_at, None);

        // First stamp wins: a repeated stamp is a no-op.
        let restamped = store
            .stamp_incoming_viewed(&account.id, &conversation.id, &["m-in-1".to_string()], 6000)
            .unwrap();
        assert_eq!(restamped, 0);
        assert_eq!(stored("m-in-1").viewed_at, Some(5000));

        // An empty selection is the trivial no-op.
        let noop = store
            .stamp_incoming_viewed(&account.id, &conversation.id, &[], 6000)
            .unwrap();
        assert_eq!(noop, 0);
    }

    /// `stamp_incoming_read` + `incoming_rows_by_sent_at` (contract 1.100,
    /// §4.81): the cross-device read sync's store half — the (sender,
    /// timestamp) entries resolve to the matching incoming rows in caller
    /// order, the read stamp lands first-stamp-wins on incoming rows only,
    /// and a timestamp naming no row is simply absent.
    #[test]
    fn sync_read_store_half_resolves_timestamps_and_stamps_first_only() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        let record = |id: &str, sent_at: u64, direction: &'static str| MessageRecord {
            id: id.into(),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction,
            sender_id: if direction == "incoming" {
                "peer".into()
            } else {
                "self".into()
            },
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        for (id, sent_at, direction) in [
            ("m-in-1", 500, "incoming"),
            ("m-in-2", 501, "incoming"),
            ("m-out", 502, "outgoing"),
        ] {
            let row = record(id, sent_at, direction);
            store
                .insert_message(&row, None, row.text.as_deref(), true)
                .unwrap();
        }

        // The timestamp resolution follows the caller's order, dedupes, and
        // skips timestamps naming nothing incoming.
        let rows = store
            .incoming_rows_by_sent_at(&account.id, &conversation.id, &[501, 999, 500, 501])
            .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["m-in-2", "m-in-1"]
        );

        // The read stamp lands on the named incoming rows only; a stamp on
        // an outgoing row's timestamp never leaves this call with it in the
        // id list (the caller resolves ids from the same resolution).
        let stamped = store
            .stamp_incoming_read(
                &account.id,
                &conversation.id,
                &["m-in-1".to_string(), "m-out".to_string()],
                5000,
            )
            .unwrap();
        assert_eq!(stamped, 1);
        let stored = |id: &str| {
            store
                .message_by_id(&account.id, &conversation.id, id)
                .unwrap()
                .unwrap()
        };
        assert_eq!(stored("m-in-1").read_at, Some(5000));
        assert_eq!(stored("m-out").read_at, None);

        // First stamp wins: a replayed sync never re-stamps.
        let restamped = store
            .stamp_incoming_read(&account.id, &conversation.id, &["m-in-1".to_string()], 6000)
            .unwrap();
        assert_eq!(restamped, 0);
        assert_eq!(stored("m-in-1").read_at, Some(5000));

        // An empty list is the trivial no-op.
        let noop = store
            .stamp_incoming_read(&account.id, &conversation.id, &[], 6000)
            .unwrap();
        assert_eq!(noop, 0);
    }

    /// The official `isConversationAccepted` factor weights on the material
    /// this chain can see: self and empty direct rows are accepted; a
    /// direct row with only incoming rows is pending until a stamped factor
    /// (profileSharing, or the inverted hidden system-contact bit) or an
    /// outgoing row accepts it; a group row derives from its outgoing window
    /// alone (no engine read face for the group factors — the §4.58 holes).
    #[test]
    fn message_request_accepted_derives_official_factor_weights() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let direct = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        // An empty direct conversation is accepted (the official
        // isEmptyPrivateConvo weight).
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(summary.message_request_accepted);

        // Only-incoming window, no stamp: the honest pending.
        store
            .insert_message(
                &mrr_record("m-in", &account.id, &direct.id, "incoming", 10),
                None,
                None,
                false,
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(!summary.message_request_accepted);

        // profileSharing (the whitelisted bit) accepts it.
        assert!(
            store
                .set_conversation_mrr_factors_for_peer(
                    &account.id,
                    "+15555550101",
                    r#"{"profileSharing":true,"hidden":true,"blocked":false}"#,
                )
                .unwrap()
        );
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(summary.message_request_accepted);

        // hidden=false (a system contact) alone accepts it.
        assert!(
            store
                .set_conversation_mrr_factors_for_peer(
                    &account.id,
                    "+15555550101",
                    r#"{"hidden":false}"#,
                )
                .unwrap()
        );
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(summary.message_request_accepted);

        // hidden=true with no other factor flips back to pending.
        assert!(
            store
                .set_conversation_mrr_factors_for_peer(
                    &account.id,
                    "+15555550101",
                    r#"{"hidden":true}"#,
                )
                .unwrap()
        );
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(!summary.message_request_accepted);

        // An outgoing row accepts it regardless of the factors
        // (sentMessageCount > 0).
        store
            .insert_message(
                &mrr_record("m-out", &account.id, &direct.id, "outgoing", 20),
                None,
                None,
                false,
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(summary.message_request_accepted);

        // System-only additions change nothing (the window is data rows).
        store
            .insert_message(
                &mrr_record("m-sys", &account.id, &direct.id, "system", 30),
                None,
                None,
                false,
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(summary.message_request_accepted);

        // Self row: always accepted.
        let self_row = store
            .ensure_conversation(&account.id, "direct", "+15555550100", "self")
            .unwrap();
        store
            .insert_message(
                &mrr_record("m-self", &account.id, &self_row.id, "incoming", 5),
                None,
                None,
                false,
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &self_row.id)
            .unwrap()
            .unwrap();
        assert!(summary.is_self);
        assert!(summary.message_request_accepted);

        // Groups: only the outgoing window (no engine read face for the
        // group factors).
        let group = store
            .ensure_conversation(&account.id, "group", "group-key-1", "group")
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &group.id)
            .unwrap()
            .unwrap();
        assert!(!summary.message_request_accepted);
        store
            .insert_message(
                &mrr_record("m-g-in", &account.id, &group.id, "incoming", 10),
                None,
                None,
                false,
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &group.id)
            .unwrap()
            .unwrap();
        assert!(!summary.message_request_accepted);
        store
            .insert_message(
                &mrr_record("m-g-out", &account.id, &group.id, "outgoing", 20),
                None,
                None,
                false,
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &group.id)
            .unwrap()
            .unwrap();
        assert!(summary.message_request_accepted);
    }

    /// The stamp degrades honestly: a malformed payload parses as the
    /// all-absent default (never fails the read), and the blocked bit —
    /// carried for the request-row family — never factors the derivation
    /// (the official isConversationAccepted does not read it).
    #[test]
    fn message_request_stamp_degrades_and_blocked_never_factors() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let direct = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        store
            .insert_message(
                &mrr_record("m-in", &account.id, &direct.id, "incoming", 10),
                None,
                None,
                false,
            )
            .unwrap();

        // Blocked alone is not a factor.
        assert!(
            store
                .set_conversation_mrr_factors_for_peer(
                    &account.id,
                    "+15555550101",
                    r#"{"blocked":true}"#,
                )
                .unwrap()
        );
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(!summary.message_request_accepted);

        // Malformed stamp degrades to absent.
        assert!(
            store
                .set_conversation_mrr_factors_for_peer(&account.id, "+15555550101", "not-json",)
                .unwrap()
        );
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(!summary.message_request_accepted);
    }

    /// The stamp targets direct rows only (groups carry no trio) and the
    /// probe counter tracks exactly the derivability-critical rows — a
    /// window with data and no outgoing row, never stamped. Empty windows
    /// (accepted by the official empty-private-conversation weight) and
    /// outgoing-bearing windows (accepted by `sentMessageCount`) are already
    /// settled by the window weights alone, so they never owe a probe and a
    /// fully probed account reads zero — the enrich keeps its engine-call
    /// fast path.
    #[test]
    fn message_request_stamp_targets_direct_rows_and_probe_counter_tracks() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let direct = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        // The group row exists so the kind='direct' guard on the stamp has a
        // peer_key it could wrongly match if the guard were missing.
        let _group = store
            .ensure_conversation(&account.id, "group", "group-key-1", "group")
            .unwrap();

        // An empty window never owes a probe: the empty-private weight
        // already answers accepted regardless of the factors.
        assert_eq!(
            store
                .count_direct_conversations_without_mrr_factors(&account.id)
                .unwrap(),
            0
        );
        // An incoming-only data window is the derivability-critical shape.
        store
            .insert_message(
                &mrr_record("m-probe", &account.id, &direct.id, "incoming", 10),
                None,
                None,
                false,
            )
            .unwrap();
        assert_eq!(
            store
                .count_direct_conversations_without_mrr_factors(&account.id)
                .unwrap(),
            1
        );
        // Group peers never take the stamp.
        assert!(
            !store
                .set_conversation_mrr_factors_for_peer(
                    &account.id,
                    "group-key-1",
                    r#"{"hidden":false}"#,
                )
                .unwrap()
        );
        // The self row also stamps (its derivation short-circuits, but the
        // probe should go quiet).
        assert!(
            store
                .set_conversation_mrr_factors_for_peer(&account.id, "+15555550101", r#"{}"#,)
                .unwrap()
        );
        assert_eq!(
            store
                .count_direct_conversations_without_mrr_factors(&account.id)
                .unwrap(),
            0
        );
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        // The probed-absent `{}` marker: the factors stay inactive, so the
        // incoming-only window keeps its pending answer (the honest
        // derivation with the kt-engine read face).
        assert!(!summary.message_request_accepted);
    }

    /// The wire key is a bare always-serialized bool (the muted/pinned/isSelf
    /// discipline): both values ride every summary, so a desktop can treat
    /// key-present as authoritative and key-absent (pre-1.65 connector) as
    /// its local-derivation fallback.
    #[test]
    fn message_request_accepted_wire_key_always_serialized() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let pending = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        store
            .insert_message(
                &mrr_record("m-in", &account.id, &pending.id, "incoming", 10),
                None,
                None,
                false,
            )
            .unwrap();
        let accepted = store
            .ensure_conversation(&account.id, "direct", "+15555550102", "contact")
            .unwrap();

        for (conversation, expected) in [(&pending, false), (&accepted, true)] {
            let summary = store
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap();
            let wire = serde_json::to_value(&summary).unwrap();
            assert_eq!(wire["messageRequestAccepted"], expected);
        }
        let page = store.list_conversations(&account.id, 10, None).unwrap();
        let wire = serde_json::to_value(&page.items).unwrap();
        let rows = wire.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|row| row["messageRequestAccepted"].is_boolean())
        );
    }

    /// Contract 1.70 (§4.58 consume batch): the authoritative
    /// `acceptResponseType` face. The ACCEPT stamp accepts an
    /// incoming-only row the window weights alone leave pending (the
    /// official factor's first weight, isConversationAccepted.preload.ts
    /// :33-39); every other known member keeps the window-derived answer;
    /// the stamp merges with the enrich trio instead of clobbering it (and
    /// survives a re-stamp); non-direct rows are never stamped; and the
    /// stamped row stops owing the probe.
    #[test]
    fn message_request_accept_response_stamp_is_authoritative_and_merges() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let direct = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        store
            .insert_message(
                &mrr_record("m-in", &account.id, &direct.id, "incoming", 10),
                None,
                None,
                false,
            )
            .unwrap();
        // Base state: the incoming-only row is pending (no stamp at all).
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(!summary.message_request_accepted);

        // DELETE (member 2) stamps not-accepted: the row stays pending —
        // the window weights are untouched by a non-ACCEPT member.
        assert!(
            store
                .set_conversation_accept_response_factor(&account.id, &direct.id, false, 2,)
                .unwrap()
        );
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(!summary.message_request_accepted);

        // ACCEPT (member 1) is the authoritative weight: the row accepts
        // even though the window is still incoming-only (no outgoing row,
        // no trio factors).
        assert!(
            store
                .set_conversation_accept_response_factor(&account.id, &direct.id, true, 1,)
                .unwrap()
        );
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(summary.message_request_accepted);

        // The merge discipline: a re-stamp of the enrich trio keeps the
        // accept pair, and the trio itself is the enrich's truth verbatim.
        assert!(
            store
                .set_conversation_mrr_factors_for_peer(
                    &account.id,
                    "+15555550101",
                    r#"{"hidden":true,"profileSharing":false,"blocked":false}"#,
                )
                .unwrap()
        );
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(
            summary.message_request_accepted,
            "the accept stamp survives the enrich re-stamp"
        );
        // The reverse order also merges: an accept face fired after the trio
        // keeps the trio keys.
        assert!(
            store
                .set_conversation_mrr_factors_for_peer(&account.id, "+15555550101", r#"{}"#,)
                .unwrap()
        );
        assert!(
            store
                .set_conversation_accept_response_factor(&account.id, &direct.id, false, 4,)
                .unwrap()
        );
        // BLOCK_AND_DELETE (member 4) keeps the row pending (the window has
        // no outgoing row and the trio is empty).
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(!summary.message_request_accepted);

        // Non-direct rows are never stamped by the response face.
        let group = store
            .ensure_conversation(&account.id, "group", "group-key-1", "group")
            .unwrap();
        assert!(
            !store
                .set_conversation_accept_response_factor(&account.id, &group.id, true, 1,)
                .unwrap()
        );

        // The stamped row no longer owes the probe (the derivation is
        // settled by the authoritative stamp's mere presence within the
        // window weights).
        assert_eq!(
            store
                .count_direct_conversations_without_mrr_factors(&account.id)
                .unwrap(),
            0
        );
    }

    /// Contract 1.71 (§4.63): the whole-history outbound evidence bit the
    /// desktop matrix §18.4 data-plane gap names. True exactly when the row's
    /// whole history holds at least one outgoing data row — window- and
    /// newest-row-independent — and false on every honest absence (empty
    /// window, incoming-only window, system-only additions, unknown row).
    /// Read-time derivation only: the EXISTS reads, it never writes the
    /// counts.
    #[test]
    fn has_user_initiated_outbound_projects_whole_history_outgoing() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let direct = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();

        // Empty window: no store evidence at all — false.
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(!summary.has_user_initiated_outbound);

        // Incoming-only history: false (the newest row says nothing about
        // the whole history).
        store
            .insert_message(
                &mrr_record("m-in", &account.id, &direct.id, "incoming", 10),
                None,
                None,
                false,
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(!summary.has_user_initiated_outbound);

        // One outgoing row anywhere in history: true — even with a newer
        // incoming row as the newest row.
        store
            .insert_message(
                &mrr_record("m-out", &account.id, &direct.id, "outgoing", 20),
                None,
                None,
                false,
            )
            .unwrap();
        store
            .insert_message(
                &mrr_record("m-in-2", &account.id, &direct.id, "incoming", 30),
                None,
                None,
                false,
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(summary.has_user_initiated_outbound);

        // System-only additions stay invisible (the data-row wall the wire
        // lastMessageDirection draws).
        store
            .insert_message(
                &mrr_record("m-sys", &account.id, &direct.id, "system", 40),
                None,
                None,
                false,
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &direct.id)
            .unwrap()
            .unwrap();
        assert!(summary.has_user_initiated_outbound);

        // A group row answers from the same whole-history rule.
        let group = store
            .ensure_conversation(&account.id, "group", "group-key-1", "group")
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &group.id)
            .unwrap()
            .unwrap();
        assert!(!summary.has_user_initiated_outbound);
        store
            .insert_message(
                &mrr_record("m-g-out", &account.id, &group.id, "outgoing", 20),
                None,
                None,
                false,
            )
            .unwrap();
        let summary = store
            .conversation_summary(&account.id, &group.id)
            .unwrap()
            .unwrap();
        assert!(summary.has_user_initiated_outbound);

        // An unknown conversation id (row deleted mid-page, the tolerance the
        // meta lookups practice) answers false, never errors.
        assert!(!conversation_has_user_initiated_outbound(
            &store.conn(),
            &account.id,
            "conv-does-not-exist"
        ));
    }

    /// The wire key is a bare always-serialized bool on both projections (the
    /// muted/pinned/isSelf discipline), and the list page and the single-row
    /// summary cannot disagree about the same row.
    #[test]
    fn has_user_initiated_outbound_wire_key_always_serialized() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(test_store_key())).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let incoming = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        store
            .insert_message(
                &mrr_record("m-in", &account.id, &incoming.id, "incoming", 10),
                None,
                None,
                false,
            )
            .unwrap();
        let outgoing = store
            .ensure_conversation(&account.id, "direct", "+15555550102", "contact")
            .unwrap();
        store
            .insert_message(
                &mrr_record("m-out", &account.id, &outgoing.id, "outgoing", 11),
                None,
                None,
                false,
            )
            .unwrap();

        for (conversation, expected) in [(&incoming, false), (&outgoing, true)] {
            let summary = store
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap();
            let wire = serde_json::to_value(&summary).unwrap();
            assert_eq!(wire["hasUserInitiatedOutbound"], expected);
        }
        let page = store.list_conversations(&account.id, 10, None).unwrap();
        let wire = serde_json::to_value(&page.items).unwrap();
        let rows = wire.as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|row| row["hasUserInitiatedOutbound"].is_boolean())
        );
    }
}
