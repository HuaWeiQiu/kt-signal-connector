// SPDX-License-Identifier: AGPL-3.0-only

use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::Serialize;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

use crate::ids::{mask_address, random_id, stable_hash_id};

const SCHEMA_VERSION: i64 = 7;
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
  muted INTEGER NOT NULL DEFAULT 0,
  pinned INTEGER NOT NULL DEFAULT 0,
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
";
/// Retention: newest rows a conversation keeps regardless of age.
const MAX_MESSAGES_PER_CONVERSATION: i64 = 2_000;
/// Retention: age past which a message is no longer kept.
const MESSAGE_RETENTION_MS: i64 = 90 * 24 * 60 * 60 * 1_000;
/// Preview length ingest stores for a conversation's newest message.
const PREVIEW_CHARS: i64 = 120;
/// Marks a message cursor that carries its own sort key.
const MESSAGE_CURSOR_PREFIX: &str = "m1:";
/// Retention floor. Receive dedupe and send idempotency both answer from stored
/// rows, so recent history is never pruned no matter which rule selected it:
/// signal-cli may still replay an envelope, and a resend may still arrive.
const RETENTION_SAFETY_WINDOW_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
pub const DEFAULT_PAGE_LIMIT: u32 = 100;
pub const MAX_PAGE_LIMIT: u32 = 200;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_at: Option<u64>,
    pub unread_count: u32,
    pub muted: bool,
    pub pinned: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageRecord {
    pub id: String,
    pub account_id: String,
    pub conversation_id: String,
    pub direction: &'static str,
    pub sender_id: String,
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
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactSummary {
    pub id: String,
    pub kind: &'static str,
    pub peer_key: String,
    pub title: String,
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
            if let Some(current) = self.conversation_title(&existing.id)? {
                if title_should_upgrade(&current, title) {
                    let _ = self.set_conversation_title(&existing.id, title);
                }
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
        let mut stmt = conn
            .prepare(
                "SELECT id, account_id, kind, title, last_message_preview, last_message_at,
                        unread_count, muted, pinned
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
                    Ok(ConversationSummary {
                        id: row.get(0)?,
                        account_id: row.get(1)?,
                        kind: static_kind(row.get::<_, String>(2)?),
                        title: row.get(3)?,
                        last_message_preview: row.get(4)?,
                        last_message_at: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                        unread_count: row.get::<_, i64>(6)? as u32,
                        muted: row.get::<_, i64>(7)? != 0,
                        pinned: row.get::<_, i64>(8)? != 0,
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
                .map(|item| encode_conversation_cursor(account_id, item))
        } else {
            None
        };
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
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id
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
        let mut items = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        let next_cursor = if items.len() as u32 > limit {
            items.truncate(limit as usize);
            items
                .last()
                .map(|item| encode_message_cursor(account_id, conversation_id, item))
        } else {
            None
        };
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
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id
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
        self.lock_conn()?
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id
                 FROM messages WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                params![message_id, account_id, conversation_id],
                message_record_from_row,
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
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
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id
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
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO messages(
                    id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                    stored_at, body, body_bytes, body_truncated, status, client_request_id,
                    quote_message_id
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?14, ?8, ?9, ?10, ?11, ?12, ?13)",
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
                    // Retention counts how long this machine has kept a row, so
                    // it reads our clock here and never a peer's claimed time.
                    crate::link::now_ms() as i64,
                ],
            )
            .map_err(|error| StoreError::Unavailable(Some(error)))?;
        if inserted == 0 {
            transaction
                .commit()
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            return Ok(false);
        }
        let conversation_updated = transaction
            .execute(
                "UPDATE conversations
                 SET last_message_preview=?2,
                     last_message_at=?3,
                     unread_count = unread_count + ?4
                 WHERE id=?1",
                params![
                    message.conversation_id,
                    preview,
                    message.sent_at as i64,
                    if increment_unread { 1 } else { 0 }
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
                "UPDATE conversations SET unread_count=0 WHERE id=?1 AND account_id=?2",
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
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id
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
        conversation_id: &str,
    ) -> Result<Option<ConversationSummary>, StoreError> {
        self.lock_conn()?
            .query_row(
                "SELECT id, account_id, kind, title, last_message_preview, last_message_at,
                        unread_count, muted, pinned
                 FROM conversations WHERE id=?1",
                params![conversation_id],
                |row| {
                    Ok(ConversationSummary {
                        id: row.get(0)?,
                        account_id: row.get(1)?,
                        kind: static_kind(row.get::<_, String>(2)?),
                        title: row.get(3)?,
                        last_message_preview: row.get(4)?,
                        last_message_at: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                        unread_count: row.get::<_, i64>(6)? as u32,
                        muted: row.get::<_, i64>(7)? != 0,
                        pinned: row.get::<_, i64>(8)? != 0,
                    })
                },
            )
            .optional()
            .map_err(|error| StoreError::Unavailable(Some(error)))
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
        let mut stmt = conn
            .prepare(
                "SELECT id, kind, peer_key, title
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
                    Ok(ContactSummary {
                        id: row.get(0)?,
                        kind: static_contact_kind(row.get::<_, String>(1)?),
                        peer_key: row.get(2)?,
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
        {
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
                       )
                       WHERE held_since < ?1
                         AND status NOT IN ('pending', 'unknown')
                         AND (held_since < ?2 OR recency > ?3)
                       ORDER BY held_since
                       LIMIT ?4
                     )
                     RETURNING conversation_id",
                )
                .map_err(|error| StoreError::Unavailable(Some(error)))?;
            let deleted = delete
                .query_map(
                    params![
                        keep_after,
                        expired_before,
                        MAX_MESSAGES_PER_CONVERSATION,
                        max_messages
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
        "messages" => "PRAGMA table_info(messages)",
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
        _ => "unknown",
    }
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
            sent_at: 10,
            received_at: None,
            text: Some("hello".into()),
            text_bytes: Some(5),
            text_truncated: false,
            text_retrievable: true,

            status: "sent",
            client_request_id: Some("client-1".into()),
            quote_message_id: None,
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
            sent_at: 1,
            received_at: None,
            text: Some("same".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,

            status: "pending",
            client_request_id: Some("request-pending".into()),
            quote_message_id: None,
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
            sent_at: 10,
            received_at: Some(10),
            text: Some("hello".into()),
            text_bytes: Some(5),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
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
                .conversation_summary(&conversation.id)
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
            sent_at: 10,
            received_at: Some(10),
            text: Some("hello".into()),
            text_bytes: Some(5),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
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
                .conversation_summary(&conversation.id)
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
            sent_at: 10,
            received_at: Some(10),
            text: Some("control notice".into()),
            text_bytes: Some(14),
            text_truncated: false,
            text_retrievable: true,

            status: "system",
            client_request_id: None,
            quote_message_id: None,
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
            sent_at: 10,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,
            status: "pending",
            client_request_id: None,
            quote_message_id: None,
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
                sent_at,
                received_at: Some(sent_at),
                text: Some(id.into()),
                text_bytes: Some(id.len() as u32),
                text_truncated: false,
                text_retrievable: true,
                status: "delivered",
                client_request_id: None,
                quote_message_id: None,
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
            sent_at: 30,
            received_at: Some(30),
            text: Some("changed after cursor".into()),
            text_bytes: Some(20),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
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
            sent_at: 10,
            received_at: Some(10),
            text: Some("other".into()),
            text_bytes: Some(5),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
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
            sent_at: 10,
            received_at: Some(11),
            text: Some("delete me".into()),
            text_bytes: Some(9),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
        };
        store.insert_message(&message, None, None, false).unwrap();

        assert!(store.delete_account_cascade(&account.id).unwrap());
        assert!(!store.delete_account_cascade(&account.id).unwrap());
        assert!(store.account_by_id(&account.id).unwrap().is_none());
        assert!(store.list_accounts().unwrap().is_empty());
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
            sent_at: now - 10 * day,
            received_at: Some(now - 10 * day),
            text: Some("kept body".into()),
            text_bytes: Some(9),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
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
        };
        store
            .insert_message(&message, None, Some("body"), true)
            .unwrap();
        store
            .conn()
            .execute_batch(
                "ALTER TABLE messages DROP COLUMN stored_at;
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
                    sent_at: now - age,
                    received_at: Some(now - age),
                    text: Some("body".into()),
                    text_bytes: Some(4),
                    text_truncated: false,
                    text_retrievable: true,
                    status: "delivered",
                    client_request_id: None,
                    quote_message_id: None,
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
            sent_at: now - 400 * day,
            received_at: None,
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,

            status: "pending",
            client_request_id: Some("request-pending".into()),
            quote_message_id: None,
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
            sent_at: now - day,
            received_at: Some(now - day),
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
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
            sent_at: now - 20 * day,
            received_at: Some(now - 20 * day),
            text: Some("kept body".into()),
            text_bytes: Some(9),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
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
            sent_at: now - 300 * day,
            received_at: Some(now - 300 * day),
            text: Some("body".into()),
            text_bytes: Some(4),
            text_truncated: false,
            text_retrievable: true,

            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
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
}
