// SPDX-License-Identifier: AGPL-3.0-only

use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::Serialize;
use thiserror::Error;

use crate::ids::{mask_address, random_id, stable_hash_id};

const SCHEMA_VERSION: i64 = 4;
const MAX_COMPLETED_ACCOUNT_DELETE_OPERATIONS: i64 = 256;
pub const DEFAULT_PAGE_LIMIT: u32 = 100;
pub const MAX_PAGE_LIMIT: u32 = 200;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("connector state directory is invalid")]
    InvalidStateDir,
    #[error("connector store is unavailable")]
    Unavailable,
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
    pub attachments: Vec<serde_json::Value>,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_message_id: Option<String>,
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
}

#[derive(Clone, Debug)]
pub struct ConversationRow {
    pub id: String,
    pub account_id: String,
    pub kind: String,
    pub peer_key: String,
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
    conn: Connection,
}

impl Store {
    pub fn open(state_dir: &Path) -> Result<Self, StoreError> {
        prepare_state_dir(state_dir)?;
        let path = state_dir.join("connector.sqlite3");
        let conn = Connection::open(&path).map_err(|_| StoreError::Unavailable)?;
        conn.busy_timeout(Duration::from_millis(250))
            .map_err(|_| StoreError::Unavailable)?;
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
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
              unread_count INTEGER NOT NULL DEFAULT 0
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
            ",
        )
        .map_err(|_| StoreError::Unavailable)?;
        migrate_schema(&conn)?;
        Ok(Self { path, conn })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn upsert_account_from_signal(
        &self,
        signal_account: &str,
        linked_at: Option<u64>,
    ) -> Result<AccountSummary, StoreError> {
        if let Some(existing) = self.account_by_signal(signal_account)? {
            self.conn
                .execute(
                    "UPDATE accounts SET state='ready', linked_at=COALESCE(linked_at, ?2)
                     WHERE id=?1",
                    params![existing.id, linked_at.map(|v| v as i64)],
                )
                .map_err(|_| StoreError::Unavailable)?;
            return self
                .account_summary(&existing.id)?
                .ok_or(StoreError::AccountNotFound);
        }
        let id = random_id();
        let masked = mask_address(signal_account);
        self.conn
            .execute(
                "INSERT INTO accounts(id, signal_account, masked_address, display_name, state, linked_at, unread_count)
                 VALUES(?1, ?2, ?3, NULL, 'ready', ?4, 0)",
                params![id, signal_account, masked, linked_at.map(|v| v as i64)],
            )
            .map_err(|_| StoreError::Unavailable)?;
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
        self.conn
            .execute(
                "UPDATE accounts SET display_name=?2 WHERE id=?1",
                params![account_id, name],
            )
            .map_err(|_| StoreError::Unavailable)?;
        self.account_summary(account_id)?
            .ok_or(StoreError::AccountNotFound)
    }

    pub fn list_accounts(&self) -> Result<Vec<AccountSummary>, StoreError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, masked_address, display_name, state, linked_at, last_message_at, unread_count
                 FROM accounts ORDER BY linked_at IS NULL, linked_at DESC, id ASC",
            )
            .map_err(|_| StoreError::Unavailable)?;
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
                })
            })
            .map_err(|_| StoreError::Unavailable)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|_| StoreError::Unavailable)
    }

    pub fn account_by_id(&self, account_id: &str) -> Result<Option<AccountRow>, StoreError> {
        self.conn
            .query_row(
                "SELECT id, signal_account FROM accounts WHERE id=?1",
                params![account_id],
                |row| {
                    Ok(AccountRow {
                        id: row.get(0)?,
                        signal_account: row.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)
    }

    pub fn account_by_signal(
        &self,
        signal_account: &str,
    ) -> Result<Option<AccountRow>, StoreError> {
        self.conn
            .query_row(
                "SELECT id, signal_account FROM accounts WHERE signal_account=?1",
                params![signal_account],
                |row| {
                    Ok(AccountRow {
                        id: row.get(0)?,
                        signal_account: row.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)
    }

    /// Signal number of any linked account, used as a read-only liveness probe target.
    pub fn any_signal_account_number(&self) -> Result<Option<String>, StoreError> {
        self.conn
            .query_row("SELECT signal_account FROM accounts LIMIT 1", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|_| StoreError::Unavailable)
    }

    pub fn account_summary(&self, account_id: &str) -> Result<Option<AccountSummary>, StoreError> {
        self.conn
            .query_row(
                "SELECT id, masked_address, display_name, state, linked_at, last_message_at, unread_count
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
                    })
                },
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)
    }

    pub fn prepare_account_delete(
        &mut self,
        account_id: &str,
        operation_id: &str,
        now_ms: u64,
    ) -> Result<AccountDeletePlan, StoreError> {
        let transaction = self
            .conn
            .transaction()
            .map_err(|_| StoreError::Unavailable)?;
        let existing = transaction
            .query_row(
                "SELECT account_id, state FROM account_delete_operations WHERE operation_id=?1",
                params![operation_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)?;
        if let Some((existing_account_id, state)) = existing.as_ref() {
            if existing_account_id != account_id {
                return Err(StoreError::OperationConflict);
            }
            if state == "completed" {
                transaction.commit().map_err(|_| StoreError::Unavailable)?;
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
            .map_err(|_| StoreError::Unavailable)?;
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
                .map_err(|_| StoreError::Unavailable)?;
            prune_completed_account_deletes(&transaction)?;
            transaction.commit().map_err(|_| StoreError::Unavailable)?;
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
                        StoreError::Unavailable
                    }
                })?;
        }
        transaction.commit().map_err(|_| StoreError::Unavailable)?;
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
            .conn
            .execute(
                "UPDATE account_delete_operations
                 SET state='unknown', updated_at=?2
                 WHERE operation_id=?1 AND state!='completed'",
                params![operation_id, now_ms as i64],
            )
            .map_err(|_| StoreError::Unavailable)?;
        if changed == 0 {
            return Err(StoreError::OperationConflict);
        }
        Ok(())
    }

    /// Remove account and dependent rows from the connector store (local exit).
    pub fn complete_account_delete(
        &mut self,
        account_id: &str,
        operation_id: Option<&str>,
        now_ms: u64,
    ) -> Result<bool, StoreError> {
        let transaction = self
            .conn
            .transaction()
            .map_err(|_| StoreError::Unavailable)?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM accounts WHERE id=?1",
                params![account_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)?
            .is_some();
        if exists {
            transaction
                .execute(
                    "DELETE FROM messages WHERE account_id=?1",
                    params![account_id],
                )
                .map_err(|_| StoreError::Unavailable)?;
            transaction
                .execute(
                    "DELETE FROM conversations WHERE account_id=?1",
                    params![account_id],
                )
                .map_err(|_| StoreError::Unavailable)?;
            transaction
                .execute("DELETE FROM accounts WHERE id=?1", params![account_id])
                .map_err(|_| StoreError::Unavailable)?;
        }
        if let Some(operation_id) = operation_id {
            let changed = transaction
                .execute(
                    "UPDATE account_delete_operations
                     SET state='completed', updated_at=?3
                     WHERE operation_id=?1 AND account_id=?2",
                    params![operation_id, account_id, now_ms as i64],
                )
                .map_err(|_| StoreError::Unavailable)?;
            if changed == 0 {
                return Err(StoreError::OperationConflict);
            }
            prune_completed_account_deletes(&transaction)?;
        }
        transaction.commit().map_err(|_| StoreError::Unavailable)?;
        Ok(exists)
    }

    pub fn delete_account_cascade(&mut self, account_id: &str) -> Result<bool, StoreError> {
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
        self.conn
            .execute(
                "INSERT INTO conversations(id, account_id, kind, peer_key, title, unread_count, muted, pinned)
                 VALUES(?1, ?2, ?3, ?4, ?5, 0, 0, 0)",
                params![id, account_id, kind, peer_key, title],
            )
            .map_err(|_| StoreError::Unavailable)?;
        Ok(ConversationRow {
            id,
            account_id: account_id.to_string(),
            kind: kind.to_string(),
            peer_key: peer_key.to_string(),
        })
    }

    pub fn conversation_title(&self, conversation_id: &str) -> Result<Option<String>, StoreError> {
        self.conn
            .query_row(
                "SELECT title FROM conversations WHERE id=?1",
                params![conversation_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)
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
        self.conn
            .execute(
                "UPDATE conversations SET title=?2 WHERE id=?1",
                params![conversation_id, trimmed],
            )
            .map_err(|_| StoreError::Unavailable)?;
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
        self.conn
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
            .map_err(|_| StoreError::Unavailable)
    }

    pub fn conversation_by_peer(
        &self,
        account_id: &str,
        kind: &str,
        peer_key: &str,
    ) -> Result<Option<ConversationRow>, StoreError> {
        self.conn
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
            .map_err(|_| StoreError::Unavailable)
    }

    /// Direct chats that may still show a masked peer id as title.
    pub fn list_direct_peers_needing_title(
        &self,
        account_id: &str,
    ) -> Result<Vec<(String /* peer_key */, String /* title */)>, StoreError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT peer_key, title FROM conversations
                 WHERE account_id=?1 AND kind='direct'",
            )
            .map_err(|_| StoreError::Unavailable)?;
        let rows = stmt
            .query_map(params![account_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| StoreError::Unavailable)?;
        let mut out = Vec::new();
        for row in rows {
            let (peer, title) = row.map_err(|_| StoreError::Unavailable)?;
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
        let mut stmt = self
            .conn
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
            .map_err(|_| StoreError::Unavailable)?;
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
            .map_err(|_| StoreError::Unavailable)?;
        let mut items = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| StoreError::Unavailable)?;
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
        let before_sent_at: Option<i64> = if let Some(before_id) = before {
            let sent_at = self
                .conn
                .query_row(
                    "SELECT sent_at FROM messages WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                    params![before_id, account_id, conversation_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|_| StoreError::Unavailable)?;
            Some(sent_at.ok_or(StoreError::InvalidCursor)?)
        } else {
            None
        };
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id
                 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2
                   AND (?3 IS NULL OR sent_at < ?3 OR (sent_at = ?3 AND id < ?4))
                 ORDER BY sent_at DESC, id DESC
                 LIMIT ?5",
            )
            .map_err(|_| StoreError::Unavailable)?;
        let rows = stmt
            .query_map(
                params![
                    account_id,
                    conversation_id,
                    before_sent_at,
                    before,
                    fetch as i64
                ],
                message_record_from_row,
            )
            .map_err(|_| StoreError::Unavailable)?;
        let mut items = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| StoreError::Unavailable)?;
        let next_cursor = if items.len() as u32 > limit {
            items.truncate(limit as usize);
            items.last().map(|item| item.id.clone())
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
        self.conn
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id
                 FROM messages WHERE account_id=?1 AND client_request_id=?2",
                params![account_id, client_request_id],
                message_record_from_row,
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)
    }

    pub fn message_by_id(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<Option<MessageRecord>, StoreError> {
        self.conn
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id
                 FROM messages WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                params![message_id, account_id, conversation_id],
                message_record_from_row,
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)
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
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id
                 FROM messages
                 WHERE account_id=?1 AND conversation_id=?2 AND direction=?3 AND sent_at=?4
                   AND sender_id IN (?5, ?6)
                 ORDER BY id ASC LIMIT 2",
            )
            .map_err(|_| StoreError::Unavailable)?;
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
            .map_err(|_| StoreError::Unavailable)?;
        let first = rows
            .next()
            .transpose()
            .map_err(|_| StoreError::Unavailable)?;
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
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(|_| StoreError::Unavailable)?;
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO messages(
                    id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                    body, body_bytes, body_truncated, status, client_request_id, quote_message_id
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
                ],
            )
            .map_err(|_| StoreError::Unavailable)?;
        if inserted == 0 {
            transaction.commit().map_err(|_| StoreError::Unavailable)?;
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
            .map_err(|_| StoreError::Unavailable)?;
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
            .map_err(|_| StoreError::Unavailable)?;
        if account_updated != 1 {
            return Err(StoreError::AccountNotFound);
        }
        transaction.commit().map_err(|_| StoreError::Unavailable)?;
        Ok(true)
    }

    /// Opening a chat: zero conversation unread and subtract from account total.
    pub fn clear_conversation_unread(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<u32, StoreError> {
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(|_| StoreError::Unavailable)?;
        let prev: i64 = transaction
            .query_row(
                "SELECT unread_count FROM conversations WHERE id=?1 AND account_id=?2",
                params![conversation_id, account_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)?
            .unwrap_or(0);
        if prev <= 0 {
            transaction.commit().map_err(|_| StoreError::Unavailable)?;
            return Ok(0);
        }
        let conversation_updated = transaction
            .execute(
                "UPDATE conversations SET unread_count=0 WHERE id=?1 AND account_id=?2",
                params![conversation_id, account_id],
            )
            .map_err(|_| StoreError::Unavailable)?;
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
            .map_err(|_| StoreError::Unavailable)?;
        if account_updated != 1 {
            return Err(StoreError::AccountNotFound);
        }
        transaction.commit().map_err(|_| StoreError::Unavailable)?;
        Ok(prev as u32)
    }

    pub fn update_message_status(
        &self,
        message_id: &str,
        status: &str,
        sent_at: Option<u64>,
    ) -> Result<Option<MessageRecord>, StoreError> {
        self.conn
            .execute(
                "UPDATE messages SET status=?2, sent_at=COALESCE(?3, sent_at) WHERE id=?1",
                params![message_id, status, sent_at.map(|v| v as i64)],
            )
            .map_err(|_| StoreError::Unavailable)?;
        self.conn
            .query_row(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, body_bytes, body_truncated, status, quote_message_id, client_request_id
                 FROM messages WHERE id=?1",
                params![message_id],
                message_record_from_row,
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)
    }

    pub fn complete_outgoing_send(
        &self,
        message_id: &str,
        account_id: &str,
        conversation_id: &str,
        sent_at: u64,
    ) -> Result<Option<MessageRecord>, StoreError> {
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(|_| StoreError::Unavailable)?;
        let duplicate_ids = {
            let mut stmt = transaction
                .prepare(
                    "SELECT id FROM messages
                     WHERE account_id=?1 AND conversation_id=?2 AND direction='outgoing'
                       AND sent_at=?3 AND id<>?4 AND sender_id=?5
                       AND client_request_id IS NULL
                     ORDER BY id ASC LIMIT 2",
                )
                .map_err(|_| StoreError::Unavailable)?;
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
                .map_err(|_| StoreError::Unavailable)?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|_| StoreError::Unavailable)?
        };
        if duplicate_ids.len() == 1 {
            transaction
                .execute(
                    "DELETE FROM messages WHERE id=?1",
                    params![duplicate_ids[0]],
                )
                .map_err(|_| StoreError::Unavailable)?;
        }
        let updated = transaction
            .execute(
                "UPDATE messages SET status='sent', sent_at=?2
                 WHERE id=?1 AND account_id=?3 AND conversation_id=?4",
                params![message_id, sent_at as i64, account_id, conversation_id],
            )
            .map_err(|_| StoreError::Unavailable)?;
        if updated != 1 {
            return Ok(None);
        }
        transaction
            .execute(
                "UPDATE conversations SET last_message_at=?2 WHERE id=?1 AND account_id=?3",
                params![conversation_id, sent_at as i64, account_id],
            )
            .map_err(|_| StoreError::Unavailable)?;
        transaction
            .execute(
                "UPDATE accounts SET last_message_at=?2 WHERE id=?1",
                params![account_id, sent_at as i64],
            )
            .map_err(|_| StoreError::Unavailable)?;
        transaction.commit().map_err(|_| StoreError::Unavailable)?;
        self.message_by_id(account_id, conversation_id, message_id)
    }

    pub fn conversation_summary(
        &self,
        conversation_id: &str,
    ) -> Result<Option<ConversationSummary>, StoreError> {
        self.conn
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
            .map_err(|_| StoreError::Unavailable)
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
        .map_err(|_| StoreError::Unavailable)?;
    Ok(())
}

fn encode_conversation_cursor(account_id: &str, item: &ConversationSummary) -> String {
    match item.last_message_at {
        Some(last_message_at) => format!("v2:{account_id}:0:{last_message_at}:{}", item.id),
        None => format!("v2:{account_id}:1:0:{}", item.id),
    }
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

fn prepare_state_dir(path: &Path) -> Result<(), StoreError> {
    if !path.is_absolute() {
        return Err(StoreError::InvalidStateDir);
    }
    std::fs::create_dir_all(path).map_err(|_| StoreError::InvalidStateDir)?;
    let metadata = std::fs::symlink_metadata(path).map_err(|_| StoreError::InvalidStateDir)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(StoreError::InvalidStateDir);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != rustix::process::getuid().as_raw() {
            return Err(StoreError::InvalidStateDir);
        }
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| StoreError::InvalidStateDir)?;
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
        return Err(StoreError::Unavailable);
    }
    if current < 2 {
        // Older DBs created before display_name column.
        if !table_has_column(conn, "accounts", "display_name")? {
            conn.execute("ALTER TABLE accounts ADD COLUMN display_name TEXT", [])
                .map_err(|_| StoreError::Unavailable)?;
        }
    }
    if current < 4 {
        if !table_has_column(conn, "messages", "body_bytes")? {
            conn.execute("ALTER TABLE messages ADD COLUMN body_bytes INTEGER", [])
                .map_err(|_| StoreError::Unavailable)?;
        }
        if !table_has_column(conn, "messages", "body_truncated")? {
            conn.execute(
                "ALTER TABLE messages ADD COLUMN body_truncated INTEGER NOT NULL DEFAULT 0",
                [],
            )
            .map_err(|_| StoreError::Unavailable)?;
        }
    }
    conn.execute(
        "INSERT INTO meta(key, value) VALUES('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![SCHEMA_VERSION.to_string()],
    )
    .map_err(|_| StoreError::Unavailable)?;
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
        _ => return Err(StoreError::Unavailable),
    };
    let mut stmt = conn.prepare(sql).map_err(|_| StoreError::Unavailable)?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|_| StoreError::Unavailable)?;
    for column in columns {
        if column.map_err(|_| StoreError::Unavailable)? == expected_column {
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
        attachments: Vec::new(),
        status: static_status(row.get::<_, String>(10)?),
        client_request_id: row.get(12)?,
        quote_message_id: row.get(11)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn account_message_idempotency_and_pagination() {
        let temp = TempDir::new().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let store = Store::open(temp.path()).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1))
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
            attachments: Vec::new(),
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
        let store = Store::open(temp.path()).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1))
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
            attachments: Vec::new(),
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

        let completed = store
            .complete_outgoing_send("pending", &account.id, &conversation.id, 99)
            .unwrap()
            .unwrap();
        assert_eq!(completed.status, "sent");
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
        let store = Store::open(temp.path()).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1))
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", "+15555550101", "contact")
            .unwrap();
        store
            .conn
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
            attachments: Vec::new(),
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
        };

        assert!(matches!(
            store.insert_message(&message, None, Some("hello"), true),
            Err(StoreError::Unavailable),
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
        let store = Store::open(temp.path()).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1))
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
            attachments: Vec::new(),
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
        };
        store
            .insert_message(&message, None, Some("hello"), true)
            .unwrap();
        store
            .conn
            .execute_batch(
                "CREATE TRIGGER fail_account_unread
                 BEFORE UPDATE ON accounts
                 BEGIN SELECT RAISE(FAIL, 'controlled test failure'); END;",
            )
            .unwrap();

        assert!(matches!(
            store.clear_conversation_unread(&account.id, &conversation.id),
            Err(StoreError::Unavailable),
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
        let store = Store::open(temp.path()).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1))
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
            attachments: Vec::new(),
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
    fn conversation_cursor_follows_full_sort_key_without_skips() {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1))
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
                attachments: Vec::new(),
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
            .upsert_account_from_signal("+15555550101", Some(1))
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
            attachments: Vec::new(),
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
        let store = Store::open(temp.path()).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1))
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
            attachments: Vec::new(),
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
        };
        store
            .insert_message(&other_message, None, Some("other-message"), false)
            .unwrap();
        let other_account = store
            .upsert_account_from_signal("+15555550101", Some(1))
            .unwrap();

        assert!(matches!(
            store.list_messages(&account.id, &conversation.id, 10, Some("unknown-message")),
            Err(StoreError::InvalidCursor),
        ));
        assert!(matches!(
            store.list_messages(&account.id, &conversation.id, 10, Some("other-message")),
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
        let mut store = Store::open(temp.path()).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1))
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
            attachments: Vec::new(),
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
        let mut store = Store::open(temp.path()).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1))
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
        let mut store = Store::open(temp.path()).unwrap();
        let account = store
            .upsert_account_from_signal("+15555550100", Some(1))
            .unwrap();
        let other = store
            .upsert_account_from_signal("+15555550101", Some(2))
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
        let mut store = Store::open(temp.path()).unwrap();

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
            .conn
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

        let store = Store::open(temp.path()).unwrap();
        assert!(table_has_column(&store.conn, "messages", "body_bytes").unwrap());
        assert!(table_has_column(&store.conn, "messages", "body_truncated").unwrap());
        let version: i64 = store
            .conn
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
        let store = Store::open(temp.path()).unwrap();
        store
            .conn
            .execute("UPDATE meta SET value='999' WHERE key='schema_version'", [])
            .unwrap();
        drop(store);

        assert!(matches!(
            Store::open(temp.path()),
            Err(StoreError::Unavailable)
        ));
    }
}
