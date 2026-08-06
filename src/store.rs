// SPDX-License-Identifier: AGPL-3.0-only

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use thiserror::Error;

use crate::ids::{mask_address, random_id, stable_hash_id};

const SCHEMA_VERSION: i64 = 2;
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
    pub attachments: Vec<serde_json::Value>,
    pub status: &'static str,
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

pub struct Store {
    path: PathBuf,
    conn: Connection,
}

impl Store {
    pub fn open(state_dir: &Path) -> Result<Self, StoreError> {
        prepare_state_dir(state_dir)?;
        let path = state_dir.join("connector.sqlite3");
        let conn = Connection::open(&path).map_err(|_| StoreError::Unavailable)?;
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
            CREATE INDEX IF NOT EXISTS conversations_account_last_message
              ON conversations(account_id, last_message_at DESC, id DESC);
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

    /// Remove account and dependent rows from the connector store (local exit).
    pub fn delete_account_cascade(&self, account_id: &str) -> Result<(), StoreError> {
        if self.account_by_id(account_id)?.is_none() {
            return Err(StoreError::AccountNotFound);
        }
        self.conn
            .execute("DELETE FROM messages WHERE account_id=?1", params![account_id])
            .map_err(|_| StoreError::Unavailable)?;
        self.conn
            .execute(
                "DELETE FROM conversations WHERE account_id=?1",
                params![account_id],
            )
            .map_err(|_| StoreError::Unavailable)?;
        self.conn
            .execute("DELETE FROM accounts WHERE id=?1", params![account_id])
            .map_err(|_| StoreError::Unavailable)?;
        Ok(())
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
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, account_id, kind, title, last_message_preview, last_message_at,
                        unread_count, muted, pinned
                 FROM conversations
                 WHERE account_id=?1
                   AND (?2 IS NULL OR id < ?2)
                 ORDER BY last_message_at IS NULL, last_message_at DESC, id DESC
                 LIMIT ?3",
            )
            .map_err(|_| StoreError::Unavailable)?;
        let rows = stmt
            .query_map(params![account_id, cursor, fetch as i64], |row| {
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
            })
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
            self.conn
                .query_row(
                    "SELECT sent_at FROM messages WHERE id=?1 AND account_id=?2 AND conversation_id=?3",
                    params![before_id, account_id, conversation_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|_| StoreError::Unavailable)?
        } else {
            None
        };
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                        body, status, quote_message_id
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
                |row| {
                    Ok(MessageRecord {
                        id: row.get(0)?,
                        account_id: row.get(1)?,
                        conversation_id: row.get(2)?,
                        direction: static_direction(row.get::<_, String>(3)?),
                        sender_id: row.get(4)?,
                        sent_at: row.get::<_, i64>(5)? as u64,
                        received_at: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                        text: row.get(7)?,
                        attachments: Vec::new(),
                        status: static_status(row.get::<_, String>(8)?),
                        quote_message_id: row.get(9)?,
                    })
                },
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
                        body, status, quote_message_id
                 FROM messages WHERE account_id=?1 AND client_request_id=?2",
                params![account_id, client_request_id],
                |row| {
                    Ok(MessageRecord {
                        id: row.get(0)?,
                        account_id: row.get(1)?,
                        conversation_id: row.get(2)?,
                        direction: static_direction(row.get::<_, String>(3)?),
                        sender_id: row.get(4)?,
                        sent_at: row.get::<_, i64>(5)? as u64,
                        received_at: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                        text: row.get(7)?,
                        attachments: Vec::new(),
                        status: static_status(row.get::<_, String>(8)?),
                        quote_message_id: row.get(9)?,
                    })
                },
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)
    }

    pub fn insert_message(
        &self,
        message: &MessageRecord,
        client_request_id: Option<&str>,
        preview: Option<&str>,
        increment_unread: bool,
    ) -> Result<bool, StoreError> {
        let inserted = self
            .conn
            .execute(
                "INSERT OR IGNORE INTO messages(
                    id, account_id, conversation_id, direction, sender_id, sent_at, received_at,
                    body, status, client_request_id, quote_message_id
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    message.id,
                    message.account_id,
                    message.conversation_id,
                    message.direction,
                    message.sender_id,
                    message.sent_at as i64,
                    message.received_at.map(|v| v as i64),
                    message.text,
                    message.status,
                    client_request_id,
                    message.quote_message_id,
                ],
            )
            .map_err(|_| StoreError::Unavailable)?;
        if inserted == 0 {
            return Ok(false);
        }
        self.conn
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
        self.conn
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
        Ok(true)
    }

    /// Opening a chat: zero conversation unread and subtract from account total.
    pub fn clear_conversation_unread(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<u32, StoreError> {
        let prev: i64 = self
            .conn
            .query_row(
                "SELECT unread_count FROM conversations WHERE id=?1 AND account_id=?2",
                params![conversation_id, account_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)?
            .unwrap_or(0);
        if prev <= 0 {
            return Ok(0);
        }
        self.conn
            .execute(
                "UPDATE conversations SET unread_count=0 WHERE id=?1 AND account_id=?2",
                params![conversation_id, account_id],
            )
            .map_err(|_| StoreError::Unavailable)?;
        self.conn
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
                        body, status, quote_message_id
                 FROM messages WHERE id=?1",
                params![message_id],
                |row| {
                    Ok(MessageRecord {
                        id: row.get(0)?,
                        account_id: row.get(1)?,
                        conversation_id: row.get(2)?,
                        direction: static_direction(row.get::<_, String>(3)?),
                        sender_id: row.get(4)?,
                        sent_at: row.get::<_, i64>(5)? as u64,
                        received_at: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                        text: row.get(7)?,
                        attachments: Vec::new(),
                        status: static_status(row.get::<_, String>(8)?),
                        quote_message_id: row.get(9)?,
                    })
                },
            )
            .optional()
            .map_err(|_| StoreError::Unavailable)
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
    if current < 2 {
        // Older DBs created before display_name column.
        let _ = conn.execute(
            "ALTER TABLE accounts ADD COLUMN display_name TEXT",
            [],
        );
    }
    conn.execute(
        "INSERT INTO meta(key, value) VALUES('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![SCHEMA_VERSION.to_string()],
    )
    .map_err(|_| StoreError::Unavailable)?;
    Ok(())
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
        _ => "unknown",
    }
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
            attachments: Vec::new(),
            status: "sent",
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
        let page = store
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap();
        assert_eq!(page.items.len(), 1);
    }
}
