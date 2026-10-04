// SPDX-License-Identifier: AGPL-3.0-only

//! Link-time history import (ADR 0005 S3; contract revision 1.39,
//! implementation-plan §4.38/§6.6): the connector side of the engine's backup5
//! archive transfer.
//!
//! The engine (kt-signal-engine 0eedc8c) streams a receive-compatible NDJSON
//! intermediate representation to `history-import.ndjson` inside the account
//! store directory after a `backup5` link — best-effort; file presence is the
//! result signal and the account→directory mapping lives in the engine's
//! documented `engine-state.json` registry (compat §1, implementation-plan §5).
//! This module consumes that face: two bounded passes (pass 1 stages
//! recipient/chat identity scratch in fixed batches, pass 2 streams messages
//! in fixed batches) that write rows under the exact live-receive identity
//! discipline — the same `signal-message-v2` stable id over
//! `(account, conversation, direction, sent_at, sender)` with `sent_at` being
//! the archive's Signal timestamp — so an import run, a re-run, and live
//! envelopes of the same Signal message all collapse onto one row (§6.4).
//!
//! Nothing proportional to the archive is held in memory: the archive is read
//! as a line stream with a hard per-line cap, staged through per-account
//! scratch tables, and written one bounded transaction at a time (ADR 0002
//! bounded discipline). The engine-side dependency registered in §4.38 stands:
//! the S2 export does not yet project recipient service identity
//! (`aci`/`e164`/`masterKey`), so against the current engine build every run
//! completes with all chats skip-counted; the parser consumes those fields the
//! moment the engine adds them, with no further connector change.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;
use thiserror::Error;

use crate::engine::{NormalizedAttachment, NormalizedQuote, truncate_utf8_bytes};
use crate::ids::{mask_address, stable_hash_id};
use crate::service::{MAX_HOST_TEXT_PREVIEW_BYTES, MAX_INBOUND_TEXT_BYTES};
use crate::store::{
    HistoryImportInsert, HistoryStageRecipient, ImportCounters, MessageRecord, Store,
};

/// The engine's export artifact inside the account store directory.
pub const ARCHIVE_FILE_NAME: &str = "history-import.ndjson";
/// The engine's account registry (compat §1: `{version: 2, accounts: [{number, dir}]}`).
pub const ENGINE_REGISTRY_FILE: &str = "engine-state.json";
/// Per-line budget (§4.38 bounded discipline): a longer line is skip-counted,
/// never buffered. Corrupt or hostile archives cannot inflate memory.
pub const LINE_BYTES_CAP: usize = 1024 * 1024;
/// Message rows per import transaction; the store lock is released between
/// batches so live receives never queue behind an import.
pub const BATCH_MESSAGES: usize = 256;
/// Attachment descriptors per message, mirroring the live receive cap (§4.13).
const MAX_IMPORT_ATTACHMENTS: usize = 32;
/// Failed-run retry budget per account; supervisor restarts are the retry
/// clock (§4.38 orchestration). After the budget the run stays `failed` until
/// the account is deleted or re-linked.
pub const MAX_ATTEMPTS: u32 = 3;
/// How long after `linked_at` the face answers `pending` while no archive has
/// appeared — the engine's own transfer long-poll plus transfer budget lives
/// well inside this window. Past it the honest answer is `unavailable`
/// (`no-archive`): the engine's failure modes are indistinguishable from "the
/// phone had nothing to transfer".
pub const LINK_WAIT_MS: u64 = 10 * 60 * 1000;
/// The backup proto's mutually exclusive attachment flag for voice notes
/// (engine `message_attachment::Flag::VoiceMessage`).
const ATTACHMENT_FLAG_VOICE_MESSAGE: i64 = 1;
/// Author display names ride the same bound the live receive ladder applies
/// (engine-normalized `sourceName`, 64 chars).
const SENDER_NAME_CHARS: usize = 64;
/// Archive conversation titles are bounded before they reach
/// `ensure_conversation` (the live path inherits the engine's name bound;
/// the archive's names are unbounded).
const CONVERSATION_TITLE_BYTES: usize = 240;

#[derive(Debug, Error)]
pub enum ImportFailure {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("store: {0}")]
    Store(#[from] crate::store::StoreError),
}

impl ImportFailure {
    /// Short classification persisted on the import row (`errorClass`); the
    /// details never leave the process (no message content, no paths).
    pub fn class(&self) -> &'static str {
        match self {
            ImportFailure::Io(_) => "io",
            ImportFailure::Store(_) => "store",
        }
    }
}

/// Resolve the account's store directory from the engine registry. Fail-closed:
/// the registry must be the documented v2 shape and the recorded directory a
/// plain `accounts/<name>` relative path — no traversal components, nothing
/// outside the engine data dir we spawned.
pub fn engine_account_dir(data_dir: &Path, signal_account: &str) -> Option<PathBuf> {
    let raw = std::fs::read(data_dir.join(ENGINE_REGISTRY_FILE)).ok()?;
    let value: Value = serde_json::from_slice(&raw).ok()?;
    if value.get("version").and_then(Value::as_u64) != Some(2) {
        return None;
    }
    for entry in value.get("accounts")?.as_array()? {
        if entry.get("number").and_then(Value::as_str) != Some(signal_account) {
            continue;
        }
        let dir = entry.get("dir").and_then(Value::as_str)?;
        return valid_account_rel(dir).map(|rel| data_dir.join(rel));
    }
    None
}

/// The archive file path for one signal account, when the registry names it.
pub fn archive_path(data_dir: &Path, signal_account: &str) -> Option<PathBuf> {
    engine_account_dir(data_dir, signal_account).map(|dir| dir.join(ARCHIVE_FILE_NAME))
}

/// The presence signal the engine contract defines (§4.38): a non-empty
/// regular file. An absent file means the best-effort download did not
/// produce an archive.
pub fn archive_ready(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.len() > 0)
        .unwrap_or(false)
}

/// Only `accounts/<single-component>` passes; `..`, absolute paths, nested
/// paths, and empty names all fail closed.
fn valid_account_rel(dir: &str) -> Option<String> {
    if dir.is_empty() {
        return None;
    }
    let mut components = Path::new(dir).components();
    if components.next() != Some(Component::Normal("accounts".as_ref())) {
        return None;
    }
    match components.next() {
        Some(Component::Normal(_)) => (),
        _ => return None,
    }
    if components.next().is_some() {
        return None;
    }
    Some(dir.to_string())
}

#[derive(Debug, PartialEq)]
enum BoundedLine {
    Line(String),
    Overlong,
    Eof,
}

/// One line per call, hard-capped: `fill_buf` chunks are appended only while
/// under the cap; an over-cap line is consumed and reported as
/// [`BoundedLine::Overlong`] at its terminating newline.
fn read_bounded_line(reader: &mut impl BufRead, buf: &mut Vec<u8>) -> std::io::Result<BoundedLine> {
    buf.clear();
    let mut overflowing = false;
    loop {
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if available.is_empty() {
            return Ok(if overflowing {
                BoundedLine::Overlong
            } else if buf.is_empty() {
                BoundedLine::Eof
            } else {
                BoundedLine::Line(lossy(buf))
            });
        }
        match available.iter().position(|byte| *byte == b'\n') {
            Some(at) => {
                // The cap check owns the newline branch too: a line whose
                // overflow only shows up in its final chunk is still over.
                if !overflowing && buf.len() + at > LINE_BYTES_CAP {
                    overflowing = true;
                    buf.clear();
                }
                if !overflowing {
                    buf.extend_from_slice(&available[..at]);
                }
                reader.consume(at + 1);
                return Ok(if overflowing {
                    BoundedLine::Overlong
                } else {
                    BoundedLine::Line(lossy(buf))
                });
            }
            None => {
                let len = available.len();
                if !overflowing {
                    if buf.len() + len > LINE_BYTES_CAP {
                        overflowing = true;
                        buf.clear();
                    } else {
                        buf.extend_from_slice(available);
                    }
                }
                reader.consume(len);
            }
        }
    }
}

fn lossy(buf: &[u8]) -> String {
    String::from_utf8_lossy(buf).into_owned()
}

/// Import one archive end to end. The caller (supervisor) runs this on the
/// blocking pool. The store row is the run's honest ledger: `running` at
/// entry (attempt counter bumped), `completed`/`failed` with final counters
/// on every exit. Partially imported rows stay on a failed run — the identity
/// discipline makes the next attempt converge without duplicates.
pub fn run_import(
    store: &Arc<Store>,
    account_id: &str,
    signal_account: &str,
    archive: &Path,
) -> Result<ImportCounters, ImportFailure> {
    store.begin_history_import(account_id, crate::link::now_ms())?;
    // A crashed predecessor's scratch never leaks into this run: the stage
    // clears on entry, and the exit path below leaves the tables empty.
    if let Err(failure) = store
        .clear_history_stage(account_id)
        .map_err(ImportFailure::from)
    {
        let class = failure.class();
        let _ = store.complete_history_import(
            account_id,
            "failed",
            &ImportCounters::default(),
            Some(class),
            crate::link::now_ms(),
        );
        return Err(failure);
    }
    let mut counters = ImportCounters::default();
    let inner = pass_one(store, account_id, archive).and_then(|()| {
        Importer::new(store, account_id, signal_account, &mut counters).run(archive)
    });
    let _ = store.clear_history_stage(account_id);
    match inner {
        Ok(()) => {
            store.complete_history_import(
                account_id,
                "completed",
                &counters,
                None,
                crate::link::now_ms(),
            )?;
            Ok(counters)
        }
        Err(failure) => {
            let class = failure.class();
            let _ = store.complete_history_import(
                account_id,
                "failed",
                &counters,
                Some(class),
                crate::link::now_ms(),
            );
            Err(failure)
        }
    }
}

/// Pass 1: stage the recipient/chat identity scratch in fixed batches. Only
/// the identity fields are read; message frames and anything unknown are left
/// for pass 2's counters.
fn pass_one(store: &Arc<Store>, account_id: &str, archive: &Path) -> Result<(), ImportFailure> {
    let file = File::open(archive)?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut line_buf = Vec::with_capacity(8 * 1024);
    let mut recipients: Vec<HistoryStageRecipient> = Vec::with_capacity(BATCH_MESSAGES);
    let mut chats: Vec<(u64, u64)> = Vec::with_capacity(BATCH_MESSAGES);
    loop {
        let line = match read_bounded_line(&mut reader, &mut line_buf)? {
            BoundedLine::Eof => break,
            // Pass 2 owns the counters; an over-long line simply stages nothing.
            BoundedLine::Overlong => continue,
            BoundedLine::Line(line) => line,
        };
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        match value.get("type").and_then(Value::as_str) {
            Some("recipient") => {
                if let Some(recipient) = stage_recipient(&value) {
                    recipients.push(recipient);
                    if recipients.len() >= BATCH_MESSAGES {
                        store.stage_history_recipients(account_id, &recipients)?;
                        recipients.clear();
                    }
                }
            }
            Some("chat") => {
                if let Some(pair) = stage_chat(&value) {
                    chats.push(pair);
                    if chats.len() >= BATCH_MESSAGES {
                        store.stage_history_chats(account_id, &chats)?;
                        chats.clear();
                    }
                }
            }
            _ => {}
        }
    }
    store.stage_history_recipients(account_id, &recipients)?;
    store.stage_history_chats(account_id, &chats)?;
    Ok(())
}

/// One chat's resolved store identity, cached per run (pass 2): conversation
/// rows are ensured once per chat, never per message.
#[derive(Clone, Debug)]
struct ResolvedChat {
    conversation_id: String,
    kind: &'static str,
    peer_key: String,
}

/// One pending conversation pin, applied when its batch lands.
struct PendingPin {
    conversation_id: String,
    target_author: String,
    target_sent_at: u64,
}

/// Pass-2 driver: streams the archive's message frames and writes them one
/// bounded batch at a time (§4.38 bounded discipline). Between batches the
/// store lock is released, so live receives never queue behind an import.
struct Importer<'a> {
    store: &'a Arc<Store>,
    account_id: &'a str,
    signal_account: &'a str,
    chats: HashMap<u64, Option<ResolvedChat>>,
    batch: Vec<HistoryImportInsert>,
    pins: Vec<PendingPin>,
    counters: &'a mut ImportCounters,
}

impl<'a> Importer<'a> {
    fn new(
        store: &'a Arc<Store>,
        account_id: &'a str,
        signal_account: &'a str,
        counters: &'a mut ImportCounters,
    ) -> Self {
        Self {
            store,
            account_id,
            signal_account,
            chats: HashMap::new(),
            batch: Vec::with_capacity(BATCH_MESSAGES),
            pins: Vec::new(),
            counters,
        }
    }

    fn run(&mut self, archive: &Path) -> Result<(), ImportFailure> {
        let file = File::open(archive)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);
        let mut line_buf = Vec::with_capacity(8 * 1024);
        loop {
            match read_bounded_line(&mut reader, &mut line_buf)? {
                BoundedLine::Eof => break,
                // A corrupt/hostile line is counted, never fatal, never buffered.
                BoundedLine::Overlong => self.counters.skipped_lines += 1,
                BoundedLine::Line(line) => self.handle_line(&line)?,
            }
        }
        self.flush()
    }

    fn handle_line(&mut self, line: &str) -> Result<(), ImportFailure> {
        if line.trim().is_empty() {
            return Ok(());
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            self.counters.skipped_lines += 1;
            return Ok(());
        };
        match value.get("type").and_then(Value::as_str) {
            // Recipient/chat frames were staged by pass 1; they carry no rows.
            Some("recipient") | Some("chat") => {}
            Some("message") => self.handle_message(&value)?,
            _ => self.counters.skipped_lines += 1,
        }
        Ok(())
    }

    fn handle_message(&mut self, value: &Value) -> Result<(), ImportFailure> {
        let Some(chat_id) = value.get("chatId").and_then(Value::as_u64) else {
            self.counters.skipped_messages += 1;
            return Ok(());
        };
        // Signal's timestamp is the protocol identity (§6.4): without it the
        // row cannot join the live-receive dedupe discipline.
        let Some(sent_at) = value
            .get("dateSent")
            .and_then(Value::as_u64)
            .filter(|sent_at| *sent_at > 0)
        else {
            self.counters.skipped_messages += 1;
            return Ok(());
        };
        let remote_deleted = value
            .get("remoteDeleted")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let direction = match value.get("direction").and_then(Value::as_str) {
            Some("incoming") => "incoming",
            Some("outgoing") => "outgoing",
            // Direction-less backup items have no place in the two-sided model.
            _ => {
                self.counters.skipped_messages += 1;
                return Ok(());
            }
        };
        let Some(chat) = self.chat_for(chat_id)? else {
            // The unresolvable chat was skip-counted once at resolution time.
            return Ok(());
        };
        let (sender_id, alt_sender_id, sender_name, author_source) = if direction == "outgoing" {
            (self.account_id.to_string(), None, None, None)
        } else {
            let Some(author_id) = value.get("authorId").and_then(Value::as_u64) else {
                self.counters.skipped_messages += 1;
                return Ok(());
            };
            let Some(author) = self.store.history_recipient(self.account_id, author_id)? else {
                self.counters.skipped_messages += 1;
                return Ok(());
            };
            let Some(source) = recipient_source(&author, self.signal_account) else {
                self.counters.skipped_messages += 1;
                return Ok(());
            };
            let alt = recipient_alt_source(&author)
                .map(|alt| stable_hash_id(&[self.account_id, chat.kind, &alt]));
            let name =
                non_empty(&author.name).map(|name| truncate_utf8_bytes(name, SENDER_NAME_CHARS));
            (
                stable_hash_id(&[self.account_id, chat.kind, &source]),
                alt,
                name,
                Some(source),
            )
        };
        // Body presence: a tombstone carries none by definition; anything else
        // without text, a long-text pointer, or an attachment stores nothing.
        let text = value
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty());
        let has_long_text = value
            .get("longText")
            .is_some_and(|pointer| !pointer.is_null());
        let raw_attachments = value.get("attachments").and_then(Value::as_array);
        if !remote_deleted
            && text.is_none()
            && !has_long_text
            && raw_attachments.is_none_or(Vec::is_empty)
        {
            self.counters.skipped_messages += 1;
            return Ok(());
        }
        let message_id = stable_hash_id(&[
            "signal-message-v2",
            self.account_id,
            &chat.conversation_id,
            direction,
            &sent_at.to_string(),
            &sender_id,
        ]);
        // The receive size ladder (§4.38): oversized archive bodies keep a
        // bounded, not-retrievable preview — there is no engine copy to fetch;
        // a long-text pointer marks a body the metadata-only archive cannot
        // carry in full.
        let mut truncated = has_long_text;
        let stored_text = text.map(|raw| {
            if raw.len() > MAX_INBOUND_TEXT_BYTES {
                truncated = true;
                truncate_utf8_bytes(raw, MAX_HOST_TEXT_PREVIEW_BYTES)
            } else {
                raw.to_string()
            }
        });
        let quote = self.quote_snapshot(value)?;
        let quote_message_id = if let Some(quote) = &quote {
            self.quote_target(&chat, quote)?
        } else {
            None
        };
        let attachments = raw_attachments
            .map(|raws| self.import_attachments(raws, &message_id))
            .unwrap_or_default();
        let record = MessageRecord {
            id: message_id,
            account_id: self.account_id.to_string(),
            conversation_id: chat.conversation_id.clone(),
            direction,
            sender_id,
            sender_name: if direction == "incoming" {
                sender_name
            } else {
                None
            },
            // Mention resolution needs the normalized rich body, which the
            // metadata-only archive does not carry (§4.38).
            mentions_self: false,
            sent_at,
            received_at: Some(crate::link::now_ms()),
            text: stored_text,
            text_bytes: text.map(|raw| raw.len().min(u32::MAX as usize) as u32),
            text_truncated: truncated,
            text_retrievable: !truncated,
            status: if remote_deleted {
                "remote-deleted"
            } else if direction == "outgoing" {
                "sent"
            } else {
                "delivered"
            },
            client_request_id: None,
            quote_message_id,
            quote_snapshot: quote,
            attachments,
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            admin_deleted: false,
            sticker: None,
        };
        let preview = record
            .text
            .as_deref()
            .map(|text| text.chars().take(120).collect::<String>());
        self.batch.push(HistoryImportInsert {
            record,
            preview,
            alt_sender_id,
        });
        if value
            .get("pinnedAtTimestamp")
            .and_then(Value::as_u64)
            .is_some_and(|at| at > 0)
        {
            // The pin resolves by (conversation, sent_at); the author rides
            // the same identity form a live pin envelope carries.
            self.pins.push(PendingPin {
                conversation_id: chat.conversation_id.clone(),
                target_author: author_source.unwrap_or_else(|| self.signal_account.to_string()),
                target_sent_at: sent_at,
            });
        }
        if self.batch.len() >= BATCH_MESSAGES {
            self.flush()?;
        }
        Ok(())
    }

    /// Resolve one chat's store identity, once per run: the staged recipient
    /// maps onto a conversation through [`chat_identity`], the conversation
    /// is ensured in place (§6.5 skeleton reuse), and the result caches. An
    /// unresolvable chat is skip-counted exactly once; its messages then fall
    /// through the cached `None` without inflating per-message counters.
    fn chat_for(&mut self, chat_id: u64) -> Result<Option<ResolvedChat>, ImportFailure> {
        if let Some(cached) = self.chats.get(&chat_id) {
            return Ok(cached.clone());
        }
        let resolved = match self
            .store
            .history_chat_recipient(self.account_id, chat_id)?
        {
            Some(recipient) => match chat_identity(self.signal_account, &recipient) {
                Some((kind, peer_key)) => {
                    let title = match non_empty(&recipient.name) {
                        Some(name) => truncate_utf8_bytes(name, CONVERSATION_TITLE_BYTES),
                        None => mask_address(&peer_key),
                    };
                    let conversation =
                        self.store
                            .ensure_conversation(self.account_id, kind, &peer_key, &title)?;
                    Some(ResolvedChat {
                        conversation_id: conversation.id,
                        kind,
                        peer_key,
                    })
                }
                None => None,
            },
            None => None,
        };
        if resolved.is_none() {
            self.counters.skipped_chats += 1;
        }
        self.chats.insert(chat_id, resolved.clone());
        Ok(resolved)
    }

    /// The quote's bounded snapshot (§4.38): the archive target timestamp and
    /// author source, with the preview text on the receive ladder. A quote
    /// whose author or timestamp cannot be resolved drops the snapshot — the
    /// message itself still imports.
    fn quote_snapshot(&self, value: &Value) -> Result<Option<NormalizedQuote>, ImportFailure> {
        let Some(quote) = value.get("quote") else {
            return Ok(None);
        };
        let id = quote
            .get("targetSentTimestamp")
            .and_then(Value::as_u64)
            .filter(|id| *id > 0);
        let author = match quote.get("authorId").and_then(Value::as_u64) {
            Some(author_id) => match self.store.history_recipient(self.account_id, author_id)? {
                Some(recipient) => recipient_source(&recipient, self.signal_account),
                None => None,
            },
            None => None,
        };
        let (Some(id), Some(author)) = (id, author) else {
            return Ok(None);
        };
        let text = quote
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        Ok(Some(NormalizedQuote {
            id,
            author,
            text: truncate_utf8_bytes(text, MAX_HOST_TEXT_PREVIEW_BYTES),
        }))
    }

    /// Resolve the quoted row when it is already in the store — imported by
    /// this run or written by live traffic — so click-to-scroll works (§4.38).
    /// The snapshot always rides the row regardless; an unresolved target is
    /// not an import failure. The pending batch is searched first because its
    /// rows are the archive's own earlier messages and not yet flushed.
    fn quote_target(
        &self,
        chat: &ResolvedChat,
        quote: &NormalizedQuote,
    ) -> Result<Option<String>, ImportFailure> {
        let author_id = stable_hash_id(&[self.account_id, chat.kind, &quote.author]);
        // Direct-chat quotes name the conversation peer; the legacy slot
        // covers the peer's other identity form.
        let legacy_id = stable_hash_id(&[self.account_id, chat.kind, &chat.peer_key]);
        if let Some(row) = self.batch.iter().find(|row| {
            let record = &row.record;
            record.account_id == self.account_id
                && record.conversation_id == chat.conversation_id
                && record.sent_at == quote.id
                && match record.direction {
                    "incoming" => record.sender_id == author_id || record.sender_id == legacy_id,
                    "outgoing" => record.sender_id == self.account_id,
                    _ => false,
                }
        }) {
            return Ok(Some(row.record.id.clone()));
        }
        if let Some(record) = self.store.message_by_signal_identity(
            self.account_id,
            &chat.conversation_id,
            "incoming",
            quote.id,
            &author_id,
            &legacy_id,
        )? {
            return Ok(Some(record.id));
        }
        if let Some(record) = self.store.message_by_signal_identity(
            self.account_id,
            &chat.conversation_id,
            "outgoing",
            quote.id,
            self.account_id,
            "self",
        )? {
            return Ok(Some(record.id));
        }
        Ok(None)
    }

    /// Metadata-only attachment descriptors (§4.13 mirror): synthetic stable
    /// ids, no bytes behind them (§6.7 — opening one answers the normal
    /// not-found failure), capped at the live receive ceiling.
    fn import_attachments(&self, raws: &[Value], message_id: &str) -> Vec<NormalizedAttachment> {
        raws.iter()
            .take(MAX_IMPORT_ATTACHMENTS)
            .enumerate()
            .map(|(index, raw)| NormalizedAttachment {
                id: stable_hash_id(&["history-attachment", message_id, &index.to_string()]),
                content_type: raw
                    .get("contentType")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string),
                filename: raw
                    .get("fileName")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string),
                size: raw.get("size").and_then(Value::as_u64),
                width: None,
                height: None,
                is_voice_note: raw.get("flag").and_then(Value::as_i64)
                    == Some(ATTACHMENT_FLAG_VOICE_MESSAGE),
            })
            .collect()
    }

    /// Land one bounded batch: message rows in a single store transaction,
    /// then the batch's pins. Duplicates are the identity discipline working
    /// (a re-run, or a live envelope that stored the row first) and surface in
    /// the honest `skippedMessages` counter.
    fn flush(&mut self) -> Result<(), ImportFailure> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let outcome = self.store.insert_history_batch(&self.batch)?;
        self.counters.imported += outcome.inserted;
        self.counters.skipped_messages += outcome.duplicates;
        for pin in self.pins.drain(..) {
            self.store.upsert_conversation_pin(
                self.account_id,
                &pin.conversation_id,
                &pin.target_author,
                pin.target_sent_at,
                None,
            )?;
        }
        self.batch.clear();
        Ok(())
    }
}

/// One archive recipient's staged scratch row (`recipient {id, kind, name,
/// aci?, e164?, masterKey?}`). The identity fields are the engine-side S3
/// dependency registered in §4.38: absent today, consumed when present.
fn stage_recipient(value: &Value) -> Option<HistoryStageRecipient> {
    let id = value.get("id").and_then(Value::as_u64)?;
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("other")
        .to_string();
    let name = value
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let aci = value
        .get("aci")
        .and_then(Value::as_str)
        .filter(|text| is_uuid_shape(text))
        .map(|text| text.to_ascii_lowercase());
    let e164 = value.get("e164").and_then(normalize_e164);
    let master_key = value
        .get("masterKey")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string);
    Some(HistoryStageRecipient {
        id,
        kind,
        name,
        aci,
        e164,
        master_key,
    })
}

/// One archive chat's staged `chat → recipient` edge.
fn stage_chat(value: &Value) -> Option<(u64, u64)> {
    let id = value.get("id").and_then(Value::as_u64)?;
    let recipient_id = value.get("recipientId").and_then(Value::as_u64)?;
    Some((id, recipient_id))
}

/// The engine may project e164 as a JSON number or a string; both normalize
/// to the canonical `+<digits>` form the live receive path stores.
fn normalize_e164(value: &Value) -> Option<String> {
    let raw = match value {
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.trim().to_string(),
        _ => return None,
    };
    let digits = raw.strip_prefix('+').unwrap_or(&raw);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(format!("+{digits}"))
}

/// Lower-case hyphenated UUID shape (8-4-4-4-12 hex). Uppercase hex is
/// accepted and normalized by the caller.
fn is_uuid_shape(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    bytes.iter().enumerate().all(|(index, byte)| match index {
        8 | 13 | 18 | 23 => *byte == b'-',
        _ => byte.is_ascii_hexdigit(),
    })
}

/// The store conversation identity for one archive recipient (§4.38): groups
/// attach through their master key (the same base64 form the live receive
/// path carries as `groupId`), contacts through the number (or ACI when the
/// archive knows no number), and the self chat onto the account's own number.
fn chat_identity(
    signal_account: &str,
    recipient: &HistoryStageRecipient,
) -> Option<(&'static str, String)> {
    match recipient.kind.as_str() {
        "group" => recipient
            .master_key
            .clone()
            .filter(|key| !key.is_empty())
            .map(|key| ("group", key)),
        "self" => Some(("direct", signal_account.to_string())),
        "contact" => Some((
            "direct",
            recipient.e164.clone().or_else(|| recipient.aci.clone())?,
        )),
        _ => None,
    }
}

/// The sender source a live envelope would carry for this recipient: the
/// number when known, else the ACI; the self recipient is the account's own
/// number. `None` means the archive names no usable identity.
fn recipient_source(recipient: &HistoryStageRecipient, signal_account: &str) -> Option<String> {
    match recipient.kind.as_str() {
        "self" => Some(signal_account.to_string()),
        _ => recipient.e164.clone().or_else(|| recipient.aci.clone()),
    }
}

/// The other identity form, for the batch writer's number/ACI dual dedupe:
/// a number-form source alternates to the ACI (and vice versa for the self
/// recipient); a sole form has no alternate.
fn recipient_alt_source(recipient: &HistoryStageRecipient) -> Option<String> {
    match recipient.kind.as_str() {
        "self" => recipient.aci.clone(),
        _ if recipient.e164.is_some() => recipient.aci.clone(),
        _ => None,
    }
}

fn non_empty(text: &str) -> Option<&str> {
    let trimmed = text.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_PROXY_GROUP_ID;
    use crate::store::StoreKey;
    use tempfile::TempDir;

    const TEST_KEY_BYTES: [u8; 32] = [0x5A; 32];
    const SIGNAL_ACCOUNT: &str = "+15555550100";
    const ALICE_ACI: &str = "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b";
    const ALICE_E164: &str = "+15550000001";
    const GROUP_MASTER_KEY: &str = "b3BzLW1hc3Rlci1rZXk=";

    fn test_store(dir: &Path) -> Arc<Store> {
        Arc::new(Store::open(dir, Some(StoreKey::from_bytes(TEST_KEY_BYTES))).unwrap())
    }

    /// Engine data dir with the documented v2 registry plus one account
    /// directory; returns the archive path.
    fn seed_engine(temp: &TempDir) -> PathBuf {
        let engine = temp.path().join("engine");
        let account_dir = engine.join("accounts").join("lk-abc");
        std::fs::create_dir_all(&account_dir).unwrap();
        std::fs::write(
            engine.join(ENGINE_REGISTRY_FILE),
            format!(
                r#"{{"version":2,"accounts":[{{"number":"{SIGNAL_ACCOUNT}","dir":"accounts/lk-abc"}}]}}"#
            ),
        )
        .unwrap();
        account_dir.join(ARCHIVE_FILE_NAME)
    }

    fn write_archive(path: &Path, lines: &[String]) {
        let body = lines.join("\n");
        std::fs::write(path, format!("{body}\n")).unwrap();
    }

    fn alice() -> Value {
        serde_json::json!({
            "type": "recipient", "id": 1, "kind": "contact", "name": "Alice",
            "aci": ALICE_ACI, "e164": ALICE_E164
        })
    }

    fn group() -> Value {
        serde_json::json!({
            "type": "recipient", "id": 2, "kind": "group", "name": "Ops",
            "masterKey": GROUP_MASTER_KEY
        })
    }

    fn standard_archive() -> Vec<String> {
        let mut lines: Vec<String> = vec![
            alice(),
            group(),
            serde_json::json!({"type": "recipient", "id": 3, "kind": "self", "name": ""}),
            serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}),
            serde_json::json!({"type": "chat", "id": 11, "recipientId": 2}),
            serde_json::json!({"type": "chat", "id": 12, "recipientId": 3}),
            serde_json::json!({
                "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                "direction": "incoming", "text": "hello"
            }),
            serde_json::json!({
                "type": "message", "chatId": 10, "authorId": 3, "dateSent": 2000,
                "direction": "outgoing", "text": "hi",
                "quote": {"targetSentTimestamp": 1000, "authorId": 1, "text": "hello"}
            }),
            serde_json::json!({
                "type": "message", "chatId": 10, "authorId": 1, "dateSent": 3000,
                "direction": "incoming", "text": "pinned one", "pinnedAtTimestamp": 4000
            }),
            serde_json::json!({
                "type": "message", "chatId": 10, "authorId": 1, "dateSent": 5000,
                "direction": "incoming", "remoteDeleted": true
            }),
            serde_json::json!({
                "type": "message", "chatId": 10, "authorId": 1, "dateSent": 6000,
                "direction": "incoming", "text": "pic",
                "attachments": [
                    {"contentType": "image/png", "fileName": "p.png", "flag": 0, "size": 12},
                    {"contentType": "audio/aac", "fileName": "v.aac", "flag": 1, "size": 9}
                ]
            }),
            serde_json::json!({
                "type": "message", "chatId": 11, "authorId": 1, "dateSent": 7000,
                "direction": "incoming", "text": "group msg"
            }),
            serde_json::json!({
                "type": "message", "chatId": 11, "authorId": 1, "dateSent": 7500,
                "direction": "incoming", "text": "a".repeat(200_000)
            }),
            // Skip classes: direction-less, missing dateSent, body-less.
            serde_json::json!({
                "type": "message", "chatId": 10, "authorId": 1, "dateSent": 8000,
                "direction": "directionless", "text": "no direction"
            }),
            serde_json::json!({
                "type": "message", "chatId": 10, "authorId": 1,
                "direction": "incoming", "text": "no timestamp"
            }),
            serde_json::json!({
                "type": "message", "chatId": 10, "authorId": 1, "dateSent": 9000,
                "direction": "incoming"
            }),
            // An orphan chat: the message references a chat no frame staged.
            serde_json::json!({
                "type": "message", "chatId": 99, "authorId": 1, "dateSent": 9500,
                "direction": "incoming", "text": "orphan"
            }),
            // Counter classes for lines: unknown type and malformed JSON.
            serde_json::json!({"type": "weird", "x": 1}),
        ]
        .into_iter()
        .map(|value| value.to_string())
        .collect();
        lines.push("not json at all".to_string());
        lines
    }

    fn alice_conversation_id(account_id: &str) -> String {
        stable_hash_id(&[account_id, "direct", ALICE_E164])
    }

    fn group_conversation_id(account_id: &str) -> String {
        stable_hash_id(&[account_id, "group", GROUP_MASTER_KEY])
    }

    #[test]
    fn registry_parsing_and_path_safety() {
        let temp = TempDir::new().unwrap();
        let archive = seed_engine(&temp);
        let engine = temp.path().join("engine");
        assert_eq!(
            engine_account_dir(&engine, SIGNAL_ACCOUNT),
            Some(engine.join("accounts").join("lk-abc"))
        );
        assert_eq!(archive_path(&engine, SIGNAL_ACCOUNT), Some(archive.clone()));
        assert!(!archive_ready(&archive), "no archive written yet");
        std::fs::write(&archive, "x").unwrap();
        assert!(archive_ready(&archive));
        // An empty file is not an archive (the engine writes the artifact at
        // first frame, so an empty file is a hostile/failed transfer signal).
        std::fs::write(&archive, "").unwrap();
        assert!(!archive_ready(&archive));

        // Unknown number, wrong version, traversal directories, broken JSON:
        // all fail closed to None.
        assert_eq!(engine_account_dir(&engine, "+19999999999"), None);
        std::fs::write(
            engine.join(ENGINE_REGISTRY_FILE),
            r#"{"version":1,"accounts":[{"number":"+15555550100","dir":"accounts/lk-abc"}]}"#,
        )
        .unwrap();
        assert_eq!(engine_account_dir(&engine, SIGNAL_ACCOUNT), None);
        std::fs::write(
            engine.join(ENGINE_REGISTRY_FILE),
            r#"{"version":2,"accounts":[{"number":"+15555550100","dir":"../escape"}]}"#,
        )
        .unwrap();
        assert_eq!(engine_account_dir(&engine, SIGNAL_ACCOUNT), None);
        std::fs::write(engine.join(ENGINE_REGISTRY_FILE), "{not json").unwrap();
        assert_eq!(engine_account_dir(&engine, SIGNAL_ACCOUNT), None);
    }

    #[test]
    fn account_relative_paths_fail_closed() {
        assert!(valid_account_rel("accounts/lk-abc").is_some());
        assert!(valid_account_rel("accounts/a/b").is_none());
        assert!(valid_account_rel("../accounts/lk-abc").is_none());
        assert!(valid_account_rel("/accounts/lk-abc").is_none());
        assert!(valid_account_rel("accounts/").is_none());
        assert!(valid_account_rel("accounts").is_none());
        assert!(valid_account_rel("").is_none());
    }

    #[test]
    fn bounded_line_reader_handles_cap_and_edges() {
        let mut input: Vec<u8> = b"short\n".to_vec();
        input.extend(std::iter::repeat_n(b'a', LINE_BYTES_CAP + 16));
        input.push(b'\n');
        input.extend(b"h\xc3\xa9llo\nno-trailing-newline");
        let mut reader = BufReader::new(input.as_slice());
        let mut buf = Vec::with_capacity(1024);
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf).unwrap(),
            BoundedLine::Line("short".into())
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf).unwrap(),
            BoundedLine::Overlong
        );
        // The reader resynchronized after the over-long line.
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf).unwrap(),
            BoundedLine::Line("héllo".into())
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf).unwrap(),
            BoundedLine::Line("no-trailing-newline".into())
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf).unwrap(),
            BoundedLine::Eof
        );
    }

    #[test]
    fn identity_normalization() {
        assert_eq!(
            normalize_e164(&serde_json::json!(15550000001_u64)),
            Some(ALICE_E164.to_string())
        );
        assert_eq!(
            normalize_e164(&serde_json::json!(" +15550000001 ")),
            Some(ALICE_E164.to_string())
        );
        assert_eq!(normalize_e164(&serde_json::json!("55a")), None);
        assert!(is_uuid_shape(ALICE_ACI));
        assert!(is_uuid_shape(&ALICE_ACI.to_ascii_uppercase()));
        assert!(!is_uuid_shape("6e08f0b61c2d4e5f8a9b0c1d2e3f4a5b"));
        assert!(!is_uuid_shape("6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5"));

        let recipient = stage_recipient(&serde_json::json!({
            "id": 1, "kind": "contact", "name": " Alice ", "aci": ALICE_ACI.to_ascii_uppercase(),
            "e164": 15550000001_u64
        }))
        .unwrap();
        assert_eq!(recipient.aci.as_deref(), Some(ALICE_ACI));
        assert_eq!(recipient.e164.as_deref(), Some(ALICE_E164));
        assert_eq!(
            recipient_source(&recipient, SIGNAL_ACCOUNT),
            Some(ALICE_E164.to_string())
        );
        assert_eq!(
            recipient_alt_source(&recipient),
            Some(ALICE_ACI.to_string())
        );

        // ACI-only recipient: source falls back to the ACI, no alternate.
        let aci_only = stage_recipient(&serde_json::json!({
            "id": 4, "kind": "contact", "name": "", "aci": ALICE_ACI
        }))
        .unwrap();
        assert_eq!(
            recipient_source(&aci_only, SIGNAL_ACCOUNT),
            Some(ALICE_ACI.to_string())
        );
        assert_eq!(recipient_alt_source(&aci_only), None);

        // The current engine shape (kind/name only) stages fine, resolves nothing.
        let bare = stage_recipient(&serde_json::json!({"id": 5, "kind": "contact", "name": "Bob"}))
            .unwrap();
        assert_eq!(recipient_source(&bare, SIGNAL_ACCOUNT), None);
        assert_eq!(chat_identity(SIGNAL_ACCOUNT, &bare), None);
        assert!(stage_recipient(&serde_json::json!({"kind": "contact"})).is_none());

        let staged_group = stage_recipient(&group()).unwrap();
        assert_eq!(
            chat_identity(SIGNAL_ACCOUNT, &staged_group),
            Some(("group", GROUP_MASTER_KEY.to_string()))
        );
        let self_recipient =
            stage_recipient(&serde_json::json!({"id": 3, "kind": "self", "name": ""})).unwrap();
        assert_eq!(
            chat_identity(SIGNAL_ACCOUNT, &self_recipient),
            Some(("direct", SIGNAL_ACCOUNT.to_string()))
        );
        assert_eq!(
            stage_chat(&serde_json::json!({"id": 10, "recipientId": 1})),
            Some((10, 1))
        );
        assert_eq!(stage_chat(&serde_json::json!({"id": 10})), None);
    }

    #[test]
    fn import_lands_rows_under_live_identity_and_counts_skips_honestly() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        // §6.5 skeleton reuse: a contacts-sync conversation is filled in place.
        let skeleton = store
            .ensure_conversation(&account.id, "direct", ALICE_E164, "8fc***01")
            .unwrap();
        let archive = seed_engine(&temp);
        write_archive(&archive, &standard_archive());

        let counters = run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive).unwrap();
        assert_eq!(
            counters,
            ImportCounters {
                imported: 7,
                skipped_lines: 2,
                skipped_chats: 1,
                skipped_messages: 3,
            }
        );
        let row = store.history_import_row(&account.id).unwrap().unwrap();
        assert_eq!(row.state, "completed");
        assert_eq!(row.attempts, 1);
        assert_eq!(row.imported_messages, 7);
        assert_eq!(row.error_class, None);
        // The staging scratch rests empty between runs.
        assert!(
            store
                .history_chat_recipient(&account.id, 10)
                .unwrap()
                .is_none()
        );

        // The skeleton conversation is reused, never duplicated.
        let alice_conversation = alice_conversation_id(&account.id);
        assert_eq!(skeleton.id, alice_conversation);
        let incoming = store
            .message_by_signal_identity(
                &account.id,
                &alice_conversation,
                "incoming",
                1000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("imported incoming row");
        assert_eq!(incoming.text.as_deref(), Some("hello"));
        assert_eq!(incoming.status, "delivered");
        assert_eq!(incoming.sender_name.as_deref(), Some("Alice"));
        assert!(!incoming.mentions_self);

        // Outgoing sender is the account; the quote resolves to the incoming row.
        let outgoing = store
            .message_by_signal_identity(
                &account.id,
                &alice_conversation,
                "outgoing",
                2000,
                &account.id,
                "self",
            )
            .unwrap()
            .expect("imported outgoing row");
        assert_eq!(outgoing.status, "sent");
        assert_eq!(
            outgoing.quote_message_id.as_deref(),
            Some(incoming.id.as_str())
        );
        let quote = outgoing.quote_snapshot.expect("quote snapshot");
        assert_eq!(quote.id, 1000);
        assert_eq!(quote.author, ALICE_E164);
        assert_eq!(quote.text, "hello");

        // The pin landed against the imported row.
        let summary = store
            .conversation_summary(&account.id, &alice_conversation)
            .unwrap()
            .unwrap();
        let pinned = summary.pinned_message.expect("imported pin");
        assert_eq!(pinned.target_sent_timestamp, 3000);
        assert_eq!(pinned.target_author, ALICE_E164);

        // Remote-deleted tombstone keeps the direction, drops the body.
        let tombstone = store
            .message_by_signal_identity(
                &account.id,
                &alice_conversation,
                "incoming",
                5000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("tombstone row");
        assert_eq!(tombstone.status, "remote-deleted");
        assert_eq!(tombstone.text, None);

        // Attachment metadata: synthetic ids, voice-note flag, no bytes.
        let with_attachments = store
            .message_by_signal_identity(
                &account.id,
                &alice_conversation,
                "incoming",
                6000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("attachment row");
        assert_eq!(with_attachments.attachments.len(), 2);
        assert_eq!(
            with_attachments.attachments[0].id,
            stable_hash_id(&["history-attachment", &with_attachments.id, "0"])
        );
        assert_eq!(
            with_attachments.attachments[0].content_type.as_deref(),
            Some("image/png")
        );
        assert_eq!(with_attachments.attachments[0].size, Some(12));
        assert!(!with_attachments.attachments[0].is_voice_note);
        assert!(with_attachments.attachments[1].is_voice_note);

        // Group chat through the master key; group sender carries the group kind.
        let group_conversation = group_conversation_id(&account.id);
        let group_message = store
            .message_by_signal_identity(
                &account.id,
                &group_conversation,
                "incoming",
                7000,
                &stable_hash_id(&[&account.id, "group", ALICE_E164]),
                &stable_hash_id(&[&account.id, "group", ALICE_ACI]),
            )
            .unwrap()
            .expect("group row");
        assert_eq!(group_message.text.as_deref(), Some("group msg"));

        // Oversized archive text follows the receive ladder.
        let oversized = store
            .message_by_signal_identity(
                &account.id,
                &group_conversation,
                "incoming",
                7500,
                &stable_hash_id(&[&account.id, "group", ALICE_E164]),
                &stable_hash_id(&[&account.id, "group", ALICE_ACI]),
            )
            .unwrap()
            .expect("oversized row");
        assert_eq!(
            oversized.text.as_ref().map(String::len),
            Some(MAX_HOST_TEXT_PREVIEW_BYTES)
        );
        assert_eq!(oversized.text_bytes, Some(200_000));
        assert!(oversized.text_truncated);
        assert!(!oversized.text_retrievable);

        // Imports never bump unread and never wrote the self chat (no rows).
        assert_eq!(
            store
                .account_summary(&account.id)
                .unwrap()
                .unwrap()
                .unread_count,
            0
        );
        assert_eq!(
            store
                .history_chat_recipient(&account.id, 12)
                .unwrap()
                .map(|_| ()),
            None
        );
    }

    #[test]
    fn rerun_is_idempotent_and_counts_duplicates() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        write_archive(&archive, &standard_archive());

        run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive).unwrap();
        let second = run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive).unwrap();
        assert_eq!(second.imported, 0);
        // Every skip class re-counts; the seven previously imported rows now
        // surface as duplicates.
        assert_eq!(second.skipped_messages, 10);
        assert_eq!(second.skipped_chats, 1);
        assert_eq!(second.skipped_lines, 2);
        let row = store.history_import_row(&account.id).unwrap().unwrap();
        assert_eq!(row.state, "completed");
        assert_eq!(row.attempts, 2);
        assert_eq!(row.imported_messages, 0);
    }

    #[test]
    fn live_stored_rows_dedupe_through_the_identity_tuple() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", ALICE_E164, "8fc***01")
            .unwrap();
        // A live envelope stored the same Signal message under the ACI-form
        // sender hash before the import ran.
        let live_sender = stable_hash_id(&[&account.id, "direct", ALICE_ACI]);
        let live = MessageRecord {
            id: stable_hash_id(&[
                "signal-message-v2",
                &account.id,
                &conversation.id,
                "incoming",
                "1000",
                &live_sender,
            ]),
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction: "incoming",
            sender_id: live_sender,
            sender_name: None,
            mentions_self: false,
            sent_at: 1000,
            received_at: Some(1),
            text: Some("live hello".into()),
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
            admin_deleted: false,
            sticker: None,
        };
        assert!(store.insert_message(&live, None, None, true).unwrap());

        let archive = seed_engine(&temp);
        write_archive(&archive, &standard_archive());
        let counters = run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive).unwrap();
        assert_eq!(counters.imported, 6);
        assert_eq!(counters.skipped_messages, 4);
        // The live row won: the archive's copy of the same Signal message
        // collapsed onto it, and the unread bump from the live receive stays.
        let stored = store
            .message_by_signal_identity(
                &account.id,
                &conversation.id,
                "incoming",
                1000,
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
            )
            .unwrap()
            .expect("live row");
        assert_eq!(stored.text.as_deref(), Some("live hello"));
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
    fn failed_runs_record_the_error_class_and_attempt_budget() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        // No archive written: the open fails and the run lands `failed`.
        let error = run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive)
            .expect_err("missing archive must fail");
        assert!(matches!(error, ImportFailure::Io(_)));
        let row = store.history_import_row(&account.id).unwrap().unwrap();
        assert_eq!(row.state, "failed");
        assert_eq!(row.error_class.as_deref(), Some("io"));
        assert_eq!(row.attempts, 1);
        run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive).expect_err("still missing");
        let row = store.history_import_row(&account.id).unwrap().unwrap();
        assert_eq!(row.attempts, 2);
    }
}
