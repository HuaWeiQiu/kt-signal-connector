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
//! scratch tables, and written one bounded transaction at a time (ADR 0004
//! bounded discipline). The engine-side dependency registered in §4.38
//! (recipient service identity) landed engine-side in E5. Since contract
//! 1.55 (§4.38 attachv2) the message frames' attachment objects also carry
//! the pointer face (`locator`: key/cdnKey/cdnNumber/uploadTimestamp/
//! digest|plaintextHash plus caption/blurHash/incrementalMac) — parsed into
//! the internal `attachment_locators` column, ingest only: the download
//! wiring (step 3) is a later batch, so descriptors keep answering the §6.7
//! not-found failure.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::Value;
use thiserror::Error;

use crate::engine::{
    NormalizedAttachment, NormalizedQuote, sanitize_inbound_filename, truncate_utf8_bytes,
};
use crate::ids::{mask_address, stable_hash_id};
use crate::service::{GROUP_NAME_MAX_BYTES, MAX_HOST_TEXT_PREVIEW_BYTES, MAX_INBOUND_TEXT_BYTES};
use crate::store::{
    HistoryAttachmentLocator, HistoryImportInsert, HistoryStageChat, HistoryStageRecipient,
    ImportCounters, MessageRecord, Store,
};

/// The engine's export artifact inside the account store directory.
pub const ARCHIVE_FILE_NAME: &str = "history-import.ndjson";
/// The engine's cloud-restore export artifact (contract revision 1.84, the
/// §4.69 consumption batch): the same NDJSON frame family as the transfer
/// archive, streamed into the same account directory — the engine writes it
/// whole, and only after its whole restore chain (credentials → CDN download
/// → decrypt) has succeeded, so file presence plus a changed freshness face
/// is the definite success signal (the engine's progress snapshot clears on
/// success, making `{present: false}` the same answer a never-started or
/// restarted engine gives — engine `src/service/cloud_backup.rs`).
pub const CLOUD_ARCHIVE_FILE_NAME: &str = "history-import-cloud.ndjson";
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
/// appeared. Past it the honest answer is `unavailable` (`no-archive`): the
/// engine's failure modes are indistinguishable from "the phone had nothing to
/// transfer".
///
/// Contract revision 1.165 (§4.163): raised 10 min → 60 min to cover the
/// engine's transfer-download budget, which the engine half is widening to the
/// official ~1 h figure in lockstep — this constant is the connector-side
/// upper bound on the same wait, so the two must move together (a watcher
/// window shorter than the engine's transfer budget abandons the poll while
/// the transfer is still legitimately running and answers `no-archive` on a
/// link that later succeeds).
pub const LINK_WAIT_MS: u64 = 60 * 60 * 1000;
/// The backup proto's mutually exclusive attachment flag for voice notes
/// (engine `message_attachment::Flag::VoiceMessage`).
const ATTACHMENT_FLAG_VOICE_MESSAGE: i64 = 1;
/// The backup proto's attachment flag bits for the P14 metadata booleans
/// (contract 1.107, §4.88; engine `message_attachment::Flag::Borderless` /
/// `::Gif` — the same enum the live pointer face projects as `isBorderless`
/// / `isGif`).
const ATTACHMENT_FLAG_BORDERLESS: i64 = 2;
const ATTACHMENT_FLAG_GIF: i64 = 8;
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
    /// The run was aborted by `history.importCancel` (contract revision
    /// 1.148, §4.146): the cooperative abort flag flipped between batches.
    /// The partial counters are the honest ledger — the exit lands them on
    /// the failed row with the `cancelled` class, retryable like any other
    /// failed run.
    #[error("cancelled")]
    Cancelled,
}

impl ImportFailure {
    /// Short classification persisted on the import row (`errorClass`); the
    /// details never leave the process (no message content, no paths).
    pub fn class(&self) -> &'static str {
        match self {
            ImportFailure::Io(_) => "io",
            ImportFailure::Store(_) => "store",
            ImportFailure::Cancelled => "cancelled",
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

/// The cloud-restore archive path for one signal account: the same registry
/// directory the transfer archive lands in, under the cloud file name.
pub fn cloud_archive_path(data_dir: &Path, signal_account: &str) -> Option<PathBuf> {
    engine_account_dir(data_dir, signal_account).map(|dir| dir.join(CLOUD_ARCHIVE_FILE_NAME))
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
///
/// `abort` is the cooperative cancel flag (contract revision 1.148, §4.146):
/// checked at entry and between archive lines, an aborted run exits through
/// the [`ImportFailure::Cancelled`] class with its partial counters — the
/// stage scratch still clears and the run still lands its failed row, so the
/// ledger never shows a phantom `running`.
pub fn run_import(
    store: &Arc<Store>,
    account_id: &str,
    signal_account: &str,
    archive: &Path,
    abort: &AtomicBool,
) -> Result<ImportCounters, ImportFailure> {
    store.begin_history_import(account_id, crate::link::now_ms())?;
    if abort.load(Ordering::Acquire) {
        let _ = store.complete_history_import(
            account_id,
            "failed",
            &ImportCounters::default(),
            Some(ImportFailure::Cancelled.class()),
            crate::link::now_ms(),
        );
        return Err(ImportFailure::Cancelled);
    }
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
    let inner = pass_one(store, account_id, archive, abort).and_then(|()| {
        Importer::new(store, account_id, signal_account, &mut counters, abort).run(archive)
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
/// for pass 2's counters. The abort flag is checked between lines.
fn pass_one(
    store: &Arc<Store>,
    account_id: &str,
    archive: &Path,
    abort: &AtomicBool,
) -> Result<(), ImportFailure> {
    let file = File::open(archive)?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut line_buf = Vec::with_capacity(8 * 1024);
    let mut recipients: Vec<HistoryStageRecipient> = Vec::with_capacity(BATCH_MESSAGES);
    let mut chats: Vec<HistoryStageChat> = Vec::with_capacity(BATCH_MESSAGES);
    loop {
        if abort.load(Ordering::Acquire) {
            return Err(ImportFailure::Cancelled);
        }
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

/// One pending conversation pin, applied when its batch lands. The fields
/// mirror the official `PinDetails` face (`protos/Backups.proto:494-500`):
/// `pinned_at` is the backup's `pinnedAtTimestamp` and `expires_at` the
/// `pinExpiry` arm's absolute deadline (`None` = `pinNeverExpires`).
struct PendingPin {
    conversation_id: String,
    target_author: String,
    target_sent_at: u64,
    pinned_at: u64,
    expires_at: Option<u64>,
}

/// The official `PinDetails.pinExpiry` oneof as the local deadline
/// (`Backups.proto:494-500`; import.preload.ts:1929-1938). `Some(Some(ts))`
/// is a timed pin, `Some(None)` a forever pin, and `None` a malformed face —
/// the official side `strictAssert`s one of the two arms, so the connector
/// drops the pin instead of inventing a forever one. An absent key (an older
/// engine face before contract 1.197) keeps the pre-1.197 forever reading,
/// because there the oneof was simply never projected.
fn backup_pin_expiry(value: &Value) -> Option<Option<u64>> {
    let Some(expiry) = value.get("pinExpiry") else {
        return Some(None);
    };
    let expiry = expiry.as_object()?;
    if let Some(timestamp) = expiry.get("pinExpiresAtTimestamp").and_then(Value::as_u64) {
        return Some(Some(timestamp));
    }
    if expiry.get("pinNeverExpires").and_then(Value::as_bool) == Some(true) {
        return Some(None);
    }
    None
}

/// Pass-2 driver: streams the archive's message frames and writes them one
/// bounded batch at a time (§4.38 bounded discipline). Between batches the
/// store lock is released, so live receives never queue behind an import.
/// The abort flag is checked between lines (contract revision 1.148).
struct Importer<'a> {
    store: &'a Arc<Store>,
    account_id: &'a str,
    signal_account: &'a str,
    chats: HashMap<u64, Option<ResolvedChat>>,
    batch: Vec<HistoryImportInsert>,
    pins: Vec<PendingPin>,
    counters: &'a mut ImportCounters,
    abort: &'a AtomicBool,
}

impl<'a> Importer<'a> {
    fn new(
        store: &'a Arc<Store>,
        account_id: &'a str,
        signal_account: &'a str,
        counters: &'a mut ImportCounters,
        abort: &'a AtomicBool,
    ) -> Self {
        Self {
            store,
            account_id,
            signal_account,
            chats: HashMap::new(),
            batch: Vec::with_capacity(BATCH_MESSAGES),
            pins: Vec::new(),
            counters,
            abort,
        }
    }

    fn run(&mut self, archive: &Path) -> Result<(), ImportFailure> {
        let file = File::open(archive)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);
        let mut line_buf = Vec::with_capacity(8 * 1024);
        loop {
            if self.abort.load(Ordering::Acquire) {
                return Err(ImportFailure::Cancelled);
            }
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
            // The call-history record frame (contract 1.81, §4.38): the
            // official backup carries call-link (ad-hoc) calls as top-level
            // frames — never chat items, they are "not tied to threads" —
            // and the official import drops one whose call-link recipient
            // does not resolve (`ts/services/backups/import.preload.ts`
            // #fromAdHocCall's drop-and-warn; behavior reference, zero code
            // copied). The connector keeps no call-history store and no
            // call-link conversation kind (§4.49: the connector is the
            // signaling pipe, it persists no call state), so the official
            // drop path is total: the line is recognized — never the junk
            // `skippedLines` bucket — and honestly skip-counted.
            Some("adhocCall") => self.counters.skipped_messages += 1,
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
        // Direction-less backup items have no place in the two-sided model —
        // except the timer-change notice (contract 1.42, §4.41) and the
        // group-update notice (contract 1.79, §4.38): conversation-scoped
        // state updates the engine exports exactly so the connector can land
        // the same system rows live traffic lands.
        let timer_change = value
            .get("expirationTimerChange")
            .and_then(Value::as_object);
        let group_update = import_group_update(value.get("groupUpdate"));
        let direction = match value.get("direction").and_then(Value::as_str) {
            Some("incoming") => "incoming",
            Some("outgoing") => "outgoing",
            _ if timer_change.is_some() || group_update.is_some() => "system",
            _ => {
                self.counters.skipped_messages += 1;
                return Ok(());
            }
        };
        let Some(chat) = self.chat_for(chat_id)? else {
            // The unresolvable chat was skip-counted once at resolution time.
            return Ok(());
        };
        // The notice faces land as system rows whatever the archive row's
        // direction says (contract 1.79, §4.38): the timer face keeps its
        // §4.41 row identity; the group face lands the live group-update
        // notice row. Both riding one archive row (a batched group change
        // that also moved the timer) is the official
        // `additionalMessages` shape — two system rows.
        if direction == "system" || group_update.is_some() {
            if let Some(change) = timer_change {
                self.handle_timer_change(change, &chat, sent_at)?;
            }
            if let Some(group) = group_update {
                self.handle_group_update(group, value, &chat, sent_at, direction)?;
            }
            return Ok(());
        }
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
        // The §4.38 expansion item faces (contract 1.81): view-once, story
        // reply, poll, payment and gift badge ride the row as bounded rich
        // faces; the admin-deleted face is the row's own tombstone flag. All
        // are directed bubble faces (official ChatItem `item` oneof members,
        // backup.proto:462-473) — a direction-less row behind one keeps the
        // plain skip routing (the official "if unset, importers should skip
        // this item" posture; only the timer and group notices are
        // conversation-scoped state and land as system rows). The engine
        // projects the faces at their live receive keys (engine 592b1d3,
        // compat §5 递补批): view-once as the top-level
        // `viewOnce`/`isErased`/`viewOnceInvalid` markers with the single
        // medium on the row's standard `attachments` list — one receive
        // pathway, no import-only parse — and the rest as `storyReply` /
        // `poll` / `payment` / `giftBadge` / `adminDeleted` object faces.
        let view_once = value
            .get("viewOnce")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let view_once_erased = value
            .get("isErased")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let view_once_invalid = value
            .get("viewOnceInvalid")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let story_reply = import_story_reply(value.get("storyReply"));
        let payment = import_payment(value.get("payment"));
        let gift_badge = import_gift_badge(value.get("giftBadge"));
        let admin_deleted_face = import_admin_deleted(value.get("adminDeleted"));
        let poll = self.import_poll(value.get("poll"))?;
        // Body presence: a tombstone carries none by definition; anything else
        // without text, a long-text pointer, an attachment, a sticker face, or
        // a contact card stores nothing (contract 1.78 §4.38: the contact
        // identity is the body of a contact row). The expansion faces are
        // bodies the same way (contract 1.81 §4.38): a poll, a payment, a
        // gift badge, a view-once marker or a story reply is the row's
        // bubble; an admin-deleted face is the tombstone exception beside
        // `remoteDeleted`.
        let text = value
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            // The story-reply body is the row's body (the official
            // textReply projection): it rides the receive ladder beside any
            // frame text.
            .or_else(|| {
                story_reply
                    .as_ref()
                    .and_then(|story| story.body.as_deref())
                    .filter(|body| !body.is_empty())
            });
        let has_long_text = value
            .get("longText")
            .is_some_and(|pointer| !pointer.is_null())
            || story_reply.as_ref().is_some_and(|story| story.long_text);
        let raw_attachments = value.get("attachments").and_then(Value::as_array);
        // The sticker face (contract 1.35 identity beside the message) parses
        // with the live-receive bounds; the data pointer rides the descriptor
        // list below, exactly as a live envelope's sticker image does.
        let sticker = import_sticker(value.get("sticker"));
        // Official sticker rows carry no body (DataMessage.Sticker semantics):
        // cut a stale archive body riding a sticker frame so the row's text
        // column stays empty — live receive and the sent mirror enforce the
        // same discipline at their normalization authorities.
        let text = text.filter(|_| sticker.is_none());
        // The contact card (contract 1.78, §4.38) parses with char bounds;
        // the avatar pointer rides the descriptor list below, live-receive
        // attachment parity.
        let contact = import_contact(value.get("contact"));
        if !remote_deleted
            && !admin_deleted_face
            && text.is_none()
            && !has_long_text
            && raw_attachments.is_none_or(Vec::is_empty)
            && sticker.is_none()
            && contact.is_none()
            && !view_once
            && story_reply.is_none()
            && poll.is_none()
            && payment.is_none()
            && gift_badge.is_none()
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
        // Descriptors: the archive's top-level attachments plus the sticker's
        // own byte pointer (contract 1.35: the sticker's data rides the
        // descriptor list, live receive parity — a live sticker envelope
        // pushes its image onto `attachments` at normalization). The pointer
        // face is metadata-only here as everywhere else: nothing downloads in
        // this path.
        let mut attachment_raws: Vec<Value> = raw_attachments.cloned().unwrap_or_default();
        if let Some((_, data)) = &sticker {
            attachment_raws.push(data.clone());
        }
        // The contact card's avatar byte pointer joins the descriptor list
        // (contract 1.78, §4.38) — metadata-only, exactly like a sticker's
        // `data`; the card itself never carries bytes.
        if let Some((_, Some(avatar))) = &contact {
            attachment_raws.push(Value::Object(avatar.clone()));
        }
        let (attachments, locators) = self.import_attachments(&attachment_raws, &message_id);
        // The archive's disappearing-message metadata (contract 1.42,
        // §4.41): `expiresInMs` converts to seconds rounding up — a row must
        // never sweep before its real deadline — and `expireStartDate` (>0)
        // is the countdown start; without it the deadline falls back to the
        // import stamp, so an already-dead historical row sweeps on the next
        // pass only when its stated start says so.
        let expire_seconds = value
            .get("expiresInMs")
            .and_then(Value::as_u64)
            .map(|ms| ms.div_ceil(1_000));
        let expire_start_at = value
            .get("expireStartDate")
            .and_then(Value::as_u64)
            .filter(|value| *value > 0);
        let record = MessageRecord {
            required_protocol_version: None,
            author_aci: None,
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
            quote: None,
            attachments,
            // The rich faces ride the additive §4.37/§4.40 channel (contract
            // 1.78/1.81, §4.38) — persisted in `rich_json` without a store
            // schema bump; an all-absent composition collapses to None so
            // plain rows carry nothing.
            rich: {
                let mut rich = crate::engine::NormalizedRich::default();
                if let Some((card, _)) = &contact {
                    rich.contact = Some(card.clone());
                }
                // The view-once markers are the engine's top-level keys, the
                // live receive projection shape: the erased/invalid form is
                // the live no-renderable-media shape (§4.25
                // `viewOnceInvalid`), which renders the same placeholder a
                // burned live row does; the medium, when the archive still
                // carries it, rode the standard attachment list above.
                if view_once {
                    rich.view_once = true;
                    rich.view_once_invalid = view_once_erased || view_once_invalid;
                }
                if story_reply.is_some() {
                    // The story author resolves connector-side (the engine
                    // exports no story context): an outgoing reply answers
                    // the conversation peer's story, an incoming reply the
                    // account's own — and only a direct conversation carries
                    // a story context at all, so a group row lands its body
                    // plain with the face honestly absent (the official
                    // replies always live in the story author's 1:1). Both
                    // resolved forms are existing row identities — the peer
                    // key and the account number — nothing is invented.
                    rich.story_reply = if chat.kind == "direct" {
                        Some(crate::engine::NormalizedStoryReply {
                            author_aci: if direction == "outgoing" {
                                chat.peer_key.clone()
                            } else {
                                self.signal_account.to_string()
                            },
                            // Stories are never imported, so the answered
                            // story carries no upstream timestamp — the zero
                            // the desktop's import-row spec pins.
                            sent_timestamp: 0,
                        })
                    } else {
                        None
                    };
                }
                rich.poll = poll;
                rich.payment = payment;
                rich.gift_badge = gift_badge;
                (rich != crate::engine::NormalizedRich::default()).then_some(rich)
            },
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            send_outcomes: None,
            // The admin-deleted tombstone (contract 1.33 wire face; contract
            // 1.81 §4.38 import): the face marks the row the way a live
            // inbound adminDelete does — the additive flag beside an intact
            // row, the body (when the archive carried one) stays for the
            // audit window, the status ladder untouched.
            admin_deleted: admin_deleted_face,
            sticker: sticker.map(|(meta, _)| meta),
            expire_seconds,
            expire_start_at,
            // The §4.38 archive frames carry no server GUID — the report face
            // stays a live-receive surface; imported rows keep NULL (§4.48).
            server_guid: None,
            server_timestamp: None,
            is_story: false,
            story_message: None,
        };
        let preview = record
            .text
            .as_deref()
            .map(|text| text.chars().take(120).collect::<String>());
        self.batch.push(HistoryImportInsert {
            record,
            preview,
            alt_sender_id,
            locators,
        });
        if let Some(pinned_at) = value
            .get("pinnedAtTimestamp")
            .and_then(Value::as_u64)
            .filter(|at| *at > 0)
            && let Some(expires_at) = backup_pin_expiry(value)
        {
            // The official import reads pinDetails.pinnedAtTimestamp as the
            // pin's own clock and pinExpiry as its absolute deadline
            // (import.preload.ts:1922-1945); the expired/forever distinction
            // is therefore the backup's, never the connector's clock. An
            // older engine face without pinExpiry keeps the pre-1.197
            // forever reading.
            // The pin resolves by (conversation, sent_at); the author rides
            // the same identity form a live pin envelope carries. The author
            // is display metadata only — the queue key is the stamp.
            self.pins.push(PendingPin {
                conversation_id: chat.conversation_id.clone(),
                target_author: author_source.unwrap_or_else(|| self.signal_account.to_string()),
                target_sent_at: sent_at,
                pinned_at,
                expires_at,
            });
        }
        if self.batch.len() >= BATCH_MESSAGES {
            self.flush()?;
        }
        Ok(())
    }

    /// One direction-less timer-change frame (contract 1.42, §4.41): the
    /// engine's export of a disappearing-timer update — `expiresInMs` is the
    /// announced timer (0 = off; an absent key is the close form), applied to
    /// the conversation state immediately and rendered as the same
    /// timer-notice system row a live change lands as, with the live system
    /// identity so a re-run (or a change live traffic stored first) dedupes
    /// through the batch writer. The updater identity is deliberately not
    /// part of the row: the notice is conversation-scoped state, and the
    /// desktop copy speaks the timer value, not the actor.
    fn handle_timer_change(
        &mut self,
        change: &serde_json::Map<String, Value>,
        chat: &ResolvedChat,
        sent_at: u64,
    ) -> Result<(), ImportFailure> {
        let seconds = change
            .get("expiresInMs")
            .and_then(Value::as_u64)
            .map(|ms| ms.div_ceil(1_000))
            .unwrap_or(0);
        self.store.set_conversation_expire_timer(
            self.account_id,
            &chat.conversation_id,
            seconds,
        )?;
        // The live system identity for this conversation: the same hash the
        // receive path derives, so identities agree across surfaces.
        let sender_id = stable_hash_id(&[self.account_id, chat.kind, &chat.peer_key]);
        let message_id = stable_hash_id(&[
            "signal-message-v2",
            self.account_id,
            &chat.conversation_id,
            "system",
            &sent_at.to_string(),
            &sender_id,
        ]);
        let text = if seconds > 0 {
            format!("已更新消息定时消失：{seconds} 秒")
        } else {
            "已更新消息定时消失：已关闭".to_string()
        };
        let record = MessageRecord {
            required_protocol_version: None,
            author_aci: None,
            id: message_id,
            account_id: self.account_id.to_string(),
            conversation_id: chat.conversation_id.clone(),
            direction: "system",
            sender_id,
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: Some(crate::link::now_ms()),
            text: Some(text.clone()),
            text_bytes: Some(text.len().min(u32::MAX as usize) as u32),
            text_truncated: false,
            text_retrievable: true,
            status: "system",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            quote: None,
            attachments: Vec::new(),
            rich: Some(crate::engine::NormalizedRich {
                is_expiration_update: true,
                ..Default::default()
            }),
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            send_outcomes: None,
            admin_deleted: false,
            sticker: None,
            // A timer notice never sweeps (§4.41): the columns stay NULL.
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
            server_timestamp: None,
            is_story: false,
            story_message: None,
        };
        let preview = Some(text.chars().take(120).collect::<String>());
        self.batch.push(HistoryImportInsert {
            record,
            preview,
            alt_sender_id: None,
            locators: Vec::new(),
        });
        if self.batch.len() >= BATCH_MESSAGES {
            self.flush()?;
        }
        Ok(())
    }

    /// One group-update archive face (contract 1.79, §4.38): the engine's
    /// export of a group change — the detail vocabulary rides the row's rich
    /// JSON for the desktop renderer; the row itself is the same
    /// live-receive system notice a group update lands as (「群聊信息已更新」，
    /// `is_group_update` rich, the first `title` detail's new title mirrored
    /// under the live `GROUP_NAME_MAX_BYTES` bound). The identity is the live
    /// receive form over the updater: an outgoing update is the account,
    /// anything else resolves the archive author exactly as an incoming
    /// author resolves — an unresolvable updater has no live-shaped identity,
    /// so the row is skip-counted rather than invented (never a placeholder).
    /// The updater identity rides the row like live traffic does; the
    /// conversation title is deliberately not mirrored here — the archive's
    /// chat frame carries the current title, and a historical update row must
    /// not regress it mid-import.
    fn handle_group_update(
        &mut self,
        group: ImportedGroupUpdate,
        value: &Value,
        chat: &ResolvedChat,
        sent_at: u64,
        archive_direction: &str,
    ) -> Result<(), ImportFailure> {
        let (sender_id, alt_sender_id) = if archive_direction == "outgoing" {
            (self.account_id.to_string(), None)
        } else {
            let author_id = value.get("authorId").and_then(Value::as_u64);
            let Some(author_id) = author_id else {
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
            (stable_hash_id(&[self.account_id, chat.kind, &source]), alt)
        };
        let message_id = stable_hash_id(&[
            "signal-message-v2",
            self.account_id,
            &chat.conversation_id,
            "system",
            &sent_at.to_string(),
            &sender_id,
        ]);
        let text = "群聊信息已更新".to_string();
        let record = MessageRecord {
            required_protocol_version: None,
            author_aci: None,
            id: message_id,
            account_id: self.account_id.to_string(),
            conversation_id: chat.conversation_id.clone(),
            direction: "system",
            sender_id,
            sender_name: None,
            mentions_self: false,
            sent_at,
            received_at: Some(crate::link::now_ms()),
            text: Some(text.clone()),
            text_bytes: Some(text.len().min(u32::MAX as usize) as u32),
            text_truncated: false,
            text_retrievable: true,
            status: "system",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            quote: None,
            attachments: Vec::new(),
            rich: Some(crate::engine::NormalizedRich {
                is_group_update: true,
                group_name: group.group_name,
                ..Default::default()
            }),
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            send_outcomes: None,
            admin_deleted: false,
            sticker: None,
            // The notice row never sweeps (§4.41 parity): the columns stay
            // NULL — a group notice carries no deadline of its own.
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
            server_timestamp: None,
            is_story: false,
            story_message: None,
        };
        let preview = Some(text.chars().take(120).collect::<String>());
        self.batch.push(HistoryImportInsert {
            record,
            preview,
            alt_sender_id,
            locators: Vec::new(),
        });
        if self.batch.len() >= BATCH_MESSAGES {
            self.flush()?;
        }
        Ok(())
    }

    /// The archive's poll face (contract 1.81, §4.38): the engine projects
    /// the official import's poll face verbatim — options carry their votes
    /// nested as `{voterId, voteCount}` (`ts/services/backups/import.preload.ts`
    /// poll arm; behavior reference, zero code copied) — and this fold is
    /// exactly the official import's aggregation: one entry per voter in
    /// first-seen order with the option indexes they picked, a voter the
    /// §4.38 recipient ladder cannot resolve dropping vote-level (the
    /// official unknown-voterId warn-and-skip, never the row). `terminated`
    /// is the desktop's boolean face of `hasEnded` — the official
    /// `terminatedAt` stamp is the poll message's own timestamp (the row's
    /// `sentAt`), so it is not repeated. The face is structural — an object
    /// answers a poll even when every field is absent (the official
    /// bare-poll projection: `question ?? ''`, `options ?? []`).
    fn import_poll(
        &self,
        raw: Option<&Value>,
    ) -> Result<Option<crate::engine::NormalizedPoll>, ImportFailure> {
        let Some(object) = raw.and_then(Value::as_object) else {
            return Ok(None);
        };
        // Options keep positional integrity: votes address options by index,
        // so an entry rides even when its label is absent (the official
        // `options.map(option => option.option ?? '')`), capped with the
        // trailing entries dropped whole — their votes dangle and drop with
        // them below.
        let options: Vec<String> = object
            .get("options")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .take(MAX_POLL_OPTIONS)
            .map(|option| {
                option
                    .get("option")
                    .and_then(Value::as_str)
                    .map(|text| bounded_chars(text, MAX_POLL_OPTION_CHARS))
                    .unwrap_or_default()
            })
            .collect();
        let option_cap = options.len() as u32;
        let mut order: Vec<String> = Vec::new();
        let mut indexes: HashMap<String, Vec<u32>> = HashMap::new();
        for (option_index, option) in object
            .get("options")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .take(MAX_POLL_OPTIONS)
            .enumerate()
        {
            for vote in option
                .get("votes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(voter_id) = vote.get("voterId").and_then(Value::as_u64) else {
                    continue;
                };
                let Some(recipient) = self.store.history_recipient(self.account_id, voter_id)?
                else {
                    continue;
                };
                let Some(source) = recipient_source(&recipient, self.signal_account) else {
                    continue;
                };
                let entry = indexes.entry(source.clone()).or_insert_with(|| {
                    order.push(source.clone());
                    Vec::new()
                });
                if entry.len() < MAX_POLL_VOTE_INDEXES && !entry.contains(&(option_index as u32)) {
                    entry.push(option_index as u32);
                }
            }
        }
        let mut votes = Vec::new();
        for voter in order.into_iter().take(MAX_POLL_VOTES) {
            let Some(voter_indexes) = indexes.remove(&voter) else {
                continue;
            };
            let kept: Vec<u32> = voter_indexes
                .into_iter()
                .filter(|index| *index < option_cap)
                .collect();
            if kept.is_empty() {
                continue;
            }
            votes.push(crate::engine::NormalizedPollVote {
                voter_id: voter,
                option_indexes: kept,
            });
        }
        Ok(Some(crate::engine::NormalizedPoll {
            question: object
                .get("question")
                .and_then(Value::as_str)
                .map(|text| bounded_chars(text, MAX_POLL_QUESTION_CHARS))
                .unwrap_or_default(),
            options,
            allow_multiple: object
                .get("allowMultiple")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            terminated: object
                .get("hasEnded")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            votes,
        }))
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
            Some((recipient, expire_timer_ms)) => {
                match chat_identity(self.signal_account, &recipient) {
                    Some((kind, peer_key)) => {
                        let title = match non_empty(&recipient.name) {
                            Some(name) => truncate_utf8_bytes(name, CONVERSATION_TITLE_BYTES),
                            None => mask_address(&peer_key),
                        };
                        let conversation = self.store.ensure_conversation(
                            self.account_id,
                            kind,
                            &peer_key,
                            &title,
                        )?;
                        // The chat frame's disappearing timer seeds the
                        // conversation state (§4.41): ms → seconds rounds up
                        // so an imported state never sweeps early. `None`
                        // leaves whatever state the live traffic last set.
                        if let Some(timer_ms) = expire_timer_ms {
                            self.store.set_conversation_expire_timer(
                                self.account_id,
                                &conversation.id,
                                timer_ms.div_ceil(1_000),
                            )?;
                        }
                        Some(ResolvedChat {
                            conversation_id: conversation.id,
                            kind,
                            peer_key,
                        })
                    }
                    None => None,
                }
            }
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
            // The §4.38 import face keeps quote snapshots metadata-only
            // (contract 1.100, §4.81 D4): backup quote attachments ride a
            // different wire shape and stay unparsed this revision — the
            // empty vec serializes as an absent key, the same pre-1.100 row.
            attachments: Vec::new(),
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

    /// Attachment descriptors (§4.13 mirror) plus their ingest-side pointer
    /// faces (contract 1.55, §4.38 attachv2). Descriptors keep the v1 shape
    /// byte-for-byte (synthetic stable ids, no bytes behind them — §6.7:
    /// opening one still answers the normal not-found failure; nothing
    /// downloads this batch). The pointer faces land beside them in
    /// `record.attachments` index order and ride the internal
    /// `attachment_locators` column only — never the host wire. Caps: the
    /// live receive ceiling (32) and the per-face bounds below mirror the
    /// §4.13 discipline — a hostile archive line cannot inflate the row.
    ///
    /// Ingest caps (recorded in §4.38): key/digest base64 ≤512 chars,
    /// plaintextHash hex ≤128 chars, cdnKey ≤512 chars, index-aligned
    /// entries only. A face without a descriptor position (engine emitted
    /// more locator faces than the cap kept) drops; a descriptor without a
    /// face is the honest v1 shape (locator absent on the engine row). A
    /// key-less face (official Locator::Invalid) stays index-aligned and
    /// key-less — the future download wiring must see the hole, not a
    /// shifted row.
    fn import_attachments(
        &self,
        raws: &[Value],
        message_id: &str,
    ) -> (Vec<NormalizedAttachment>, Vec<HistoryAttachmentLocator>) {
        let mut descriptors = Vec::new();
        let mut locators = Vec::new();
        for (index, raw) in raws.iter().take(MAX_IMPORT_ATTACHMENTS).enumerate() {
            // The P14 metadata keys (contract 1.107, §4.88) parse with the
            // same names the live face carries — caption/blurHash verbatim
            // (empty drops, the zero-information discipline), the integrity
            // pair all-or-nothing, the flag bits the same enum members the
            // live pointer projects — so imported and live descriptors share
            // one wire form.
            let mac = raw
                .get("incrementalMac")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty());
            let chunk_size = raw
                .get("chunkSize")
                .and_then(Value::as_u64)
                .filter(|size| *size > 0 && *size <= u32::MAX as u64)
                .map(|size| size as u32);
            let (incremental_mac, chunk_size) = match (mac, chunk_size) {
                (Some(mac), Some(chunk_size)) => (Some(mac.to_string()), Some(chunk_size)),
                _ => (None, None),
            };
            let flag = raw.get("flag").and_then(Value::as_i64);
            descriptors.push(NormalizedAttachment {
                id: stable_hash_id(&["history-attachment", message_id, &index.to_string()]),
                content_type: raw
                    .get("contentType")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string),
                filename: raw
                    .get("fileName")
                    .and_then(Value::as_str)
                    // The archive line carries the sender's name the same way
                    // the live pointer does — the §4.184 face applies to both
                    // (verbatim through the official filters, never a length
                    // rewrite; the empty filter turns a flattened-nothing
                    // name into key absence).
                    .map(sanitize_inbound_filename)
                    .filter(|text| !text.is_empty()),
                size: raw.get("size").and_then(Value::as_u64),
                width: None,
                height: None,
                is_voice_note: flag == Some(ATTACHMENT_FLAG_VOICE_MESSAGE),
                caption: raw
                    .get("caption")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string),
                blur_hash: raw
                    .get("blurHash")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_string),
                incremental_mac,
                chunk_size,
                is_borderless: flag == Some(ATTACHMENT_FLAG_BORDERLESS),
                is_gif: flag == Some(ATTACHMENT_FLAG_GIF),
                // The backup archive's FilePointer proto has no waveform or
                // duration members (the pair exists only on the live
                // AttachmentPointer face the engine generates at send time,
                // §4.149), so import rows structurally never carry them; the
                // fixed key list above ignores any stray same-named JSON key
                // a hostile archive line invents.
                waveform: None,
                duration: None,
                locator: None,
            });
            locators.push(import_locator(raw.get("locator")));
        }
        // A message whose attachments all lacked a locator key (v1-shape or
        // Invalid-only) stores no locator row at all — `[{}]` noise never
        // reaches the column. Alignment is preserved where it matters: any
        // message with a real face carries the full index-aligned list
        // (key-less holes included).
        if locators
            .iter()
            .all(|face| *face == HistoryAttachmentLocator::default())
        {
            return (descriptors, Vec::new());
        }
        (descriptors, locators)
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
            self.store.append_conversation_pin(
                self.account_id,
                &pin.conversation_id,
                &pin.target_author,
                pin.target_sent_at,
                pin.pinned_at,
                pin.expires_at,
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
fn stage_chat(value: &Value) -> Option<HistoryStageChat> {
    let id = value.get("id").and_then(Value::as_u64)?;
    let recipient_id = value.get("recipientId").and_then(Value::as_u64)?;
    // The chat frame's disappearing timer in ms (§4.41): stated, positive
    // values stage; anything else leaves the conversation state untouched.
    let expire_timer_ms = value
        .get("expirationTimerMs")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0);
    Some(HistoryStageChat {
        id,
        recipient_id,
        expire_timer_ms,
    })
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

/// Ingest-cap constants for the pointer faces (§4.38 attachv2): the engine's
/// projection is 32B keys (base64 44 chars), 32B digests (44) and hashes
/// (64 hex) — the caps are generous multiples over the real shapes, tight
/// enough that a hostile line cannot bloat the column. cdnKey is an opaque
/// server string (official URLs carry ~40-80 chars).
const MAX_LOCATOR_BASE64_CHARS: usize = 512;
const MAX_LOCATOR_HEX_CHARS: usize = 128;

/// Ingest caps for the §4.38 expansion faces (contract 1.81): generous
/// multiples over the real official shapes — the proto comments bound poll
/// question/option text at 1-100 chars and payment notes are one line —
/// tight enough that a hostile line cannot bloat the rich column. String
/// faces truncate at the cap (renderable-prefix honesty, the contact
/// precedent); the only faces dropped whole are poll entries past the caps,
/// entries whose votes dangle with them.
const MAX_POLL_QUESTION_CHARS: usize = 256;
const MAX_POLL_OPTION_CHARS: usize = 256;
const MAX_POLL_OPTIONS: usize = 32;
const MAX_POLL_VOTES: usize = 64;
const MAX_POLL_VOTE_INDEXES: usize = 32;
const MAX_PAYMENT_NOTE_CHARS: usize = 512;

/// Parse one engine NDJSON attachment's `locator` object into the ingest
/// face (contract 1.55). Proto-only discipline on the connector side too:
/// unknown keys are ignored, wrong-shaped keys are dropped (never guessed),
/// an absent/empty object answers a key-less face — the official
/// Locator::Invalid form the download wiring must keep seeing as a hole.
/// Integrity faces are kept one-of as the engine projected them (digest
/// base64 / plaintextHash hex); both arriving together cannot happen from
/// the pinned engine (oneof upstream) and would serialize as-is — the
/// download wiring's step-3 gate owns refusing that combination.
fn import_locator(raw: Option<&Value>) -> HistoryAttachmentLocator {
    let mut face = HistoryAttachmentLocator {
        key: None,
        cdn_key: None,
        cdn_number: None,
        upload_timestamp: None,
        digest: None,
        plaintext_hash: None,
    };
    let Some(object) = raw.and_then(Value::as_object) else {
        return face;
    };
    let bounded_b64 = |name: &str| {
        object
            .get(name)
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty() && text.len() <= MAX_LOCATOR_BASE64_CHARS)
            .map(str::to_string)
    };
    face.key = bounded_b64("key");
    face.cdn_key = bounded_b64("cdnKey");
    face.digest = bounded_b64("digest");
    face.plaintext_hash = object
        .get("plaintextHash")
        .and_then(Value::as_str)
        .filter(|text| {
            !text.is_empty()
                && text.len() <= MAX_LOCATOR_HEX_CHARS
                && text.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        .map(|text| text.to_ascii_lowercase())
        // The engine projects lowercase; an uppercase archive value would
        // only come from a hostile line — normalize, don't store verbatim.
        ;
    face.cdn_number = object
        .get("cdnNumber")
        .and_then(Value::as_u64)
        .filter(|number| *number <= u32::MAX as u64)
        .map(|number| number as u32);
    face.upload_timestamp = object
        .get("uploadTimestamp")
        .and_then(Value::as_u64)
        .filter(|timestamp| *timestamp > 0);
    face
}

/// Parse one engine NDJSON message's `sticker` object into the 1.35 face
/// (contract 1.76 §4.38): bounded pack identity beside the message plus the
/// `data` byte pointer, which the caller appends to the descriptor list —
/// the exact shape a live envelope's sticker normalization produces. Bounds
/// are the 1.35 live-face caps (`MAX_STICKER_*`, one source of truth across
/// live receive and import): packId ≤64, packKey ≤128 base64, stickerId
/// within u32, emoji ≤32 and optional. The face rides only whole — a
/// missing identity field or a missing/malformed `data` pointer answers
/// None so the row keeps plain routing instead of carrying half a sticker
/// (the live `normalized_sticker` discipline, engine.rs; the engine export
/// gate keeps malformed pack byte lengths off the wire entirely, so what
/// reaches here is hostile-line territory only).
fn import_sticker(raw: Option<&Value>) -> Option<(crate::engine::NormalizedSticker, Value)> {
    let object = raw?.as_object()?;
    let pack_id = object
        .get("packId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.chars()
                .take(crate::engine::MAX_STICKER_PACK_ID_CHARS)
                .collect::<String>()
        })?;
    let pack_key = object
        .get("packKey")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.chars()
                .take(crate::engine::MAX_STICKER_PACK_KEY_CHARS)
                .collect::<String>()
        })?;
    let sticker_id = object
        .get("stickerId")
        .and_then(Value::as_u64)
        .filter(|id| *id <= u32::MAX as u64)
        .map(|id| id as u32)?;
    let emoji = object
        .get("emoji")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.chars()
                .take(crate::engine::MAX_STICKER_EMOJI_CHARS)
                .collect::<String>()
        });
    // The byte pointer must be an object for the face to ride (usable data
    // pointer, live parity); its inner fields parse through
    // `import_attachments` like any archive attachment.
    let data = object.get("data")?.as_object()?; // shape gate: object only
    Some((
        crate::engine::NormalizedSticker {
            pack_id,
            pack_key,
            sticker_id,
            emoji,
        },
        Value::Object(data.clone()),
    ))
}

/// Parse one engine NDJSON message's `contact` object into the bounded card
/// face (contract 1.78, §4.38): name parts, phone/email entries and the
/// organization, with the avatar byte pointer returned beside the card for
/// the caller's descriptor list — the same split a live attachment face
/// makes. Every string is char-truncated and the lists are capped
/// (`MAX_CONTACT_*`), the type vocabulary coerces unknown/absent to HOME (the
/// official `phoneToContactFormType` parse face), and a card with no usable
/// content answers None so the row keeps plain routing instead of storing a
/// placeholder card (never a half/empty contact — the sticker whole-face
/// discipline). A malformed entry drops individually; the avatar pointer must
/// be an object to ride. The live receive face (§4.132) reuses the same
/// bounded reader through [`crate::engine::normalized_live_contact`] — one
/// authority for the card's bounds, both arrival chains.
pub(crate) fn import_contact(
    raw: Option<&Value>,
) -> Option<(
    crate::engine::NormalizedContact,
    Option<serde_json::Map<String, Value>>,
)> {
    let object = raw?.as_object()?;
    let name = object.get("name").and_then(Value::as_object).map(|name| {
        let part = |key: &str| {
            name.get(key)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| {
                    s.chars()
                        .take(crate::engine::MAX_CONTACT_NAME_CHARS)
                        .collect::<String>()
                })
        };
        crate::engine::NormalizedContactName {
            given_name: part("givenName"),
            family_name: part("familyName"),
            prefix: part("prefix"),
            suffix: part("suffix"),
            middle_name: part("middleName"),
            nickname: part("nickname"),
        }
    });
    let entry_value = |object: &serde_json::Map<String, Value>| {
        object
            .get("value")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.chars()
                    .take(crate::engine::MAX_CONTACT_VALUE_CHARS)
                    .collect::<String>()
            })
    };
    let entry_label = |object: &serde_json::Map<String, Value>| {
        object
            .get("label")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.chars()
                    .take(crate::engine::MAX_CONTACT_LABEL_CHARS)
                    .collect::<String>()
            })
    };
    let entry_kind = |object: &serde_json::Map<String, Value>| {
        object
            .get("type")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_uppercase)
            .filter(|s| matches!(s.as_str(), "HOME" | "MOBILE" | "WORK" | "CUSTOM"))
            .unwrap_or_else(|| "HOME".to_string())
    };
    let field_entries = |raw: Option<&Value>| -> Vec<crate::engine::NormalizedContactField> {
        raw.and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_object)
            .filter_map(|entry| {
                Some(crate::engine::NormalizedContactField {
                    value: entry_value(entry)?,
                    kind: entry_kind(entry),
                    label: entry_label(entry),
                })
            })
            .take(crate::engine::MAX_CONTACT_ENTRIES)
            .collect()
    };
    let organization = object
        .get("organization")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.chars()
                .take(crate::engine::MAX_CONTACT_ORGANIZATION_CHARS)
                .collect::<String>()
        });
    let avatar = object.get("avatar").and_then(Value::as_object);
    let name = name.filter(|name| {
        name.given_name.is_some()
            || name.family_name.is_some()
            || name.prefix.is_some()
            || name.suffix.is_some()
            || name.middle_name.is_some()
            || name.nickname.is_some()
    });
    let number = field_entries(object.get("number"));
    let email = field_entries(object.get("email"));
    let empty = name.is_none()
        && number.is_empty()
        && email.is_empty()
        && organization.is_none()
        && avatar.is_none();
    if empty {
        return None;
    }
    Some((
        crate::engine::NormalizedContact {
            name,
            number,
            email,
            organization,
        },
        avatar.cloned(),
    ))
}

/// The archive's groupUpdate face (contract 1.79, §4.38): the official
/// `#fromGroupUpdateMessage` detail vocabulary the engine projected — the
/// connector consumes presence (the notice identity) and mirrors the first
/// `title` detail's new title under the live `GROUP_NAME_MAX_BYTES` bound,
/// exactly what a live group-update envelope carries in its rich payload.
/// Presence without material (`details` missing/empty — the engine skips
/// such items upstream) answers no face: the row behind it keeps its plain
/// routing. The detail entries themselves are never re-validated here — the
/// engine owns the vocabulary — and ride verbatim in the persisted rich JSON.
struct ImportedGroupUpdate {
    group_name: Option<String>,
}

fn import_group_update(raw: Option<&Value>) -> Option<ImportedGroupUpdate> {
    let details = raw?.as_object()?.get("details")?.as_array()?;
    if details.is_empty() {
        return None;
    }
    let group_name = details
        .iter()
        .find_map(|detail| {
            if detail.get("type").and_then(Value::as_str) != Some("title") {
                return None;
            }
            detail
                .get("newTitle")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty())
        })
        .map(|title| truncate_utf8_bytes(title, GROUP_NAME_MAX_BYTES));
    Some(ImportedGroupUpdate { group_name })
}

/// Char-truncate to `cap` characters (the contact precedent's bounded-string
/// discipline: a renderable prefix is honest, a dropped tail is recorded by
/// the face's own bounds).
fn bounded_chars(text: &str, cap: usize) -> String {
    text.chars().take(cap).collect()
}

/// A bounded optional string face: present, string-typed, verbatim (no
/// trim — the payment faces keep the archive's values as stated), capped.
fn bounded_field(object: &serde_json::Map<String, Value>, key: &str, cap: usize) -> Option<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(|text| bounded_chars(text, cap))
}

/// The archive's `storyReply` face (contract 1.81, §4.38): the engine
/// projects the official textReply arm as `{body?, bodyAttachment?}` — the
/// body non-empty, the long-text pointer as `bodyAttachment` — and the emoji
/// arm as `{emoji}` (`ts/services/backups/import.preload.ts`
/// #fromDirectStoryReplyMessage; behavior reference, zero code copied). The
/// body is the row's body: it rides the receive ladder beside any frame
/// text, byte-bounded by the ladder with its honest truncation flag. A
/// `bodyAttachment` marks the metadata-only long-text overflow, no
/// descriptor (the standard long-text row posture). The emoji arm is the
/// official storyReaction face — a reaction against a story the archive
/// never imports — and has no row face connector-side: it answers None (a
/// recorded v1 gap), exactly like a bare object. The story's author resolves
/// at the row from direction and conversation, not here.
struct ImportedStoryReply {
    body: Option<String>,
    long_text: bool,
}

fn import_story_reply(raw: Option<&Value>) -> Option<ImportedStoryReply> {
    let object = raw?.as_object()?;
    let body = object
        .get("body")
        .and_then(Value::as_str)
        .filter(|body| !body.is_empty())
        .map(str::to_string);
    let long_text = object
        .get("bodyAttachment")
        .is_some_and(|pointer| !pointer.is_null());
    (body.is_some() || long_text).then_some(ImportedStoryReply { body, long_text })
}

/// The archive's `payment` face (contract 1.81, §4.38): the engine projects
/// the official import face verbatim (`amountMob`/`feeMob`/`note`/
/// `transactionDetailsBase64`; `ts/services/backups/import.preload.ts`
/// paymentNotification arm; behavior reference, zero code copied) but only
/// `note` crosses — amounts, fees and the transaction blob have no desktop
/// render face (KT carries no MobileCoin ledger), the projection gap the
/// desktop batch records. The face is structural — an object answers the
/// face even when the note is absent, exactly the official all-undefined
/// projection (a different rule from the contact card's usable-content
/// gate: a payment notification with no stated note is still the
/// notification). A non-object face answers None.
fn import_payment(raw: Option<&Value>) -> Option<crate::engine::NormalizedPayment> {
    let object = raw?.as_object()?;
    Some(crate::engine::NormalizedPayment {
        note: bounded_field(object, "note", MAX_PAYMENT_NOTE_CHARS),
    })
}

/// The archive's `giftBadge` face (contract 1.81, §4.38): the engine
/// projects the official `GiftBadgeStates` vocabulary verbatim —
/// "Unopened"/"Opened"/"Redeemed"/"Failed", the FAILED arm carrying no
/// presentation (`ts/services/backups/import.preload.ts` giftBadge arm;
/// behavior reference, zero code copied) — and the connector lowercases it
/// onto the desktop's wire vocabulary. The receipt-credential presentation
/// stays engine-side: zk credential material with no desktop data face (the
/// recorded projection gap). A non-object face, or a state that is not a
/// string, answers None — plain routing behind it. Contract 1.181 (§4.181)
/// parses the countdown pair when the archive carries it (the current
/// engine archive arm records it has no zk parse face, so archive rows keep
/// both keys absent — additive, byte-identical for them).
fn import_gift_badge(raw: Option<&Value>) -> Option<crate::engine::NormalizedGiftBadge> {
    let object = raw?.as_object()?;
    let state = object.get("state")?.as_str()?.to_ascii_lowercase();
    Some(crate::engine::NormalizedGiftBadge {
        state,
        expiration_ms: object
            .get("expiration")
            .and_then(Value::as_u64)
            .filter(|stamp| *stamp > 0),
        level: object.get("level").and_then(Value::as_u64),
    })
}

/// The archive's `adminDeleted` face (contract 1.81, §4.38): the row's own
/// tombstone — the official import lands the item as `deletedForEveryone`
/// with the admin's ACI (`ts/services/backups/import.preload.ts`
/// adminDeletedMessage arm; behavior reference, zero code copied). The KT
/// row model carries the live admin-delete flag only (contract 1.33), so
/// the face consumes presence: the official gate requires the `adminId` key
/// (strictAssert) and the connector mirrors that shape gate while the admin
/// identity itself has no row face (a recorded v1 gap). A missing
/// /non-object face or a missing `adminId` answers false — plain routing
/// behind it.
fn import_admin_deleted(raw: Option<&Value>) -> bool {
    raw.and_then(Value::as_object)
        .is_some_and(|object| object.get("adminId").and_then(Value::as_u64).is_some())
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

    /// §4.163: the link-time wait window covers the engine's transfer
    /// download budget (the engine half widened to the official ~1 h in
    /// lockstep) — the constant is the connector-side upper bound on the
    /// same wait, so the value is pinned: a regression below the engine's
    /// budget would abandon the poll while the transfer legitimately runs.
    #[test]
    fn link_wait_window_covers_the_engine_transfer_budget() {
        assert_eq!(LINK_WAIT_MS, 60 * 60 * 1000);
        assert_eq!(LINK_WAIT_MS, 3_600_000);
    }

    /// An idle cancel flag for the unit runs that never get cancelled.
    fn idle_abort() -> AtomicBool {
        AtomicBool::new(false)
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
                "direction": "incoming", "text": "pinned one",
                "pinnedAtTimestamp": 4000,
                "pinExpiry": {"pinExpiresAtTimestamp": 4_000_000_000_000u64}
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

    /// Contract 1.55 (§4.38 attachv2): the engine's attachment pointer faces
    /// land in the internal column — full transit face (key/cdnKey/
    /// cdnNumber 0/uploadTimestamp/digest|plaintextHash), the digest form
    /// (sender-original attachment), a partial face, and the official
    /// Locator::Invalid forms (no locator / empty object → key-less aligned
    /// hole). The host wire descriptor stays byte-identical to v1 (no key
    /// material on `attachments_json`), width/height are not projected by
    /// the pinned engine face (recorded), and a re-run collapses onto the
    /// same rows (identity discipline covers the new column implicitly).
    #[test]
    fn imported_attachments_carry_pointer_faces_in_internal_column_only() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        let key_b64 = "q83vLNoechoM6CiZ+hr3P5ePE3YGAibLE4bAl1jv20Y=";
        let digest_b64 = "dpDdCNlXKpo1VUFTWDbNWByz2CbtLIrwQ3THlMcpS2w=";
        let hash_hex = "a".repeat(64);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                // Full transit face, plaintextHash integrity (downloaded form).
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming", "text": "pic",
                    "attachments": [{
                        "contentType": "image/png", "fileName": "p.png", "flag": 0, "size": 12,
                        "caption": "hello", "blurHash": "LEHV6nWB2yk8pyo0adR*.7kCMdnj",
                        "incrementalMac": "AQIDBA==", "chunkSize": 8192,
                        "locator": {
                            "key": key_b64, "cdnKey": "t1a2b3c", "cdnNumber": 0,
                            "uploadTimestamp": 1_700_000_000_000_u64,
                            "plaintextHash": hash_hex
                        }
                    }]
                })
                .to_string(),
                // Digest form (sender-original) with transit pointers.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2000,
                    "direction": "incoming", "text": "video",
                    "attachments": [{
                        "contentType": "video/mp4", "fileName": "v.mp4", "flag": 0, "size": 99,
                        "locator": {
                            "key": key_b64, "cdnKey": "k2", "cdnNumber": 3,
                            "digest": digest_b64
                        }
                    }]
                })
                .to_string(),
                // Partial face (key + hash, no transit) beside an Invalid one.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 3000,
                    "direction": "incoming", "text": "mixed",
                    "attachments": [
                        {"contentType": "application/pdf", "fileName": "d.pdf", "size": 7,
                         "locator": {"key": key_b64, "plaintextHash": hash_hex}},
                        {"contentType": "image/webp", "fileName": "gone.webp", "size": 4,
                         "locator": {}}
                    ]
                })
                .to_string(),
                // v1 shape attachment (no locator key at all) — descriptors
                // only, no locators column entry, byte-identical wire row.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 4000,
                    "direction": "incoming", "text": "legacy",
                    "attachments": [
                        {"contentType": "audio/aac", "fileName": "v.aac", "flag": 1, "size": 9}
                    ]
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 4);
        let conversation = alice_conversation_id(&account.id);

        let message_at = |sent_at: u64| {
            store
                .message_by_signal_identity(
                    &account.id,
                    &conversation,
                    "incoming",
                    sent_at,
                    &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                    &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
                )
                .unwrap()
                .expect("imported row")
        };

        // Full face: locator persisted in the internal column; the wire
        // descriptor carries caption/blurHash but never key material.
        let full = message_at(1000);
        let faces = store
            .history_attachment_locators(&account.id, &full.id)
            .unwrap();
        assert_eq!(faces.len(), 1);
        let face = &faces[0];
        assert_eq!(face.key.as_deref(), Some(key_b64));
        assert_eq!(face.cdn_key.as_deref(), Some("t1a2b3c"));
        assert_eq!(face.cdn_number, Some(0), "CDN0 is a legal cdn number");
        assert_eq!(face.upload_timestamp, Some(1_700_000_000_000));
        assert_eq!(face.plaintext_hash.as_deref(), Some(hash_hex.as_str()));
        assert_eq!(face.digest, None);
        let full_attachment = &full.attachments[0];
        assert_eq!(full_attachment.content_type.as_deref(), Some("image/png"));
        assert_eq!(full_attachment.size, Some(12));
        assert_eq!(
            full.id,
            stable_hash_id(&[
                "signal-message-v2",
                &account.id,
                &conversation,
                "incoming",
                "1000",
                &stable_hash_id(&[&account.id, "direct", ALICE_E164])
            ]),
            "identity discipline unchanged"
        );
        let wire: serde_json::Value =
            serde_json::to_value(&full.attachments).expect("wire descriptor");
        let wire_text = wire.to_string();
        assert!(
            !wire_text.contains("q83vLNoecho"),
            "key material never rides the host wire descriptor"
        );
        assert!(
            !wire_text.contains("locator"),
            "locator faces are not part of the wire descriptor"
        );

        // Digest form (sender-original): digest face one-of, no plaintext.
        let video = message_at(2000);
        let faces = store
            .history_attachment_locators(&account.id, &video.id)
            .unwrap();
        assert_eq!(faces.len(), 1);
        assert_eq!(faces[0].digest.as_deref(), Some(digest_b64));
        assert_eq!(faces[0].plaintext_hash, None);
        assert_eq!(faces[0].cdn_number, Some(3));

        // Mixed: index-aligned faces including the key-less Invalid hole —
        // the future download wiring must see the hole, not a shifted row.
        let mixed = message_at(3000);
        let faces = store
            .history_attachment_locators(&account.id, &mixed.id)
            .unwrap();
        assert_eq!(faces.len(), 2, "faces stay index-aligned with descriptors");
        assert_eq!(faces[0].key.as_deref(), Some(key_b64));
        assert_eq!(faces[1].key, None, "Invalid locator stays a key-less hole");
        assert_eq!(faces[1].cdn_key, None);
        // v1 metadata (size ladder) intact beside the faces.
        assert_eq!(mixed.attachments.len(), 2);
        assert_eq!(mixed.attachments[1].size, Some(4));

        // v1-shape message: descriptors only, no locators row at all.
        let legacy = message_at(4000);
        assert!(legacy.attachments[0].is_voice_note);
        assert_eq!(
            store
                .history_attachment_locators(&account.id, &legacy.id)
                .unwrap(),
            Vec::new(),
            "no locator key on the engine row → empty face list"
        );

        // A re-run converges onto the same rows (identity dedupe) — the new
        // column never duplicates.
        let second =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(second.imported, 0);
        let faces = store
            .history_attachment_locators(&account.id, &full.id)
            .unwrap();
        assert_eq!(faces.len(), 1);
        assert_eq!(faces[0].key.as_deref(), Some(key_b64));
    }

    /// The import filename face (contract 1.184, §4.184): the archive line
    /// carries the sender's name the same way the live pointer does, so the
    /// §4.184 posture applies to both — verbatim through the official
    /// filters (long Unicode names ride whole; traversal components flatten;
    /// the bidi set maps to U+FFFD; flatten-to-nothing answers absent), the
    /// retired live-arm length rewrite never appearing here either.
    #[test]
    fn imported_filename_shares_the_inbound_posture() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        let long_unicode = format!("{}季度 总结 会 v2.final.pdf", "文".repeat(200));
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming", "text": "names",
                    "attachments": [
                        {"contentType": "image/png", "fileName": long_unicode, "size": 12},
                        {"contentType": "application/pdf", "fileName": "合同/..\\账单.pdf", "size": 3},
                        {"contentType": "text/plain", "fileName": "\u{202E}fdp.txt", "size": 2},
                        {"contentType": "text/plain", "fileName": "..", "size": 1}
                    ]
                })
                .to_string(),
            ],
        );
        run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        let conversation = alice_conversation_id(&account.id);
        let row = store
            .message_by_signal_identity(
                &account.id,
                &conversation,
                "incoming",
                1000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("imported row");
        let names: Vec<Option<&str>> = row
            .attachments
            .iter()
            .map(|entry| entry.filename.as_deref())
            .collect();
        assert_eq!(names.len(), 4);
        assert_eq!(names[0], Some(long_unicode.as_str()));
        assert_eq!(names[1], Some("账单.pdf"));
        assert_eq!(names[2], Some("\u{FFFD}fdp.txt"));
        assert_eq!(
            names[3], None,
            "pure traversal answers absent, never a name"
        );
    }

    /// The voice-waveform pair (contract 1.150, §4.149) never rides an
    /// import row: the backup archive's FilePointer proto has no such
    /// members, so the importer reads neither key — even a hostile archive
    /// line inventing same-named JSON keys cannot smuggle the pair onto a
    /// descriptor, and a voice-flagged archive attachment stays exactly its
    /// pre-1.150 face (the flag bit yes, the pair never).
    #[test]
    fn imported_attachments_never_carry_the_voice_waveform_pair() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming", "text": "voice",
                    "attachments": [{
                        "contentType": "audio/aac", "fileName": "v.aac", "flag": 1, "size": 9,
                        "waveform": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiYw==",
                        "duration": 3.7
                    }]
                })
                .to_string(),
            ],
        );
        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 1);
        let conversation = alice_conversation_id(&account.id);
        let row = store
            .message_by_signal_identity(
                &account.id,
                &conversation,
                "incoming",
                1000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("imported row");
        let voice = &row.attachments[0];
        assert!(voice.is_voice_note, "the flag bit still projects");
        assert_eq!(voice.waveform, None, "FilePointer has no waveform member");
        assert_eq!(voice.duration, None, "FilePointer has no duration member");
        let wire = serde_json::to_value(&row.attachments).unwrap().to_string();
        assert!(!wire.contains("waveform"));
        assert!(!wire.contains("duration"));
    }

    /// Ingest caps and shape gates on the pointer faces (§4.38 attachv2):
    /// oversized key/digest/cdnKey, oversized or non-hex plaintextHash, a
    /// cdnNumber past u32, and uploadTimestamp 0 (the engine never projects
    /// it; a hostile line would) all drop — never stored, never fatal.
    #[test]
    fn locator_ingest_caps_drop_hostile_shapes() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming", "text": "hostile",
                    "attachments": [{
                        "contentType": "image/png", "size": 1,
                        "locator": {
                            "key": "x".repeat(600),
                            "cdnKey": "y".repeat(600),
                            "digest": "z".repeat(600),
                            "plaintextHash": "G".repeat(200),
                            "cdnNumber": 4294967296_u64,
                            "uploadTimestamp": 0
                        }
                    }]
                })
                .to_string(),
            ],
        );
        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 1, "caps drop fields, never the message");
        let conversation = alice_conversation_id(&account.id);
        let row = store
            .message_by_signal_identity(
                &account.id,
                &conversation,
                "incoming",
                1000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("row");
        // Every field over-cap/malformed → the face collapses to empty and
        // the message stores no locator row at all (the `no faces at all`
        // normalization); the descriptor and the row itself are untouched.
        assert_eq!(
            store
                .history_attachment_locators(&account.id, &row.id)
                .unwrap(),
            Vec::new()
        );
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
        let staged = stage_chat(&serde_json::json!({"id": 10, "recipientId": 1}));
        assert_eq!(
            staged.as_ref().map(|chat| (chat.id, chat.recipient_id)),
            Some((10, 1))
        );
        assert_eq!(staged.unwrap().expire_timer_ms, None);
        assert_eq!(
            stage_chat(&serde_json::json!({
                "id": 10, "recipientId": 1, "expirationTimerMs": 86400000
            }))
            .unwrap()
            .expire_timer_ms,
            Some(86_400_000)
        );
        assert_eq!(stage_chat(&serde_json::json!({"id": 10})), None);
    }

    /// Official `PinDetails.pinExpiry` mapping (Backups.proto:494-500): the
    /// timed arm keeps its absolute deadline, the never-expires arm is
    /// forever, an absent key keeps the older-engine forever reading, and a
    /// present-but-armless face is malformed (`strictAssert` upstream) — the
    /// pin is dropped, never invented into forever.
    #[test]
    fn backup_pin_expiry_reads_the_official_oneof_and_rejects_malformed_faces() {
        assert_eq!(
            backup_pin_expiry(&serde_json::json!({
                "pinExpiry": {"pinExpiresAtTimestamp": 9000}
            })),
            Some(Some(9000))
        );
        assert_eq!(
            backup_pin_expiry(&serde_json::json!({
                "pinExpiry": {"pinNeverExpires": true}
            })),
            Some(None)
        );
        assert_eq!(
            backup_pin_expiry(&serde_json::json!({})),
            Some(None),
            "an older engine face without the key reads forever"
        );
        assert_eq!(
            backup_pin_expiry(&serde_json::json!({
                "pinExpiry": {"pinNeverExpires": false}
            })),
            None,
            "neither arm is the official malformed face"
        );
        assert_eq!(
            backup_pin_expiry(&serde_json::json!({"pinExpiry": "bad"})),
            None
        );
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

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
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
        // The backup's own PinDetails clocks ride through verbatim: the pin
        // time is `pinnedAtTimestamp`, the deadline `pinExpiresAtTimestamp`
        // (never the connector clock at import time).
        assert_eq!(pinned.pinned_at, 4000);
        assert_eq!(pinned.expires_at, Some(4_000_000_000_000));

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

        run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        let second =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
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
            required_protocol_version: None,
            author_aci: None,
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
            quote: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            send_outcomes: None,
            admin_deleted: false,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
            server_timestamp: None,
            is_story: false,
            story_message: None,
        };
        assert!(store.insert_message(&live, None, None, true).unwrap());

        let archive = seed_engine(&temp);
        write_archive(&archive, &standard_archive());
        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
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

    /// Post-relink repair: the text-only backup artifact restores a row with
    /// no attachment descriptors, then the archive copy of the same Signal
    /// message (or a live echo that already landed) carries them. Identity
    /// dedupe must not leave the row descriptor-less — the import fills the
    /// empty attachments/locator cells while leaving the existing body alone.
    #[test]
    fn duplicate_import_repairs_a_restored_rows_missing_attachments() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let conversation = store
            .ensure_conversation(&account.id, "direct", ALICE_E164, "8fc***01")
            .unwrap();
        // The restore copy: same identity, body present, no descriptors.
        let live_sender = stable_hash_id(&[&account.id, "direct", ALICE_E164]);
        let restored = MessageRecord {
            required_protocol_version: None,
            author_aci: None,
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
            text: Some("restored body".into()),
            text_bytes: Some(13),
            text_truncated: false,
            text_retrievable: true,
            status: "delivered",
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: None,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            send_outcomes: None,
            admin_deleted: false,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
            server_timestamp: None,
            is_story: false,
            story_message: None,
        };
        assert!(
            store
                .insert_message(&restored, None, Some("restored body"), true)
                .unwrap()
        );

        let archive = seed_engine(&temp);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming", "text": "restored body",
                    "attachments": [{
                        "contentType": "image/png", "fileName": "restored.png", "size": 12,
                        "locator": {
                            "key": "q83vLNoechoM6CiZ+hr3P5ePE3YGAibLE4bAl1jv20Y=",
                            "cdnKey": "restored-cdn", "cdnNumber": 0,
                            "uploadTimestamp": 1_700_000_000_000_u64,
                            "plaintextHash": "a".repeat(64)
                        }
                    }]
                })
                .to_string(),
            ],
        );
        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 0, "the row identity already exists");
        assert_eq!(counters.skipped_messages, 1);
        let row = store
            .message_by_signal_identity(
                &account.id,
                &conversation.id,
                "incoming",
                1000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("restored row");
        assert_eq!(row.text.as_deref(), Some("restored body"), "body untouched");
        assert_eq!(row.attachments.len(), 1, "descriptor repaired");
        assert_eq!(row.attachments[0].filename.as_deref(), Some("restored.png"));
        let faces = store
            .history_attachment_locators(&account.id, &row.id)
            .unwrap();
        assert_eq!(faces.len(), 1);
        assert_eq!(
            faces[0].key.as_deref(),
            Some("q83vLNoechoM6CiZ+hr3P5ePE3YGAibLE4bAl1jv20Y=")
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
        let error = run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort())
            .expect_err("missing archive must fail");
        assert!(matches!(error, ImportFailure::Io(_)));
        let row = store.history_import_row(&account.id).unwrap().unwrap();
        assert_eq!(row.state, "failed");
        assert_eq!(row.error_class.as_deref(), Some("io"));
        assert_eq!(row.attempts, 1);
        run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort())
            .expect_err("still missing");
        let row = store.history_import_row(&account.id).unwrap().unwrap();
        assert_eq!(row.attempts, 2);
    }

    /// Contract 1.42 (§4.41): the archive's disappearing-timer material
    /// lands — a chat frame's `expirationTimerMs` seeds the conversation
    /// state (ms → s, rounded up), a message frame's `expiresInMs`/
    /// `expireStartDate` ride the row verbatim, and a direction-less
    /// `expirationTimerChange` frame applies the announced state and renders
    /// the same timer-notice system row (and identity) a live change lands
    /// as — so a re-run or a live change at the same stamp dedupes.
    #[test]
    fn timer_frames_import_state_notice_rows_and_row_expire_columns() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "recipient", "id": 1, "kind": "contact",
                                   "name": "Alice", "aci": ALICE_ACI, "e164": ALICE_E164})
                .to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1,
                                   "expirationTimerMs": 3_600_000})
                .to_string(),
                // A regular row that states its conversation's timer.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming", "text": "vanishing",
                    "expiresInMs": 1500, "expireStartDate": 500
                })
                .to_string(),
                // The engine's timer-update export: direction-less, ms-keyed.
                serde_json::json!({
                    "type": "message", "chatId": 10, "dateSent": 2000,
                    "expirationTimerChange": {"expiresInMs": 86_400_000}
                })
                .to_string(),
                // The close form: no key inside the change reads as off.
                serde_json::json!({
                    "type": "message", "chatId": 10, "dateSent": 3000,
                    "expirationTimerChange": {}
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 3, "two notices and one regular row");
        assert_eq!(counters.skipped_messages, 0);

        let alice_conversation = alice_conversation_id(&account.id);
        // The chat timer seeded 3600s, the change raised it to 86400s, the
        // close form read it off — the final mirror is the last applied state.
        let summary = store
            .conversation_summary(&account.id, &alice_conversation)
            .unwrap()
            .unwrap();
        assert_eq!(summary.expire_timer_seconds, Some(0));

        // The regular row keeps the envelope-stated timer verbatim (ms → s
        // rounded up), and its sweep deadline derives from the stated start.
        let regular = store
            .message_by_signal_identity(
                &account.id,
                &alice_conversation,
                "incoming",
                1000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("regular row");
        assert_eq!(regular.expire_seconds, Some(2), "1500ms rounds up to 2s");
        assert_eq!(regular.expire_start_at, Some(500));

        // The notice rows carry the live system identity — a live change at
        // the same stamp resolves to the same row — and never sweep.
        let system_sender = stable_hash_id(&[&account.id, "direct", ALICE_E164]);
        let notice = store
            .message_by_signal_identity(
                &account.id,
                &alice_conversation,
                "system",
                2000,
                &system_sender,
                &system_sender,
            )
            .unwrap()
            .expect("timer notice row");
        assert_eq!(notice.text.as_deref(), Some("已更新消息定时消失：86400 秒"));
        assert!(
            notice
                .rich
                .as_ref()
                .is_some_and(|rich| rich.is_expiration_update)
        );
        assert_eq!(notice.expire_seconds, None);
        let close = store
            .message_by_signal_identity(
                &account.id,
                &alice_conversation,
                "system",
                3000,
                &system_sender,
                &system_sender,
            )
            .unwrap()
            .expect("close notice row");
        assert_eq!(close.text.as_deref(), Some("已更新消息定时消失：已关闭"));

        // The deadlines the import stamped still sweep honestly: with the
        // stated starts all in the past, both notice-free rows... the notice
        // rows never appear (NULL deadline), only the regular row does.
        let groups = store
            .sweep_expired_messages(crate::link::now_ms(), crate::store::EXPIRE_SWEEP_BATCH)
            .unwrap();
        let swept: Vec<&str> = groups
            .iter()
            .flat_map(|group| group.message_ids.iter().map(String::as_str))
            .collect();
        assert_eq!(
            swept,
            vec![regular.id.as_str()],
            "only the live-timer row sweeps"
        );
    }

    /// Contract 1.76 (§4.38): the archive's sticker rows land whole — the
    /// 1.35 pack identity rides the row's `sticker` wire field (emoji
    /// optional, absent stays absent) and the `data` byte pointer joins the
    /// descriptor list exactly like a live envelope's sticker image, so its
    /// locator face lands in the internal column only (no key material on
    /// the wire). A v1-shape sticker data (no locator key) keeps the
    /// descriptor-only face.
    #[test]
    fn imported_sticker_rows_carry_identity_and_ride_the_descriptor_list() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        let pack_id = "9b".repeat(32);
        let pack_key = "5j9vTQnMDqGtYvXq3KpeNbJZ2fM8x4mLr6hUcA1sW0E=";
        let key_b64 = "q83vLNoechoM6CiZ+hr3P5ePE3YGAibLE4bAl1jv20Y=";
        let hash_hex = "b".repeat(64);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                // Full face: identity + emoji + transit-locator byte pointer.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming",
                    "sticker": {
                        "packId": pack_id, "packKey": pack_key, "stickerId": 7,
                        "emoji": "🎉",
                        "data": {
                            "contentType": "image/webp", "fileName": "s.webp", "size": 128,
                            "locator": {
                                "key": key_b64, "cdnKey": "sk1", "cdnNumber": 0,
                                "uploadTimestamp": 1_700_000_000_000_u64,
                                "plaintextHash": hash_hex
                            }
                        }
                    }
                })
                .to_string(),
                // Emoji-less face over v1-shape data: the sticker still rides,
                // descriptor-only, no locator row.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2000,
                    "direction": "incoming",
                    "sticker": {
                        "packId": pack_id, "packKey": pack_key, "stickerId": 9,
                        "data": {"contentType": "image/webp", "fileName": "t.webp", "size": 64}
                    }
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 2);
        assert_eq!(counters.skipped_messages, 0);
        let conversation = alice_conversation_id(&account.id);
        let message_at = |sent_at: u64| {
            store
                .message_by_signal_identity(
                    &account.id,
                    &conversation,
                    "incoming",
                    sent_at,
                    &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                    &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
                )
                .unwrap()
                .expect("imported sticker row")
        };

        // Full face: the identity rides the row's own wire field — never
        // smuggled through the descriptor list — and the byte pointer rides
        // the descriptor list with its locator face in the internal column.
        let full = message_at(1000);
        let sticker = full.sticker.as_ref().expect("sticker identity");
        assert_eq!(sticker.pack_id, pack_id);
        assert_eq!(sticker.pack_key, pack_key);
        assert_eq!(sticker.sticker_id, 7);
        assert_eq!(sticker.emoji.as_deref(), Some("🎉"));
        assert_eq!(full.attachments.len(), 1, "data joined the descriptor list");
        assert_eq!(
            full.attachments[0].content_type.as_deref(),
            Some("image/webp")
        );
        assert_eq!(full.attachments[0].size, Some(128));
        let faces = store
            .history_attachment_locators(&account.id, &full.id)
            .unwrap();
        assert_eq!(faces.len(), 1);
        assert_eq!(faces[0].key.as_deref(), Some(key_b64));
        assert_eq!(faces[0].cdn_key.as_deref(), Some("sk1"));
        let wire = serde_json::to_value(&full).expect("wire row");
        assert_eq!(wire["sticker"]["packId"], pack_id.as_str());
        assert_eq!(wire["sticker"]["stickerId"], 7);
        let wire_text = wire.to_string();
        assert!(
            wire_text.contains(pack_key),
            "the pack identity is the 1.35 wire face"
        );
        assert!(
            !wire_text.contains(key_b64),
            "locator key material never rides the wire"
        );
        assert!(
            !wire_text.contains("cdnKey"),
            "no transit face on the wire descriptor"
        );

        // Emoji-less face: the identity still rides, emoji stays absent on
        // the wire, and v1-shape data lands descriptor-only (no faces row).
        let bare = message_at(2000);
        let sticker = bare.sticker.as_ref().expect("sticker identity");
        assert_eq!(sticker.sticker_id, 9);
        assert_eq!(sticker.emoji, None);
        assert_eq!(bare.attachments[0].size, Some(64));
        let wire = serde_json::to_value(&bare).expect("wire row");
        assert!(
            wire["sticker"].get("emoji").is_none(),
            "absent stays absent"
        );
        assert_eq!(
            store
                .history_attachment_locators(&account.id, &bare.id)
                .unwrap(),
            Vec::new(),
            "no locator key → no faces row"
        );

        // A re-run converges (identity discipline covers sticker rows too).
        let second =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(second.imported, 0);
    }

    /// Official sticker rows carry no body (DataMessage.Sticker semantics): an
    /// archive sticker frame riding a stale `text` lands the sticker face with
    /// an empty text column — the same cutoff the live receive arm and the
    /// sent mirror enforce at their normalization authorities.
    #[test]
    fn imported_sticker_rows_never_store_a_body() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        let pack_id = "9b".repeat(32);
        let pack_key = "5j9vTQnMDqGtYvXq3KpeNbJZ2fM8x4mLr6hUcA1sW0E=";
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming",
                    "text": "stale-archive-body",
                    "sticker": {
                        "packId": pack_id, "packKey": pack_key, "stickerId": 7,
                        "emoji": "🎉",
                        "data": {"contentType": "image/webp", "fileName": "s.webp", "size": 128}
                    }
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 1);
        let conversation = alice_conversation_id(&account.id);
        let row = store
            .message_by_signal_identity(
                &account.id,
                &conversation,
                "incoming",
                1000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("imported sticker row");
        assert!(row.sticker.is_some(), "sticker face survives the body cut");
        assert!(row.text.is_none(), "sticker row never stores a body");
        let wire = serde_json::to_value(&row).expect("wire row");
        assert!(wire.get("text").is_none(), "no text key on the wire either");
    }

    /// Contract 1.76 (§4.38): the sticker face rides only whole — a missing
    /// or non-object `data` pointer answers None, and a bodyless message
    /// behind it keeps plain skip routing instead of storing an empty
    /// sticker row (the live `normalized_sticker` discipline).
    #[test]
    fn sticker_rows_without_a_usable_data_pointer_skip() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                // Identity only: no data key at all.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming",
                    "sticker": {"packId": "9b".repeat(32), "packKey": "5j9vTQnMDqGtYvXq3KpeNbJZ2fM8x4mLr6hUcA1sW0E=", "stickerId": 7}
                })
                .to_string(),
                // A non-object data pointer is the same skip class.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2000,
                    "direction": "incoming",
                    "sticker": {"packId": "9b".repeat(32), "packKey": "5j9vTQnMDqGtYvXq3KpeNbJZ2fM8x4mLr6hUcA1sW0E=", "stickerId": 8, "data": "gone"}
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 0);
        assert_eq!(counters.skipped_messages, 2, "the face rode incomplete");
        let row = store.history_import_row(&account.id).unwrap().unwrap();
        assert_eq!(row.state, "completed");
        assert_eq!(row.imported_messages, 0);
    }

    /// Contract 1.76 (§4.38): identity gaps skip the row — a missing packId
    /// or packKey, or a stickerId past u32, drops the whole face (never
    /// stored, never fatal), mirroring the live bounded parse.
    #[test]
    fn imported_sticker_identity_gaps_skip_the_row() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        let data =
            serde_json::json!({"contentType": "image/webp", "fileName": "s.webp", "size": 128});
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                // No packId.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming",
                    "sticker": {"packKey": "5j9vTQnMDqGtYvXq3KpeNbJZ2fM8x4mLr6hUcA1sW0E=", "stickerId": 7, "data": data}
                })
                .to_string(),
                // No packKey.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2000,
                    "direction": "incoming",
                    "sticker": {"packId": "9b".repeat(32), "stickerId": 7, "data": data}
                })
                .to_string(),
                // stickerId past u32.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 3000,
                    "direction": "incoming",
                    "sticker": {"packId": "9b".repeat(32), "packKey": "5j9vTQnMDqGtYvXq3KpeNbJZ2fM8x4mLr6hUcA1sW0E=", "stickerId": 4294967296_u64, "data": data}
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 0);
        assert_eq!(counters.skipped_messages, 3);
    }

    /// Contract 1.78 (§4.38): the archive's contact rows land whole — the
    /// bounded card rides the row's `contact` wire field (rich channel, no
    /// schema bump) and the avatar byte pointer joins the descriptor list
    /// exactly like a live attachment, its locator face in the internal
    /// column only (no key material on the wire). An avatar-less card keeps
    /// the card-only face; entry-level malformation (empty value) drops the
    /// entry, never the row.
    #[test]
    fn imported_contact_rows_carry_the_card_and_ride_the_descriptor_list() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        let key_b64 = "q83vLNoechoM6CiZ+hr3P5ePE3YGAibLE4bAl1jv20Y=";
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                // Full face: name parts + phone + email + organization + an
                // avatar byte pointer with a transit locator.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming",
                    "contact": {
                        "name": {"givenName": "Ada", "familyName": "Lovelace", "prefix": "", "middleName": "Byron"},
                        "number": [
                            {"value": "+15550000003", "type": "HOME"},
                            {"value": "+15550000004", "type": "MOBILE", "label": "cell"},
                            {"value": "", "type": "WORK"}
                        ],
                        "email": [{"value": "ada@example.org", "type": "WORK"}],
                        "organization": "Analytical Engines Ltd",
                        "avatar": {
                            "contentType": "image/jpeg", "fileName": "a.jpg", "size": 55,
                            "locator": {"key": key_b64, "cdnKey": "ac1", "cdnNumber": 0,
                                "plaintextHash": "a".repeat(64)}
                        }
                    }
                })
                .to_string(),
                // Card-only face: no avatar, unknown type coerces to HOME,
                // missing label stays absent.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2000,
                    "direction": "incoming",
                    "contact": {
                        "name": {"givenName": "Grace"},
                        "number": [{"value": "+15550000005", "type": "SATELLITE"}]
                    }
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 2);
        assert_eq!(counters.skipped_messages, 0);
        let conversation = alice_conversation_id(&account.id);
        let message_at = |sent_at: u64| {
            store
                .message_by_signal_identity(
                    &account.id,
                    &conversation,
                    "incoming",
                    sent_at,
                    &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                    &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
                )
                .unwrap()
                .expect("imported contact row")
        };

        let full = message_at(1000);
        let contact = full
            .rich
            .as_ref()
            .expect("rich")
            .contact
            .as_ref()
            .expect("card");
        let name = contact.name.as_ref().expect("name parts");
        assert_eq!(name.given_name.as_deref(), Some("Ada"));
        assert_eq!(name.family_name.as_deref(), Some("Lovelace"));
        assert_eq!(name.middle_name.as_deref(), Some("Byron"));
        assert!(name.prefix.is_none(), "empty parts stay absent");
        assert_eq!(contact.number.len(), 2, "empty-value entry dropped");
        assert_eq!(contact.number[0].kind, "HOME");
        assert_eq!(contact.number[1].kind, "MOBILE");
        assert_eq!(contact.number[1].label.as_deref(), Some("cell"));
        assert_eq!(contact.email[0].value, "ada@example.org");
        assert_eq!(
            contact.organization.as_deref(),
            Some("Analytical Engines Ltd")
        );
        assert_eq!(
            full.attachments.len(),
            1,
            "avatar joined the descriptor list"
        );
        assert_eq!(
            full.attachments[0].content_type.as_deref(),
            Some("image/jpeg")
        );
        let faces = store
            .history_attachment_locators(&account.id, &full.id)
            .unwrap();
        assert_eq!(faces.len(), 1);
        assert_eq!(faces[0].key.as_deref(), Some(key_b64));
        let wire = serde_json::to_value(&full).expect("wire row");
        assert_eq!(wire["contact"]["name"]["givenName"], "Ada");
        assert_eq!(wire["contact"]["number"][0]["value"], "+15550000003");
        assert!(
            !wire.to_string().contains(key_b64),
            "locator key material never rides the wire"
        );

        // Card-only face: unknown type coerces to HOME, no avatar → no
        // descriptor, no faces row.
        let bare = message_at(2000);
        let contact = bare
            .rich
            .as_ref()
            .expect("rich")
            .contact
            .as_ref()
            .expect("card");
        assert_eq!(
            contact.name.as_ref().unwrap().given_name.as_deref(),
            Some("Grace")
        );
        assert_eq!(
            contact.number[0].kind, "HOME",
            "unknown type coerces to HOME"
        );
        assert_eq!(bare.attachments.len(), 0);
        assert_eq!(
            store
                .history_attachment_locators(&account.id, &bare.id)
                .unwrap(),
            Vec::new()
        );

        // A re-run converges (identity discipline covers contact rows too).
        let second =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(second.imported, 0);
    }

    /// Contract 1.78 (§4.38): a contact face with no usable content — a
    /// non-object, an empty object, or a shape whose every field is absent —
    /// answers no card, and the bodyless message behind it keeps plain skip
    /// routing (never a placeholder contact row).
    #[test]
    fn contact_rows_without_a_usable_card_skip() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                // Not an object.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming", "contact": "gone"
                })
                .to_string(),
                // Empty object: no renderable field.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2000,
                    "direction": "incoming", "contact": {}
                })
                .to_string(),
                // All-empty name parts and empty lists.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 3000,
                    "direction": "incoming",
                    "contact": {"name": {"givenName": ""}, "number": [], "email": []}
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 0);
        assert_eq!(
            counters.skipped_messages, 3,
            "the face carried nothing usable"
        );
        let row = store.history_import_row(&account.id).unwrap().unwrap();
        assert_eq!(row.state, "completed");
        assert_eq!(row.imported_messages, 0);
    }

    /// Contract 1.79 (§4.38): a groupUpdate face lands the live group-update
    /// system notice row — 「群聊信息已更新」 with `is_group_update` rich and
    /// the first `title` detail mirrored into `group_name` — under the live
    /// updater identity (outgoing = the account, otherwise the archive author
    /// resolved exactly as an incoming author). A batched change that also
    /// moved the timer splits into the §4.41 timer row and the group row
    /// (official `additionalMessages` semantics); an empty `details` face is
    /// no face at all; a re-run converges.
    #[test]
    fn imported_group_update_rows_land_as_the_live_system_notice() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                group().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                serde_json::json!({"type": "chat", "id": 11, "recipientId": 2}).to_string(),
                // A title change: the notice row carries the mirrored name.
                serde_json::json!({
                    "type": "message", "chatId": 11, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming",
                    "groupUpdate": {"details": [
                        {"type": "title", "newTitle": "New Name"},
                        {"type": "avatar", "removed": false}
                    ]}
                })
                .to_string(),
                // A batched change that also moved the timer: two system rows
                // from one archive row.
                serde_json::json!({
                    "type": "message", "chatId": 11, "authorId": 1, "dateSent": 1100,
                    "direction": "incoming",
                    "expirationTimerChange": {"expiresInMs": 60_000},
                    "groupUpdate": {"details": [{"type": "title", "newTitle": "Second Name"}]}
                })
                .to_string(),
                // The account itself renamed the group: the live outgoing form.
                serde_json::json!({
                    "type": "message", "chatId": 11, "authorId": 1, "dateSent": 1200,
                    "direction": "outgoing",
                    "groupUpdate": {"details": [{"type": "summary"}]}
                })
                .to_string(),
                // Presence without material: plain skip routing.
                serde_json::json!({
                    "type": "message", "chatId": 11, "authorId": 1, "dateSent": 1300,
                    "groupUpdate": {"details": []}
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(
            counters.imported, 4,
            "three group rows and the split timer row"
        );
        assert_eq!(counters.skipped_messages, 1, "empty details are no face");

        let conversation = group_conversation_id(&account.id);
        let group_row = |sent_at: u64| {
            store
                .message_by_signal_identity(
                    &account.id,
                    &conversation,
                    "system",
                    sent_at,
                    &stable_hash_id(&[&account.id, "group", ALICE_E164]),
                    &stable_hash_id(&[&account.id, "group", ALICE_ACI]),
                )
                .unwrap()
                .expect("group notice row")
        };

        let renamed = group_row(1000);
        assert_eq!(renamed.text.as_deref(), Some("群聊信息已更新"));
        assert_eq!(renamed.status, "system");
        let rich = renamed.rich.as_ref().expect("rich");
        assert!(rich.is_group_update);
        assert_eq!(rich.group_name.as_deref(), Some("New Name"));
        let wire = serde_json::to_value(&renamed).expect("wire row");
        assert_eq!(wire["isGroupUpdate"], true);
        assert_eq!(wire["groupName"], "New Name");
        assert_eq!(renamed.expire_seconds, None, "notice rows never sweep");

        // The batched row split: the timer face keeps its §4.41 identity, the
        // group face its live updater identity — two rows, one archive line.
        let timer_sender = stable_hash_id(&[&account.id, "group", GROUP_MASTER_KEY]);
        let timer = store
            .message_by_signal_identity(
                &account.id,
                &conversation,
                "system",
                1100,
                &timer_sender,
                &timer_sender,
            )
            .unwrap()
            .expect("timer row of the split");
        assert_eq!(timer.text.as_deref(), Some("已更新消息定时消失：60 秒"));
        assert!(
            timer
                .rich
                .as_ref()
                .is_some_and(|rich| rich.is_expiration_update)
        );
        let split_group = group_row(1100);
        assert_eq!(split_group.text.as_deref(), Some("群聊信息已更新"));
        assert_ne!(
            split_group.id, timer.id,
            "distinct senders give the two faces distinct rows"
        );

        // Outgoing: the account itself is the sender, no name material in a
        // summary-only update.
        let own = store
            .message_by_signal_identity(
                &account.id,
                &conversation,
                "system",
                1200,
                &account.id,
                &account.id,
            )
            .unwrap()
            .expect("outgoing group notice row");
        assert_eq!(own.sender_id, account.id);
        assert_eq!(
            own.rich.as_ref().and_then(|rich| rich.group_name.clone()),
            None
        );

        // A re-run converges (identity discipline covers group rows too).
        let second =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(second.imported, 0);
    }

    /// Contract 1.81 (§4.38): the seven non-text expansion faces land whole.
    /// The poll folds per-voter through the recipient ladder (contact →
    /// E164, self → the account itself), the payment crosses note-only, the
    /// gift badge lowercases onto the wire vocabulary, a burned view-once
    /// marker is a bodyless-yet-real placeholder row, the story reply
    /// resolves its author from direction (outgoing = the peer, incoming =
    /// the account, sentTimestamp pinned 0), and the admin-deleted face is
    /// a tombstone flag beside an intact row with the status ladder
    /// untouched.
    #[test]
    fn imported_expansion_faces_land_whole() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "recipient", "id": 3, "kind": "self", "name": ""})
                    .to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                // Poll: alice picks option 0, the account picks option 1
                // twice (the dedupe is the official aggregation's).
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming",
                    "poll": {
                        "question": "Lunch?",
                        "options": [
                            {"option": "Ramen", "votes": [{"voterId": 1, "voteCount": 1}]},
                            {"option": "Sushi", "votes": [{"voterId": 3, "voteCount": 1},
                                {"voterId": 3, "voteCount": 1}]}
                        ],
                        "allowMultiple": true, "hasEnded": true
                    }
                })
                .to_string(),
                // Payment: the note crosses, the ledger material never does.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2000,
                    "direction": "incoming",
                    "payment": {"amountMob": 100, "feeMob": 1, "note": "coffee",
                        "transactionDetailsBase64": "AAAA"}
                })
                .to_string(),
                // Gift badge: the capitalized official state lowercases.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 3000,
                    "direction": "incoming",
                    "giftBadge": {"state": "Unopened"}
                })
                .to_string(),
                // Burned view-once: no medium, no text — the placeholder row.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 4000,
                    "direction": "incoming", "viewOnce": true, "isErased": true
                })
                .to_string(),
                // Incoming story reply: the answered story is the account's.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 5000,
                    "direction": "incoming",
                    "storyReply": {"body": "nice story"}
                })
                .to_string(),
                // Outgoing story reply with the long-text overflow: the
                // answered story is the peer's.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 6000,
                    "direction": "outgoing",
                    "storyReply": {"bodyAttachment": {"contentType": "text/plain"}}
                })
                .to_string(),
                // Admin-deleted tombstone beside an intact body.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 7000,
                    "direction": "incoming", "text": "spam",
                    "adminDeleted": {"adminId": 0}
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 7);
        assert_eq!(counters.skipped_messages, 0);
        let conversation = alice_conversation_id(&account.id);
        let message_at = |sent_at: u64| {
            store
                .message_by_signal_identity(
                    &account.id,
                    &conversation,
                    "incoming",
                    sent_at,
                    &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                    &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
                )
                .unwrap()
                .expect("imported expansion row")
        };

        // Poll: first-seen voter order, deduped option indexes, camelCase
        // wire keys.
        let poll_row = message_at(1000);
        let poll = poll_row
            .rich
            .as_ref()
            .expect("rich")
            .poll
            .as_ref()
            .expect("poll");
        assert_eq!(poll.question, "Lunch?");
        assert_eq!(poll.options, vec!["Ramen", "Sushi"]);
        assert!(poll.allow_multiple);
        assert!(poll.terminated);
        assert_eq!(poll.votes.len(), 2);
        assert_eq!(poll.votes[0].voter_id, ALICE_E164);
        assert_eq!(poll.votes[0].option_indexes, vec![0]);
        assert_eq!(poll.votes[1].voter_id, SIGNAL_ACCOUNT);
        assert_eq!(poll.votes[1].option_indexes, vec![1]);
        let poll_wire = serde_json::to_value(&poll_row).expect("wire");
        assert_eq!(poll_wire["poll"]["question"], "Lunch?");
        assert_eq!(poll_wire["poll"]["allowMultiple"], true);
        assert_eq!(poll_wire["poll"]["terminated"], true);
        assert_eq!(poll_wire["poll"]["votes"][0]["voterId"], ALICE_E164);
        assert_eq!(
            poll_wire["poll"]["votes"][0]["optionIndexes"],
            serde_json::json!([0])
        );
        assert_eq!(poll_wire["poll"]["votes"][1]["voterId"], SIGNAL_ACCOUNT);
        assert!(
            !poll_wire.to_string().contains("voteCount"),
            "the official per-vote tally has no desktop face"
        );

        // Payment: note-only on the wire.
        let payment_row = message_at(2000);
        let payment = payment_row
            .rich
            .as_ref()
            .expect("rich")
            .payment
            .as_ref()
            .expect("payment");
        assert_eq!(payment.note.as_deref(), Some("coffee"));
        let payment_wire = serde_json::to_value(&payment_row).expect("wire");
        assert_eq!(payment_wire["payment"]["note"], "coffee");
        let payment_wire_text = payment_wire.to_string();
        assert!(
            !payment_wire_text.contains("amountMob")
                && !payment_wire_text.contains("feeMob")
                && !payment_wire_text.contains("transactionDetailsBase64")
                && !payment_wire_text.contains("AAAA"),
            "the ledger material never rides the wire"
        );

        // Gift badge: lowercased state.
        let badge_row = message_at(3000);
        let badge = badge_row
            .rich
            .as_ref()
            .expect("rich")
            .gift_badge
            .as_ref()
            .expect("gift badge");
        assert_eq!(badge.state, "unopened");
        let badge_wire = serde_json::to_value(&badge_row).expect("wire");
        assert_eq!(badge_wire["giftBadge"]["state"], "unopened");

        // Burned view-once placeholder: no medium, the live no-renderable
        // flags, standard row identity.
        let burned = message_at(4000);
        let burned_rich = burned.rich.as_ref().expect("rich");
        assert!(burned_rich.view_once);
        assert!(burned_rich.view_once_invalid);
        assert!(burned.text.is_none());
        assert!(burned.attachments.is_empty());

        // Incoming story reply: the account's own story, timestamp pinned 0.
        let reply_in = message_at(5000);
        assert_eq!(reply_in.text.as_deref(), Some("nice story"));
        let story_in = reply_in
            .rich
            .as_ref()
            .expect("rich")
            .story_reply
            .as_ref()
            .expect("story reply");
        assert_eq!(story_in.author_aci, SIGNAL_ACCOUNT);
        assert_eq!(story_in.sent_timestamp, 0);
        let story_in_wire = serde_json::to_value(&reply_in).expect("wire");
        assert_eq!(story_in_wire["storyReply"]["authorAci"], SIGNAL_ACCOUNT);
        assert_eq!(story_in_wire["storyReply"]["sentTimestamp"], 0);

        // Outgoing story reply: the peer's story; the long-text pointer
        // marks the metadata-only body.
        let reply_out = store
            .message_by_signal_identity(
                &account.id,
                &conversation,
                "outgoing",
                6000,
                &account.id,
                &account.id,
            )
            .unwrap()
            .expect("outgoing story reply row");
        let story_out = reply_out
            .rich
            .as_ref()
            .expect("rich")
            .story_reply
            .as_ref()
            .expect("story reply");
        assert_eq!(story_out.author_aci, ALICE_E164);
        assert!(reply_out.text_truncated, "long-text pointer = truncated");
        assert!(!reply_out.text_retrievable);

        // Admin-deleted: the tombstone flag beside the intact body, the
        // status ladder untouched.
        let admin_row = message_at(7000);
        assert_eq!(admin_row.text.as_deref(), Some("spam"));
        assert!(admin_row.admin_deleted);
        assert_eq!(admin_row.status, "delivered");

        // A re-run converges (identity discipline covers every face).
        let second =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(second.imported, 0);
    }

    /// Contract 1.81 (§4.38): honest routing around damaged faces — a voter
    /// the ladder cannot resolve drops vote-level (never the poll), a
    /// wrong-typed face answers no face so the bodyless row behind it keeps
    /// plain skip routing, the adhocCall frame is recognized (skip-counted
    /// as a message, never the junk-lines bucket), and a group conversation
    /// carries no story context (the body lands plain, the face honestly
    /// absent).
    #[test]
    fn expansion_face_malformation_keeps_plain_routing() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        write_archive(
            &archive,
            &[
                alice().to_string(),
                group().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                serde_json::json!({"type": "chat", "id": 11, "recipientId": 2}).to_string(),
                // Unresolvable voter: vote-level drop, the poll stays.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming",
                    "poll": {"question": "Q?", "options": [{"option": "A",
                        "votes": [{"voterId": 99, "voteCount": 1}]}]}
                })
                .to_string(),
                // Wrong-typed faces behind bodyless rows: plain skip.
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2000,
                    "direction": "incoming", "payment": "junk"
                })
                .to_string(),
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2100,
                    "direction": "incoming", "poll": "junk"
                })
                .to_string(),
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2200,
                    "direction": "incoming", "giftBadge": {"state": 42}
                })
                .to_string(),
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2300,
                    "direction": "incoming", "storyReply": "junk"
                })
                .to_string(),
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 2400,
                    "direction": "incoming", "adminDeleted": "junk"
                })
                .to_string(),
                // Group story reply: no story context, the body lands plain.
                serde_json::json!({
                    "type": "message", "chatId": 11, "authorId": 1, "dateSent": 3000,
                    "direction": "incoming",
                    "storyReply": {"body": "group reply"}
                })
                .to_string(),
                // The adhocCall frame: recognized, skip-counted as a message.
                serde_json::json!({
                    "type": "adhocCall", "callId": "c1", "recipientId": 1,
                    "state": "GenericAdhocCall", "callTimestamp": 4000
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 2, "the poll and the group reply");
        assert_eq!(counters.skipped_messages, 6, "5 wrong-typed + adhocCall");
        assert_eq!(counters.skipped_lines, 0, "adhocCall is never junk");

        // The poll survives with no votes (the key stays off the wire).
        let conversation = alice_conversation_id(&account.id);
        let poll_row = store
            .message_by_signal_identity(
                &account.id,
                &conversation,
                "incoming",
                1000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("poll row");
        let poll = poll_row
            .rich
            .as_ref()
            .expect("rich")
            .poll
            .as_ref()
            .expect("poll");
        assert_eq!(poll.question, "Q?");
        assert!(poll.votes.is_empty());
        let poll_wire = serde_json::to_value(&poll_row).expect("wire");
        assert!(poll_wire["poll"].get("votes").is_none());

        // The group reply keeps its body, the face honestly absent.
        let group_conversation = stable_hash_id(&[&account.id, "group", GROUP_MASTER_KEY]);
        let group_row = store
            .message_by_signal_identity(
                &account.id,
                &group_conversation,
                "incoming",
                3000,
                &stable_hash_id(&[&account.id, "group", ALICE_E164]),
                &stable_hash_id(&[&account.id, "group", ALICE_ACI]),
            )
            .unwrap()
            .expect("group reply row");
        assert_eq!(group_row.text.as_deref(), Some("group reply"));
        assert!(
            group_row
                .rich
                .as_ref()
                .is_none_or(|rich| rich.story_reply.is_none()),
            "a group carries no story context"
        );
    }

    /// Contract 1.81 (§4.38): the hostile-line caps truncate with the
    /// renderable-prefix honesty — option entries past the cap drop whole
    /// (their votes dangle with them), the question truncates at its char
    /// cap, and neither is fatal.
    #[test]
    fn poll_caps_truncate_never_fatal() {
        let temp = TempDir::new().unwrap();
        let store = test_store(&temp.path().join("store"));
        let account = store
            .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let archive = seed_engine(&temp);
        let mut options: Vec<Value> = (0..MAX_POLL_OPTIONS)
            .map(|index| serde_json::json!({"option": format!("opt{index}"), "votes": []}))
            .collect();
        // The entry past the cap carries the only vote — it drops whole.
        options.push(serde_json::json!({"option": "overflow",
            "votes": [{"voterId": 1, "voteCount": 1}]}));
        write_archive(
            &archive,
            &[
                alice().to_string(),
                serde_json::json!({"type": "chat", "id": 10, "recipientId": 1}).to_string(),
                serde_json::json!({
                    "type": "message", "chatId": 10, "authorId": 1, "dateSent": 1000,
                    "direction": "incoming",
                    "poll": {"question": "長".repeat(MAX_POLL_QUESTION_CHARS + 50),
                        "options": options}
                })
                .to_string(),
            ],
        );

        let counters =
            run_import(&store, &account.id, SIGNAL_ACCOUNT, &archive, &idle_abort()).unwrap();
        assert_eq!(counters.imported, 1);
        let conversation = alice_conversation_id(&account.id);
        let row = store
            .message_by_signal_identity(
                &account.id,
                &conversation,
                "incoming",
                1000,
                &stable_hash_id(&[&account.id, "direct", ALICE_E164]),
                &stable_hash_id(&[&account.id, "direct", ALICE_ACI]),
            )
            .unwrap()
            .expect("capped poll row");
        let poll = row
            .rich
            .as_ref()
            .expect("rich")
            .poll
            .as_ref()
            .expect("poll");
        assert_eq!(poll.question.chars().count(), MAX_POLL_QUESTION_CHARS);
        assert_eq!(poll.options.len(), MAX_POLL_OPTIONS);
        assert_eq!(poll.options[0], "opt0");
        assert_eq!(
            poll.options[MAX_POLL_OPTIONS - 1],
            format!("opt{}", MAX_POLL_OPTIONS - 1)
        );
        assert!(
            poll.votes.is_empty(),
            "the overflowing entry's vote dangles"
        );
    }
}
