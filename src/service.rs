// SPDX-License-Identifier: AGPL-3.0-only

use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use unicode_segmentation::UnicodeSegmentation;

use crate::engine::{
    ControlReceive, EngineError, NormalizedAttachment, NormalizedReceive, SignalCliMode,
    truncate_utf8_bytes,
};
use crate::groups::MAX_ACCOUNTS_PER_ENGINE;
use crate::ids::{mask_address, random_id, stable_hash_id};
use crate::link::{ActiveLinkSession, LINK_SESSION_TTL, now_ms};
use crate::media::{MEDIA_CHUNK_BYTES, MediaGovernor, MediaHandleError, MediaHandleTable};
use crate::protocol::ApiError;
use crate::store::{
    AccountDeletePlan, AccountRow, AccountSummary, ContactSummary, ConversationRow,
    ConversationSummary, MessageRecord, Page, Store, StoreError, SyncedContact,
};

/// Bounds below are the code-side enforcement of the host-facing schema
/// (schemas/connector-api-v1.schema.json); tests/schema_consistency.rs diffs
/// each constant against its schema constraint, so the two cannot drift
/// apart silently.
pub const MAX_TEXT_BYTES: usize = 64 * 1024;
/// Receive text ladder (§6.4/§4.38): a body at or below this bound persists in
/// full; anything larger keeps only the bounded preview (shared by the
/// link-time history import, which has no engine copy to fetch on demand).
pub(crate) const MAX_INBOUND_TEXT_BYTES: usize = 128 * 1024;
pub(crate) const MAX_HOST_TEXT_PREVIEW_BYTES: usize = 4 * 1024;
pub const MAX_DEVICE_NAME_BYTES: usize = 64;
pub const MAX_EMOJI_BYTES: usize = 32;
/// messages.sendSticker bounds (contract revision 1.35, §4.31): the official
/// pack identity shapes — hex pack id, standard base64 pack key, and the
/// emoji label, each mirrored by the engine's receive projection bounds.
pub const MAX_STICKER_PACK_ID_CHARS: usize = 64;
pub const MAX_STICKER_PACK_KEY_CHARS: usize = 128;
pub const MAX_STICKER_EMOJI_CHARS: usize = 32;
/// Sticker pack browsing bounds (contract revision 1.36, §4.32): the browse
/// face aligns with the official `isPackIdValid` (exactly 32 hex characters)
/// and the official pack key (base64 decoding to exactly 32 bytes); title and
/// author carry a 256-character KT bound the official decoder does not
/// enforce; the manifest entry cap mirrors the official 200-sticker display
/// truncation semantics at the decoder level.
pub const STICKER_PACK_BROWSE_ID_HEX_CHARS: usize = 32;
pub const STICKER_PACK_BROWSE_KEY_BYTES: usize = 32;
pub const MAX_STICKER_MANIFEST_TITLE_CHARS: usize = 256;
pub const MAX_STICKER_MANIFEST_AUTHOR_CHARS: usize = 256;
pub const MAX_STICKER_MANIFEST_ENTRIES: usize = 1024;
/// The decrypted sticker image budget (§4.32): the official CDN caps sticker
/// uploads at 300 KB; the base64 result bound is the exact padded encoding of
/// that ceiling — 4 * ceil(307200 / 3) = 409600 characters.
pub const MAX_STICKER_IMAGE_BYTES: usize = 300 * 1024;
pub const MAX_STICKER_IMAGE_BASE64_CHARS: usize = 4 * MAX_STICKER_IMAGE_BYTES.div_ceil(3);
/// Conversation pin sync bounds (contract revision 1.36, §4.33): the host
/// may address a conversation wider than the local opaqueId family, and the
/// returned pin list is capped (KT bound; the official clients declare no
/// explicit cap) so one response frame stays bounded.
pub const MAX_PIN_CONVERSATION_ID_CHARS: usize = 256;
pub const MAX_PINNED_CONVERSATIONS: usize = 128;
/// Safety-number faces (contract revision 1.68, §4.61): the caller-echoed
/// verification string bound. The pinned upstream parses spaced 60-digit
/// groups (71 chars), 66-char hex fingerprints, or base64 scannable
/// payloads (IdentityVerificationCode.java:20-32); 256 keeps one wire frame
/// bounded while every legitimate pinned form fits with room to spare. The
/// connector enforces only the bound — the grammar itself is the upstream's
/// definite UserError, never a local re-derivation.
pub const IDENTITY_SAFETY_NUMBER_MAX_CHARS: usize = 256;
/// Sticker pack sync bounds (contract revision 1.37, §4.34): the pack key
/// when present is the §4.32 browse shape — exactly 32 bytes, whose padded
/// standard-base64 encoding is exactly 44 characters — and the returned pack
/// list is capped (KT bound; the official clients declare no explicit cap) so
/// one response frame stays bounded.
pub const STICKER_PACK_SYNC_KEY_B64_CHARS: usize = 44;
pub const MAX_STICKER_PACK_SYNCS: usize = 256;
/// Account settings bounds (contract revision 1.46, §4.45): the profile text
/// budgets mirror the official desktop ProfileEditor (v8.31
/// `ts/components/ProfileEditor.dom.tsx`) — both name components maxBytes 128,
/// the bio maxByteCount 512 — and the emoji label reuses the repo-wide emoji
/// bound. The avatar budget is the engine's stated 10 MiB decoded ceiling
/// (AvatarHelper-scale); the blocked list cap is a KT bound (the official
/// clients declare no explicit cap) so one response frame stays bounded.
pub const MAX_PROFILE_NAME_BYTES: usize = 128;
pub const MAX_PROFILE_ABOUT_BYTES: usize = 512;
pub const MAX_AVATAR_BYTES: usize = 10 * 1024 * 1024;
/// Username bounds (contract revision 1.59, §4.55): the fail-closed mirror of
/// the pinned signal-cli username rules — `AccountHelper.java:363-364` pins
/// USERNAME_MIN_LENGTH 3 / USERNAME_MAX_LENGTH 32 and passes them to
/// `Username.candidatesFrom(nickname, ...)`, so the window binds the
/// *nickname* (the libsignal denominators); the official desktop
/// `ts/util/Username.dom.ts` grammar is: nickname `[a-zA-Z_][a-zA-Z0-9_]*`,
/// optional discriminator `.N` with N all-digits, no leading-zero forms
/// (`0…` beyond exactly-two-digits). The connector enforces this shape
/// locally before any upstream call; the server-side reserve/confirm remains
/// the authority (a shape-valid but taken username answers UPSTREAM_ERROR).
pub const USERNAME_MIN_CHARS: usize = 3;
pub const USERNAME_MAX_CHARS: usize = 32;
/// Whole-string wire bound for `accounts.setUsername` (contract revision
/// 1.59, §4.55): the nickname window plus the separator and the widest
/// legal discriminator digit run (5 digits for the 0..65535 range; the
/// two-digit `0N` forms are shorter) — a KT frame bound, enforced before
/// the grammar walk.
pub const USERNAME_MAX_WIRE_CHARS: usize = 38;
/// username.lookup batch cap (contract revision 1.59, §4.55): a KT bound for
/// one host frame — the official getUserStatus takes an open array, the
/// connector refuses anything wider.
pub const USERNAME_LOOKUP_MAX_ENTRIES: usize = 20;
/// Result-face coarse guard for `conversations.getAvatar` (§4.47): the
/// engine caps a download at [`MAX_AVATAR_BYTES`] decoded, so the data URI
/// — `data:<mime>;base64,` prefix plus the padded base64 encoding of the
/// ceiling — stays under this margin (the §4.45 upload schema margin); the
/// precise decoded budget is enforced in `avatar_from_engine` by decoding
/// and cross-checking `byteSize`, not by this length.
pub const MAX_AVATAR_DATA_URI_CHARS: usize = 13_981_080;
pub const MAX_BLOCKED_CONVERSATIONS: usize = 256;
/// Call signaling (contract revision 1.50, §4.48): the `calls.sendSignal`
/// shape gate. `type` is the five-value RingRTC frame-family enum, `callId`
/// is a positive u32 (the legacy frames' `id` face and the state machines'
/// address space), and the opaque payload is a non-empty string kept under
/// the same coarse 3 MiB budget the receive face enforces — the connector
/// validates the envelope, never the signaling content (the engine and the
/// RingRTC layer each validate their own half).
pub const CALL_SIGNAL_TYPES: [&str; 5] = ["offer", "answer", "ice", "busy", "hangup"];
pub const MAX_CALL_SIGNAL_PAYLOAD_CHARS: usize = 3 * 1024 * 1024;
/// The `calls.getState` projection note (contract revision 1.50, §4.48):
/// one honest sentence about where the call state machine actually lives.
pub const CALL_STATE_PROJECTION_NOTE: &str = "connector-pipe";
/// contacts.setLocalAlias bound (contract revision 1.10): the alias is a
/// short display name, not a free-form profile field — 128 bytes matches the
/// peerKey/opaqueId bound and keeps the upstream `updateContact` payload
/// trivially small.
pub const MAX_ALIAS_BYTES: usize = 128;
/// Attachment size budget (contract revision 1.16, implementation-plan
/// §4.15): the official clients' 100 MiB ceiling. The engine's upstream line
/// limit and the host frame budget move with it (both 160 MiB, see lib.rs
/// DEFAULT_UPSTREAM_LINE_LIMIT / DEFAULT_HOST_FRAME_LIMIT) so one maximum
/// attachment's base64 always fits a single line/frame.
pub const MAX_ATTACHMENT_BYTES: usize = 100 * 1024 * 1024;
/// Encoded length of [`MAX_ATTACHMENT_BYTES`]: 4 * ceil(n / 3), the exact
/// padded-base64 length java.util.Base64 emits.
const MAX_ATTACHMENT_BASE64_CHARS: usize = 4 * MAX_ATTACHMENT_BYTES.div_ceil(3);
/// Attachment send bounds (contract revision 1.13, implementation-plan
/// §4.12): filename/contentType are caller-supplied display descriptors for
/// the upstream data URI, bounded like the opaqueId family; the decoded
/// payload must match the declared `sizeBytes` exactly (§4.7 discipline).
pub const MAX_ATTACHMENT_FILENAME_BYTES: usize = 128;
pub const MAX_ATTACHMENT_CONTENT_TYPE_BYTES: usize = 128;
/// An upstream attachment id (receive-time metadata field `id`), bounded to
/// the schema's `attachmentId` maxLength (schemas/connector-api-v1.schema.json
/// is the single source for this length); Signal ids stay far below it, the
/// bound only rejects absurd values early.
pub const MAX_ATTACHMENT_ID_BYTES: usize = 128;
/// Every opaque identifier the host frame carries (accountId, conversationId,
/// messageId, clientRequestId, operationId, peerKey, peerTitle, query,
/// groupKey — the schema's opaqueId family and its inline 128 peers).
pub const MAX_OPAQUE_ID_BYTES: usize = 128;
/// The receipt selection cap (contract revision 1.34, §4.29): both the
/// absent-`messageIds` scan and an explicit list are bounded at 512 incoming
/// rows — the schema declares the same bound as the early shape guard, this
/// constant is the authoritative code-side enforcement.
pub const MARK_RECEIPT_MESSAGE_IDS_LIMIT: usize = 512;
/// The outbound @mention cap (contract revision 1.34): the §4.20 receive
/// projection cap mirrored — entries beyond 64 are dropped, mirroring the
/// official bounded BodyRange list.
pub const MAX_SEND_MENTIONS: usize = 64;
/// The multi-attachment send cap (contract revision 1.66, §4.59): the
/// upstream engine's own per-message attachment ceiling (pinned signal-cli /
/// kt-engine `ATTACHMENT_MAX_COUNT` = 32) mirrored — a `messages.attachments.
/// send` plural `attachments` array answers INVALID_REQUEST past 32 entries
/// before any pending row exists, instead of the engine's late internal
/// error. A forwarded multi-attachment message never exceeds this shape.
pub const MAX_SEND_ATTACHMENTS: usize = 32;
/// The outbound text-style cap (contract revision 1.69, §4.62): the §4.20
/// receive projection cap mirrored — a `messages.sendText` `textStyle` array
/// answers INVALID_REQUEST past 64 entries before any pending row exists.
pub const MAX_SEND_TEXT_STYLES: usize = 64;
/// The group roster cap (contract revision 1.40, §4.39): the official
/// GroupsV2 group-size ceiling. The contacts sync projects at most this many
/// members into the cached group row; entries past the cap drop.
pub const MAX_GROUP_MEMBERS: usize = 1001;
/// One roster member address (contract 1.40): a number or UUID-shaped ACI,
/// bounded like the opaqueId family it must stay joinable against.
pub const MAX_GROUP_MEMBER_ID_CHARS: usize = 128;
/// Group-management string bounds (contract 1.45, §4.44): the display-name
/// and member-label byte ceiling, the description byte ceiling. The schema
/// mirrors them as character bounds; the service enforces bytes (stricter or
/// equal for every input).
pub const GROUP_NAME_MAX_BYTES: usize = 256;
pub const GROUP_DESCRIPTION_MAX_BYTES: usize = 4096;
pub const GROUP_MEMBER_LABEL_MAX_BYTES: usize = 256;
/// The upstream member-label read budgets (contract 1.67, §4.60): the pinned
/// server-side caps the official client applies to labels it stores for the
/// group (`ts/types/GroupMemberLabels.std.ts` `SERVER_STRING_BYTE_LIMIT` /
/// `SERVER_EMOJI_BYTE_LIMIT`). The §4.60 sync capture passes label fields
/// through under these budgets — a value past its budget drops the key rather
/// than truncating, so the read projection never renders a clipped label.
pub const GROUP_MEMBER_LABEL_SERVER_MAX_BYTES: usize = 512;
pub const GROUP_MEMBER_LABEL_EMOJI_MAX_BYTES: usize = 64;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error("api error: {0}")]
    Api(ApiError),
    /// Local filesystem work outside the store (download temp dirs, media
    /// directory creation). Log-safe class only — never the OS error text.
    #[error("local filesystem io failed")]
    Io,
}

impl ServiceError {
    /// Log-safe classification: variant category only, never any content the
    /// error may carry. Logs use this instead of the Display text.
    pub fn class(&self) -> &'static str {
        match self {
            ServiceError::Store(_) => "store",
            ServiceError::Engine(_) => "engine",
            ServiceError::Api(_) => "api",
            ServiceError::Io => "io",
        }
    }

    pub fn into_api(self) -> ApiError {
        match self {
            ServiceError::Api(error) => error,
            ServiceError::Store(StoreError::AccountNotFound) => {
                ApiError::new("ACCOUNT_NOT_FOUND", "account was not found", false)
            }
            ServiceError::Store(StoreError::ConversationNotFound) => ApiError::new(
                "CONVERSATION_NOT_FOUND",
                "conversation was not found",
                false,
            ),
            ServiceError::Store(StoreError::MessageNotFound) => {
                ApiError::new("MESSAGE_NOT_FOUND", "message was not found", false)
            }
            ServiceError::Store(StoreError::OperationConflict) => ApiError::new(
                "INVALID_REQUEST",
                "account delete operation conflicts with existing state",
                false,
            ),
            ServiceError::Store(StoreError::InvalidCursor) => {
                ApiError::new("INVALID_REQUEST", "pagination cursor is invalid", false)
            }
            ServiceError::Store(_) => {
                ApiError::new("INTERNAL_ERROR", "connector store failed", true)
            }
            ServiceError::Engine(EngineError::NotRunning) => ApiError::new(
                "RUNTIME_NOT_RUNNING",
                "signal-cli runtime is not running",
                false,
            ),
            ServiceError::Engine(EngineError::Timeout) => {
                ApiError::new("UPSTREAM_TIMEOUT", "signal-cli request timed out", true)
            }
            ServiceError::Engine(EngineError::UnknownOutcome) => ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "mutating request has an unknown outcome",
                false,
            ),
            ServiceError::Engine(EngineError::Exited) => {
                ApiError::new("UPSTREAM_EXITED", "signal-cli exited", true)
            }
            ServiceError::Engine(EngineError::Protocol) => {
                ApiError::new("UPSTREAM_PROTOCOL_ERROR", "signal-cli protocol error", true)
            }
            ServiceError::Engine(EngineError::Upstream) => {
                ApiError::new("UPSTREAM_ERROR", "signal-cli returned an error", true)
            }
            // Contract 1.36 structured engine errors (§4.32/§4.33): the code
            // is a fixed engine literal from the engine.rs allowlist — mapped
            // 1:1 onto the same-named host code, with the retryability the
            // contract fixes per family (key-invalid/too-large/not-resolved
            // are deterministic refusals; fetch/malformed/storage failures
            // may succeed on a later attempt). STORAGE_* is the engine-final
            // family (the connector declares the current members, it does not
            // invent new ones); an engine code outside the allowlist never
            // reaches this arm — it degrades to plain UPSTREAM_ERROR.
            ServiceError::Engine(EngineError::UpstreamCode { code, data }) => {
                // Contract 1.50: PROFILE_UPLOAD_FAILED carries the engine's
                // partial-success data object ({retryable, textSaved})
                // verbatim — the only code whose host face depends on the
                // payload. Every other allowlisted code keeps the fixed
                // content-free face below; their data (if any) is dropped
                // here rather than leaking engine internals unpromised.
                if code == "PROFILE_UPLOAD_FAILED"
                    && let Some(data) = data
                {
                    return ApiError::with_data(
                        "PROFILE_UPLOAD_FAILED",
                        "the profile avatar upload failed after the text face was saved",
                        data,
                    );
                }
                match code {
                    "STICKER_PACK_KEY_INVALID" => ApiError::new(
                        "STICKER_PACK_KEY_INVALID",
                        "sticker pack key is invalid",
                        false,
                    ),
                    "STICKER_PACK_FETCH_FAILED" => ApiError::new(
                        "STICKER_PACK_FETCH_FAILED",
                        "sticker pack could not be fetched",
                        true,
                    ),
                    "STICKER_PACK_MALFORMED" => ApiError::new(
                        "STICKER_PACK_MALFORMED",
                        "sticker pack manifest is malformed",
                        true,
                    ),
                    "STICKER_IMAGE_TOO_LARGE" => ApiError::new(
                        "STICKER_IMAGE_TOO_LARGE",
                        "sticker image exceeds the size budget",
                        false,
                    ),
                    // §4.47 avatar face (contract 1.48): UNAVAILABLE is the
                    // honest no-avatar answer (peer/group has none, profile key
                    // never synced, server 403/404) — the state is the answer,
                    // not a failure, so not retryable; FETCH_FAILED is
                    // network/download/decrypt and may succeed on a later
                    // attempt; TOO_LARGE is deterministic (the same avatar does
                    // not shrink between attempts).
                    "AVATAR_UNAVAILABLE" => ApiError::new(
                        "AVATAR_UNAVAILABLE",
                        "the conversation has no downloadable avatar",
                        false,
                    ),
                    "AVATAR_FETCH_FAILED" => {
                        ApiError::new("AVATAR_FETCH_FAILED", "the avatar download failed", true)
                    }
                    "AVATAR_TOO_LARGE" => ApiError::new(
                        "AVATAR_TOO_LARGE",
                        "the avatar exceeds the 10 MiB download budget",
                        false,
                    ),
                    "CONVERSATION_NOT_RESOLVED" => ApiError::new(
                        "CONVERSATION_NOT_RESOLVED",
                        "conversation reference does not resolve for this account",
                        false,
                    ),
                    "STORAGE_UNAVAILABLE" => ApiError::new(
                        "STORAGE_UNAVAILABLE",
                        "the engine's storage service is unavailable",
                        true,
                    ),
                    "STORAGE_WRITE_FAILED" => ApiError::new(
                        "STORAGE_WRITE_FAILED",
                        "the engine's storage service write failed",
                        true,
                    ),
                    "STORAGE_READ_FAILED" => ApiError::new(
                        "STORAGE_READ_FAILED",
                        "the engine's storage service read failed",
                        true,
                    ),
                    // §4.46 Android local-backup export (engine backup-export
                    // contract §10.7): the engine's task faces pass through under
                    // their own literals. ALREADY_RUNNING/ARTIFACT_* are
                    // deterministic states (poll, rebuild, fix the builder);
                    // SNAPSHOT_EXISTS/IO_FAILED are transient disk conditions.
                    "BACKUP_EXPORT_ALREADY_RUNNING" => ApiError::new(
                        "BACKUP_EXPORT_ALREADY_RUNNING",
                        "a backup export task is already running for this account",
                        false,
                    ),
                    "BACKUP_ARTIFACT_MISSING" => ApiError::new(
                        "BACKUP_ARTIFACT_MISSING",
                        "the backup export artifact is missing",
                        false,
                    ),
                    "BACKUP_ARTIFACT_INVALID" => ApiError::new(
                        "BACKUP_ARTIFACT_INVALID",
                        "the backup export artifact is invalid",
                        false,
                    ),
                    "BACKUP_SNAPSHOT_EXISTS" => ApiError::new(
                        "BACKUP_SNAPSHOT_EXISTS",
                        "a backup snapshot for this second already exists",
                        true,
                    ),
                    "BACKUP_IO_FAILED" => ApiError::new(
                        "BACKUP_IO_FAILED",
                        "the backup export hit a filesystem failure",
                        true,
                    ),
                    "REPORT_SPAM_FAILED" => {
                        ApiError::new("REPORT_SPAM_FAILED", "the report was rejected", false)
                    }
                    // No data object (or an older engine face): the code still
                    // maps — retryable=true per the engine's contract note (the
                    // desktop may retry the avatar face), textSaved unknowable.
                    "PROFILE_UPLOAD_FAILED" => ApiError::new(
                        "PROFILE_UPLOAD_FAILED",
                        "the profile avatar upload failed after the text face was saved",
                        true,
                    ),
                    _ => ApiError::new("UPSTREAM_ERROR", "signal-cli returned an error", true),
                }
            }
            ServiceError::Engine(EngineError::Unauthorized) => ApiError::new(
                "ACCOUNT_UNLINKED",
                "device was unlinked by the account holder",
                false,
            ),
            ServiceError::Engine(EngineError::Backpressure) => {
                ApiError::new("INTERNAL_ERROR", "signal-cli request queue is full", true)
            }
            ServiceError::Engine(EngineError::StartFailed) => ApiError::new(
                "RUNTIME_START_FAILED",
                "signal-cli runtime could not be started",
                true,
            ),
            // Local filesystem work outside the store (§4.38 step-3 download
            // media-directory handling): the established local-world answer.
            ServiceError::Io => {
                ApiError::new("UPSTREAM_ERROR", "local attachment storage failed", true)
            }
        }
    }
}

#[derive(Clone, Debug)]
// The reactions projection (contract 1.16) makes `MessageUpserted` the large
// variant. Events are transient and single-consumer on an in-process lane, so
// boxing every payload to appease the size lint would trade a real allocation
// per message event for nothing.
#[allow(clippy::large_enum_variant)]
pub enum HostSideEvent {
    StorageChanged {
        state: &'static str,
    },
    AccountChanged(AccountSummary),
    ConversationChanged(ConversationSummary),
    MessageUpserted(MessageRecord),
    MessageStatusChanged {
        account_id: String,
        message_id: String,
        status: &'static str,
        /// Contract 1.32: receipt stamps carried on the transition that wrote
        /// them (absent on transitions that did not stamp, so the host merge
        /// never clears an existing stamp with a no-op).
        delivered_at: Option<u64>,
        read_at: Option<u64>,
    },
    /// Ephemeral typing indicator (contract 1.15): never persisted, rate
    /// limited per account+peer before it reaches the host lane.
    ConversationTyping {
        account_id: String,
        conversation_id: String,
        action: String,
    },
    /// A view-once message was viewed and its bytes erased (contract 1.38):
    /// emitted exactly once per message on the first burn transition — sync
    /// duplicates and replays emit nothing. Deliberately not folded into
    /// `MessageStatusChanged`: this is the local viewed state, not a peer
    /// receipt.
    MessageViewOnceOpened {
        account_id: String,
        conversation_id: String,
        message_id: String,
        opened_at: u64,
    },
    /// One conversation's rows whose disappearing timer ran out were deleted
    /// by the bounded sweep (contract 1.42, §4.41): batched per conversation
    /// per pass, oldest deadline first. The host drops the rows it still
    /// holds; a missed event self-heals on the next conversation read.
    MessageExpired {
        account_id: String,
        conversation_id: String,
        message_ids: Vec<String>,
    },
    /// Inbound call signaling (contract 1.50, §4.48): the engine's
    /// `callMessage` envelope face forwarded as a transient event — the
    /// connector keeps no call state, so there is nothing to self-heal from
    /// a missed delivery and the event never persists. The payload is the
    /// bounded signaling material verbatim; the desktop + calling sidecar
    /// run the RingRTC state machine that consumes it.
    CallIncoming {
        account_id: String,
        conversation_id: String,
        call_id: u64,
        signal_type: &'static str,
        payload: Value,
    },
}

/// Ephemeral typing notifications keep the event lane quiet: at most one
/// notification per account+conversation pair per second, bounded map, oldest
/// entry evicted (§4.13).
const TYPING_RATE_LIMIT_MS: u64 = 1_000;
const TYPING_RATE_MAP_CAPACITY: usize = 256;

/// The filesystem-and-handles half of media ingest (ADR 0002), injected into
/// one service per proxy group when the launcher passes `--media-ingest`.
/// `None` is the pre-1.17 shape: every media method answers
/// `CAPABILITY_UNAVAILABLE` before anything else happens. The handle table is
/// process-wide (a `readChunk` carries no accountId, so the table — not the
/// routing — is the single-origin authority) and is therefore handed in as a
/// shared `Arc` across all groups.
#[derive(Clone)]
pub struct MediaIngest {
    governor: MediaGovernor,
    handles: Arc<MediaHandleTable>,
}

impl MediaIngest {
    pub fn new(data_dir: PathBuf, handles: Arc<MediaHandleTable>) -> Self {
        Self {
            governor: MediaGovernor::new(data_dir),
            handles,
        }
    }

    /// The retention machine the supervisor drives (process start + every
    /// [`crate::media::GOVERNOR_INBOUND_MESSAGE_INTERVAL`] inbound messages).
    pub fn governor(&self) -> &MediaGovernor {
        &self.governor
    }

    /// The process-wide chunk-stream table.
    pub fn handles(&self) -> &MediaHandleTable {
        &self.handles
    }

    /// Resolve a sanitized attachment id to its on-disk file and size by
    /// probing the engine's candidate `attachments/` directories
    /// (signal-cli `PathConfig` layout — see
    /// [`MediaGovernor::attachments_dirs`]). `None` means the id names no
    /// downloaded file: not-downloaded and governor-deleted are
    /// deliberately indistinguishable (UPSTREAM_ERROR, as before the PoC).
    fn locate_attachment(&self, sanitized_id: &str) -> Option<(PathBuf, u64)> {
        for dir in self.governor.attachments_dirs() {
            let candidate = dir.join(sanitized_id);
            if let Ok(metadata) = std::fs::metadata(&candidate)
                && metadata.is_file()
            {
                return Some((candidate, metadata.len()));
            }
        }
        None
    }

    /// The directory a history download writes into: the first existing
    /// probe candidate — the same directory `locate_attachment` reads — or
    /// the plain `<data-dir>/attachments` the eager probe would name, so a
    /// fresh install creates exactly the layout signal-cli itself uses.
    fn write_dir(&self) -> Result<PathBuf, ServiceError> {
        if let Some(dir) = self.governor.attachments_dirs().into_iter().next() {
            return Ok(dir);
        }
        let dir = self.governor.data_dir().join("attachments");
        std::fs::create_dir_all(&dir).map_err(|_| ServiceError::Io)?;
        Ok(dir)
    }
}

/// The capability gate every media method shares (same shape as the
/// `messages.getText` gate): without the launcher opt-in the answer is
/// deterministic and precedes every other check.
fn media_capability_error() -> ServiceError {
    ServiceError::Api(ApiError::new(
        "CAPABILITY_UNAVAILABLE",
        "media ingest is not enabled for this connector",
        false,
    ))
}

/// Map the handle table's content-free failures onto the wire codes. Unknown,
/// expired, non-sequential, and over-ceiling are all caller mistakes
/// (INVALID_REQUEST); an I/O failure mid-stream is the upstream file
/// world (UPSTREAM_ERROR, same answer a missing attachment gives).
fn map_media_handle_error(error: MediaHandleError) -> ServiceError {
    let (code, message, retryable) = match error {
        MediaHandleError::NotFound => (
            "INVALID_REQUEST",
            "mediaHandle is unknown, expired, or already closed",
            false,
        ),
        // The wire answer states the rule, never the handle's internal read
        // position: state helps a probing caller, the rule helps a buggy one.
        MediaHandleError::NotSequential { .. } => (
            "INVALID_REQUEST",
            "offset must match the bytes already read on this handle",
            false,
        ),
        MediaHandleError::Exhausted => (
            "INVALID_REQUEST",
            "the account already holds the maximum number of open media handles",
            false,
        ),
        MediaHandleError::Io => ("UPSTREAM_ERROR", "attachment read failed", true),
    };
    ServiceError::Api(ApiError::new(code, message, retryable))
}

/// Whether an outgoing row's `sent_at` is the upstream Signal protocol
/// timestamp (real protocol identity) rather than a local clock value.
/// `complete_outgoing_send` overwrites `sent_at` with the send response's
/// upstream timestamp when the row settles at `sent`; later delivery/read
/// receipts only advance `status` — `update_message_status` preserves
/// `sent_at` (COALESCE) — so `sent`/`delivered`/`read` rows are all
/// addressable upstream. `pending`/`failed`/`unknown` rows hold a local
/// `now_ms()` value and are indistinguishable from a missing row
/// (remote-delete plan §3.2 precedent).
fn outgoing_status_is_addressable(status: &str) -> bool {
    matches!(status, "sent" | "delivered" | "read")
}

pub struct ConnectorService {
    store: Arc<Store>,
    link: Option<ActiveLinkSession>,
    /// Typing rate limiter: (account_id, conversation_id) → last emission.
    typing_last_emission: std::collections::HashMap<(String, String), u64>,
    /// Media ingest backing (ADR 0002); `None` keeps every media method on
    /// the pre-PoC `CAPABILITY_UNAVAILABLE` answer.
    media: Option<MediaIngest>,
}

impl ConnectorService {
    pub fn new(store: Arc<Store>) -> Self {
        Self::with_media(store, None)
    }

    /// Service with the media ingest backing enabled (`--media-ingest`).
    /// The handle table comes in as a shared `Arc` because it is
    /// process-wide: `readChunk`/`closeHandle` carry no accountId on the
    /// wire, so all groups must resolve handles from one table.
    pub fn with_media(store: Arc<Store>, media: Option<MediaIngest>) -> Self {
        Self {
            store,
            link: None,
            typing_last_emission: std::collections::HashMap::new(),
            media,
        }
    }

    pub fn store_ref(&self) -> &Store {
        &self.store
    }

    pub fn clear_link(&mut self) {
        self.link = None;
    }

    /// Sync live signal-cli numbers of ONE group's engine into the store. New
    /// numbers bind to that group (R3); known numbers keep their immutable
    /// binding. Returns the accounts visible in this group.
    pub fn sync_accounts_from_numbers(
        &self,
        numbers: &[String],
        proxy_group: &str,
    ) -> Result<Vec<AccountSummary>, ServiceError> {
        for number in numbers {
            let _ = self
                .store
                .upsert_account_from_signal(number, None, proxy_group)?;
        }
        Ok(self.store.list_accounts_in_group(proxy_group)?)
    }

    pub fn list_accounts_in_group(
        &self,
        proxy_group: &str,
    ) -> Result<Vec<AccountSummary>, ServiceError> {
        Ok(self.store.list_accounts_in_group(proxy_group)?)
    }

    /// Proxy group an account is bound to, or ACCOUNT_NOT_FOUND. The routing
    /// key for every account-addressed host method (implementation-plan §4.4).
    pub fn account_proxy_group(&self, account_id: &str) -> Result<String, ServiceError> {
        self.store
            .account_by_id(account_id)?
            .map(|row| row.proxy_group)
            .ok_or(ServiceError::Store(StoreError::AccountNotFound))
    }

    /// Best-effort `accountCount` for one runtime group entry: a store hiccup
    /// degrades the status field to 0 rather than failing the status report.
    pub fn count_accounts_in_group_lossy(&self, proxy_group: &str) -> u64 {
        self.store.count_accounts_in_group(proxy_group).unwrap_or(0)
    }

    pub fn account_signal_number(&self, account_id: &str) -> Result<String, ServiceError> {
        self.store
            .account_by_id(account_id)?
            .map(|row| row.signal_account)
            .ok_or(ServiceError::Store(StoreError::AccountNotFound))
    }

    pub fn account_signal_number_optional(
        &self,
        account_id: &str,
    ) -> Result<Option<String>, ServiceError> {
        Ok(self
            .store
            .account_by_id(account_id)?
            .map(|row| row.signal_account))
    }

    pub fn set_account_display_name(
        &self,
        account_id: &str,
        display_name: Option<&str>,
    ) -> Result<AccountSummary, ServiceError> {
        Ok(self
            .store
            .set_account_display_name(account_id, display_name)?)
    }

    pub fn prepare_account_delete(
        &mut self,
        account_id: &str,
        operation_id: &str,
    ) -> Result<AccountDeletePlan, ServiceError> {
        Ok(self
            .store
            .prepare_account_delete(account_id, operation_id, now_ms())?)
    }

    pub fn mark_account_delete_unknown(&self, operation_id: &str) -> Result<(), ServiceError> {
        Ok(self
            .store
            .mark_account_delete_unknown(operation_id, now_ms())?)
    }

    pub fn complete_account_delete(
        &mut self,
        account_id: &str,
        operation_id: Option<&str>,
    ) -> Result<bool, ServiceError> {
        Ok(self
            .store
            .complete_account_delete(account_id, operation_id, now_ms())?)
    }

    pub fn begin_link(
        &mut self,
        device_name: String,
        device_link_uri: String,
    ) -> Result<Value, ServiceError> {
        validate_device_name(&device_name)?;
        if let Some(existing) = self.link.as_ref() {
            if !existing.is_expired(now_ms()) {
                return Err(ServiceError::Api(ApiError::new(
                    "LINK_IN_PROGRESS",
                    "a link session is already active",
                    false,
                )));
            }
            self.link = None;
        }
        let session = ActiveLinkSession::new(device_name, device_link_uri, LINK_SESSION_TTL);
        let response = json!({
            "linkSessionId": session.session_id,
            "qrPayload": session.qr_payload(),
            "expiresAt": session.expires_at_ms,
        });
        self.link = Some(session);
        Ok(response)
    }

    pub fn ensure_link_available(&mut self) -> Result<(), ServiceError> {
        if let Some(existing) = self.link.as_ref() {
            if !existing.is_expired(now_ms()) {
                return Err(ServiceError::Api(ApiError::new(
                    "LINK_IN_PROGRESS",
                    "a link session is already active",
                    false,
                )));
            }
            self.link = None;
        }
        Ok(())
    }

    /// Clone link credentials without clearing the session, so a timed-out finishLink can be retried.
    pub fn peek_link_for_finish(
        &mut self,
        link_session_id: &str,
    ) -> Result<(String, String), ServiceError> {
        let session = self.require_active_link(link_session_id)?;
        Ok((
            session.device_name.clone(),
            session.qr_payload().to_string(),
        ))
    }

    pub fn complete_link_session(
        &mut self,
        link_session_id: &str,
        number: &str,
        proxy_group: &str,
    ) -> Result<AccountSummary, ServiceError> {
        let _ = self.require_active_link(link_session_id)?;
        // Per-engine account ceiling (optimization-plan §6.4 M3.3): refuse a
        // finish that would add a ninth account before the session is
        // consumed or any row is written. Re-linking a number already bound
        // to this group is an update, not an addition, and stays allowed.
        if self.link_would_exceed_ceiling(number, proxy_group)? {
            return Err(account_limit_error(proxy_group));
        }
        let _ = self.take_link_session(link_session_id)?;
        // The binding is fixed here, at link.finish success, and immutable for
        // the life of the account (ADR 0001 R3).
        Ok(self
            .store
            .upsert_account_from_signal(number, Some(now_ms()), proxy_group)?)
    }

    /// Whether this group already holds the per-engine account ceiling, for
    /// the link entries' early rejection (M3.3). Store failures propagate.
    pub fn group_at_account_ceiling(&self, proxy_group: &str) -> Result<bool, ServiceError> {
        Ok(self.store.count_accounts_in_group(proxy_group)? >= MAX_ACCOUNTS_PER_ENGINE as u64)
    }

    /// Store-level ceiling check for one finishing link (M3.3): true only
    /// when the group is at the ceiling AND this number would be a new
    /// account in it (a re-link of a number already bound here is an
    /// upsert that does not grow the count).
    fn link_would_exceed_ceiling(
        &self,
        number: &str,
        proxy_group: &str,
    ) -> Result<bool, ServiceError> {
        if self
            .store
            .account_by_signal(number)?
            .is_some_and(|row| row.proxy_group == proxy_group)
        {
            return Ok(false);
        }
        self.group_at_account_ceiling(proxy_group)
    }

    pub fn cancel_link(&mut self, link_session_id: &str) -> Result<Value, ServiceError> {
        let _ = self.take_link_session(link_session_id)?;
        Ok(json!({}))
    }

    /// True while a non-expired link session exists. Restarting signal-cli would
    /// invalidate its deviceLinkUri, so the watchdog must not restart meanwhile.
    pub fn has_pending_link(&self) -> bool {
        self.link
            .as_ref()
            .is_some_and(|session| !session.is_expired(now_ms()))
    }

    /// Whether this group's service owns the link session, expired or not:
    /// routing a finish/cancel to its owning group must preserve the distinct
    /// LINK_EXPIRED answer an expired session produces at dispatch time.
    pub fn has_link_session(&self, link_session_id: &str) -> bool {
        self.link
            .as_ref()
            .is_some_and(|session| session.session_id == link_session_id)
    }

    /// Test-only: plant a link session with an explicit TTL so registry and
    /// service tests can pin expired-session routing without waiting out the
    /// real five-minute [`LINK_SESSION_TTL`].
    #[cfg(test)]
    pub(crate) fn plant_link_session_for_test(
        &mut self,
        device_name: &str,
        ttl: Duration,
    ) -> String {
        let session =
            ActiveLinkSession::new(device_name.to_string(), "sgnl://link?test".into(), ttl);
        self.link = Some(session);
        self.link
            .as_ref()
            .expect("session planted")
            .session_id
            .clone()
    }

    /// Signal number of any account bound to this group; the watchdog ping target.
    pub fn any_signal_account_number_in_group(
        &self,
        proxy_group: &str,
    ) -> Result<Option<String>, ServiceError> {
        Ok(self.store.any_signal_account_number_in_group(proxy_group)?)
    }

    /// Signal number of any linked account, used as the watchdog ping target.
    pub fn any_signal_account_number(&self) -> Result<Option<String>, ServiceError> {
        Ok(self.store.any_signal_account_number()?)
    }

    pub fn list_conversations(
        &self,
        account_id: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<ConversationSummary>, ServiceError> {
        store_list_conversations(&self.store, account_id, limit, cursor)
    }

    /// Read-only view of the contacts cache; never touches the upstream engine.
    pub fn list_contacts(
        &self,
        account_id: &str,
        query: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<ContactSummary>, ServiceError> {
        store_list_contacts(&self.store, account_id, query, limit, cursor)
    }

    /// Read-only projection of one cached group row (contract revision 1.9,
    /// implementation-plan §4.8; the terminated flag added in 1.53, §4.50
    /// appendix): served entirely from the contacts cache
    /// written by contacts.sync — the upstream is never called, so an unsynced
    /// or departed group answers a deterministic GROUP_NOT_FOUND instead of a
    /// listGroups round trip. `syncedAt` lets the host judge staleness itself;
    /// the connector adds no second cache layer.
    pub fn get_group(
        &self,
        account_id: &str,
        group_key: &str,
    ) -> Result<GroupDetails, ServiceError> {
        store_get_group(&self.store, account_id, group_key)
    }

    // --- Shared target resolution for the prepare_* family -----------------
    // (optimization-plan §5.2 B3): every prepare_* resolves the addressed rows
    // through this one ladder, in wire order — account, then conversation,
    // then message — so a bogus id answers its NOT_FOUND code before any
    // upstream call (implementation-plan §4.4). Per-function validation runs
    // first and stays in the caller.

    /// The addressed account row, or ACCOUNT_NOT_FOUND.
    fn resolve_account(&self, account_id: &str) -> Result<AccountRow, ServiceError> {
        Ok(self
            .store
            .account_by_id(account_id)?
            .ok_or(StoreError::AccountNotFound)?)
    }

    /// The addressed conversation row, or CONVERSATION_NOT_FOUND.
    fn resolve_conversation(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<ConversationRow, ServiceError> {
        Ok(self
            .store
            .conversation_by_id(account_id, conversation_id)?
            .ok_or(StoreError::ConversationNotFound)?)
    }

    /// The addressed message row, or MESSAGE_NOT_FOUND.
    fn resolve_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<MessageRecord, ServiceError> {
        Ok(self
            .store
            .message_by_id(account_id, conversation_id, message_id)?
            .ok_or(StoreError::MessageNotFound)?)
    }

    /// Account → conversation → message in wire order, for the
    /// message-addressed trio (`messages.remoteDelete`, `messages.sendReaction`,
    /// `messages.attachments.open`).
    fn resolve_target(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<(AccountRow, ConversationRow, MessageRecord), ServiceError> {
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        let message = self.resolve_message(account_id, conversation_id, message_id)?;
        Ok((account, conversation, message))
    }

    /// contacts.setLocalAlias (contract revision 1.10, implementation-plan
    /// §4.9): resolve the rename locally, then build the upstream
    /// `updateContact` dispatch. The peer must already be known to the
    /// account — a cached `kind='contact'` contacts row or the peer of an
    /// existing direct conversation — so a bogus peerKey answers a
    /// deterministic INVALID_REQUEST before any upstream call.
    pub fn prepare_set_local_alias(
        &self,
        account_id: &str,
        peer_key: &str,
        alias: &str,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(peer_key, "peerKey")?;
        validate_alias(alias)?;
        let account = self.resolve_account(account_id)?;
        let known = self
            .store
            .contact_by_peer(account_id, "contact", peer_key)?
            .is_some()
            || self
                .store
                .conversation_by_peer(account_id, "direct", peer_key)?
                .is_some();
        if !known {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "peerKey must name a known contact or direct conversation peer",
                false,
            )));
        }
        // signal-cli JSON-RPC updateContact parameters (verified against the
        // pinned 0.14.7 distribution: UpdateContactCommand reads `recipient`
        // with ns.getString — a single string, not an array; the earlier
        // feasibility note's `recipient: [peerKey]` would ClassCastException
        // into an upstream INTERNAL_ERROR): `account` is consumed by the
        // multi-account dispatcher, `name` is the new alias.
        Ok(json!({
            "account": account.signal_account,
            "recipient": peer_key,
            "name": alias,
        }))
    }

    /// presence.setTypingMessage (contract revision 1.11, implementation-plan
    /// §4.10): resolve the conversation locally, then build the upstream
    /// `sendTyping` dispatch. Targeting is by conversationId only and nothing
    /// is checked against the message history — a typing indicator references
    /// no message. The `stop` boolean is always sent explicitly: with the key
    /// absent the upstream `getBoolean` answers null, and the connector does
    /// not depend on null handling outside the pinned contract.
    pub fn prepare_set_typing_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        stop: bool,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        // signal-cli JSON-RPC sendTyping parameters (verified against the
        // pinned 0.14.7 distribution: SendTypingCommand dests `recipient`
        // (nargs=*, consumed with getList), `group-id` and `stop` (a boolean
        // dest read with getBoolean); JsonRpcNamespace maps dash-separated
        // dests to camelCase JSON keys). The recipient array follows the
        // sendReaction addressing precedent (getList wraps a scalar anyway).
        let mut params = json!({
            "account": account.signal_account,
            "stop": stop,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(params)
    }

    /// Cache one full contacts sync atomically: all rows and the sync marker
    /// commit in a single transaction, so a failed batch leaves nothing behind.
    pub fn upsert_synced_contacts(
        &self,
        account_id: &str,
        entries: &[SyncedContact<'_>],
        synced_at: u64,
    ) -> Result<(), ServiceError> {
        Ok(self
            .store
            .upsert_synced_contacts(account_id, entries, synced_at)?)
    }

    pub fn contacts_synced_at(&self, account_id: &str) -> Result<Option<u64>, ServiceError> {
        Ok(self.store.contacts_synced_at(account_id)?)
    }

    pub fn count_contacts(&self, account_id: &str) -> Result<(u64, u64), ServiceError> {
        Ok(self.store.count_contacts(account_id)?)
    }

    pub fn list_messages(
        &self,
        account_id: &str,
        conversation_id: &str,
        limit: u32,
        before: Option<&str>,
    ) -> Result<Page<MessageRecord>, ServiceError> {
        store_list_messages(&self.store, account_id, conversation_id, limit, before)
    }

    pub fn search_messages(
        &self,
        account_id: &str,
        query: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<Page<MessageRecord>, ServiceError> {
        store_search_messages(&self.store, account_id, query, limit, cursor)
    }

    pub fn get_message_text(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<MessageText, ServiceError> {
        store_get_message_text(&self.store, account_id, conversation_id, message_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_send_text(
        &self,
        account_id: &str,
        conversation_id: &str,
        text: &str,
        client_request_id: &str,
        quote_message_id: Option<&str>,
        previews: Option<Vec<SendTextPreviewParams>>,
        mentions: Option<Vec<SendTextMentionParams>>,
        text_styles: Option<Vec<SendTextStyleParams>>,
    ) -> Result<PreparedSend, ServiceError> {
        let preview = previews_to_upstream(previews, text)?;
        validate_text(text)?;
        validate_opaque_id(client_request_id, "clientRequestId")?;
        if let Some(quote) = quote_message_id {
            validate_opaque_id(quote, "quoteMessageId")?;
        }
        if let Some(existing) = self
            .store
            .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(Box::new(existing)));
        }
        let account = self.resolve_account(account_id)?;
        let mentions = self.mentions_to_upstream(&account, mentions)?;
        let text_styles = self.text_styles_to_upstream(text_styles)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        self.dispatch_send(
            &account,
            &conversation,
            text,
            client_request_id,
            quote_message_id,
            preview,
            mentions,
            text_styles,
            None,
            Vec::new(),
            false,
        )
    }

    /// Prepare a send addressed by peer (kind + peer_key) instead of an
    /// existing conversation id. When no conversation for the peer exists yet,
    /// it is created here and carries the first outgoing message. Since
    /// contract revision 1.14 a contacts.sync already materializes skeleton
    /// conversations for synced peers (§6.5), so this path mostly attaches the
    /// first message to an existing skeleton; unknown peers still get their
    /// conversation from this send.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_send_text_to_peer(
        &self,
        account_id: &str,
        peer: &PeerTarget<'_>,
        text: &str,
        client_request_id: &str,
        quote_message_id: Option<&str>,
        previews: Option<Vec<SendTextPreviewParams>>,
        mentions: Option<Vec<SendTextMentionParams>>,
        text_styles: Option<Vec<SendTextStyleParams>>,
    ) -> Result<PreparedSend, ServiceError> {
        let preview = previews_to_upstream(previews, text)?;
        validate_text(text)?;
        validate_opaque_id(client_request_id, "clientRequestId")?;
        if let Some(quote) = quote_message_id {
            validate_opaque_id(quote, "quoteMessageId")?;
        }
        // contacts.list reports 'contact'; conversations use 'direct'. Both are
        // accepted for the same direct-chat target.
        let kind = match peer.kind {
            "direct" | "contact" => "direct",
            "group" => "group",
            _ => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "kind must be 'contact', 'direct', or 'group'",
                    false,
                )));
            }
        };
        validate_opaque_id(peer.peer_key, "peerKey")?;
        let peer_title = peer
            .peer_title
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .map(|title| title.chars().take(64).collect::<String>());
        if let Some(existing) = self
            .store
            .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(Box::new(existing)));
        }
        let account = self.resolve_account(account_id)?;
        let mentions = self.mentions_to_upstream(&account, mentions)?;
        let text_styles = self.text_styles_to_upstream(text_styles)?;
        let title = peer_title.unwrap_or_else(|| {
            if kind == "group" {
                "group".to_string()
            } else {
                mask_address(peer.peer_key)
            }
        });
        let conversation =
            self.store
                .ensure_conversation(account_id, kind, peer.peer_key, &title)?;
        self.dispatch_send(
            &account,
            &conversation,
            text,
            client_request_id,
            quote_message_id,
            preview,
            mentions,
            text_styles,
            None,
            Vec::new(),
            false,
        )
    }

    /// Validate and normalize the caller-supplied mentions (contract revision
    /// 1.34) into the exact shape the upstream `send` expects. Enforced here —
    /// not at the engine — because the engine answers a malformed or
    /// unresolvable mention entry with a late upstream internal error, which
    /// would surface as an unknown-outcome-shaped send failure instead of
    /// deterministic local behavior.
    ///
    /// Rules (spec §4.29): more than 64 entries are dropped (the §4.20
    /// receive cap mirrored); an empty/whitespace number fails the whole
    /// request closed INVALID_REQUEST; a UUID-shaped number passes through
    /// (the engine resolves ACIs directly); anything else must digit-suffix
    /// match the linked account's own number or the cached contacts — the
    /// engine `resolve_author_aci` rule — and is dropped when nothing matches
    /// (the official behavior for unresolvable mention addresses). Entries
    /// that survive keep their `number` verbatim: the engine re-resolves
    /// against its own store, which stays the single resolution authority.
    fn mentions_to_upstream(
        &self,
        account: &AccountRow,
        mentions: Option<Vec<SendTextMentionParams>>,
    ) -> Result<Option<Vec<UpstreamSendMention>>, ServiceError> {
        let Some(mentions) = mentions else {
            return Ok(None);
        };
        let mut upstream = Vec::with_capacity(mentions.len().min(MAX_SEND_MENTIONS));
        let mut contacts: Option<Vec<String>> = None;
        for entry in mentions.into_iter().take(MAX_SEND_MENTIONS) {
            if entry.number.trim().is_empty() {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "mention number must not be empty",
                    false,
                )));
            }
            let keep = is_uuid_shaped(&entry.number) || {
                let digits = address_digits(&entry.number);
                !digits.is_empty()
                    && (address_digits(&account.signal_account) == digits
                        || contacts
                            .get_or_insert_with(|| {
                                self.store
                                    .contact_peer_keys(&account.id)
                                    .unwrap_or_default()
                            })
                            .iter()
                            .any(|peer_key| {
                                let candidate = address_digits(peer_key);
                                !candidate.is_empty() && candidate.ends_with(&digits)
                            }))
            };
            if keep {
                upstream.push(UpstreamSendMention {
                    number: entry.number,
                    start: entry.start,
                    length: entry.length,
                });
            }
        }
        Ok((!upstream.is_empty()).then_some(upstream))
    }

    /// Validate and normalize the caller-supplied text-style ranges (contract
    /// revision 1.69, §4.62) into the exact shape the upstream `send`
    /// expects. Enforced here — not at the engine — for the same reason the
    /// §4.29 mentions are: the engine answers a malformed entry with a late
    /// upstream internal error, which would surface as an
    /// unknown-outcome-shaped send failure instead of deterministic local
    /// behavior.
    ///
    /// Rules: more than 64 entries are rejected INVALID_REQUEST (the §4.20
    /// receive cap mirrored; the mentions family drops extras, but a style
    /// list is caller-authored formatting, not protocol-received data, so the
    /// deterministic rejection is the honest shape); the style value is one
    /// of the five protocol-renderable upstream enum values (BOLD / ITALIC /
    /// SPOILER / STRIKETHROUGH / MONOSPACE — the pinned `TextStyle.java`
    /// enum minus the no-op `NONE`, which the official composer never emits
    /// and which would silently strip the range); start/length are validated
    /// non-negative by the u32 param types (UTF-16 code-unit offsets, the
    /// official BodyRange semantics). There is deliberately **no
    /// body-length bounds gate**: the pinned upstream carries no such check
    /// (`SendCommand.parseTextStyles` accepts any `\d+:\d+:STYLE` triple) and
    /// the official desktop sender clips ranges to the trimmed body at
    /// composition time (`quill/util.dom.ts getTextAndRangesFromOps`) — a
    /// connector-side rejection would fail sends the pinned face delivers,
    /// and connector-side clipping would hide a caller bug the official face
    /// leaves visible upstream.
    fn text_styles_to_upstream(
        &self,
        text_styles: Option<Vec<SendTextStyleParams>>,
    ) -> Result<Option<Vec<UpstreamSendTextStyle>>, ServiceError> {
        let Some(text_styles) = text_styles else {
            return Ok(None);
        };
        if text_styles.len() > MAX_SEND_TEXT_STYLES {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "textStyle must contain at most 64 entries",
                false,
            )));
        }
        let mut upstream = Vec::with_capacity(text_styles.len());
        for entry in text_styles {
            let style = match entry.style.as_str() {
                "BOLD" | "ITALIC" | "SPOILER" | "STRIKETHROUGH" | "MONOSPACE" => entry.style,
                _ => {
                    return Err(ServiceError::Api(ApiError::new(
                        "INVALID_REQUEST",
                        "textStyle entry style must be one of BOLD, ITALIC, SPOILER, \
                         STRIKETHROUGH, MONOSPACE",
                        false,
                    )));
                }
            };
            upstream.push(UpstreamSendTextStyle {
                style,
                start: entry.start,
                length: entry.length,
            });
        }
        Ok((!upstream.is_empty()).then_some(upstream))
    }

    /// Send one attachment (optionally with a caption) — implementation-plan
    /// §4.12. Validation runs entirely before the pending row exists, so a
    /// rejected request leaves nothing behind and the same clientRequestId
    /// stays a fresh request. The base64 payload is size-verified against the
    /// declared `sizeBytes` and re-encoded as an RFC 2397 data URI: upstream
    /// decodes it itself (AttachmentHelper, pinned 0.14.7), uploads via CDN,
    /// and owns any temp file lifetime — bytes never touch connector disk and
    /// no caller-controlled path reaches upstream. The pending row carries the
    /// metadata-only descriptor (same wire shape as inbound, contract 1.15) so
    /// the host renders the outgoing attachment from its first tick.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_send_attachment(
        &self,
        account_id: &str,
        target: &AttachmentSendTarget<'_>,
        client_request_id: &str,
        data_base64: &str,
        size_bytes: u64,
        filename: Option<&str>,
        content_type: Option<&str>,
        text: Option<&str>,
        quote_message_id: Option<&str>,
        voice_note: bool,
        view_once: bool,
    ) -> Result<PreparedSend, ServiceError> {
        validate_attachment_send_payload(data_base64, size_bytes)?;
        validate_attachment_descriptor(filename, "filename")?;
        validate_attachment_content_type(content_type)?;
        // §4.30: the voice flag requires an audio attachment — the official
        // recorder only produces audio, and a non-audio voice flag would
        // render as an unplayable bubble. Deterministic rejection before the
        // pending row exists.
        if voice_note && !content_type.is_some_and(|value| value.starts_with("audio/")) {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "voiceNote requires an audio/* contentType",
                false,
            )));
        }
        // §4.36: the view-once flag requires a photo or video, no caption, no
        // quote, and no voice flag — the official view-once faces, enforced
        // deterministically before the pending row exists (the single-
        // attachment structure of §4.12 makes the combination unambiguous).
        if view_once {
            if !content_type
                .is_some_and(|value| value.starts_with("image/") || value.starts_with("video/"))
            {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "viewOnce requires an image/* or video/* contentType",
                    false,
                )));
            }
            if voice_note {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "viewOnce and voiceNote are mutually exclusive",
                    false,
                )));
            }
            if quote_message_id.is_some() {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "viewOnce does not support quoteMessageId",
                    false,
                )));
            }
            if text.is_some_and(|caption| !caption.is_empty()) {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "viewOnce does not support a caption",
                    false,
                )));
            }
        }
        let caption = text.unwrap_or_default();
        if !caption.is_empty() {
            validate_text(caption)?;
        }
        validate_opaque_id(client_request_id, "clientRequestId")?;
        if let Some(quote) = quote_message_id {
            validate_opaque_id(quote, "quoteMessageId")?;
        }
        if let Some(existing) = self
            .store
            .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(Box::new(existing)));
        }
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_send_conversation(account_id, target)?;
        let data_uri = build_attachment_data_uri(data_base64, filename, content_type, voice_note)?;
        // The pending row carries the same metadata-only descriptor the
        // renderer projects (contract 1.15 wire shape), so the desktop bubble
        // and conversation preview exist from the first pending tick instead
        // of collapsing into an invisible empty row. The voice flag rides the
        // descriptor (§4.30), so the sender's own bubble renders the voice UI
        // across restarts.
        let descriptor = NormalizedAttachment {
            locator: None,
            id: stable_hash_id(&[account_id, client_request_id, "attachment", "0"]),
            content_type: content_type.map(str::to_string),
            filename: filename.map(str::to_string),
            size: Some(size_bytes),
            width: None,
            height: None,
            is_voice_note: voice_note,
        };
        self.dispatch_send(
            &account,
            &conversation,
            caption,
            client_request_id,
            quote_message_id,
            None,
            None,
            None,
            Some(vec![json!(data_uri)]),
            vec![descriptor],
            view_once,
        )
    }

    /// Send one message carrying several attachments (contract revision
    /// 1.66, §4.59) — the official forward face: a forwarded multi-
    /// attachment message is one upstream message whose array-typed
    /// `attachments` parameter carries every entry's data URI (the engine
    /// has always accepted the array; the singular §4.12 entry point was the
    /// only one-element gate). Validation runs entirely before the pending
    /// row exists. Shape rules, all deterministic INVALID_REQUEST: 1..=32
    /// entries (the engine's own per-message ceiling, `MAX_SEND_ATTACHMENTS`);
    /// the entries' combined raw size stays within the §4.12 100 MiB budget
    /// so the whole request still fits one host frame; voiceNote/viewOnce are
    /// rejected on this form (a voice note is always a single-attachment
    /// message and view-once media is a single unadorned medium — neither
    /// official face produces a multi-attachment message). Per-entry
    /// validation reuses the §4.12 family verbatim (payload, filename,
    /// content type); the caption and quote are message-level and shared.
    /// Settlement is the §4.12 chain unchanged: one pending row carrying one
    /// descriptor per entry, then the shared `dispatch_send` completion, so
    /// the desktop renders every outgoing attachment from the first tick.
    pub fn prepare_send_attachment_multi(
        &self,
        account_id: &str,
        target: &AttachmentSendTarget<'_>,
        client_request_id: &str,
        entries: &[MessagesSendAttachmentEntry],
        text: Option<&str>,
        quote_message_id: Option<&str>,
    ) -> Result<PreparedSend, ServiceError> {
        if entries.is_empty() {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "attachments must contain at least one entry",
                false,
            )));
        }
        if entries.len() > MAX_SEND_ATTACHMENTS {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "attachments must contain at most 32 entries",
                false,
            )));
        }
        // The combined raw budget equals the singular §4.12 ceiling: one
        // multi-attachment request is still one host frame (base64 expansion
        // plus JSON envelope under the 160 MiB frame limit), and no entry can
        // exceed the budget the singular form already proves fits.
        let mut total_bytes = 0usize;
        for entry in entries {
            total_bytes =
                total_bytes.saturating_add(usize::try_from(entry.size_bytes).unwrap_or(usize::MAX));
            if total_bytes > MAX_ATTACHMENT_BYTES {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "combined attachment payload exceeds the 100 MiB message budget",
                    false,
                )));
            }
            validate_attachment_send_payload(&entry.data_base64, entry.size_bytes)?;
            validate_attachment_descriptor(entry.filename.as_deref(), "filename")?;
            validate_attachment_content_type(entry.content_type.as_deref())?;
        }
        if let Some(caption) = text
            && !caption.is_empty()
        {
            validate_text(caption)?;
        }
        validate_opaque_id(client_request_id, "clientRequestId")?;
        if let Some(quote) = quote_message_id {
            validate_opaque_id(quote, "quoteMessageId")?;
        }
        if let Some(existing) = self
            .store
            .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(Box::new(existing)));
        }
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_send_conversation(account_id, target)?;
        let mut data_uris = Vec::with_capacity(entries.len());
        let mut descriptors = Vec::with_capacity(entries.len());
        for (index, entry) in entries.iter().enumerate() {
            let data_uri = build_attachment_data_uri(
                &entry.data_base64,
                entry.filename.as_deref(),
                entry.content_type.as_deref(),
                false,
            )?;
            data_uris.push(json!(data_uri));
            descriptors.push(NormalizedAttachment {
                locator: None,
                id: stable_hash_id(&[
                    account_id,
                    client_request_id,
                    "attachment",
                    &index.to_string(),
                ]),
                content_type: entry.content_type.clone(),
                filename: entry.filename.clone(),
                size: Some(entry.size_bytes),
                width: None,
                height: None,
                is_voice_note: false,
            });
        }
        self.dispatch_send(
            &account,
            &conversation,
            text.unwrap_or_default(),
            client_request_id,
            quote_message_id,
            None,
            None,
            None,
            Some(data_uris),
            descriptors,
            false,
        )
    }

    /// Shared §4.12 addressing resolution for body-carrying and sticker
    /// sends alike: a conversation id resolves the stored row; kind+peerKey
    /// progressive-fills a skeleton the same way (contacts.list reports
    /// 'contact', conversations use 'direct' — both accepted).
    fn resolve_send_conversation(
        &self,
        account_id: &str,
        target: &AttachmentSendTarget<'_>,
    ) -> Result<ConversationRow, ServiceError> {
        match target {
            AttachmentSendTarget::Conversation(conversation_id) => {
                self.resolve_conversation(account_id, conversation_id)
            }
            AttachmentSendTarget::Peer(peer) => {
                let kind = match peer.kind {
                    "direct" | "contact" => "direct",
                    "group" => "group",
                    _ => {
                        return Err(ServiceError::Api(ApiError::new(
                            "INVALID_REQUEST",
                            "kind must be 'contact', 'direct', or 'group'",
                            false,
                        )));
                    }
                };
                validate_opaque_id(peer.peer_key, "peerKey")?;
                let peer_title = peer
                    .peer_title
                    .map(str::trim)
                    .filter(|title| !title.is_empty())
                    .map(|title| title.chars().take(64).collect::<String>());
                let title = peer_title.unwrap_or_else(|| {
                    if kind == "group" {
                        "group".to_string()
                    } else {
                        mask_address(peer.peer_key)
                    }
                });
                Ok(self
                    .store
                    .ensure_conversation(account_id, kind, peer.peer_key, &title)?)
            }
        }
    }

    /// messages.sendSticker (contract revision 1.35, §4.31; quote face 1.37,
    /// §4.35): §4.12 settlement discipline with a body-less upstream `send`
    /// carrying the `sticker` object — pending row (sticker metadata + image
    /// descriptor) before the upstream call, then the shared
    /// `dispatch_prepared` completion. Bounds failures reject before the
    /// pending row exists. The optional quote resolves through the same
    /// ladder as `dispatch_send` — after the idempotency short-circuit and
    /// addressing resolution, before the pending row is inserted, so a
    /// rejected quote leaves nothing behind while a replay keeps returning
    /// the existing row without re-resolving.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_send_sticker(
        &self,
        account_id: &str,
        target: &AttachmentSendTarget<'_>,
        client_request_id: &str,
        pack_id: &str,
        pack_key: &str,
        sticker_id: u32,
        emoji: Option<&str>,
        image: &MessagesSendStickerImage,
        quote_message_id: Option<&str>,
    ) -> Result<PreparedSend, ServiceError> {
        validate_sticker_pack_id(pack_id)?;
        validate_sticker_pack_key(pack_key)?;
        validate_attachment_send_payload(&image.data_base64, image.size_bytes)?;
        validate_attachment_content_type(Some(&image.content_type))?;
        // The official client sniffs image MIME (falling back to image/webp)
        // and refuses video/text; the connector refuses them deterministically
        // so a rejected sticker never leaves a pending row behind.
        if !image.content_type.starts_with("image/") {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "sticker image contentType must be image/*",
                false,
            )));
        }
        if let Some(emoji) = emoji {
            let count = emoji.chars().count();
            if count == 0 || count > MAX_STICKER_EMOJI_CHARS {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "emoji must contain between 1 and 32 characters",
                    false,
                )));
            }
        }
        validate_opaque_id(client_request_id, "clientRequestId")?;
        if let Some(existing) = self
            .store
            .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(Box::new(existing)));
        }
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_send_conversation(account_id, target)?;
        // Quote resolution is part of send validation and runs before the
        // pending row exists (§4.35): a rejected quote leaves nothing behind,
        // so the same clientRequestId stays a fresh (re-validated) request.
        let quote = quote_message_id
            .map(|id| self.resolve_quote(&account, &conversation, id))
            .transpose()?;
        let data_uri =
            build_attachment_data_uri(&image.data_base64, None, Some(&image.content_type), false)?;
        // The sent row's descriptor records the sticker image (metadata only)
        // and the row itself records the pack identity, so the sender's bubble
        // renders as a sticker across restarts (§4.31).
        let descriptor = NormalizedAttachment {
            locator: None,
            id: stable_hash_id(&[account_id, client_request_id, "sticker", "0"]),
            content_type: Some(image.content_type.clone()),
            filename: None,
            size: Some(image.size_bytes),
            width: image.width,
            height: image.height,
            is_voice_note: false,
        };
        let sticker = crate::engine::NormalizedSticker {
            pack_id: pack_id.to_string(),
            pack_key: pack_key.to_string(),
            sticker_id,
            emoji: emoji.map(str::to_string),
        };
        let pending_id = stable_hash_id(&[
            account_id,
            conversation.id.as_str(),
            "outgoing",
            client_request_id,
        ]);
        let pending = MessageRecord {
            id: pending_id.clone(),
            account_id: account_id.to_string(),
            conversation_id: conversation.id.clone(),
            direction: "outgoing",
            sender_id: account_id.to_string(),
            sender_name: None,
            mentions_self: false,
            sent_at: now_ms(),
            received_at: None,
            text: None,
            text_bytes: None,
            text_truncated: false,
            text_retrievable: false,
            status: "pending",
            client_request_id: Some(client_request_id.to_string()),
            quote_message_id: quote_message_id.map(str::to_string),
            quote_snapshot: None,
            attachments: vec![descriptor],
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            admin_deleted: false,
            sticker: Some(sticker),
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
        };
        let inserted = self
            .store
            .insert_message(&pending, Some(client_request_id), None, false)?;
        if !inserted
            && let Some(existing) = self
                .store
                .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(Box::new(existing)));
        }
        // Upstream shape (official `sendStickerMessage`): the sticker object
        // with the image re-encoded as a data URI, plus the shared upstream
        // target — and deliberately no `message` or `attachments` key, the
        // mutual exclusivity the official face enforces.
        let mut sticker_object = json!({
            "packId": pack_id,
            "packKey": pack_key,
            "stickerId": sticker_id,
            "image": data_uri,
        });
        if let Some(emoji) = emoji {
            sticker_object["emoji"] = json!(emoji);
        }
        if let Some(width) = image.width {
            sticker_object["width"] = json!(width);
        }
        if let Some(height) = image.height {
            sticker_object["height"] = json!(height);
        }
        let mut params = json!({
            "account": account.signal_account,
            "sticker": sticker_object,
        });
        set_upstream_target(&mut params, &conversation);
        // The quote keys ride the send top level beside the sticker object —
        // the quote belongs to the send, not to the sticker (§4.35, the same
        // wire keys the text face uses).
        if let Some((quote_timestamp, quote_author)) = quote {
            params["quoteTimestamp"] = json!(quote_timestamp);
            params["quoteAuthor"] = json!(quote_author);
        }
        Ok(PreparedSend::Dispatch {
            pending_id,
            account_id: account_id.to_string(),
            conversation_id: conversation.id.clone(),
            params,
            pending_sent_at: pending.sent_at,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_send(
        &self,
        account: &AccountRow,
        conversation: &ConversationRow,
        text: &str,
        client_request_id: &str,
        quote_message_id: Option<&str>,
        link_preview: Option<UpstreamSendPreview>,
        mentions: Option<Vec<UpstreamSendMention>>,
        text_styles: Option<Vec<UpstreamSendTextStyle>>,
        attachments: Option<Vec<Value>>,
        attachment_descriptors: Vec<NormalizedAttachment>,
        view_once: bool,
    ) -> Result<PreparedSend, ServiceError> {
        let account_id = account.id.as_str();
        let conversation_id = conversation.id.as_str();
        // Quote resolution is part of send validation and runs before the
        // pending row exists: a rejected quote leaves nothing behind, so the
        // same clientRequestId stays a fresh (re-validated) request.
        let quote = quote_message_id
            .map(|id| self.resolve_quote(account, conversation, id))
            .transpose()?;
        let pending_id =
            stable_hash_id(&[account_id, conversation_id, "outgoing", client_request_id]);
        let pending = MessageRecord {
            id: pending_id.clone(),
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            direction: "outgoing",
            sender_id: account_id.to_string(),
            sender_name: None,
            mentions_self: false,
            sent_at: now_ms(),
            received_at: None,
            text: Some(text.to_string()),
            text_bytes: Some(text.len() as u32),
            text_truncated: false,
            text_retrievable: true,
            status: "pending",
            client_request_id: Some(client_request_id.to_string()),
            quote_message_id: quote_message_id.map(str::to_string),
            quote_snapshot: None,
            attachments: attachment_descriptors,
            edited_at: None,
            // §4.36: a view-once send persists the marker on its own pending
            // row, so the sender's bubble renders the view-once UI across
            // restarts; every other send stays a plain row (None).
            // §4.62: a styled text send persists its ranges the same way —
            // the sender's bubble keeps its own formatting across restarts
            // (the official sender echo renders bodyRanges). Mentions stay
            // unpersisted (the 1.34 shape): a styled send carries both, the
            // retry rebuilds neither (documented deviation, §4.29 precedent).
            rich: (view_once || text_styles.is_some()).then(|| crate::engine::NormalizedRich {
                view_once,
                text_styles: text_styles
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|style| crate::engine::NormalizedTextStyle {
                        style: style.style.clone(),
                        start: style.start,
                        length: style.length,
                    })
                    .collect(),
                ..crate::engine::NormalizedRich::default()
            }),
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            admin_deleted: false,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
        };
        // Preview mirrors the visible row: the caption when present, else the
        // attachment filename — never an empty string for an attachment send.
        let preview = (!text.is_empty()).then(|| text.to_string()).or_else(|| {
            pending
                .attachments
                .iter()
                .find_map(|a| a.filename.as_deref().filter(|f| !f.is_empty()))
                .map(str::to_string)
        });
        let inserted = self.store.insert_message(
            &pending,
            Some(client_request_id),
            preview.as_deref(),
            false,
        )?;
        if !inserted
            && let Some(existing) = self
                .store
                .message_by_client_request(account_id, client_request_id)?
        {
            return Ok(PreparedSend::Existing(Box::new(existing)));
        }

        let params = Self::upstream_text_send_params(
            &account.signal_account,
            text,
            link_preview.as_ref(),
            mentions.as_deref(),
            text_styles.as_deref(),
            attachments,
            quote,
            conversation,
            view_once,
        );
        Ok(PreparedSend::Dispatch {
            pending_id,
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            params,
            pending_sent_at: pending.sent_at,
        })
    }

    /// stickerPacks.getManifest (contract revision 1.36, §4.32): bounds
    /// validation, then the engine call is composed by the supervisor — the
    /// service contributes no state and returns nothing persistent. The
    /// packId/packKey rules are the browse-face shapes (32-hex id, base64
    /// key decoding to exactly 32 bytes), stricter than the 1.35 send
    /// projection on purpose (§4.32).
    pub fn prepare_sticker_pack_manifest(
        &self,
        params: &StickerPackManifestParams,
    ) -> Result<(), ServiceError> {
        validate_sticker_browse_pack_id(&params.pack_id)?;
        validate_sticker_browse_pack_key(&params.pack_key)?;
        Ok(())
    }

    /// stickerPacks.getImage (contract revision 1.36, §4.32): same pack
    /// identity bounds plus the u32 stickerId — the u32 type already fails
    /// non-integer/out-of-range values at params deserialization, so the
    /// prepare step adds no second check.
    pub fn prepare_sticker_pack_image(
        &self,
        params: &StickerPackImageParams,
    ) -> Result<(), ServiceError> {
        validate_sticker_browse_pack_id(&params.pack_id)?;
        validate_sticker_browse_pack_key(&params.pack_key)?;
        Ok(())
    }

    /// conversations.getPinned (contract revision 1.36, §4.33): account
    /// bounds and existence — the pinned list itself lives upstream, the
    /// service reads nothing locally (no persistence on this face).
    pub fn prepare_conversations_get_pinned(
        &self,
        account_id: &str,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        let account = self.resolve_account(account_id)?;
        Ok(json!({ "account": account.signal_account }))
    }

    /// conversations.setPinned (contract revision 1.36, §4.33): bounds and
    /// resolution only — the conversation id resolves through the shared
    /// ladder (account, then conversation; CONVERSATION_NOT_FOUND before
    /// any upstream call) and the kind enum is validated here, so the
    /// upstream call carries a resolved reference and a legal kind. No
    /// local row is written: the cloud record is the whole state.
    pub fn prepare_conversations_set_pinned(
        &self,
        account_id: &str,
        conversation_id: &str,
        kind: &str,
        pinned: bool,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        if conversation_id.is_empty()
            || conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        validate_pinned_kind(kind)?;
        let account = self.resolve_account(account_id)?;
        // Resolution uses the plain ladder: only conversations already
        // materialized locally resolve. A pinned reference for a conversation
        // this connector has never seen is the engine's CONVERSATION_
        // NOT_RESOLVED answer, not a local guess (§4.33). The upstream key is
        // `conversationId` carrying the resolved upstream conversation
        // reference (peer key, the identity every other group/direct
        // addressing projects), never the host's opaque local id.
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        Ok(json!({
            "account": account.signal_account,
            "conversationId": conversation.peer_key,
            "kind": kind,
            "pinned": pinned,
        }))
    }

    /// conversations.setExpireTimer (contract revision 1.42, §4.41; group
    /// route re-cut by contract revision 1.45, §4.44; kt-engine gate lifted
    /// by the 2026-10-06 dated note): bounds, resolution, and the mode-shaped
    /// upstream payload under the service lock. A direct conversation routes
    /// per mode — the engine face's `setExpirationTimer` takes `recipient`
    /// (a single string) + `expirationInSeconds`; the signal-cli face's
    /// `updateContact` takes `recipient` + `expiration` (int seconds) —
    /// same-value no-ops and broadcast semantics stay upstream. A group
    /// conversation routes the timer change through the group-management
    /// write it always traveled by upstream: `updateGroup {account, groupId,
    /// expiration}` (the §4.41 group refusal is overturned) — in every mode
    /// since the engine batch landed `updateGroup {expiration}` (engine main
    /// @ 216a4c7, compat §3/§5 registration block), so the former kt-engine
    /// INVALID_REQUEST debt is retired and all three faces share one group
    /// payload. Returns the upstream method name, the payload, and the
    /// resolved timer value the success path mirrors into the conversation
    /// row.
    pub fn prepare_conversations_set_expire_timer(
        &self,
        account_id: &str,
        conversation_id: &str,
        expire_seconds: u32,
        mode: SignalCliMode,
    ) -> Result<(&'static str, Value, u64), ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        if conversation_id.is_empty()
            || conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        if expire_seconds > MAX_EXPIRE_TIMER_SECONDS {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "expireSeconds exceeds the upstream int face",
                false,
            )));
        }
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        if conversation.kind != "direct" {
            return Ok((
                "updateGroup",
                json!({
                    "account": account.signal_account,
                    "groupId": conversation.peer_key,
                    "expiration": expire_seconds,
                }),
                u64::from(expire_seconds),
            ));
        }
        let (method, upstream) = match mode {
            SignalCliMode::KtEngine => (
                "setExpirationTimer",
                json!({
                    "account": account.signal_account,
                    "recipient": conversation.peer_key,
                    "expirationInSeconds": expire_seconds,
                }),
            ),
            _ => (
                "updateContact",
                json!({
                    "account": account.signal_account,
                    "recipient": conversation.peer_key,
                    "expiration": expire_seconds,
                }),
            ),
        };
        Ok((method, upstream, u64::from(expire_seconds)))
    }

    /// groups.update (contract revision 1.45, §4.44, capability
    /// group-management): the full local validation + resolution ladder, then
    /// the mode-shaped upstream `updateGroup` payload under the service lock.
    /// Field bounds mirror the schema; the change keys are individually
    /// optional, at least one must be present (`resetLink: false` counts as
    /// absent), and explicit empty member arrays are refused — upstream reads
    /// absence as "no change" and an empty set would commit a no-delta group
    /// change broadcast. The conversation id must resolve to a kind='group'
    /// row (direct answers INVALID_REQUEST before any upstream call); the
    /// member addresses forward verbatim — resolving and rejecting unknown
    /// addresses is upstream work. The kt-engine face has no updateGroup
    /// method and answers CAPABILITY_UNAVAILABLE (retryable=false) after the
    /// local validation completed — engine-batch debt, honest by design.
    /// Zero local writes: group state converges through the receive
    /// projection.
    pub fn prepare_groups_update(
        &self,
        params: &GroupsUpdateParams,
        mode: SignalCliMode,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        validate_opaque_id(&params.conversation_id, "conversationId")?;
        if params.conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        if let Some(operation_id) = params.operation_id.as_deref() {
            validate_opaque_id(operation_id, "operationId")?;
        }
        let bounded = |value: &str, max: usize| !value.is_empty() && value.len() <= max;
        let mut changes: Vec<(&'static str, Value)> = Vec::new();
        if let Some(name) = params.group_name.as_deref() {
            if !bounded(name, GROUP_NAME_MAX_BYTES) {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "groupName must contain between 1 and 256 bytes",
                    false,
                )));
            }
            changes.push(("name", json!(name)));
        }
        if let Some(description) = params.group_description.as_deref() {
            if !bounded(description, GROUP_DESCRIPTION_MAX_BYTES) {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "groupDescription must contain between 1 and 4096 bytes",
                    false,
                )));
            }
            changes.push(("description", json!(description)));
        }
        for (field, list) in [
            ("addMembers", &params.add_members),
            ("removeMembers", &params.remove_members),
            ("promoteAdmins", &params.promote_admins),
            ("demoteAdmins", &params.demote_admins),
            ("banMembers", &params.ban_members),
            ("unbanMembers", &params.unban_members),
        ] {
            let Some(members) = list else {
                continue;
            };
            if members.is_empty() {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "an empty member array carries no change upstream; omit the key instead",
                    false,
                )));
            }
            if members.len() > MAX_GROUP_MEMBERS
                || members.iter().any(|member| {
                    member.is_empty() || member.chars().count() > MAX_GROUP_MEMBER_ID_CHARS
                })
            {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "member arrays carry at most 1001 entries of 1-128 characters",
                    false,
                )));
            }
            let key = match field {
                "addMembers" => "members",
                "promoteAdmins" => "admins",
                "demoteAdmins" => "removeAdmins",
                "banMembers" => "bans",
                "unbanMembers" => "unbans",
                _ => "removeMembers",
            };
            changes.push((key, json!(members)));
        }
        if let Some(link_state) = params.link_state.as_deref() {
            // The connector's enabled-without-approval is the upstream
            // "enabled" choice under a stricter name: the official link state
            // knows enabled / enabled-with-approval / disabled (pinned
            // UpdateGroupCommand --link choices).
            let upstream = match link_state {
                "enabled-with-approval" => "enabled-with-approval",
                "enabled-without-approval" => "enabled",
                "disabled" => "disabled",
                _ => {
                    return Err(ServiceError::Api(ApiError::new(
                        "INVALID_REQUEST",
                        "linkState must be one of enabled-with-approval, enabled-without-approval, disabled",
                        false,
                    )));
                }
            };
            changes.push(("link", json!(upstream)));
        }
        if params.reset_link == Some(true) {
            changes.push(("resetLink", json!(true)));
        }
        for (value, key) in [
            (
                params.permission_add_member.as_deref(),
                "setPermissionAddMember",
            ),
            (
                params.permission_edit_details.as_deref(),
                "setPermissionEditDetails",
            ),
            (
                params.permission_send_messages.as_deref(),
                "setPermissionSendMessages",
            ),
        ] {
            let Some(value) = value else {
                continue;
            };
            match value {
                // The pinned upstream accepts both spellings per choice
                // (every-member/everyMember); the connector forwards its
                // schema vocabulary verbatim.
                "every-member" | "only-admins" => changes.push((key, json!(value))),
                _ => {
                    return Err(ServiceError::Api(ApiError::new(
                        "INVALID_REQUEST",
                        "permission values must be every-member or only-admins",
                        false,
                    )));
                }
            }
        }
        if let Some(label) = params.member_label.as_deref() {
            if !bounded(label, GROUP_MEMBER_LABEL_MAX_BYTES) {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "memberLabel must contain between 1 and 256 bytes",
                    false,
                )));
            }
            changes.push(("memberLabel", json!(label)));
        }
        if let Some(emoji) = params.member_label_emoji.as_deref() {
            validate_emoji(emoji)?;
            changes.push(("memberLabelEmoji", json!(emoji)));
        }
        if changes.is_empty() {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "at least one group change is required",
                false,
            )));
        }
        let account = self.resolve_account(&params.account_id)?;
        let conversation =
            self.resolve_conversation(&params.account_id, &params.conversation_id)?;
        if conversation.kind != "group" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "groups.update addresses a group conversation",
                false,
            )));
        }
        match mode {
            SignalCliMode::KtEngine => Err(ServiceError::Api(ApiError::new(
                "CAPABILITY_UNAVAILABLE",
                "the kt-engine face has no updateGroup method yet",
                false,
            ))),
            _ => {
                let mut upstream = json!({
                    "account": account.signal_account,
                    "groupId": conversation.peer_key,
                });
                if let Some(object) = upstream.as_object_mut() {
                    for (key, value) in changes {
                        object.insert(key.to_string(), value);
                    }
                }
                Ok(upstream)
            }
        }
    }

    /// groups.quit (contract revision 1.45, §4.44, capability
    /// group-management): resolution ladder then the upstream `quitGroup
    /// {account, groupId}` payload. The upstream --delete local-data flag and
    /// the --admin last-admin handoff list are deliberately not exposed: the
    /// official client keeps the conversation as a non-member after leaving,
    /// and a last-admin quit without a handoff answers the upstream user
    /// error verbatim (UPSTREAM_ERROR family). The kt-engine face has no
    /// quitGroup method and answers CAPABILITY_UNAVAILABLE after local
    /// validation. The local conversation row is kept — non-member state is
    /// upstream convergence, like groups.update.
    pub fn prepare_groups_quit(
        &self,
        params: &GroupsQuitParams,
        mode: SignalCliMode,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        validate_opaque_id(&params.conversation_id, "conversationId")?;
        if params.conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        if let Some(operation_id) = params.operation_id.as_deref() {
            validate_opaque_id(operation_id, "operationId")?;
        }
        let account = self.resolve_account(&params.account_id)?;
        let conversation =
            self.resolve_conversation(&params.account_id, &params.conversation_id)?;
        if conversation.kind != "group" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "groups.quit addresses a group conversation",
                false,
            )));
        }
        match mode {
            SignalCliMode::KtEngine => Err(ServiceError::Api(ApiError::new(
                "CAPABILITY_UNAVAILABLE",
                "the kt-engine face has no quitGroup method yet",
                false,
            ))),
            _ => Ok(json!({
                "account": account.signal_account,
                "groupId": conversation.peer_key,
            })),
        }
    }

    /// groups.terminate (contract revision 1.52, §4.50, capability
    /// group-terminate): resolution ladder then the upstream `terminateGroup
    /// {account, groupId}` payload — the pinned 0.14.8 JSON-RPC face
    /// (`TerminateGroupCommand`, a `JsonRpcLocalCommand`) wrapping
    /// `GroupHelper.terminateGroup` → `GroupV2Helper.terminateGroup` →
    /// libsignal `createTerminateGroup()`. The upstream command answers
    /// `GroupSendingNotAllowedException` (non-admin, the GroupsV2
    /// UNAUTHORIZED face) and `NotAGroupMemberException` /
    /// `GroupNotFoundException` as user errors — all forward verbatim as the
    /// UPSTREAM_ERROR family; the v1-group refusal ("only supported for
    /// Signal group v2 groups") rides the same family. Zero local writes: the
    /// terminated state converges through the receive projection like every
    /// group change (the §4.44 discipline).
    pub fn prepare_groups_terminate(
        &self,
        params: &GroupsTerminateParams,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        validate_opaque_id(&params.conversation_id, "conversationId")?;
        if params.conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        if let Some(operation_id) = params.operation_id.as_deref() {
            validate_opaque_id(operation_id, "operationId")?;
        }
        let account = self.resolve_account(&params.account_id)?;
        let conversation =
            self.resolve_conversation(&params.account_id, &params.conversation_id)?;
        if conversation.kind != "group" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "groups.terminate addresses a group conversation",
                false,
            )));
        }
        Ok(json!({
            "account": account.signal_account,
            "groupId": conversation.peer_key,
        }))
    }

    /// groups.approveJoinRequests (contract revision 1.58, §4.53, capability
    /// group-join-approve): the local validation + resolution ladder, then
    /// the candidate payload the supervisor folds against the engine's own
    /// requesting-members read. `prepare` owns every fail-closed gate that
    /// needs no engine call: member-array bounds (the §4.44 discipline — an
    /// empty array carries no change upstream, so it is refused by shape),
    /// the group-row resolution (direct answers INVALID_REQUEST before any
    /// upstream call), and the audit id shape. The candidate list is passed
    /// through to the supervisor verbatim — the intersection with the live
    /// requesting set is upstream-state work and lives there; duplicates
    /// collapse so the upstream set semantics (JSON sets) can never be
    /// surprised by repeated entries. The kt-engine face has no
    /// updateGroup member-family method (the engine's updateGroup takes the
    /// expiration family only — its compat §5 batch boundary) and answers
    /// CAPABILITY_UNAVAILABLE (retryable=false) after the local validation
    /// completed — the groups.update/groups.quit engine-batch debt
    /// precedent, honest by design: answering the fold instead would
    /// disguise "the engine cannot approve" as "nobody is requesting".
    pub fn prepare_groups_approve_join_requests(
        &self,
        params: &GroupsApproveJoinRequestsParams,
        mode: SignalCliMode,
    ) -> Result<(String, Vec<String>, bool), ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        validate_opaque_id(&params.conversation_id, "conversationId")?;
        if params.conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        if let Some(operation_id) = params.operation_id.as_deref() {
            validate_opaque_id(operation_id, "operationId")?;
        }
        if params.members.is_empty() {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "an empty member array carries no change upstream; omit the call instead",
                false,
            )));
        }
        if params.members.len() > MAX_GROUP_MEMBERS
            || params.members.iter().any(|member| {
                member.is_empty() || member.chars().count() > MAX_GROUP_MEMBER_ID_CHARS
            })
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "members carries at most 1001 entries of 1-128 characters",
                false,
            )));
        }
        // The account row resolves first (the shared ladder's wire order —
        // ACCOUNT_NOT_FOUND before CONVERSATION_NOT_FOUND); the payload
        // itself needs only the group peer key, the supervisor re-resolves
        // the account number for the upstream call.
        let _account = self.resolve_account(&params.account_id)?;
        let conversation =
            self.resolve_conversation(&params.account_id, &params.conversation_id)?;
        if conversation.kind != "group" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "groups.approveJoinRequests addresses a group conversation",
                false,
            )));
        }
        match mode {
            SignalCliMode::KtEngine => Err(ServiceError::Api(ApiError::new(
                "CAPABILITY_UNAVAILABLE",
                "the kt-engine face has no updateGroup member-family method yet",
                false,
            ))),
            _ => {
                let mut members = params.members.clone();
                members.sort();
                members.dedup();
                Ok((conversation.peer_key, members, params.approve))
            }
        }
    }

    /// groups.revokeInvites (contract revision 1.64, §4.57, capability
    /// group-invite-revoke): the local validation + resolution ladder, then
    /// the candidate payload the supervisor folds against the engine's own
    /// pending-members read. The `prepare_groups_approve_join_requests`
    /// ladder verbatim — bounds, resolution, audit id, duplicate collapse —
    /// minus the approve bit. The kt-engine face has no updateGroup
    /// member-family method (the same compat §5 batch boundary the §4.53
    /// approval recorded) and answers CAPABILITY_UNAVAILABLE
    /// (retryable=false) after the local validation completed — honest by
    /// design: answering the fold instead would disguise "the engine cannot
    /// revoke" as "nobody is pending".
    pub fn prepare_groups_revoke_invites(
        &self,
        params: &GroupsRevokeInvitesParams,
        mode: SignalCliMode,
    ) -> Result<(String, Vec<String>), ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        validate_opaque_id(&params.conversation_id, "conversationId")?;
        if params.conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        if let Some(operation_id) = params.operation_id.as_deref() {
            validate_opaque_id(operation_id, "operationId")?;
        }
        if params.members.is_empty() {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "an empty member array carries no change upstream; omit the call instead",
                false,
            )));
        }
        if params.members.len() > MAX_GROUP_MEMBERS
            || params.members.iter().any(|member| {
                member.is_empty() || member.chars().count() > MAX_GROUP_MEMBER_ID_CHARS
            })
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "members carries at most 1001 entries of 1-128 characters",
                false,
            )));
        }
        // The account row resolves first (the shared ladder's wire order —
        // ACCOUNT_NOT_FOUND before CONVERSATION_NOT_FOUND); the payload
        // itself needs only the group peer key, the supervisor re-resolves
        // the account number for the upstream call.
        let _account = self.resolve_account(&params.account_id)?;
        let conversation =
            self.resolve_conversation(&params.account_id, &params.conversation_id)?;
        if conversation.kind != "group" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "groups.revokeInvites addresses a group conversation",
                false,
            )));
        }
        match mode {
            SignalCliMode::KtEngine => Err(ServiceError::Api(ApiError::new(
                "CAPABILITY_UNAVAILABLE",
                "the kt-engine face has no updateGroup member-family method yet",
                false,
            ))),
            _ => {
                let mut members = params.members.clone();
                members.sort();
                members.dedup();
                Ok((conversation.peer_key, members))
            }
        }
    }

    /// messages.reportSpam (contract revision 1.49, §4.48, capability
    /// report-spam): resolve the direct conversation's rows to the engine's
    /// `reportSpam {account, targetAci, serverGuids}` payload. The report is
    /// an upstream protocol action — zero local writes; rows that never
    /// carried a serverGuid (sent rows, history imports) cannot be reported
    /// and land in the caller's per-id failures instead. A group
    /// conversation is refused before any upstream call, and a numeric peer
    /// without a recorded ACI mapping is a deterministic refusal — the
    /// report would mis-address upstream.
    pub fn prepare_report_spam(
        &self,
        params: &MessagesReportSpamParams,
    ) -> Result<PreparedReportSpam, ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        validate_opaque_id(&params.conversation_id, "conversationId")?;
        if params.conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        if params.message_ids.is_empty() {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "messageIds must carry at least one id",
                false,
            )));
        }
        if params.message_ids.len() > REPORT_SPAM_MESSAGE_IDS_LIMIT
            || params
                .message_ids
                .iter()
                .any(|id| id.is_empty() || id.chars().count() > MAX_OPAQUE_ID_BYTES)
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "messageIds must carry at most 64 entries of 1-128 characters",
                false,
            )));
        }
        let account = self.resolve_account(&params.account_id)?;
        let conversation =
            self.resolve_conversation(&params.account_id, &params.conversation_id)?;
        if conversation.kind != "direct" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "messages.reportSpam addresses a direct conversation",
                false,
            )));
        }
        let target_aci = self.report_target_aci(&account.id, &conversation.peer_key)?;
        let guids =
            self.store
                .server_guids_by_ids(&account.id, &conversation.id, &params.message_ids)?;
        let mut upstream = json!({
            "account": account.signal_account,
            "targetAci": target_aci,
        });
        let pairs = params
            .message_ids
            .iter()
            .zip(guids)
            .filter_map(|(message_id, server_guid)| {
                server_guid.map(|guid| (message_id.clone(), guid))
            })
            .collect::<Vec<_>>();
        upstream["serverGuids"] = json!(pairs.iter().map(|(_, guid)| guid).collect::<Vec<_>>());
        Ok(PreparedReportSpam { upstream, pairs })
    }

    /// The report address (contract revision 1.49, §4.48): a UUID-shaped
    /// peer key is the ACI itself; otherwise the ingest-recorded
    /// ACI↔peer-key mapping answers. A numeric peer with no recorded
    /// mapping never falls back to the number — the upstream targetAci
    /// parses only ServiceIds, and guessing would mis-address the report.
    fn report_target_aci(&self, account_id: &str, peer_key: &str) -> Result<String, ServiceError> {
        if is_uuid_shaped(peer_key) {
            return Ok(peer_key.to_string());
        }
        self.store
            .aci_for_peer_key(account_id, peer_key)?
            .ok_or_else(|| {
                ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "report target ACI is not resolvable for this conversation",
                    false,
                ))
            })
    }

    /// stickerPacks.getSyncs (contract revision 1.37, §4.34): account bounds
    /// and existence — the pack records live in the account's storage
    /// session, the service reads nothing locally (no persistence on this
    /// face).
    pub fn prepare_sticker_pack_get_syncs(&self, account_id: &str) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        let account = self.resolve_account(account_id)?;
        Ok(json!({ "account": account.signal_account }))
    }

    /// stickerPacks.setSync (contract revision 1.37, §4.34): pack identity
    /// bounds and the installed-shape rule — `installed=true` requires the
    /// §4.32 pack key (44-character base64 decoding to exactly 32 bytes);
    /// `installed=false` writes the tombstone and ignores `packKey`/
    /// `position`, so both forward as null regardless of what the caller
    /// supplied (the engine contract: the uninstall path never reads them).
    /// No local row is written: the cloud record is the whole state.
    pub fn prepare_sticker_pack_set_sync(
        &self,
        account_id: &str,
        pack_id: &str,
        pack_key: Option<&str>,
        installed: bool,
        position: Option<u32>,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_sticker_browse_pack_id(pack_id)?;
        let key = match (installed, pack_key) {
            (true, Some(key)) => {
                validate_sticker_browse_pack_key(key)?;
                Some(key)
            }
            (true, None) => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "packKey is required when installed is true",
                    false,
                )));
            }
            // The uninstall path ignores key and position; the caller may
            // still have echoed the old values back — they are dropped here
            // so the upstream write is exactly the tombstone.
            (false, _) => None,
        };
        let account = self.resolve_account(account_id)?;
        Ok(json!({
            "account": account.signal_account,
            "packId": pack_id,
            "packKey": key,
            "installed": installed,
            "position": if installed { position } else { None },
        }))
    }

    /// accounts.updateConfiguration (contract revision 1.46, §4.45):
    /// at-least-one-writable-key fail-closed, the sharing-mode enum, and
    /// account resolution under the service lock; only the provided keys are
    /// forwarded so the upstream partial-write semantics (official
    /// updateConfiguration: write exactly the provided fields, read the rest
    /// back untouched) survive the hop. No local row is written.
    pub fn prepare_accounts_update_configuration(
        &self,
        params: &AccountsUpdateConfigurationParams,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        let provided = [
            params.read_receipts.is_some(),
            params.typing_indicators.is_some(),
            params.phone_number_sharing_mode.is_some(),
            params.unlisted_phone_number.is_some(),
        ];
        if !provided.iter().any(|present| *present) {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "at least one of readReceipts, typingIndicators, phoneNumberSharingMode, unlistedPhoneNumber is required",
                false,
            )));
        }
        if let Some(mode) = params.phone_number_sharing_mode.as_deref()
            && !matches!(mode, "everybody" | "nobody")
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "phoneNumberSharingMode must be 'everybody' or 'nobody'",
                false,
            )));
        }
        let account = self.resolve_account(&params.account_id)?;
        let mut upstream = json!({ "account": account.signal_account });
        if let Some(read_receipts) = params.read_receipts {
            upstream["readReceipts"] = json!(read_receipts);
        }
        if let Some(typing_indicators) = params.typing_indicators {
            upstream["typingIndicators"] = json!(typing_indicators);
        }
        if let Some(mode) = params.phone_number_sharing_mode.as_deref() {
            upstream["phoneNumberSharingMode"] = json!(mode);
        }
        if let Some(unlisted_phone_number) = params.unlisted_phone_number {
            upstream["unlistedPhoneNumber"] = json!(unlisted_phone_number);
        }
        Ok(upstream)
    }

    /// accounts.setProfile (contract revision 1.46, §4.45): the text-face
    /// bounds (official desktop ProfileEditor budgets, §4.45) and the avatar
    /// tri-state resolve under the service lock; the optional text keys pass
    /// through as-is because the upstream text face is a full write — a key
    /// the caller omits is cleared upstream, exactly the official
    /// updateProfile semantics. No local row is written.
    pub fn prepare_accounts_set_profile(
        &self,
        params: &AccountsSetProfileParams,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        if params.given_name.is_empty() || params.given_name.len() > MAX_PROFILE_NAME_BYTES {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "givenName must contain between 1 and 128 bytes",
                false,
            )));
        }
        if let Some(family_name) = params.family_name.as_deref()
            && family_name.len() > MAX_PROFILE_NAME_BYTES
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "familyName must contain at most 128 bytes",
                false,
            )));
        }
        if let Some(about) = params.about.as_deref()
            && about.len() > MAX_PROFILE_ABOUT_BYTES
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "about must contain at most 512 bytes",
                false,
            )));
        }
        if let Some(emoji) = params.emoji.as_deref()
            && emoji.len() > MAX_EMOJI_BYTES
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "emoji must contain at most 32 bytes",
                false,
            )));
        }
        let avatar = match &params.avatar {
            None => AvatarUpdate::Keep,
            Some(Value::Null) => AvatarUpdate::Remove,
            Some(Value::String(data_uri)) => {
                AvatarUpdate::Replace(validate_avatar_data_uri(data_uri)?)
            }
            Some(_) => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "avatar must be a data:image/*;base64, data URI, null, or absent",
                    false,
                )));
            }
        };
        let account = self.resolve_account(&params.account_id)?;
        let mut upstream = json!({
            "account": account.signal_account,
            "givenName": params.given_name,
        });
        if let Some(family_name) = params.family_name.as_deref() {
            upstream["familyName"] = json!(family_name);
        }
        if let Some(about) = params.about.as_deref() {
            upstream["about"] = json!(about);
        }
        if let Some(emoji) = params.emoji.as_deref() {
            upstream["emoji"] = json!(emoji);
        }
        match avatar {
            AvatarUpdate::Keep => {}
            AvatarUpdate::Remove => upstream["avatar"] = Value::Null,
            AvatarUpdate::Replace(data_uri) => upstream["avatar"] = json!(data_uri),
        }
        Ok(upstream)
    }

    /// accounts.setExpireTimer (contract revision 1.46, §4.45): account
    /// resolution under the service lock; the u32 face is params-shaped (the
    /// engine face and the official AccountRecord field are both uint32).
    /// Same-value sets are upstream idempotent no-writes — no pre-check, the
    /// passthrough never guesses. No local row is written: the universal
    /// timer carries no broadcast, other devices converge through storage
    /// sync.
    pub fn prepare_accounts_set_expire_timer(
        &self,
        account_id: &str,
        expire_seconds: u32,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        let account = self.resolve_account(account_id)?;
        Ok(json!({
            "account": account.signal_account,
            "expirationInSeconds": expire_seconds,
        }))
    }

    /// conversations.getBlocked (contract revision 1.46, §4.45): account
    /// bounds and existence — the blocked list lives in the account's
    /// storage records, the service reads nothing locally (no persistence on
    /// this face).
    pub fn prepare_conversations_get_blocked(
        &self,
        account_id: &str,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        let account = self.resolve_account(account_id)?;
        Ok(json!({ "account": account.signal_account }))
    }

    /// accounts.setUsername (contract revision 1.59, §4.55, capability
    /// username): the resolution ladder ends at the pinned signal-cli
    /// `updateAccount` mutually exclusive group — `username` XOR
    /// `deleteUsername` (UpdateAccountCommand.java:41-46), both absent being
    /// a legitimate attributes-only call the connector does not expose (the
    /// desktop face is username-shaped). The local shape gate mirrors the
    /// official desktop grammar (`ts/util/Username.dom.ts`): nickname
    /// `[a-zA-Z_][a-zA-Z0-9_]*`, optional all-digits discriminator `.N` with
    /// no `0…` forms beyond exactly-one-zero, 3..32 characters total — the
    /// pinned libsignal bounds (AccountHelper.java:363-364). A bare nickname
    /// (no dot) is allowed: `ManagerImpl.setUsername` routes it to
    /// `reserveUsernameFromNickname`, which lets the server pick the
    /// discriminator. The kt-engine face has no username method and answers
    /// CAPABILITY_UNAVAILABLE after local validation (groups.update/quit
    /// debt shape). Nothing is persisted locally — the write result
    /// projection is the desktop's mirror source.
    pub fn prepare_accounts_set_username(
        &self,
        params: &AccountsSetUsernameParams,
        mode: SignalCliMode,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        let setting = match (params.username.as_deref(), params.delete_username) {
            (Some(_), true) => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "username and deleteUsername are mutually exclusive",
                    false,
                )));
            }
            (Some(username), false) => Some(username),
            (None, false) => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "one of username or deleteUsername is required",
                    false,
                )));
            }
            (None, true) => None,
        };
        if let Some(username) = setting {
            if username.is_empty() || username.chars().count() > USERNAME_MAX_WIRE_CHARS {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "username must contain between 1 and 38 characters",
                    false,
                )));
            }
            validate_username_shape(username)?;
        }
        let account = self.resolve_account(&params.account_id)?;
        match mode {
            SignalCliMode::KtEngine => Err(ServiceError::Api(ApiError::new(
                "CAPABILITY_UNAVAILABLE",
                "the kt-engine face has no username method yet",
                false,
            ))),
            _ => Ok(match setting {
                Some(username) => json!({
                    "account": account.signal_account,
                    "username": username,
                }),
                None => json!({
                    "account": account.signal_account,
                    "deleteUsername": true,
                }),
            }),
        }
    }

    /// accounts.getUsername (contract revision 1.59, §4.55, capability
    /// username): the pinned signal-cli has no JSON-RPC reader for the local
    /// account's username — `JsonAccountResponse` is a private record inside
    /// UpdateAccountCommand fed by the write-back only, and no listAccounts/
    /// getAccount face carries the field. The read face therefore answers
    /// CAPABILITY_UNAVAILABLE after the standard ladder: honest engine-surface
    /// debt (same shape as the kt-engine arms), with display fed by the
    /// setUsername write-back mirror instead of a live read. Mode-independent:
    /// neither engine face has the reader.
    pub fn prepare_accounts_get_username(&self, account_id: &str) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        self.resolve_account(account_id)?;
        Err(ServiceError::Api(ApiError::new(
            "CAPABILITY_UNAVAILABLE",
            "the pinned signal-cli has no username read face; the connector mirrors the setUsername write-back instead",
            false,
        )))
    }

    /// username.lookup (contract revision 1.59, §4.55, capability username):
    /// entries resolve through the pinned `getUserStatus --username` face
    /// (`resolveRecipientByUsernameOrLink`, so entries are full usernames or
    /// username-link URLs, not bare nicknames). The batch cap bounds one host
    /// frame (the upstream takes an open array); entries stay 1..64 chars —
    /// a bound, not the upstream grammar, which the link resolution owns.
    pub fn prepare_username_lookup(
        &self,
        params: &UsernameLookupParams,
        mode: SignalCliMode,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        if params.usernames.is_empty() || params.usernames.len() > USERNAME_LOOKUP_MAX_ENTRIES {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "usernames must contain between 1 and 20 entries",
                false,
            )));
        }
        for entry in &params.usernames {
            if entry.is_empty() || entry.chars().count() > USERNAME_LOOKUP_ENTRY_MAX_CHARS {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "each username entry must contain between 1 and 64 characters",
                    false,
                )));
            }
        }
        let account = self.resolve_account(&params.account_id)?;
        match mode {
            SignalCliMode::KtEngine => Err(ServiceError::Api(ApiError::new(
                "CAPABILITY_UNAVAILABLE",
                "the kt-engine face has no username method yet",
                false,
            ))),
            _ => Ok(json!({
                "account": account.signal_account,
                "usernames": params.usernames,
            })),
        }
    }

    /// backups.export / backups.exportStatus (contract revision 1.47, §4.46):
    /// resolve the account and name the engine face. The upstream account key
    /// is always the resolved signal number; the raw number never reaches the
    /// host (the exportStatus projection strips the engine's `account` echo).
    pub fn prepare_backups_account(&self, account_id: &str) -> Result<String, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        Ok(self.resolve_account(account_id)?.signal_account)
    }

    /// conversations.getAvatar (contract revision 1.48, §4.47): bounds +
    /// resolution only — the avatar bytes live entirely inside the engine
    /// and the response. The local conversation row resolves first
    /// (CONVERSATION_NOT_FOUND before any upstream call) and the upstream
    /// key is the row's peer reference, never the host's opaque local id;
    /// the engine classifies the reference shape itself (ServiceId UUID /
    /// phone number / 32-byte base64 group master key).
    pub fn prepare_conversations_get_avatar(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        if conversation_id.is_empty()
            || conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        Ok(json!({
            "account": account.signal_account,
            "conversationId": conversation.peer_key,
        }))
    }

    /// conversations.setBlocked (contract revision 1.48, §4.47): bounds,
    /// kind enum, and resolution — the §4.33 pin-write ladder verbatim. No
    /// local row is written: the cloud record is the whole state and the
    /// engine answers the post-write blocked list.
    pub fn prepare_conversations_set_blocked(
        &self,
        account_id: &str,
        conversation_id: &str,
        kind: &str,
        blocked: bool,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        if conversation_id.is_empty()
            || conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        validate_pinned_kind(kind)?;
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        Ok(json!({
            "account": account.signal_account,
            "conversationId": conversation.peer_key,
            "kind": kind,
            "blocked": blocked,
        }))
    }

    /// conversations.acceptRequest / conversations.declineRequest (contract
    /// revision 1.63, §4.56): bounds, kind classification, and resolution —
    /// the §4.47 blocked-write ladder verbatim. No local row is written: the
    /// request state lives in the upstream sync channel and converges through
    /// the §4.56 receive projection. The upstream keys are the resolved peer
    /// reference (direct conversation) or the raw group master key (group —
    /// the contact extra carries it, not the local opaque id). An empty peer
    /// reference (the §4.42 self row) fails closed here: the upstream face
    /// takes real recipients only.
    pub fn prepare_conversations_message_request(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        if conversation_id.is_empty()
            || conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        match conversation.kind.as_str() {
            "direct" => {
                if conversation.peer_key.is_empty() {
                    return Err(ServiceError::Api(ApiError::new(
                        "INVALID_REQUEST",
                        "this conversation has no upstream address",
                        false,
                    )));
                }
                Ok(json!({
                    "account": account.signal_account,
                    "recipient": conversation.peer_key,
                }))
            }
            "group" | "legacyGroup" => {
                // The group conversations row's peer_key is the upstream
                // group master key verbatim — the sync materializes both the
                // contact-cache row and the conversation skeleton from the
                // same `listGroups` id, so no extra read is needed.
                if conversation.peer_key.is_empty() {
                    return Err(ServiceError::Api(ApiError::new(
                        "INVALID_REQUEST",
                        "conversation has no upstream group reference",
                        false,
                    )));
                }
                Ok(json!({
                    "account": account.signal_account,
                    "groupId": conversation.peer_key,
                }))
            }
            _ => Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "unsupported conversation kind",
                false,
            ))),
        }
    }

    /// identities.get / identities.verify (contract revision 1.68, §4.61):
    /// bounds and resolution — the §4.47/§4.56 ladder verbatim, gated to the
    /// 1:1 shape. Identity keys are a per-contact concept (the official
    /// desktop gates the details row on `!isGroup`, ConversationDetails.dom
    /// .tsx:654), so group/legacy rows fail closed before any upstream call;
    /// the §4.42 self row (empty peer reference) fails closed the same way.
    /// The upstream key is the resolved peer reference verbatim — the pinned
    /// `listIdentities`/`trust` recipient parameter accepts a phone number,
    /// ACI UUID, `PNI:` or `u:` username form (RecipientIdentifier.java:26-49),
    /// which is exactly the peer_key shape taxonomy, so the connector passes
    /// it through and never re-classifies.
    fn prepare_identities_face(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<(Value, String), ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        if conversation_id.is_empty()
            || conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        match conversation.kind.as_str() {
            "direct" => {
                if conversation.peer_key.is_empty() {
                    return Err(ServiceError::Api(ApiError::new(
                        "INVALID_REQUEST",
                        "this conversation has no upstream address",
                        false,
                    )));
                }
                Ok((
                    json!({ "account": account.signal_account }),
                    conversation.peer_key,
                ))
            }
            "group" | "legacyGroup" => Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "safety numbers are a 1:1 conversation face",
                false,
            ))),
            _ => Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "unsupported conversation kind",
                false,
            ))),
        }
    }

    /// identities.get (contract revision 1.68, §4.61, capability
    /// safety-number): the prepared pinned `listIdentities` call — the
    /// upstream reads one identity when the recipient parameter is present,
    /// so the connector narrows the read to the resolved peer reference and
    /// never requests the account-wide list.
    pub fn prepare_identities_get(
        &self,
        account_id: &str,
        conversation_id: &str,
    ) -> Result<Value, ServiceError> {
        let (mut upstream, peer_key) = self.prepare_identities_face(account_id, conversation_id)?;
        if let Some(map) = upstream.as_object_mut() {
            map.insert("number".to_string(), Value::String(peer_key));
        }
        Ok(upstream)
    }

    /// identities.verify (contract revision 1.68, §4.61, capability
    /// safety-number): the prepared pinned `trust` call in its
    /// `verifiedSafetyNumber` form — the safety number string is the exact
    /// digits the desktop just displayed (the official verify semantics:
    /// only use this option if you have verified the safety number,
    /// TrustCommand.java:37). The upstream parses spaced 60-digit groups,
    /// 66-char hex fingerprints, or base64 scannable payloads
    /// (IdentityVerificationCode.java:20-32); the connector enforces only
    /// the wire bound and never invents a second grammar — a malformed
    /// string is the upstream's definite UserError, not a local guess.
    /// The testing-only `trustAllKnownKeys` mode is never sent.
    pub fn prepare_identities_verify(
        &self,
        account_id: &str,
        conversation_id: &str,
        safety_number: &str,
    ) -> Result<Value, ServiceError> {
        if safety_number.is_empty()
            || safety_number.chars().count() > IDENTITY_SAFETY_NUMBER_MAX_CHARS
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "safetyNumber must contain between 1 and 256 characters",
                false,
            )));
        }
        let (mut upstream, peer_key) = self.prepare_identities_face(account_id, conversation_id)?;
        if let Some(map) = upstream.as_object_mut() {
            map.insert("recipient".to_string(), Value::String(peer_key));
            map.insert(
                "verifiedSafetyNumber".to_string(),
                Value::String(safety_number.to_string()),
            );
        }
        Ok(upstream)
    }

    /// calls.sendSignal (contract revision 1.53, §4.49): the shape gate plus
    /// resolution — the signaling envelope (`recipient`, `data`/`legacy`,
    /// `urgency`, `destinationDeviceId`) is the engine `sendCallMessage` face;
    /// the connector forwards it and never inspects the signaling content.
    /// `type` gates against the five-value frame-family enum, `callId` a
    /// positive u32. `payload` is mandatory for the modern faces and must be
    /// absent-or-present only as the engine face allows: offer/answer/ice/
    /// opaque carry RingRTC bytes (non-empty, bounded); busy/hangup are the
    /// official legacy bare-frame family (RingRTC 2.72.1 emits them with no
    /// opaque material) and may omit the payload — the connector then routes
    /// the engine's `legacy` face instead of inventing bytes. `urgency` (1.53)
    /// is the pass-through wake-up bit (`droppable` | `handleImmediately`).
    /// Group shapes answer INVALID_REQUEST here (the upstream 1:1 face is
    /// contact-only — its official fail-closed).
    pub fn prepare_calls_send_signal(
        &self,
        account_id: &str,
        conversation_id: &str,
        signal_type: &str,
        call_id: u32,
        payload: Option<&str>,
        urgency: Option<&str>,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        if conversation_id.is_empty()
            || conversation_id.chars().count() > MAX_PIN_CONVERSATION_ID_CHARS
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "conversationId must contain between 1 and 256 characters",
                false,
            )));
        }
        if !CALL_SIGNAL_TYPES.contains(&signal_type) {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "type must be one of offer, answer, ice, busy, hangup",
                false,
            )));
        }
        if call_id == 0 {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "callId must be a positive integer",
                false,
            )));
        }
        // The payload face: mandatory non-empty bounded bytes for the modern
        // faces; the legacy busy/hangup family may omit it (the engine builds
        // the official bare frames). A present payload keeps the modern bound
        // regardless of type (the connector never inspects content, so an
        // explicit payload on a legacy type forwards verbatim).
        if payload.is_none() && !matches!(signal_type, "busy" | "hangup") {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "payload is mandatory for the offer, answer, ice, and opaque faces",
                false,
            )));
        }
        let legacy_face = matches!(signal_type, "busy" | "hangup") && payload.is_none();
        if let Some(payload) = payload
            && (payload.is_empty() || payload.chars().count() > MAX_CALL_SIGNAL_PAYLOAD_CHARS)
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "payload must contain between 1 and 3145728 characters",
                false,
            )));
        }
        // The urgency face (1.53): absent/null = the engine's droppable
        // default; otherwise the two-value enum, anything else refuses before
        // any upstream call.
        let urgency_value = match urgency {
            None => Value::Null,
            Some("droppable") | Some("handleImmediately") => json!(urgency),
            Some(_) => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "urgency must be droppable or handleImmediately",
                    false,
                )));
            }
        };
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        if conversation.kind != "direct" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "call signaling is 1:1 only — group conversations are not addressable",
                false,
            )));
        }
        let mut upstream = json!({
            "account": account.signal_account,
            "recipient": conversation.peer_key,
        });
        if legacy_face {
            // The official legacy bare-frame family: busy/hangup ride the
            // engine's `legacy` face — `CallMessage{busy{id}}` /
            // `CallMessage{hangup{id}}` with no opaque field (the callId the
            // caller addressed is the frames' id face; hangup's type vocabulary
            // is default-normal here — the desktop layer owns richer
            // projections and this connector forwards, never invents).
            upstream["legacy"] = json!({ "kind": signal_type, "id": call_id });
        } else {
            upstream["data"] = json!(payload);
        }
        if !urgency_value.is_null() {
            upstream["urgency"] = urgency_value;
        }
        Ok(upstream)
    }

    /// calls.getState (contract revision 1.50, §4.48): the honest
    /// projection of what this connector knows — always the empty registry.
    /// The call state machine lives in the desktop + calling sidecar and
    /// the engine transports; the connector is the signaling pipe between
    /// them and persists no call state of its own.
    pub fn prepare_calls_get_state(
        &self,
        account_id: &str,
    ) -> Result<CallStateRegistry, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        self.resolve_account(account_id)?;
        Ok(CallStateRegistry::default())
    }

    /// Upstream JSON-RPC send params shared by first dispatch (`dispatch_send`)
    /// and same-row retry (`prepare_retry_text`) — one owner for the wire key
    /// names so the two paths cannot drift.
    ///
    /// Outbound link preview (contract §5.10 v1.26), projected 1:1 onto the
    /// signal-cli JSON-RPC send keys (verified against the pinned
    /// distribution: previewUrl/previewTitle/previewDescription plus an
    /// optional previewImage carrying a path or RFC 2397 data URI — the
    /// connector passes data URIs only, mirroring `attachments`).
    ///
    /// Outbound @mentions (contract revision 1.34) project onto the engine
    /// `send` `mentions` entry keys verbatim — `number`/`start`/`length`;
    /// validation and address resolution happened in `mentions_to_upstream`,
    /// so every entry that gets here is upstream-safe.
    ///
    /// signal-cli jsonRpc send accepts `attachments` entries as file paths or
    /// RFC 2397 data URIs (SendCommand --attachment, pinned 0.14.7); the
    /// connector passes data URIs only — bytes stay in memory, no
    /// caller-controlled path ever reaches upstream.
    ///
    /// signal-cli JSON-RPC send quote parameters (verified against the pinned
    /// 0.14.7 distribution): quoteTimestamp is the quoted message's Signal
    /// timestamp, quoteAuthor its author's number — both required.
    ///
    /// Outbound text styles (contract revision 1.69, §4.62) project onto the
    /// engine `send` `textStyle` entry keys verbatim — `style`/`start`/
    /// `length` (the camelCase of signal-cli's `--text-style`
    /// `start:length:STYLE` triples, JsonRpcNamespace getList); validation
    /// and bounds happened in `text_styles_to_upstream`, so every entry that
    /// gets here is upstream-safe. The key is absent when no styles are
    /// given, so every existing wire shape stays byte-identical.
    #[allow(clippy::too_many_arguments)]
    fn upstream_text_send_params(
        signal_account: &str,
        text: &str,
        preview: Option<&UpstreamSendPreview>,
        mentions: Option<&[UpstreamSendMention]>,
        text_styles: Option<&[UpstreamSendTextStyle]>,
        attachments: Option<Vec<Value>>,
        quote: Option<(u64, String)>,
        conversation: &ConversationRow,
        view_once: bool,
    ) -> Value {
        let mut params = json!({
            "account": signal_account,
            "message": text,
        });
        if let Some(link_preview) = preview {
            params["previewUrl"] = json!(link_preview.url);
            params["previewTitle"] = json!(link_preview.title);
            if let Some(description) = &link_preview.description {
                params["previewDescription"] = json!(description);
            }
            if let Some(image) = &link_preview.image_data_uri {
                params["previewImage"] = json!(image);
            }
        }
        if let Some(mentions) = mentions {
            // The engine `send` mention entry keys, verbatim (number/start/
            // length); offsets are UTF-16 code units passed through — the
            // caller indexes its own body.
            params["mentions"] = json!(
                mentions
                    .iter()
                    .map(|mention| {
                        json!({
                            "number": mention.number,
                            "start": mention.start,
                            "length": mention.length,
                        })
                    })
                    .collect::<Vec<_>>()
            );
        }
        if let Some(text_styles) = text_styles {
            // The engine `send` textStyle entry keys, verbatim (style/start/
            // length); style is the pinned TextStyle enum vocabulary and the
            // offsets are UTF-16 code units passed through — the caller
            // indexes its own body.
            params["textStyle"] = json!(
                text_styles
                    .iter()
                    .map(|style| {
                        json!({
                            "style": style.style,
                            "start": style.start,
                            "length": style.length,
                        })
                    })
                    .collect::<Vec<_>>()
            );
        }
        if let Some(attachments) = attachments {
            params["attachments"] = Value::Array(attachments);
        }
        set_upstream_target(&mut params, conversation);
        if let Some((quote_timestamp, quote_author)) = quote {
            params["quoteTimestamp"] = json!(quote_timestamp);
            params["quoteAuthor"] = json!(quote_author);
        }
        // §4.36: the view-once flag rides the send top level; the key is
        // absent when false, so every existing wire shape stays byte-identical.
        if view_once {
            params["viewOnce"] = json!(true);
        }
        params
    }

    /// Retry one definitively-failed outgoing text in place (contract 1.31,
    /// §4.26): the row addressed by its original clientRequestId is rearmed
    /// `failed -> pending` under a store-level guard and re-dispatched with
    /// upstream params rebuilt from the persisted record — a retry never
    /// mints a new clientRequestId, so the history keeps exactly one row per
    /// logical message and settlement (any terminal outcome) completes the
    /// SAME row the desktop bubble already renders.
    pub fn prepare_retry_text(
        &self,
        account_id: &str,
        conversation_id: &str,
        client_request_id: &str,
    ) -> Result<PreparedSend, ServiceError> {
        validate_opaque_id(client_request_id, "clientRequestId")?;
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        let row = self
            .store
            .message_by_client_request(account_id, client_request_id)?
            .ok_or(ServiceError::Store(StoreError::MessageNotFound))?;
        if row.conversation_id != conversation.id {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "clientRequestId does not belong to this conversation",
                false,
            )));
        }
        if row.direction != "outgoing" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "only outgoing messages can be retried",
                false,
            )));
        }
        let text = row.text.clone().unwrap_or_default();
        if text.is_empty() {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "retry supports text messages only",
                false,
            )));
        }
        if row.status == "pending" {
            return Err(ServiceError::Api(ApiError::new(
                "RETRY_IN_FLIGHT",
                "retry already in flight",
                false,
            )));
        }
        if row.status != "failed" {
            // 'unknown' is never retryable: its wire outcome was never
            // settled, so a blind resend could double-deliver.
            return Err(ServiceError::Api(ApiError::new(
                "RETRY_NOT_ALLOWED",
                "only a definitively failed send can be retried",
                false,
            )));
        }
        // Store-level rearm is the authority: `WHERE status='failed'` makes
        // the transition atomic, so a status push that settled the row
        // between the guard above and this write cannot be overwritten.
        let (rearmed, changed) = self
            .store
            .rearm_failed_message(&row.id)?
            .ok_or(ServiceError::Store(StoreError::MessageNotFound))?;
        if !changed {
            return Err(ServiceError::Api(ApiError::new(
                "RETRY_IN_FLIGHT",
                "retry already in flight",
                false,
            )));
        }
        let quote = row
            .quote_message_id
            .as_deref()
            .map(|quote_id| self.resolve_quote(&account, &conversation, quote_id))
            .transpose()?;
        // Mentions and text styles are not persisted for retry rebuilds: the
        // row carries only the plain text, so a retried send goes out
        // un-highlighted and un-styled (documented deviation: the text
        // arrives unchanged).
        let params = Self::upstream_text_send_params(
            &account.signal_account,
            &text,
            None,
            None,
            None,
            None,
            quote,
            &conversation,
            false,
        );
        Ok(PreparedSend::Dispatch {
            pending_id: row.id,
            account_id: account.id,
            conversation_id: conversation.id,
            params,
            pending_sent_at: rearmed.sent_at,
        })
    }

    /// Resolve a local quoteMessageId to the upstream (quoteTimestamp,
    /// quoteAuthor) pair. A quote targets a message in the same conversation;
    /// an unknown target is MESSAGE_NOT_FOUND, and a target whose Signal
    /// author/timestamp is not established is a deterministic INVALID_REQUEST
    /// — never a silently dropped or mis-addressed quote upstream.
    fn resolve_quote(
        &self,
        account: &AccountRow,
        conversation: &ConversationRow,
        quote_message_id: &str,
    ) -> Result<(u64, String), ServiceError> {
        let quoted = self
            .store
            .message_by_id(&account.id, &conversation.id, quote_message_id)?
            .ok_or(StoreError::MessageNotFound)?;
        let author = match quoted.direction {
            // We authored it: the author is the linked account itself. Only an
            // addressable send carries the upstream timestamp Signal quotes
            // match on; pending/failed/unknown rows would misquote.
            "outgoing" if outgoing_status_is_addressable(quoted.status) => {
                account.signal_account.clone()
            }
            // In a direct chat the only other possible author is the peer.
            // Incoming rows store the envelope timestamp, which is the
            // protocol identity a quote references.
            "incoming" if conversation.kind == "direct" => conversation.peer_key.clone(),
            // Group messages do not persist the member address (only a local
            // sender hash), and system rows have no author at all.
            _ => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "quoted message author is not resolvable",
                    false,
                )));
            }
        };
        Ok((quoted.sent_at, author))
    }

    /// Pure local validation for `messages.edit` (contract revision 1.15,
    /// upstream `send` with `editTimestamp`): resolve the target row exactly
    /// like `prepare_remote_delete` — only an outgoing row in the terminal
    /// state `sent` carries the upstream protocol identity an edit must
    /// reference — and build the exact upstream jsonRpc params.
    pub fn prepare_edit(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        text: &str,
    ) -> Result<PreparedEdit, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        validate_text(text)?;
        let (account, conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        if message.direction != "outgoing" || !outgoing_status_is_addressable(message.status) {
            return Err(StoreError::MessageNotFound.into());
        }
        // signal-cli `send` with editTimestamp (verified against the jsonRpc
        // surface: SendCommand dest `edit-timestamp` maps to the camelCase
        // jsonRpc key) retargets the earlier message; addressing matches send.
        let mut params = json!({
            "account": account.signal_account,
            "message": text,
            "editTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(PreparedEdit {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            text: text.to_string(),
            params,
        })
    }

    /// Settlement of a confirmed upstream edit (contract 1.15): replace the
    /// row's body and stamp `edited_at`; status is untouched.
    pub fn complete_edit_success(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        text: &str,
    ) -> Result<Option<MessageRecord>, ServiceError> {
        let text_bytes = text.len().min(u32::MAX as usize) as u32;
        let mut record = self.store.apply_outgoing_edit(
            message_id,
            account_id,
            conversation_id,
            text,
            text_bytes,
        )?;
        if let Some(record) = record.as_mut() {
            self.store
                .attach_reactions(account_id, std::slice::from_mut(record))?;
            self.store
                .attach_edits(account_id, std::slice::from_mut(record))?;
        }
        Ok(record)
    }

    /// Pure local validation for `messages.remoteDelete` (docs/remote-delete-l2-plan.md §3.2):
    /// resolve the target row and build the exact upstream jsonRpc params. Only an
    /// addressable outgoing row carries a real Signal protocol identity
    /// — its `sent_at` was overwritten with the send response's upstream timestamp by
    /// `complete_outgoing_send` and is preserved across delivery/read receipts
    /// (`outgoing_status_is_addressable`), the same precedent as quote resolution
    /// (`resolve_quote`). Pending/failed/unknown rows hold a local clock value there,
    /// so they are indistinguishable from a missing row and answer `MESSAGE_NOT_FOUND`.
    /// Nothing is written locally and no event is emitted: the connector does not
    /// mutate the local `messages` row on a remote delete (plan §3.6).
    pub fn prepare_remote_delete(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<PreparedRemoteDelete, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        let (account, conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        if message.direction != "outgoing" || !outgoing_status_is_addressable(message.status) {
            return Err(StoreError::MessageNotFound.into());
        }
        // signal-cli JSON-RPC remoteDelete parameters (verified against the pinned
        // 0.14.7 distribution: RemoteDeleteCommand dests `target-timestamp` /
        // `recipient` / `group-id` / `note-to-self`, and JsonRpcNamespace maps
        // dash-separated dests to camelCase JSON keys): `targetTimestamp` is the
        // deleted message's Signal timestamp, `recipient`/`groupId` address the
        // receiving side — the same addressing shape as `send`.
        let mut params = json!({
            "account": account.signal_account,
            "targetTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(PreparedRemoteDelete {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            params,
        })
    }

    /// Pure local validation for `messages.sendReaction`
    /// (docs/remote-delete-l2-plan.md §3.2 shape, upstream `sendReaction`):
    /// resolve the target row and build the exact upstream jsonRpc params.
    /// A reaction targets a message that already exists in the local history,
    /// so both directions are addressable — an own outgoing row qualifies in
    /// an addressable state (`sent`/`delivered`/`read`; its `sent_at` is the
    /// upstream Signal timestamp preserved across receipts, same precedent as
    /// `resolve_quote`/`prepare_remote_delete`),
    /// an incoming row carries the envelope timestamp. The target author
    /// follows the direction: the linked account's own number for outgoing
    /// rows, the conversation peer for incoming rows. Outgoing rows without an
    /// addressable state carry no protocol identity and answer
    /// `MESSAGE_NOT_FOUND`; group incoming rows persist only a local sender
    /// hash, not the member's number, so their author is not resolvable
    /// (deterministic `INVALID_REQUEST`). Nothing is written locally and no
    /// event is emitted.
    pub fn prepare_send_reaction(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        emoji: &str,
        remove: bool,
    ) -> Result<PreparedSendReaction, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        validate_emoji(emoji)?;
        let (account, conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        // signal-cli JSON-RPC sendReaction parameters (verified against the
        // pinned 0.14.7 distribution: SendReactionCommand dests `emoji`
        // (required, "should be a single unicode grapheme cluster"),
        // `target-author`, `target-timestamp` (required Long), `remove`
        // (storeTrue) and `recipient`/`group-id`/`username`/`note-to-self`;
        // JsonRpcNamespace.get falls back to
        // Util.dashSeparatedToCamelCaseString, so jsonRpc carries camelCase
        // keys): `targetTimestamp` is the reacted-to message's Signal
        // timestamp, `targetAuthor` its author's number, and
        // `recipient`/`groupId` address the receiving side — the same
        // addressing shape as `send`/`remoteDelete`.
        let target_author = match message.direction {
            // We authored it: the author is the linked account itself. Only an
            // addressable send carries the upstream timestamp Signal reactions
            // match on; pending/failed/unknown rows would mis-target.
            "outgoing" if outgoing_status_is_addressable(message.status) => {
                account.signal_account.clone()
            }
            // An outgoing row without an addressable state carries no
            // upstream protocol identity (its sent_at is a local clock value)
            // and is indistinguishable from a missing row (remoteDelete
            // precedent).
            "outgoing" => return Err(StoreError::MessageNotFound.into()),
            // In a direct chat the only other possible author is the peer.
            // Incoming rows store the envelope timestamp, which is the
            // protocol identity a reaction references (same precedent as
            // quote resolution).
            "incoming" if conversation.kind == "direct" => conversation.peer_key.clone(),
            // Group messages do not persist the member address (only a local
            // sender hash), and system rows have no author at all: reacting
            // cannot be addressed upstream, so it fails deterministically
            // instead of mis-targeting (quote-resolution precedent).
            _ => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "reaction target author is not resolvable",
                    false,
                )));
            }
        };
        let mut params = json!({
            "account": account.signal_account,
            "emoji": emoji,
            "remove": remove,
            "targetAuthor": target_author,
            "targetTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(PreparedSendReaction {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            params,
            emoji: emoji.to_string(),
            remove,
            target_sent_at: message.sent_at,
        })
    }

    /// sendReaction 上行确认后的本地落库（对齐 edit 的 prepare/complete 两段
    /// 式）：本机回应以 account.id 为 actor、无显示名——与收件侧多设备 echo
    /// 同口径（contract 1.27），pill 由聚合投影自然带出 mine=true。返回的会话
    /// 摘要供调用方推 conversation.changed，app 即时刷新，不等轮询。
    #[allow(clippy::too_many_arguments)]
    pub fn complete_send_reaction(
        &self,
        account_id: &str,
        conversation_id: &str,
        emoji: &str,
        remove: bool,
        target_sent_at: u64,
    ) -> Result<Option<ConversationSummary>, ServiceError> {
        let account = self
            .store
            .account_by_id(account_id)?
            .ok_or(StoreError::MessageNotFound)?;
        self.store.upsert_reaction_event(
            account_id,
            conversation_id,
            emoji,
            target_sent_at,
            &account.id,
            remove,
            None,
        )?;
        Ok(self
            .store
            .conversation_summary(account_id, conversation_id)?)
    }

    /// The §4.6 reaction addressing model, reused verbatim by the pin family
    /// (contract 1.33): the upstream `targetAuthor` follows the row
    /// direction — the linked account's own number for addressable outgoing
    /// rows, the conversation peer for incoming direct rows. A group incoming
    /// row persists only a local sender hash and a system row has no author
    /// at all, so neither resolves and the request fails with the
    /// deterministic INVALID_REQUEST instead of mis-addressing upstream.
    fn resolve_pin_target(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<(AccountRow, ConversationRow, MessageRecord, String), ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        let (account, conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        let target_author = match message.direction {
            // We authored it: the author is the linked account itself. Only
            // an addressable send carries the upstream timestamp the pin
            // family matches on; pending/failed/unknown rows would
            // mis-target.
            "outgoing" if outgoing_status_is_addressable(message.status) => {
                account.signal_account.clone()
            }
            // An outgoing row without an addressable state carries no
            // upstream protocol identity (its sent_at is a local clock value)
            // and is indistinguishable from a missing row (remoteDelete
            // precedent).
            "outgoing" => return Err(StoreError::MessageNotFound.into()),
            // In a direct chat the only other possible author is the peer.
            // Incoming rows store the envelope timestamp, which is the
            // protocol identity the pin family references.
            "incoming" if conversation.kind == "direct" => conversation.peer_key.clone(),
            // Group messages do not persist the member address, and system
            // rows have no author at all.
            _ => {
                return Err(ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "pin target author is not resolvable",
                    false,
                )));
            }
        };
        Ok((account, conversation, message, target_author))
    }

    /// messages.sendPinMessage (contract 1.33): resolve the target with the
    /// reaction addressing model and build the exact upstream jsonRpc
    /// params. Groups map to the engine `groupId` form, direct chats to the
    /// `recipient` array; `pinDurationSeconds` is passed through when the
    /// caller supplied one (absent = the official forever pin — the key is
    /// not sent).
    pub fn prepare_send_pin_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        pin_duration_seconds: Option<u32>,
    ) -> Result<PreparedPin, ServiceError> {
        let (account, conversation, message, target_author) =
            self.resolve_pin_target(account_id, conversation_id, message_id)?;
        let mut params = json!({
            "account": account.signal_account,
            "targetAuthor": target_author,
            "targetTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        if let Some(seconds) = pin_duration_seconds {
            params["pinDurationSeconds"] = json!(seconds);
        }
        Ok(PreparedPin {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            params,
            target_author,
            target_sent_at: message.sent_at,
            pin_duration_seconds,
        })
    }

    /// messages.sendUnpinMessage (contract 1.33): same addressing as the
    /// pin, no duration.
    pub fn prepare_send_unpin_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<PreparedPin, ServiceError> {
        let (account, conversation, message, target_author) =
            self.resolve_pin_target(account_id, conversation_id, message_id)?;
        let mut params = json!({
            "account": account.signal_account,
            "targetAuthor": target_author,
            "targetTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(PreparedPin {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            params,
            target_author,
            target_sent_at: message.sent_at,
            pin_duration_seconds: None,
        })
    }

    /// messages.sendAdminDelete (contract 1.33): group conversations only —
    /// direct chats have no admin concept and answer INVALID_REQUEST before
    /// any upstream call. Admin eligibility against group state is not
    /// pre-checked: the server is the authority and rejects non-admin
    /// attempts, which surface as the upstream rejection (official behavior:
    /// send and show the failure).
    pub fn prepare_send_admin_delete(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<PreparedPin, ServiceError> {
        let (account, conversation, message, target_author) =
            self.resolve_pin_target(account_id, conversation_id, message_id)?;
        if conversation.kind != "group" {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "admin delete applies to group conversations only",
                false,
            )));
        }
        let mut params = json!({
            "account": account.signal_account,
            "targetAuthor": target_author,
            "targetTimestamp": message.sent_at,
        });
        set_upstream_target(&mut params, &conversation);
        Ok(PreparedPin {
            account_id: account_id.to_string(),
            conversation_id: conversation_id.to_string(),
            message_id: message_id.to_string(),
            params,
            target_author,
            target_sent_at: message.sent_at,
            pin_duration_seconds: None,
        })
    }

    /// sendPinMessage 上行确认后的本地落库（reaction 先例的 complete 段）：
    /// 本机置顶以发送方口径写入每会话 pinned 状态（新 pin 替换旧 pin，
    /// 过期时间用 connector 时钟计算），并返回会话摘要供调用方推
    /// conversation.changed——重载窗口由此免重放即可渲染置顶条。
    pub fn complete_send_pin_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_author: &str,
        target_sent_at: u64,
        pin_duration_seconds: Option<u32>,
    ) -> Result<Option<ConversationSummary>, ServiceError> {
        self.store.upsert_conversation_pin(
            account_id,
            conversation_id,
            target_author,
            target_sent_at,
            pin_duration_seconds,
        )?;
        Ok(self
            .store
            .conversation_summary(account_id, conversation_id)?)
    }

    /// sendUnpinMessage 上行确认后的本地落库：仅当本地 pinned 状态仍指向
    /// 被解 pin 的 (author, timestamp) 时清除；不匹配的 unpin 不动更新的
    /// pin。清除发生时返回会话摘要供调用方推 conversation.changed。
    pub fn complete_send_unpin_message(
        &self,
        account_id: &str,
        conversation_id: &str,
        target_author: &str,
        target_sent_at: u64,
    ) -> Result<Option<ConversationSummary>, ServiceError> {
        if !self.store.clear_conversation_pin(
            account_id,
            conversation_id,
            target_author,
            target_sent_at,
        )? {
            return Ok(None);
        }
        Ok(self
            .store
            .conversation_summary(account_id, conversation_id)?)
    }

    /// messages.markRead (contract revision 1.34, §4.29): select the
    /// receipt-eligible incoming rows of one conversation and group their
    /// upstream timestamps by author — one engine `sendReadReceipt` per
    /// author, the official per-author fan-out (the engine chunks 100 per
    /// envelope). No local row changes — the desktop owns its read state —
    /// so there is no complete step; an empty group list is the trivial
    /// no-op the supervisor answers `{"status":"sent"}` for.
    pub fn prepare_mark_read(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_ids: Option<Vec<String>>,
    ) -> Result<PreparedReceipts, ServiceError> {
        self.prepare_receipt(account_id, conversation_id, message_ids)
    }

    /// messages.markViewed: identical addressing, selection, and grouping,
    /// upstream `sendViewedReceipt`.
    pub fn prepare_mark_viewed(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_ids: Option<Vec<String>>,
    ) -> Result<PreparedReceipts, ServiceError> {
        self.prepare_receipt(account_id, conversation_id, message_ids)
    }

    /// messages.sendViewOnceOpen (contract revision 1.38, §4.36): bounds and
    /// account resolution only — the bare engine `sendViewOnceOpen` face is a
    /// passthrough with no local state. The upstream key is the resolved
    /// `signal_account`, never the host's opaque id.
    pub fn prepare_send_view_once_open(
        &self,
        params: &MessagesSendViewOnceOpenParams,
    ) -> Result<Value, ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        validate_opaque_id(&params.sender_aci, "senderAci")?;
        let account = self.resolve_account(&params.account_id)?;
        Ok(json!({
            "account": account.signal_account,
            "senderAci": params.sender_aci,
            "timestamp": params.timestamp,
        }))
    }

    /// messages.markViewOnceOpened (contract revision 1.38, §4.37): the
    /// desktop's single view-once trigger point. Resolves the target through
    /// the shared ladder, refuses non-view-once rows deterministically, burns
    /// the row bytes and media on the first transition (replays are a trivial
    /// no-op), and returns the event list plus the best-effort upstream
    /// fan-out plan — the supervisor sends the events and runs the official
    /// VIEWED-then-ViewOnceOpen pair under receipt-class discipline, so an
    /// upstream failure degrades `unknown` without touching the burn.
    pub fn prepare_mark_view_once_opened(
        &self,
        params: &MessagesMarkViewOnceOpenedParams,
    ) -> Result<PreparedMarkViewOnceOpened, ServiceError> {
        validate_opaque_id(&params.account_id, "accountId")?;
        validate_opaque_id(&params.conversation_id, "conversationId")?;
        validate_opaque_id(&params.message_id, "messageId")?;
        let (account, conversation, record) = self.resolve_target(
            &params.account_id,
            &params.conversation_id,
            &params.message_id,
        )?;
        let is_view_once = record
            .rich
            .as_ref()
            .is_some_and(|rich| rich.view_once || rich.view_once_invalid);
        if !is_view_once {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "message is not a view-once message",
                false,
            )));
        }
        let opened_at = now_ms();
        let Some((_, changed)) = self.store.burn_view_once_message(
            &record.account_id,
            &record.conversation_id,
            &record.id,
            opened_at,
        )?
        else {
            // The row vanished between the resolve and the burn (local data
            // deletion raced the request): MESSAGE_NOT_FOUND is the honest
            // answer, the same one a fully absent row would give.
            return Err(StoreError::MessageNotFound.into());
        };
        let mut events = Vec::new();
        if !changed {
            // The replay: already burned — a trivial no-op with no event and
            // no upstream fan-out (the supervisor's receipt-class discipline
            // treats the skipped legs as complete and answers plain `sent`).
            return Ok(PreparedMarkViewOnceOpened {
                events,
                account_signal: account.signal_account,
                timestamp: record.sent_at,
                receipt_recipient: None,
                open_sender_aci: None,
            });
        }
        self.burn_view_once_media(&record);
        events.push(HostSideEvent::MessageViewOnceOpened {
            account_id: record.account_id.clone(),
            conversation_id: record.conversation_id.clone(),
            message_id: record.id.clone(),
            opened_at,
        });
        // The upstream fan-out plan. The receipt leg addresses the author of
        // an incoming row — a direct conversation's incoming rows are all the
        // peer's (the §4.29 model); a group row's author is unresolvable
        // (§4.28) and both legs skip, while the burn still ran. An outgoing
        // row has no author to receipt; its open sync names the account
        // itself (the engine resolves the registered number to the own ACI).
        let (receipt_recipient, open_sender_aci) = if record.direction == "outgoing" {
            (None, Some(account.signal_account.clone()))
        } else if conversation.kind == "direct" {
            (
                Some(conversation.peer_key.clone()),
                Some(conversation.peer_key.clone()),
            )
        } else {
            (None, None)
        };
        Ok(PreparedMarkViewOnceOpened {
            events,
            account_signal: account.signal_account,
            timestamp: record.sent_at,
            receipt_recipient,
            open_sender_aci,
        })
    }

    /// Erase one view-once row's recoverable media channels (§4.37): the
    /// attachment files and previews under the connector-owned directories,
    /// then any open chunk-stream handle for the message. Best-effort — a
    /// vanished file is success, and `--media-ingest` being off means there
    /// is nothing on disk to erase.
    fn burn_view_once_media(&self, record: &MessageRecord) {
        let Some(media) = self.media.as_ref() else {
            return;
        };
        for descriptor in &record.attachments {
            if let Ok(sanitized_id) = sanitize_attachment_id(&descriptor.id) {
                media.governor().delete_attachment(&sanitized_id);
            }
        }
        media
            .handles()
            .close_for_message(&record.account_id, &record.id);
    }

    /// Shared mark-read/mark-viewed body. Validation runs in the 1.33
    /// `resolve_pin_target` order — ids, account, conversation, then the
    /// bounded selection — so a bogus address answers its deterministic
    /// NOT_FOUND before any store scan. Author resolution follows the
    /// reaction addressing model read-side: a direct conversation's incoming
    /// rows are all the peer's; group rows persist only a local sender hash
    /// (§4.28), so their author is unknown and they are skipped from the
    /// fan-out — skipped, not an error (spec: rows whose author cannot be
    /// resolved are skipped).
    fn prepare_receipt(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_ids: Option<Vec<String>>,
    ) -> Result<PreparedReceipts, ServiceError> {
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        let account = self.resolve_account(account_id)?;
        let conversation = self.resolve_conversation(account_id, conversation_id)?;
        let rows = match message_ids {
            Some(ids) => {
                for id in ids.iter().take(MARK_RECEIPT_MESSAGE_IDS_LIMIT) {
                    validate_opaque_id(id, "messageId")?;
                }
                // Explicit lists beyond the cap are truncated, not rejected:
                // a receipt marks what it can, and the bound keeps one
                // request's fan-out bounded (the schema guards the same 512
                // earlier).
                let bounded: Vec<String> = ids
                    .into_iter()
                    .take(MARK_RECEIPT_MESSAGE_IDS_LIMIT)
                    .collect();
                self.store
                    .incoming_rows_by_ids(account_id, conversation_id, &bounded)?
            }
            None => self.store.incoming_rows_for_receipts(
                account_id,
                conversation_id,
                MARK_RECEIPT_MESSAGE_IDS_LIMIT as u32,
            )?,
        };
        let groups = if conversation.kind == "direct" {
            // Rows arrive in receipt order (`sent_at ASC, id ASC`) from both
            // store paths, so per-author timestamps stay ascending; same-
            // timestamp rows collapse (duplicates are protocol-idempotent,
            // but one entry per upstream timestamp is what the official
            // desktop sends).
            let mut timestamps = rows
                .into_iter()
                .map(|row| row.sent_at)
                .collect::<Vec<u64>>();
            timestamps.dedup();
            if timestamps.is_empty() {
                Vec::new()
            } else {
                vec![ReceiptGroup {
                    recipient: conversation.peer_key.clone(),
                    timestamps,
                }]
            }
        } else {
            Vec::new()
        };
        Ok(PreparedReceipts {
            account_id: account.id,
            conversation_id: conversation.id,
            account_signal: account.signal_account,
            groups,
        })
    }

    /// messages.attachments.open (ADR 0002, contract revision 1.17): resolve
    /// the addressed rows, then locate the already-downloaded file in the
    /// engine's data directory and mint a short-lived chunk-stream handle for
    /// it. The addressing resolves before any file is touched, the id passes
    /// the sanitizeId-parity gate before any path join, and a missing file
    /// answers UPSTREAM_ERROR — not-downloaded and governor-deleted are
    /// deliberately indistinguishable (ADR 0002).
    pub fn open_media_handle(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        attachment_id: &str,
    ) -> Result<MediaOpenView, ServiceError> {
        let Some(media) = self.media.as_ref() else {
            return Err(media_capability_error());
        };
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        if attachment_id.is_empty() || attachment_id.len() > MAX_ATTACHMENT_ID_BYTES {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "attachmentId must contain between 1 and 128 bytes",
                false,
            )));
        }
        let sanitized_id = sanitize_attachment_id(attachment_id)?;
        let (_account, _conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        // §4.37: an opened view-once attachment answers exactly what a
        // not-downloaded one answers — the burn erased the bytes, and the
        // wire must not leak the state transition through a distinct error.
        if message
            .rich
            .as_ref()
            .is_some_and(|rich| rich.view_once_opened_at.is_some())
        {
            return Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "attachment is not available in the signal-cli data directory",
                true,
            )));
        }
        let Some((path, size_bytes)) = media.locate_attachment(&sanitized_id) else {
            return Err(ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "attachment is not available in the signal-cli data directory",
                true,
            )));
        };
        let media_handle = media
            .handles
            .open(
                account_id,
                conversation_id,
                message_id,
                attachment_id,
                path,
                size_bytes,
            )
            .map_err(map_media_handle_error)?;
        Ok(MediaOpenView {
            media_handle,
            size_bytes,
            chunk_bytes: MEDIA_CHUNK_BYTES,
        })
    }

    /// history.attachmentDownload (contract revision 1.57, §4.38 step 3) —
    /// the local half of the download chain, run synchronously inside the
    /// service lock: gate on the media-ingest capability, resolve the
    /// message-addressing triple, find the attachment descriptor and its
    /// stored pointer face, and stage the destination. The network and
    /// crypto halves run in the supervisor (they stream for a while); this
    /// layer answers every deterministic caller mistake before any of that.
    pub fn prepare_history_attachment_download(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        attachment_id: &str,
    ) -> Result<PreparedHistoryDownload, ServiceError> {
        let Some(media) = self.media.as_ref() else {
            return Err(media_capability_error());
        };
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        if attachment_id.is_empty() || attachment_id.len() > MAX_ATTACHMENT_ID_BYTES {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "attachmentId must contain between 1 and 128 bytes",
                false,
            )));
        }
        let sanitized_id = sanitize_attachment_id(attachment_id)?;
        let (_account, _conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        let index = message
            .attachments
            .iter()
            .position(|descriptor| descriptor.id == attachment_id)
            .ok_or_else(|| {
                ServiceError::Api(ApiError::new(
                    "INVALID_REQUEST",
                    "attachmentId does not name an attachment of this message",
                    false,
                ))
            })?;
        let descriptor_size = message.attachments[index].size;
        let locators = self
            .store
            .history_attachment_locators(account_id, &message.id)?;
        let locator = locators.get(index).cloned().unwrap_or_default();
        if locator.key.is_none()
            && locator.cdn_key.is_none()
            && locator.cdn_number.is_none()
            && locator.digest.is_none()
            && locator.plaintext_hash.is_none()
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "attachment has no downloadable pointer face",
                false,
            )));
        }
        let write_dir = media.write_dir()?;
        let target = write_dir.join(&sanitized_id);
        if let Ok(metadata) = std::fs::metadata(&target)
            && metadata.is_file()
        {
            // Idempotent dedup: the bytes already landed (an earlier call, or
            // the engine downloaded the same attachment live). Answering the
            // measured size keeps the success honest without re-fetching.
            let size_bytes = metadata.len();
            return Ok(PreparedHistoryDownload::AlreadyPresent { size_bytes });
        }
        let temp_path = write_dir.join(format!(".{}.{}.tmp", sanitized_id, random_id()));
        let plaintext_path = write_dir.join(format!(".{}.{}.dec", sanitized_id, random_id()));
        Ok(PreparedHistoryDownload::Ready {
            locator,
            descriptor_size,
            target,
            temp_path,
            plaintext_path,
        })
    }

    /// attachments.download (contract revision 1.61, §4.54) — the local half
    /// of the live attachment download chain, the
    /// `prepare_history_attachment_download` shape addressed by
    /// `attachmentIndex` instead of `attachmentId`. Same capability gate,
    /// id ladder, and idempotent-dedup staging; the pointer face resolves
    /// through the live/import locator ladder (see
    /// [`Self::attachment_locator_for_index`]).
    pub fn prepare_attachment_download(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        attachment_index: usize,
    ) -> Result<PreparedHistoryDownload, ServiceError> {
        let Some(media) = self.media.as_ref() else {
            return Err(media_capability_error());
        };
        validate_opaque_id(account_id, "accountId")?;
        validate_opaque_id(conversation_id, "conversationId")?;
        validate_opaque_id(message_id, "messageId")?;
        if attachment_index >= crate::engine::MAX_INBOUND_ATTACHMENTS {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "attachmentIndex must name an attachment of this message",
                false,
            )));
        }
        let (_account, _conversation, message) =
            self.resolve_target(account_id, conversation_id, message_id)?;
        let descriptor = message.attachments.get(attachment_index).ok_or_else(|| {
            ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "attachmentIndex does not name an attachment of this message",
                false,
            ))
        })?;
        let descriptor_size = descriptor.size;
        let attachment_id = descriptor.id.clone();
        let locator = self.attachment_locator_for_index(account_id, &message, attachment_index)?;
        let sanitized_id = sanitize_attachment_id(&attachment_id)?;
        let write_dir = media.write_dir()?;
        let target = write_dir.join(&sanitized_id);
        if let Ok(metadata) = std::fs::metadata(&target)
            && metadata.is_file()
        {
            // Idempotent dedup, same posture as the history face: the bytes
            // already landed (an earlier call, or the engine's own live
            // download of the same id). The measured size is the honest
            // answer; the CDN is never re-asked.
            let size_bytes = metadata.len();
            return Ok(PreparedHistoryDownload::AlreadyPresent { size_bytes });
        }
        let temp_path = write_dir.join(format!(".{}.{}.tmp", sanitized_id, random_id()));
        let plaintext_path = write_dir.join(format!(".{}.{}.dec", sanitized_id, random_id()));
        Ok(PreparedHistoryDownload::Ready {
            locator,
            descriptor_size,
            target,
            temp_path,
            plaintext_path,
        })
    }

    /// The pointer face behind one descriptor index (contract revision 1.61,
    /// §4.54): the live-receive column first (schema 19 — an engine that
    /// projects receive locators fills it), then the schema-18 import column
    /// (the index-aligned archive faces, already persisted since 1.56). Both
    /// empty / hole answers the honest no-pointer INVALID_REQUEST — a
    /// key-less import face stays a hole, never a shifted row (the 1.56
    /// alignment ruling).
    fn attachment_locator_for_index(
        &self,
        account_id: &str,
        message: &crate::store::MessageRecord,
        attachment_index: usize,
    ) -> Result<crate::store::HistoryAttachmentLocator, ServiceError> {
        let default = crate::store::HistoryAttachmentLocator::default();
        let live = self
            .store
            .live_attachment_locators(account_id, &message.id)?
            .get(attachment_index)
            .cloned()
            .unwrap_or(default.clone());
        let face = if live != default {
            live
        } else {
            self.store
                .history_attachment_locators(account_id, &message.id)?
                .get(attachment_index)
                .cloned()
                .unwrap_or(default)
        };
        if face.key.is_none()
            && face.cdn_key.is_none()
            && face.cdn_number.is_none()
            && face.digest.is_none()
            && face.plaintext_hash.is_none()
        {
            return Err(ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                "attachment has no downloadable pointer face",
                false,
            )));
        }
        Ok(face)
    }

    /// messages.attachments.readChunk (ADR 0002): deliver exactly one bounded
    /// chunk of one live handle at a strictly sequential offset. The handle
    /// table validates origin, TTL, and offset; this layer base64-encodes the
    /// raw chunk (≤ 4 * ceil(262_144 / 3) = 349_528 characters, far inside
    /// the 160 MiB host frame budget) and reports this chunk's byte count
    /// plus the EOF flag that terminates the caller's loop.
    pub fn read_media_chunk(
        &self,
        media_handle: &str,
        offset: u64,
    ) -> Result<MediaChunkView, ServiceError> {
        let Some(media) = self.media.as_ref() else {
            return Err(media_capability_error());
        };
        validate_opaque_id(media_handle, "mediaHandle")?;
        let chunk = media
            .handles
            .read_chunk(media_handle, offset)
            .map_err(map_media_handle_error)?;
        Ok(MediaChunkView {
            data_base64: base64_encode(&chunk.data),
            size_bytes: chunk.data.len() as u64,
            eof: chunk.eof,
        })
    }

    /// messages.attachments.closeHandle (ADR 0002): explicit early release.
    /// Idempotent — a handle the TTL already reaped still closes cleanly
    /// (`released: false`) instead of turning the backstop into a race the
    /// caller must survive.
    pub fn close_media_handle(&self, media_handle: &str) -> Result<MediaCloseView, ServiceError> {
        let Some(media) = self.media.as_ref() else {
            return Err(media_capability_error());
        };
        validate_opaque_id(media_handle, "mediaHandle")?;
        Ok(MediaCloseView {
            released: media.handles.close(media_handle),
        })
    }

    /// ADR 0002 gate 3: handles cannot outlive their account session. The
    /// delete-local-data path calls this once the account rows are gone; the
    /// 300 s TTL backstops every other kind of abandonment.
    pub fn clear_media_handles_for_account(&self, account_id: &str) {
        if let Some(media) = self.media.as_ref() {
            media.handles.clear_account(account_id);
        }
    }

    pub fn complete_send_success(
        &self,
        pending_id: &str,
        account_id: &str,
        conversation_id: &str,
        sent_at: u64,
    ) -> Result<(MessageRecord, Vec<HostSideEvent>), ServiceError> {
        // A missing pending row at this point means the local state vanished
        // underneath a send that already completed upstream (e.g. the account's
        // rows were deleted in a race): the message may well be delivered, so
        // the outcome is final and must never be auto-retried. The schema has
        // no dedicated code for this; the closest existing one is
        // SEND_OUTCOME_UNKNOWN with retryable=false.
        let mut updated = self
            .store
            .complete_outgoing_send(pending_id, account_id, conversation_id, sent_at)?
            .ok_or_else(|| {
                ServiceError::Api(ApiError::new(
                    "SEND_OUTCOME_UNKNOWN",
                    "send completed upstream but the local pending record is gone",
                    false,
                ))
            })?;
        self.store
            .attach_reactions(account_id, std::slice::from_mut(&mut updated.0))?;
        let (updated, status_transitioned) = updated;
        let mut events = vec![HostSideEvent::MessageUpserted(project_message_for_host(
            updated.clone(),
        ))];
        if status_transitioned {
            events.push(status_changed_event(&updated));
        }
        if let Some(conversation) = self
            .store
            .conversation_summary(account_id, conversation_id)?
        {
            events.push(HostSideEvent::ConversationChanged(conversation));
        }
        if let Some(account) = self.store.account_summary(account_id)? {
            events.push(HostSideEvent::AccountChanged(account));
        }
        Ok((updated, events))
    }

    pub fn complete_send_unknown(
        &self,
        pending_id: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        Ok(status_change_events(
            self.store
                .update_message_status(pending_id, "unknown", None)?,
        ))
    }

    /// Contract 1.21: the engine classified an upstream authorization failure
    /// for `signal_account` — the account holder unlinked this device. Persist
    /// the dead state and surface it as an `account.changed` event so the
    /// host can auto-release stale session bindings. Unknown numbers (row
    /// already deleted) are a no-op.
    pub fn mark_account_device_unlinked(
        &self,
        signal_account: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        let Some(account) = self.store.mark_account_device_unlinked(signal_account)? else {
            return Ok(Vec::new());
        };
        Ok(vec![HostSideEvent::AccountChanged(account)])
    }

    pub fn complete_send_failed(
        &self,
        pending_id: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        Ok(status_change_events(
            self.store
                .update_message_status(pending_id, "failed", None)?,
        ))
    }

    /// Persist one normalized receive from ONE group's engine. The owning
    /// group scopes the single-account fallback: signal-cli may omit
    /// `account`, and with several engines the fallback must never cross
    /// group boundaries.
    pub fn ingest_receive(
        &mut self,
        receive: NormalizedReceive,
        owner_group: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        if receive.direction == "skip" {
            return Ok(Vec::new());
        }
        // Any control-carrying receive routes to the control plane — not just
        // direction=="control": a peer's envelope-level editMessage arrives as
        // direction "incoming" with control Edit and must edit the original
        // row, never insert a second message.
        if receive.control.is_some() {
            return self.ingest_control_receive(receive, owner_group);
        }
        if !matches!(receive.direction, "incoming" | "outgoing" | "system") {
            return Ok(Vec::new());
        }
        // Signal's timestamp is part of the protocol identity. Inventing one locally would
        // turn a replay into a second message after a restart or receive retry.
        let Some(sent_at) = receive.timestamp else {
            return Ok(Vec::new());
        };
        // signal-cli may omit `account` on single-account jsonRpc; fall back to the
        // sole account bound to the receiving engine's own group.
        let signal_account = match receive.account.as_deref().filter(|s| !s.is_empty()) {
            Some(account) => account.to_string(),
            None => {
                let accounts = self.store.list_accounts_in_group(owner_group)?;
                if accounts.len() != 1 {
                    return Ok(Vec::new());
                }
                match self.store.account_by_id(&accounts[0].id)? {
                    Some(row) => row.signal_account,
                    None => return Ok(Vec::new()),
                }
            }
        };
        let account = self
            .store
            .upsert_account_from_signal(&signal_account, None, owner_group)?;
        let (kind, peer_key, title) = if let Some(group_id) = receive.group_id.as_deref() {
            ("group", group_id.to_string(), "group".to_string())
        } else if let Some(peer) = receive.source.as_deref().filter(|s| !s.is_empty()) {
            // Prefer profile/contact label over masked peer id (e.g. 8fc***e2).
            let label = receive
                .peer_name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| mask_address(peer));
            ("direct", peer.to_string(), label)
        } else {
            return Ok(Vec::new());
        };
        let conversation = self
            .store
            .ensure_conversation(&account.id, kind, &peer_key, &title)?;
        let direction = receive.direction;
        let legacy_sender_id = stable_hash_id(&[&account.id, kind, &peer_key]);
        let sender_id = if direction == "outgoing" {
            account.id.clone()
        } else {
            stable_hash_id(&[
                &account.id,
                kind,
                receive.source.as_deref().unwrap_or(&peer_key),
            ])
        };
        if self
            .store
            .message_by_signal_identity(
                &account.id,
                &conversation.id,
                direction,
                sent_at,
                &sender_id,
                if direction == "outgoing" {
                    "self"
                } else {
                    &legacy_sender_id
                },
            )?
            .is_some()
        {
            return Ok(Vec::new());
        }
        let message_id = stable_hash_id(&[
            "signal-message-v2",
            &account.id,
            &conversation.id,
            direction,
            &sent_at.to_string(),
            &sender_id,
        ]);
        let text_bytes = receive.text_bytes.or_else(|| {
            receive
                .text
                .as_ref()
                .map(|text| text.len().min(u32::MAX as usize) as u32)
        });
        let text_complete = !receive.text_truncated
            && text_bytes.is_none_or(|bytes| bytes as usize <= MAX_INBOUND_TEXT_BYTES);
        let stored_text = receive.text.map(|text| {
            if text_complete {
                text
            } else {
                truncate_utf8_bytes(&text, MAX_HOST_TEXT_PREVIEW_BYTES)
            }
        });
        let status = match direction {
            "outgoing" => "sent",
            "system" => "system",
            _ => "delivered",
        };
        // Contract 1.29: a row @mentions the linked account when one of its
        // normalized mention authors resolves to the account's own number.
        // The match is number-based because the pinned upstream jsonRpc
        // surface exposes the account only by number (`listAccounts` returns
        // `{number}`; the account UUID is not queryable), so a mention author
        // carrying only a UUID cannot be attributed to self — recorded as the
        // revision's known boundary. Mention authors prefer the resolved
        // number (`number` first, `uuid` fallback), so the common group case
        // matches. Computed before the record consumes the rich payload.
        let mentions_self = direction == "incoming"
            && receive
                .rich
                .as_ref()
                .is_some_and(|rich| rich.mentions.iter().any(|m| m.author == signal_account));
        // Contract 1.42 (§4.41): a timer-notice system row mirrors the
        // conversation's new timer state before the row lands, so the summary
        // refetched below for conversation.changed already carries it. The
        // close form (no stated value) reads as off; the notice row itself
        // never sweeps (its record carries no expire columns).
        let is_timer_change = receive
            .rich
            .as_ref()
            .is_some_and(|rich| rich.is_expiration_update);
        if is_timer_change {
            self.store.set_conversation_expire_timer(
                &account.id,
                &conversation.id,
                receive.expire_seconds.unwrap_or(0),
            )?;
        }
        // Contract 1.45 (§4.44): a group-update row mirrors the engine's
        // post-change title into the conversation row before the summary for
        // conversation.changed is refetched. The engine group state is the
        // upstream truth, so the raw setter bypasses the placeholder-only
        // title upgrade rule; every UPDATE row with title material converges
        // the mirror (a body-carrying update keeps its message row and still
        // mirrors), and direct conversations carry no group state.
        if kind == "group"
            && let Some(group_name) = receive
                .rich
                .as_ref()
                .filter(|rich| rich.is_group_update)
                .and_then(|rich| rich.group_name.as_deref())
        {
            self.store
                .set_conversation_title(&conversation.id, group_name)?;
        }
        let message = MessageRecord {
            id: message_id,
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            direction,
            sender_id,
            // Contract 1.27: the incoming author's display name, captured from
            // the envelope `sourceName` (already bounded by the engine).
            // Outgoing rows are the account itself; system rows have no author.
            sender_name: if direction == "incoming" {
                receive.peer_name.clone()
            } else {
                None
            },
            mentions_self,
            sent_at,
            received_at: Some(now_ms()),
            text: stored_text,
            text_bytes,
            text_truncated: !text_complete,
            text_retrievable: text_complete,
            status,
            client_request_id: None,
            quote_message_id: None,
            quote_snapshot: receive.quote,
            attachments: receive.attachments,
            rich: receive.rich,
            edited_at: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            admin_deleted: false,
            sticker: receive.sticker,
            // A timer notice records the announced value in the text and the
            // conversation state only (§4.41); regular rows carry the
            // envelope-stated timer and start verbatim.
            expire_seconds: if is_timer_change {
                None
            } else {
                receive.expire_seconds
            },
            expire_start_at: if is_timer_change {
                None
            } else {
                receive.expire_start_at
            },
            // The reportSpam companion (contract revision 1.49, §4.48): the
            // envelope's server-assigned GUID rides the row; the report face
            // resolves it at call time. Outgoing mirrors/system rows carry the
            // envelope key too when the engine projected one (the sync shape
            // shares the public metadata), honest verbatim.
            server_guid: receive.server_guid,
        };
        let preview = message
            .text
            .as_deref()
            .map(|text| text.chars().take(120).collect::<String>());
        let increment_unread = direction == "incoming";
        let inserted =
            self.store
                .insert_message(&message, None, preview.as_deref(), increment_unread)?;
        if !inserted {
            return Ok(Vec::new());
        }
        let mut events = vec![HostSideEvent::MessageUpserted(project_message_for_host(
            message,
        ))];
        // §4.37: record the ACI→peer-key identity the envelope revealed, so a
        // later view-once open sync (which names the author by ACI only) can
        // find this conversation. One bounded row per (account, ACI); only
        // incoming envelopes carry someone else's identity, and only when the
        // resolved peer key differs from the raw ACI.
        if direction == "incoming"
            && let Some(aci) = receive.source_uuid.as_deref()
            && aci != peer_key
        {
            self.store
                .upsert_peer_identity(&account.id, aci, &peer_key)?;
        }
        if let Some(conversation) = self
            .store
            .conversation_summary(&account.id, &conversation.id)?
        {
            events.push(HostSideEvent::ConversationChanged(conversation));
        }
        if let Some(account) = self.store.account_summary(&account.id)? {
            events.push(HostSideEvent::AccountChanged(account));
        }
        Ok(events)
    }

    /// Persist-and-notify one control-plane receive (contract 1.15): reaction
    /// upserts, remote-delete marks, edits, and rate-limited typing. Control
    /// receives never create a conversation, never create a message row, and
    /// are dropped when they address nothing local.
    fn ingest_control_receive(
        &mut self,
        receive: NormalizedReceive,
        owner_group: &str,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        let Some(control) = receive.control else {
            return Ok(Vec::new());
        };
        // Route to the account the same way messages do.
        let signal_account = match receive.account.as_deref().filter(|s| !s.is_empty()) {
            Some(account) => account.to_string(),
            None => {
                let accounts = self.store.list_accounts_in_group(owner_group)?;
                if accounts.len() != 1 {
                    return Ok(Vec::new());
                }
                match self.store.account_by_id(&accounts[0].id)? {
                    Some(row) => row.signal_account,
                    None => return Ok(Vec::new()),
                }
            }
        };
        let account = match self.store.account_by_signal(&signal_account).ok().flatten() {
            Some(account) => account,
            None => return Ok(Vec::new()),
        };
        // §4.37: the view-once open sync names its target by (author ACI,
        // timestamp) across conversations — the envelope's own source is our
        // account, so the generic conversation routing below would misroute
        // it into a self-chat. Intercepted after account resolution, before
        // the (kind, peer_key) ladder.
        if let ControlReceive::ViewOnceOpen {
            sender_aci,
            timestamp,
        } = &control
        {
            return self.apply_view_once_open_sync(&account, sender_aci, *timestamp);
        }
        // Resolve the conversation: group id, or peer (incoming source /
        // outgoing destination). Control receives address existing
        // conversations only — they never create one.
        let (kind, peer_key) = if let Some(group_id) = receive.group_id.as_deref() {
            ("group", group_id)
        } else if let Some(peer) = receive.source.as_deref().filter(|s| !s.is_empty()) {
            ("direct", peer)
        } else {
            // The groupUpdated projection always names its group; a timer
            // update with no group reference has nothing to converge and is
            // warn-only dropped like the unknown-group drop below.
            if matches!(control, ControlReceive::GroupTimerUpdate { .. }) {
                tracing::warn!(
                    account_id = %account.id,
                    "groupUpdated carries no group reference; dropping the timer convergence"
                );
            }
            return Ok(Vec::new());
        };
        let Ok(conversation) = self.store.conversation_by_peer(&account.id, kind, peer_key) else {
            return Ok(Vec::new());
        };
        let Some(conversation) = conversation else {
            // The engine's groupUpdated projection addresses a group master
            // key (2026-10-06 engine batch): a group nothing local resolves is
            // warn-only dropped — better to converge late than to invent a
            // row (宁缺勿错). Every other control family keeps its silent
            // skip discipline.
            if matches!(control, ControlReceive::GroupTimerUpdate { .. }) {
                tracing::warn!(
                    account_id = %account.id,
                    "groupUpdated names an unknown group; dropping the timer convergence"
                );
            }
            return Ok(Vec::new());
        };
        match control {
            ControlReceive::Reaction {
                emoji,
                target_author,
                target_timestamp,
                remove,
            } => {
                // The actor is the envelope sender; the protocol's
                // targetAuthor cross-check is advisory (a group member
                // reacting to another member's message still keys on the
                // reacting actor). The display name rides the same envelope
                // (contract 1.27); own multi-device echoes are the account
                // itself and carry no name.
                let _ = target_author;
                let actor_id = match receive.direction {
                    "outgoing" => account.id.clone(),
                    _ => stable_hash_id(&[
                        &account.id,
                        kind,
                        receive.source.as_deref().unwrap_or(peer_key),
                    ]),
                };
                let actor_name = if receive.direction == "outgoing" {
                    None
                } else {
                    receive.peer_name.as_deref()
                };
                self.store.upsert_reaction_event(
                    &account.id,
                    &conversation.id,
                    &emoji,
                    target_timestamp,
                    &actor_id,
                    remove,
                    actor_name,
                )?;
                let mut events = Vec::new();
                if let Some(summary) = self
                    .store
                    .conversation_summary(&account.id, &conversation.id)?
                {
                    events.push(HostSideEvent::ConversationChanged(summary));
                }
                Ok(events)
            }
            ControlReceive::RemoteDelete { target_timestamp } => {
                match self.store.mark_remote_deleted(
                    &account.id,
                    &conversation.id,
                    target_timestamp,
                )? {
                    Some((record, true)) => Ok(vec![HostSideEvent::MessageStatusChanged {
                        account_id: record.account_id,
                        message_id: record.id,
                        status: record.status,
                        delivered_at: None,
                        read_at: None,
                    }]),
                    _ => Ok(Vec::new()),
                }
            }
            ControlReceive::Edit { target_timestamp } => {
                let Some(new_text) = receive.text else {
                    return Ok(Vec::new());
                };
                let new_bytes = new_text.len().min(u32::MAX as usize) as u32;
                match receive.direction {
                    // Peer edit: keyed on the upstream target timestamp.
                    "incoming" => {
                        let sender_id = stable_hash_id(&[
                            &account.id,
                            kind,
                            receive.source.as_deref().unwrap_or(peer_key),
                        ]);
                        match self.store.apply_inbound_edit(
                            &account.id,
                            &conversation.id,
                            target_timestamp,
                            &sender_id,
                            &legacy_sender_id(kind, peer_key, &account.id),
                            &new_text,
                            new_bytes,
                        )? {
                            Some(mut record) => {
                                self.store.attach_reactions(
                                    &account.id,
                                    std::slice::from_mut(&mut record),
                                )?;
                                self.store
                                    .attach_edits(&account.id, std::slice::from_mut(&mut record))?;
                                Ok(vec![HostSideEvent::MessageUpserted(record)])
                            }
                            None => Ok(Vec::new()),
                        }
                    }
                    // Our own multi-device edit of a phone-sent message: the
                    // sync mirror targets the row by its upstream timestamp
                    // through the same store path.
                    _ => {
                        match self.store.apply_outgoing_edit_by_signal(
                            &account.id,
                            &conversation.id,
                            target_timestamp,
                            &new_text,
                            new_bytes,
                        )? {
                            Some(mut record) => {
                                self.store.attach_reactions(
                                    &account.id,
                                    std::slice::from_mut(&mut record),
                                )?;
                                self.store
                                    .attach_edits(&account.id, std::slice::from_mut(&mut record))?;
                                Ok(vec![HostSideEvent::MessageUpserted(record)])
                            }
                            None => Ok(Vec::new()),
                        }
                    }
                }
            }
            ControlReceive::Typing { action } => {
                let key = (account.id.clone(), conversation.id.clone());
                let now = now_ms();
                if self.typing_last_emission.len() >= TYPING_RATE_MAP_CAPACITY
                    && !self.typing_last_emission.contains_key(&key)
                {
                    // Bounded map: drop the oldest entry (a linear scan of a
                    // 256-entry map is cheaper than tracking an order
                    // structure for a limiter that mostly sees fresh keys).
                    if let Some(oldest) = self
                        .typing_last_emission
                        .iter()
                        .min_by_key(|(_, at)| **at)
                        .map(|(key, _)| key.clone())
                    {
                        self.typing_last_emission.remove(&oldest);
                    }
                }
                let last = self.typing_last_emission.get(&key).copied();
                if last.is_some_and(|at| now.saturating_sub(at) < TYPING_RATE_LIMIT_MS) {
                    return Ok(Vec::new());
                }
                self.typing_last_emission.insert(key, now);
                Ok(vec![HostSideEvent::ConversationTyping {
                    account_id: account.id,
                    conversation_id: conversation.id,
                    action,
                }])
            }
            ControlReceive::Receipt {
                kind,
                timestamps,
                when,
            } => {
                // Contract 1.32: the receipt's arrival instant (envelope
                // timestamp; connector clock as fallback) stamps the rows the
                // receipt moves, and each status event carries the stamp it
                // just wrote.
                let when = when.unwrap_or_else(crate::link::now_ms);
                let moved = self.store.upgrade_outgoing_receipts(
                    &account.id,
                    &conversation.id,
                    &timestamps,
                    kind,
                    when,
                )?;
                Ok(moved
                    .into_iter()
                    .map(|record| HostSideEvent::MessageStatusChanged {
                        account_id: record.account_id,
                        message_id: record.id,
                        status: record.status,
                        delivered_at: record.delivered_at,
                        read_at: record.read_at,
                    })
                    .collect())
            }
            ControlReceive::PinMessage {
                target_author,
                target_timestamp,
                duration_seconds,
            } => {
                // Contract 1.33: the derived per-conversation pinned state —
                // a newer pin replaces the older one, the expiry is connector
                // clock computed, and the host refreshes the pin bar through
                // the summary that rides conversation.changed (the reaction
                // notification precedent).
                self.store.upsert_conversation_pin(
                    &account.id,
                    &conversation.id,
                    &target_author,
                    target_timestamp,
                    duration_seconds,
                )?;
                let mut events = Vec::new();
                if let Some(summary) = self
                    .store
                    .conversation_summary(&account.id, &conversation.id)?
                {
                    events.push(HostSideEvent::ConversationChanged(summary));
                }
                Ok(events)
            }
            ControlReceive::UnpinMessage {
                target_author,
                target_timestamp,
            } => {
                // Only the pinned (author, timestamp) the unpin names clears;
                // a stale or mismatched unpin leaves a newer pin alone and
                // stays silent (replays never re-notify).
                if !self.store.clear_conversation_pin(
                    &account.id,
                    &conversation.id,
                    &target_author,
                    target_timestamp,
                )? {
                    return Ok(Vec::new());
                }
                let mut events = Vec::new();
                if let Some(summary) = self
                    .store
                    .conversation_summary(&account.id, &conversation.id)?
                {
                    events.push(HostSideEvent::ConversationChanged(summary));
                }
                Ok(events)
            }
            ControlReceive::AdminDelete {
                target_author,
                target_timestamp,
            } => {
                // Contract 1.33: the official "deleted by admin" tombstone is
                // a row-level marker — the status ladder is untouched and the
                // body stays for the audit window (retention prunes the row
                // normally). The host re-renders the row through the
                // upserted record; if the removed message is also the pinned
                // one, the pin state clears and the summary refreshes.
                let mut events = Vec::new();
                if let Some((mut record, true)) = self.store.mark_admin_deleted(
                    &account.id,
                    &conversation.id,
                    target_timestamp,
                )? {
                    self.store
                        .attach_reactions(&account.id, std::slice::from_mut(&mut record))?;
                    self.store
                        .attach_edits(&account.id, std::slice::from_mut(&mut record))?;
                    events.push(HostSideEvent::MessageUpserted(project_message_for_host(
                        record,
                    )));
                }
                if self.store.clear_conversation_pin(
                    &account.id,
                    &conversation.id,
                    &target_author,
                    target_timestamp,
                )? && let Some(summary) = self
                    .store
                    .conversation_summary(&account.id, &conversation.id)?
                {
                    events.push(HostSideEvent::ConversationChanged(summary));
                }
                Ok(events)
            }
            ControlReceive::GroupTimerUpdate {
                expire_timer_seconds,
            } => {
                // The engine's groupUpdated projection (2026-10-06 engine
                // batch): the group timer changed upstream — this group's own
                // edit or a member's — and the conversation mirror converges
                // exactly like the §4.41 direct timer-notice path (0 = off;
                // the setter stores 0 literally, never NULL). The notification
                // is state convergence, not chat content: no message row, no
                // system row — the group timer change rides the change event,
                // and the notice-row presentation stays a later desktop
                // decision (the §4.44 dated note records it). A same-value
                // event changes nothing (the setter is idempotent); the
                // summary still re-pushes so a lagging window converges.
                self.store.set_conversation_expire_timer(
                    &account.id,
                    &conversation.id,
                    u64::from(expire_timer_seconds),
                )?;
                let mut events = Vec::new();
                if let Some(summary) = self
                    .store
                    .conversation_summary(&account.id, &conversation.id)?
                {
                    events.push(HostSideEvent::ConversationChanged(summary));
                }
                Ok(events)
            }
            // The view-once open sync is intercepted before the conversation
            // ladder (it addresses rows across conversations by author +
            // timestamp), so this arm is the unreachable fall-through.
            ControlReceive::ViewOnceOpen { .. } => Ok(Vec::new()),
            // Call signaling (contract 1.51, §4.49): no row, no local state
            // — the signaling payload forwards as the transient
            // `call.incoming` event addressed by the conversation it routed
            // to; unresolvable conversations drop silently like every other
            // control face (the caller has no conversation with this
            // account, or the row predates the link).
            ControlReceive::Call {
                call_id,
                signal_type,
                payload,
            } => Ok(vec![HostSideEvent::CallIncoming {
                account_id: account.id,
                conversation_id: conversation.id,
                call_id,
                signal_type,
                payload,
            }]),
        }
    }

    /// Consume one view-once open sync (contract revision 1.38, §4.37): find
    /// the view-once row the (author, timestamp) pair names through the
    /// bounded candidate ladder and burn it on the first transition. Warn-only
    /// on every miss — timestamp 0 is the official unknown marker, a sync
    /// that arrived before its message has no staging (the sender's resync
    /// cycle re-delivers it), and a duplicate envelope finds no transition and
    /// emits nothing. Never sends upstream: the fan-out belongs to the
    /// desktop's `messages.markViewOnceOpened` trigger, not to a sync echo.
    fn apply_view_once_open_sync(
        &mut self,
        account: &AccountRow,
        sender_aci: &str,
        timestamp: u64,
    ) -> Result<Vec<HostSideEvent>, ServiceError> {
        if timestamp == 0 {
            tracing::warn!(
                kind = "view_once_open",
                "open sync without a timestamp dropped"
            );
            return Ok(Vec::new());
        }
        // The candidate ladder: the ACI itself (the fixture and direct-identity
        // shape), then the recorded ACI→peer-key mappings (§4.37), then
        // contacts digit-suffix matches (the §4.29 mention resolution rule).
        let mut peer_keys = vec![sender_aci.to_string()];
        peer_keys.extend(self.store.peer_keys_for_aci(&account.id, sender_aci)?);
        let digits = address_digits(sender_aci);
        if !digits.is_empty() {
            for key in self.store.contact_peer_keys(&account.id)? {
                let candidate = address_digits(&key);
                if !candidate.is_empty() && candidate.ends_with(&digits) {
                    peer_keys.push(key);
                }
            }
        }
        peer_keys.sort();
        peer_keys.dedup();
        let candidates =
            self.store
                .view_once_open_candidates(&account.id, timestamp, &peer_keys)?;
        let target = candidates.into_iter().find(|record| {
            record
                .rich
                .as_ref()
                .is_some_and(|rich| rich.view_once || rich.view_once_invalid)
        });
        let Some(record) = target else {
            tracing::warn!(
                kind = "view_once_open",
                "open sync matched no local view-once row; dropped"
            );
            return Ok(Vec::new());
        };
        let opened_at = now_ms();
        let Some((_, changed)) = self.store.burn_view_once_message(
            &record.account_id,
            &record.conversation_id,
            &record.id,
            opened_at,
        )?
        else {
            return Ok(Vec::new());
        };
        if !changed {
            // The engine's pinned-rev resync re-delivers the same sync — the
            // row is already burned, no second event.
            return Ok(Vec::new());
        }
        self.burn_view_once_media(&record);
        Ok(vec![HostSideEvent::MessageViewOnceOpened {
            account_id: record.account_id,
            conversation_id: record.conversation_id,
            message_id: record.id,
            opened_at,
        }])
    }
}

// Store-backed read paths below: the single implementation behind the
// matching [`ConnectorService`] methods and the supervisor's `spawn_blocking`
// lane over the shared `Arc<Store>` (no service lock involved).

pub(crate) fn store_list_conversations(
    store: &Store,
    account_id: &str,
    limit: u32,
    cursor: Option<&str>,
) -> Result<Page<ConversationSummary>, ServiceError> {
    if store.account_by_id(account_id)?.is_none() {
        return Err(ServiceError::Store(StoreError::AccountNotFound));
    }
    Ok(store.list_conversations(account_id, limit, cursor)?)
}

pub(crate) fn store_list_contacts(
    store: &Store,
    account_id: &str,
    query: Option<&str>,
    limit: u32,
    cursor: Option<&str>,
) -> Result<Page<ContactSummary>, ServiceError> {
    if store.account_by_id(account_id)?.is_none() {
        return Err(ServiceError::Store(StoreError::AccountNotFound));
    }
    let query = query.map(str::trim).filter(|value| !value.is_empty());
    if let Some(query) = query {
        validate_opaque_id(query, "query")?;
    }
    Ok(store.list_contacts(account_id, query, limit, cursor)?)
}

pub(crate) fn store_get_group(
    store: &Store,
    account_id: &str,
    group_key: &str,
) -> Result<GroupDetails, ServiceError> {
    store_get_group_with_requesting(store, account_id, group_key, None, None)
}

/// The store read with injected live requesting-members and pending-invite
/// lists (contract revisions 1.62 and 1.64, §4.53/§4.57): the supervisor
/// passes the engine's own `listGroups` answer for this group; `None` keeps
/// the pre-1.62 face (the keys stay absent — store-only callers, tests).
pub(crate) fn store_get_group_with_requesting(
    store: &Store,
    account_id: &str,
    group_key: &str,
    requesting: Option<&[Value]>,
    pending: Option<&[Value]>,
) -> Result<GroupDetails, ServiceError> {
    validate_opaque_id(account_id, "accountId")?;
    validate_opaque_id(group_key, "groupKey")?;
    let account = store
        .account_by_id(account_id)?
        .ok_or(ServiceError::Store(StoreError::AccountNotFound))?;
    let Some((title, extra, synced_at)) = store.contact_by_peer(account_id, "group", group_key)?
    else {
        return Err(ServiceError::Api(ApiError::new(
            "GROUP_NOT_FOUND",
            "no cached group row for this accountId and groupKey",
            false,
        )));
    };
    let extra = extra
        .as_deref()
        .and_then(|extra| serde_json::from_str::<Value>(extra).ok());
    let member_count = extra
        .as_ref()
        .and_then(|extra| extra.get("memberCount").and_then(Value::as_u64));
    // Contract 1.40 (§4.39): the bounded roster the sync captured, resolved
    // against the contact cache at read time. A malformed or absent roster
    // (every pre-1.40 row) leaves `members` absent — the response stays
    // byte-identical to the §4.8 shape.
    let members = extra
        .as_ref()
        .and_then(|extra| extra.get("members"))
        .and_then(Value::as_array)
        .map(|roster| project_group_roster(store, account_id, &account, roster))
        .transpose();
    // Contract 1.53 (§4.50 appendix): the terminated flag the sync captured
    // from the upstream JsonGroup — stored only when true, so absent
    // (pre-1.53 rows, engines without the flag) answers the default false.
    let is_terminated = extra
        .as_ref()
        .and_then(|extra| extra.get("isTerminated").and_then(Value::as_bool))
        == Some(true);
    // Contract 1.62 (§4.53 appendix): the live requesting-members projection —
    // resolved against the same read-time contacts-cache name map the roster
    // uses, bounded to the roster cap. `None` (store-only callers, pre-1.62
    // face) and an engine without the face (empty slice) both collapse to an
    // absent key.
    let requesting_members = requesting
        .map(|entries| project_requesting_members(store, account_id, entries))
        .transpose()?
        .filter(|roster| !roster.is_empty());
    // Contract 1.64 (§4.57): the live pending-invite projection — the same
    // read-time name map and the same bounds as the requesting face.
    // `None` (store-only callers, pre-1.64 face) and an engine without the
    // face (empty slice) both collapse to an absent key.
    let pending_members = pending
        .map(|entries| project_pending_members(store, account_id, entries))
        .transpose()?
        .filter(|roster| !roster.is_empty());
    Ok(GroupDetails {
        peer_key: group_key.to_string(),
        title,
        member_count,
        members: members?,
        is_terminated,
        requesting_members,
        pending_members,
        synced_at,
    })
}

/// One live requesting-member projection (contract revision 1.62, §4.53
/// appendix): the connector-wide first-wins address ladder picks the id the
/// desktop addresses the approval write with (the fold accepts either
/// identity, so the ladder id always folds), the uuid rides only when it
/// differs from the id, and the name resolves from the same read-time
/// contacts-cache map the roster uses. Malformed entries and entries past
/// [`MAX_GROUP_MEMBERS`] drop individually — a hostile answer can never fail
/// the read.
fn project_requesting_members(
    store: &Store,
    account_id: &str,
    entries: &[Value],
) -> Result<Vec<GroupRequestingMemberView>, ServiceError> {
    let titles = store.contact_title_map(account_id)?;
    Ok(entries
        .iter()
        .take(MAX_GROUP_MEMBERS)
        .filter_map(|entry| {
            let object = entry.as_object()?;
            // The first-wins address ladder (number before uuid), the same
            // [`member_address_id`] shape the sync capture and the §4.39
            // roster projection run — kept local: the supervisor twin is a
            // private fn of its module.
            let id = ["number", "uuid"]
                .iter()
                .find_map(|key| object.get(*key).and_then(Value::as_str))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| {
                    value
                        .chars()
                        .take(MAX_GROUP_MEMBER_ID_CHARS)
                        .collect::<String>()
                })?;
            let uuid = ["uuid"]
                .iter()
                .find_map(|key| object.get(*key).and_then(Value::as_str))
                .map(str::trim)
                .filter(|value| !value.is_empty() && *value != id)
                .map(|value| {
                    value
                        .chars()
                        .take(MAX_GROUP_MEMBER_ID_CHARS)
                        .collect::<String>()
                });
            let name = titles
                .get(&id)
                .or_else(|| uuid.as_deref().and_then(|uuid| titles.get(uuid)))
                .cloned();
            Some(GroupRequestingMemberView { id, uuid, name })
        })
        .collect())
}

/// One live pending-invite projection (contract revision 1.64, §4.57): the
/// [`project_requesting_members`] shape verbatim — the connector-wide
/// first-wins address ladder picks the id the desktop addresses the revoke
/// write with (the fold accepts either identity, so the ladder id always
/// folds), the uuid rides only when it differs from the id, and the name
/// resolves from the same read-time contacts-cache map the roster uses.
/// Malformed entries and entries past [`MAX_GROUP_MEMBERS`] drop
/// individually — a hostile answer can never fail the read. The upstream
/// face carries no inviter metadata (the recorded §4.57 projection
/// difference), so every row is a bare invitee.
fn project_pending_members(
    store: &Store,
    account_id: &str,
    entries: &[Value],
) -> Result<Vec<GroupPendingMemberView>, ServiceError> {
    let titles = store.contact_title_map(account_id)?;
    Ok(entries
        .iter()
        .take(MAX_GROUP_MEMBERS)
        .filter_map(|entry| {
            let object = entry.as_object()?;
            // The first-wins address ladder (number before uuid), the same
            // [`member_address_id`] shape the sync capture and the §4.39
            // roster projection run — kept local: the supervisor twin is a
            // private fn of its module.
            let id = ["number", "uuid"]
                .iter()
                .find_map(|key| object.get(*key).and_then(Value::as_str))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| {
                    value
                        .chars()
                        .take(MAX_GROUP_MEMBER_ID_CHARS)
                        .collect::<String>()
                })?;
            let uuid = ["uuid"]
                .iter()
                .find_map(|key| object.get(*key).and_then(Value::as_str))
                .map(str::trim)
                .filter(|value| !value.is_empty() && *value != id)
                .map(|value| {
                    value
                        .chars()
                        .take(MAX_GROUP_MEMBER_ID_CHARS)
                        .collect::<String>()
                });
            let name = titles
                .get(&id)
                .or_else(|| uuid.as_deref().and_then(|uuid| titles.get(uuid)))
                .cloned();
            Some(GroupPendingMemberView { id, uuid, name })
        })
        .collect())
}

/// One projected roster entry (§4.39): the stored identity, the read-time
/// contacts-cache name, the number-based self marker, and the admin flag the
/// sync captured from the upstream (absent when the engine exposes none).
/// Stored entries without an address drop individually. The stored label pair
/// (contract 1.67, §4.60) passes through under the same server budgets the
/// capture enforced — the stored roster is connector-written, so this second
/// bound is cheap defense in depth against any older row that predates the
/// capture budgets.
fn project_group_roster(
    store: &Store,
    account_id: &str,
    account: &AccountRow,
    roster: &[Value],
) -> Result<Vec<GroupMemberView>, ServiceError> {
    let titles = store.contact_title_map(account_id)?;
    Ok(roster
        .iter()
        .take(MAX_GROUP_MEMBERS)
        .filter_map(|entry| {
            let object = entry.as_object()?;
            let id = ["id"]
                .iter()
                .find_map(|key| object.get(*key).and_then(Value::as_str))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| {
                    value
                        .chars()
                        .take(MAX_GROUP_MEMBER_ID_CHARS)
                        .collect::<String>()
                })?;
            let uuid = ["uuid"]
                .iter()
                .find_map(|key| object.get(*key).and_then(Value::as_str))
                .map(str::trim)
                .filter(|value| !value.is_empty() && *value != id)
                .map(|value| {
                    value
                        .chars()
                        .take(MAX_GROUP_MEMBER_ID_CHARS)
                        .collect::<String>()
                });
            let name = titles
                .get(&id)
                .or_else(|| uuid.as_deref().and_then(|uuid| titles.get(uuid)))
                .cloned();
            let label_emoji = super_supervisor_label_bound(
                object.get("labelEmoji").and_then(Value::as_str),
                GROUP_MEMBER_LABEL_EMOJI_MAX_BYTES,
            );
            let label = super_supervisor_label_bound(
                object.get("label").and_then(Value::as_str),
                GROUP_MEMBER_LABEL_SERVER_MAX_BYTES,
            );
            Some(GroupMemberView {
                is_self: id == account.signal_account,
                id,
                uuid,
                name,
                admin: object.get("admin").and_then(Value::as_bool) == Some(true),
                label_emoji,
                label,
            })
        })
        .collect())
}

/// The read-side twin of the capture budget (contract 1.67, §4.60): trimmed,
/// non-empty, whole-value-only. Local copy — the capture helper is a private
/// fn of the supervisor module, and the projection must not truncate either.
fn super_supervisor_label_bound(value: Option<&str>, max_bytes: usize) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= max_bytes)
        .map(str::to_owned)
}

pub(crate) fn store_list_messages(
    store: &Store,
    account_id: &str,
    conversation_id: &str,
    limit: u32,
    before: Option<&str>,
) -> Result<Page<MessageRecord>, ServiceError> {
    if store.account_by_id(account_id)?.is_none() {
        return Err(ServiceError::Store(StoreError::AccountNotFound));
    }
    if store
        .conversation_by_id(account_id, conversation_id)?
        .is_none()
    {
        return Err(ServiceError::Store(StoreError::ConversationNotFound));
    }
    // Viewing the thread marks it read (local badge only; no Signal receipt RPC yet).
    let _ = store.clear_conversation_unread(account_id, conversation_id)?;
    let mut page = store.list_messages(account_id, conversation_id, limit, before)?;
    page.items = page
        .items
        .into_iter()
        .map(project_message_for_host)
        .collect();
    Ok(page)
}

pub(crate) fn store_search_messages(
    store: &Store,
    account_id: &str,
    query: &str,
    limit: u32,
    cursor: Option<&str>,
) -> Result<Page<MessageRecord>, ServiceError> {
    if store.account_by_id(account_id)?.is_none() {
        return Err(ServiceError::Store(StoreError::AccountNotFound));
    }
    // An empty (or whitespace-only) query would match every stored body;
    // the client treats that as "no query", so answer with an empty page.
    let query = query.trim();
    if query.is_empty() {
        return Ok(Page {
            items: Vec::new(),
            next_cursor: None,
        });
    }
    // Search terms are user input echoed into a LIKE pattern: bound them so
    // a paste cannot inflate the query (contract 1.30).
    if query.chars().count() > 128 {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "query must be at most 128 characters",
            false,
        )));
    }
    let mut page = store.search_messages(account_id, query, limit, cursor)?;
    page.items = page
        .items
        .into_iter()
        .map(project_message_for_host)
        .collect();
    Ok(page)
}

pub(crate) fn store_get_message_text(
    store: &Store,
    account_id: &str,
    conversation_id: &str,
    message_id: &str,
) -> Result<MessageText, ServiceError> {
    validate_opaque_id(account_id, "accountId")?;
    validate_opaque_id(conversation_id, "conversationId")?;
    validate_opaque_id(message_id, "messageId")?;
    let message = store
        .message_by_id(account_id, conversation_id, message_id)?
        .ok_or(StoreError::MessageNotFound)?;
    if !message.text_retrievable {
        return Err(ServiceError::Api(ApiError::new(
            "CAPABILITY_UNAVAILABLE",
            "complete message text is unavailable",
            false,
        )));
    }
    let text = message.text.ok_or_else(|| {
        ServiceError::Api(ApiError::new(
            "CAPABILITY_UNAVAILABLE",
            "message has no text body",
            false,
        ))
    })?;
    Ok(MessageText {
        message_id: message.id,
        text_bytes: message.text_bytes.unwrap_or(text.len() as u32),
        text,
    })
}

/// The legacy sender identity a store generation may have written for this
/// peer (receive dedupe pairs the current and legacy hash).
fn legacy_sender_id(kind: &str, peer_key: &str, account_id: &str) -> String {
    stable_hash_id(&[account_id, kind, peer_key])
}

impl ConnectorService {
    fn require_active_link(
        &mut self,
        link_session_id: &str,
    ) -> Result<&ActiveLinkSession, ServiceError> {
        match self.link.as_ref() {
            Some(session) if session.session_id == link_session_id => {
                if session.is_expired(now_ms()) {
                    self.link = None;
                    return Err(ServiceError::Api(ApiError::new(
                        "LINK_EXPIRED",
                        "link session has expired",
                        false,
                    )));
                }
            }
            Some(_) | None => {
                return Err(ServiceError::Api(ApiError::new(
                    "LINK_NOT_FOUND",
                    "link session was not found",
                    false,
                )));
            }
        }
        Ok(self.link.as_ref().expect("link session present"))
    }

    fn take_link_session(
        &mut self,
        link_session_id: &str,
    ) -> Result<ActiveLinkSession, ServiceError> {
        let _ = self.require_active_link(link_session_id)?;
        Ok(self.link.take().expect("link session present"))
    }
}

#[derive(Debug)]
pub enum PreparedSend {
    Existing(Box<MessageRecord>),
    Dispatch {
        pending_id: String,
        account_id: String,
        conversation_id: String,
        params: Value,
        pending_sent_at: u64,
    },
}

/// Everything the supervisor needs to run one upstream `remoteDelete` call,
/// produced by the pure local `prepare_remote_delete` validation.
#[derive(Debug)]
pub struct PreparedRemoteDelete {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub params: Value,
}

/// Everything the supervisor needs to run one upstream `sendReaction` call,
/// produced by the pure local `prepare_send_reaction` validation.
#[derive(Debug)]
pub struct PreparedSendReaction {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub params: Value,
    /// Upstream-confirmed reaction delta, carried for the complete step
    /// (own reactions persist with the account itself as the actor).
    pub emoji: String,
    pub remove: bool,
    pub target_sent_at: u64,
}

/// Everything the supervisor needs to run one upstream pin-family call
/// (`sendPinMessage` / `sendUnpinMessage` / `sendAdminDelete`, contract
/// 1.33), produced by the pure local prepare validations. The target
/// identity rides along for the complete step (own pins and own unpins
/// converge the locally derived pinned state).
#[derive(Debug)]
pub struct PreparedPin {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub params: Value,
    pub target_author: String,
    pub target_sent_at: u64,
    pub pin_duration_seconds: Option<u32>,
}

/// One per-author receipt group (contract revision 1.34): the recipient the
/// engine addresses — the message author, never the whole group (official
/// receipts are always per-author direct sends) — plus that author's upstream
/// timestamps in receipt order.
#[derive(Debug)]
pub struct ReceiptGroup {
    pub recipient: String,
    pub timestamps: Vec<u64>,
}

/// The `messages.markRead` / `messages.markViewed` prepare result: the
/// resolved account plus the per-author fan-out. An empty group list is the
/// trivial no-op the supervisor answers `{"status":"sent"}` for; the
/// supervisor calls the engine once per group and never fails the request on
/// a receipt-call failure.
#[derive(Debug)]
pub struct PreparedReceipts {
    pub account_id: String,
    pub conversation_id: String,
    pub account_signal: String,
    pub groups: Vec<ReceiptGroup>,
}

/// The `messages.markViewOnceOpened` prepare result (contract revision 1.38,
/// §4.37): the burn already ran (the events carry the one
/// `message.viewOnceOpened` the transition earned; empty on a replay), and
/// the supervisor still owes the best-effort upstream pair. `timestamp` is
/// the burned row's Signal timestamp both legs reference. A `None` leg skips:
/// the receipt leg only ever addresses an incoming direct row's author, the
/// open-sync leg names that author — or the account itself for an outgoing
/// row — and an unresolvable group author skips both (§4.28) while the burn
/// still ran.
#[derive(Debug)]
pub struct PreparedMarkViewOnceOpened {
    pub events: Vec<HostSideEvent>,
    pub account_signal: String,
    pub timestamp: u64,
    pub receipt_recipient: Option<String>,
    pub open_sender_aci: Option<String>,
}

/// `messages.attachments.open` result (ADR 0002, contract revision 1.17): an
/// unguessable 128-bit handle bound to the resolved
/// (account, conversation, message, attachment) tuple, the file size the
/// stream will deliver, and the raw chunk size every `readChunk` offsets
/// advance by. The filesystem path never appears anywhere on the wire.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaOpenView {
    pub media_handle: String,
    pub size_bytes: u64,
    pub chunk_bytes: u64,
}

/// `messages.attachments.readChunk` result (ADR 0002): exactly one chunk.
/// `size_bytes` counts this chunk's raw bytes; `dataBase64` inflates them by
/// the usual per-chunk base64 factor to at most
/// `4 * ceil(262_144 / 3)` = 349_528 characters — bounded, and far inside
/// the 160 MiB host frame budget. `eof` terminates the caller's read loop.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaChunkView {
    pub data_base64: String,
    pub size_bytes: u64,
    pub eof: bool,
}

/// `messages.attachments.closeHandle` result: `released` is false when the
/// handle was already gone (expired or closed earlier) — closing is
/// idempotent by design.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaCloseView {
    pub released: bool,
}

/// The groups.get response (contract revision 1.9): the cached kind='group'
/// contacts row projected for the host. `member_count` is absent when the
/// sync batch captured no member list; `synced_at` is the row's last sync
/// timestamp — the response is exactly as fresh as that sync, nothing more.
/// `members` (contract revision 1.40, §4.39) is the bounded roster the sync
/// captured; absent on pre-1.40 rows, where the response stays byte-identical
/// to the §4.8 shape; its entries carry the upstream member label pair since
/// contract 1.67 (§4.60), absent keys when the member (or the whole engine
/// face) has none. `is_terminated` (contract revision 1.53, §4.50
/// appendix) is the upstream `JsonGroup.isTerminated` flag as of the row's
/// last sync — false for every pre-1.53 row and every engine without the
/// flag, and the receive chain cannot refresh it (the `JsonGroupInfo`
/// envelope exposes no terminated field), so `synced_at` remains the only
/// staleness signal for it too. `requesting_members` (contract revision
/// 1.62, §4.53 appendix) is the live read, not the cached row — see the
/// supervisor projection. `pending_members` (contract revision 1.64, §4.57)
/// is the same-shaped live read of the invited-but-not-yet-joined set — see
/// the supervisor projection.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDetails {
    pub peer_key: String,
    pub title: String,
    pub member_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub members: Option<Vec<GroupMemberView>>,
    pub is_terminated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requesting_members: Option<Vec<GroupRequestingMemberView>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_members: Option<Vec<GroupPendingMemberView>>,
    pub synced_at: u64,
}

/// One projected group member (contract revision 1.40, §4.39): the official
/// member row's minimal set. `id` follows the connector-wide first-wins
/// address ladder so roster entries join against contact peer keys, message
/// senders, and mention authors; `uuid` is present only when the upstream
/// supplied one; `name` resolves from the contacts cache at read time and is
/// absent for a peer the cache does not know; `self` is the number-based §4.24
/// boundary (the own ACI is not queryable upstream); `admin` is the upstream
/// admin mark the sync captured, collapsed to an absent key when false or
/// never provided (engine-mode rosters carry none — §4.39 records the
/// projection difference). `labelEmoji` / `label` (contract 1.67, §4.60) are
/// the upstream member's own label the sync captured verbatim, collapsed to
/// absent keys when the member carries none (the pinned upstream emits both
/// `@JsonInclude(NON_NULL)`; engine-mode rosters carry neither — same
/// projection difference as `admin`). The write face (§4.44
/// `groups.update memberLabel`/`memberLabelEmoji`, self only per the pinned
/// `GroupHelper.updateGroupV2`) converges the values through the next sync —
/// this read projects, it never composes.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMemberView {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "self", skip_serializing_if = "std::ops::Not::not")]
    pub is_self: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub admin: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_emoji: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// One projected requesting-member row (contract revision 1.62, §4.53
/// appendix): the same minimal face as the roster [`GroupMemberView`] — the
/// address the desktop addresses the approval write with rides the
/// connector-wide first-wins ladder, the name resolves from the contacts
/// cache at read time, the admin/self marks never ride (a requesting
/// candidate is by definition not yet a member). `uuid` is present only when
/// the upstream supplied one.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupRequestingMemberView {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// One projected pending-invite row (contract revision 1.64, §4.57): the
/// same minimal face as [`GroupRequestingMemberView`] — the address the
/// desktop addresses the revoke write with rides the connector-wide
/// first-wins ladder (the §4.57 fold accepts either identity), the name
/// resolves from the contacts cache at read time, the admin/self marks
/// never ride (a pending invitee is by definition not yet a member).
/// `uuid` is present only when the upstream supplied one. The official
/// row's inviter metadata (`metadata.addedByUserId`) has no upstream face —
/// the pinned signal-cli projects `pendingMembers` as bare
/// `JsonGroupMemberAddress` records (`ListGroupsCommand.resolveJsonMembers`
/// at :125-133; `GroupInfoV2.getPendingMembers` :144-151 resolves the
/// member's own address only) — so no invited-by grouping rides this face
/// (the recorded projection difference).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupPendingMemberView {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}
/// with. `emoji`/`contentType` are absent when the pack did not declare
/// them (the official proto treats both as optional).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StickerManifestEntry {
    pub id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

/// The `stickerPacks.getManifest` result (contract revision 1.36, §4.32).
/// `cover` is absent when the pack declares none (the official manifest
/// shape treats cover as optional).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StickerPackManifest {
    pub title: String,
    pub author: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover: Option<StickerManifestEntry>,
    pub stickers: Vec<StickerManifestEntry>,
    pub sticker_count: u32,
}

/// The `stickerPacks.getImage` result (contract revision 1.36, §4.32): the
/// decrypted image bytes as standard base64 — pass-through of the engine's
/// result, nothing persisted, nothing decoded or re-encoded connector-side.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StickerPackImage {
    pub data_base64: String,
    pub content_type: String,
    pub size: u32,
}

/// One pinned-conversation entry of the §4.33 result: the upstream reference
/// exactly as the cloud order carries it — `conversationId` is the engine's
/// resolved conversation identifier, `kind` the three-value taxonomy
/// (`contact` | `group` | `legacyGroup`).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PinnedConversation {
    pub conversation_id: String,
    pub kind: String,
}

/// The §4.33 result shape shared by `conversations.getPinned` and
/// `conversations.setPinned`: the pinned list in cloud order, capped at
/// [`MAX_PINNED_CONVERSATIONS`].
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PinnedConversations {
    pub pinned: Vec<PinnedConversation>,
}

/// One sticker-pack sync record of the §4.34 result: the Storage Service
/// `StickerPackRecord` projected 1:1 — an installed pack carries the key and
/// its cloud position, an uninstalled pack the tombstone timestamp.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StickerPackSync {
    pub pack_id: String,
    pub pack_key: Option<String>,
    pub position: Option<u32>,
    pub deleted_at_timestamp_ms: Option<u64>,
}

/// The §4.34 result shape shared by `stickerPacks.getSyncs` and
/// `stickerPacks.setSync`: the record list in cloud order, capped at
/// [`MAX_STICKER_PACK_SYNCS`].
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StickerPackSyncs {
    pub packs: Vec<StickerPackSync>,
}

/// One blocked-conversation entry of the §4.45 result: the storage record's
/// blocked-bit projection, the same shape and kind taxonomy as the §4.33
/// pinned entry.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockedConversation {
    pub conversation_id: String,
    pub kind: String,
}

/// The §4.45 result of `conversations.getBlocked`: the blocked list in
/// storage order, capped at [`MAX_BLOCKED_CONVERSATIONS`].
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockedConversations {
    pub blocked: Vec<BlockedConversation>,
}

/// The §4.47 result of `conversations.getAvatar` (contract revision 1.48):
/// the decrypted avatar as a data URI (MIME sniffed engine-side from the
/// magic bytes, `application/octet-stream` when unrecognized) plus the
/// decoded byte count. Pass-through of the engine's result — nothing
/// decoded or re-encoded connector-side beyond the result-face budget check,
/// nothing persisted, nothing logged.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AvatarDownload {
    pub data_uri: String,
    pub byte_size: u64,
}

/// The §4.55 result of `accounts.setUsername`: on the set path the username
/// as confirmed by the upstream write-back (`updateAccount` →
/// `m.getUsername()`) plus the username link URL when the server generated
/// one (NON_NULL upstream, so the field is skipped when absent); on the
/// delete path both are `null`/absent (the upstream writes no result body).
/// This write-back is the connector's only username read face — there is no
/// local-account getter in the pinned signal-cli (see
/// `prepare_accounts_get_username`).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountUsername {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username_link: Option<String>,
}

/// One §4.55 `username.lookup` entry: the caller's entry echoed verbatim,
/// plus the upstream status. `number`/`username`/`uuid` are the upstream
/// NON_NULL projections (`JsonUserStatus`): `uuid != null` means the entry
/// resolved to a registered account.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsernameLookupEntry {
    pub recipient: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    pub is_registered: bool,
}

/// The §4.55 result of `username.lookup`: entries in upstream order, capped
/// at [`USERNAME_LOOKUP_MAX_ENTRIES`] by the params gate.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsernameLookupResult {
    pub results: Vec<UsernameLookupEntry>,
}

/// The §4.48 result of `calls.getState` (contract revision 1.50): the
/// connector's call-state registry, honestly empty — the connector is a
/// signaling pipe; the RingRTC state machine lives in the desktop + calling
/// sidecar, upstream call state lives in the engine's transport, and this
/// connector persists neither. The `calls` list and the projection note are
/// the stable face a desktop builds against once a later batch gives the
/// connector a real registry to project.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallStateRegistry {
    pub calls: Vec<CallStateEntry>,
    /// Where the real state machine lives — the honest projection note the
    /// §4.48 contract records (connector ≠ RingRTC state owner).
    pub projection: &'static str,
}

/// One registered call (presently never produced — see
/// [`CallStateRegistry`]).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallStateEntry {
    pub call_id: u64,
    pub conversation_id: String,
    pub signal_type: &'static str,
}

impl Default for CallStateRegistry {
    fn default() -> Self {
        Self {
            calls: Vec::new(),
            projection: CALL_STATE_PROJECTION_NOTE,
        }
    }
}

/// `accounts.setProfile` avatar tri-state (contract revision 1.46, §4.45):
/// `Keep` (key absent) forwards no avatar key, `Remove` (explicit null)
/// forwards `avatar: null`, `Replace` (a data URI) forwards the validated
/// string. The distinction survives params deserialization via
/// `Option<Value>` — a plain `Option<String>` would erase the absent/null
/// line the engine's avatar semantics are built on.
#[derive(Clone, Debug, PartialEq)]
pub enum AvatarUpdate {
    Keep,
    Remove,
    Replace(String),
}

/// Target of an outgoing text send: either an existing conversation, or a peer
/// (kind + peer_key) for which a conversation is resolved/created on demand.
#[derive(Clone, Debug)]
pub enum SendTarget {
    Conversation(String),
    Peer {
        kind: String,
        peer_key: String,
        peer_title: Option<String>,
    },
}

/// Borrowed peer addressing for a send, validated by the service.
#[derive(Clone, Copy, Debug)]
pub struct PeerTarget<'a> {
    pub kind: &'a str,
    pub peer_key: &'a str,
    pub peer_title: Option<&'a str>,
}

/// Borrowed addressing for an attachment send (implementation-plan §4.12):
/// same two forms as [`SendTarget`] but borrowed, so validation happens in
/// the service before any row is written.
#[derive(Clone, Copy, Debug)]
pub enum AttachmentSendTarget<'a> {
    Conversation(&'a str),
    Peer(PeerTarget<'a>),
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactsSyncOutcome {
    pub contact_count: u64,
    pub group_count: u64,
    pub synced_at: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountDeleteLocalDataParams {
    pub account_id: String,
    pub operation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LinkStartParams {
    pub device_name: String,
    /// Optional target proxy group (ADR 0001 R3). Absent means `default`; an
    /// unknown id fails with PROXY_GROUP_NOT_FOUND at the registry. Groups are
    /// selected here, never created or reconfigured over IPC (R1).
    pub proxy_group: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkSessionParams {
    pub link_session_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationsListParams {
    pub account_id: String,
    pub cursor: Option<String>,
    pub limit: u32,
}

/// history.importStatus params (contract revision 1.39, §4.38): the account
/// whose link-time import status is polled.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoryImportStatusParams {
    pub account_id: String,
}

/// history.importRetry params (contract revision 1.53, §4.38): the account
/// whose failed link-time import is manually retried.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoryImportRetryParams {
    pub account_id: String,
}

/// history.attachmentDownload params (contract revision 1.57, §4.38 step 3):
/// the message-addressing triple plus the attachment id from the message's
/// received metadata — exactly the `messages.attachments.open` shape.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HistoryAttachmentDownloadParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub attachment_id: String,
}

/// u64-bounded usize parse for `attachmentIndex`: JSON numbers are unsigned
/// in the schema; anything else is the params-gate INVALID_REQUEST.
fn parse_attachment_index<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    usize::try_from(value).map_err(|_| {
        serde::de::Error::invalid_value(
            serde::de::Unexpected::Unsigned(value),
            &"an index inside the unsigned platform range",
        )
    })
}

/// history.attachmentDownload result: the plaintext byte count that landed —
/// the same size `messages.attachments.open` will report for the file. No
/// path, no handle: the bytes stay behind the chunked media triple.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryAttachmentDownloadView {
    pub size_bytes: u64,
}

/// attachments.download params (contract revision 1.61, §4.54): the message
/// addressing plus the descriptor's positional index — the wire-free
/// addressing the byte-identical descriptor ids cannot offer on every engine
/// face (§4.54).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AttachmentDownloadParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    #[serde(deserialize_with = "parse_attachment_index")]
    pub attachment_index: usize,
}

/// attachments.download result: the `HistoryAttachmentDownloadView` shape
/// verbatim — afterwards `messages.attachments.open` resolves the bytes.
pub type AttachmentDownloadView = HistoryAttachmentDownloadView;

/// The staged download the service hands its supervisor: either the file
/// already exists (idempotent dedup — the honest measured size comes back),
/// or the full inputs for one network chain. Paths are internal; they never
/// appear on any wire.
#[derive(Debug)]
pub enum PreparedHistoryDownload {
    AlreadyPresent {
        size_bytes: u64,
    },
    Ready {
        locator: crate::store::HistoryAttachmentLocator,
        descriptor_size: Option<u64>,
        target: PathBuf,
        temp_path: PathBuf,
        plaintext_path: PathBuf,
    },
}

/// backups.export / backups.exportStatus (contract revision 1.47, §4.46):
/// the account-addressed faces of the Android local-backup export.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupsExportParams {
    pub account_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupsExportStatusParams {
    pub account_id: String,
}

/// backups.exportFile.open (contract revision 1.47, §4.46): `file` is the
/// two-value snapshot-file enum — no path component is accepted anywhere.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupsExportFileOpenParams {
    pub account_id: String,
    pub file: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupsExportFileReadChunkParams {
    pub file_handle: String,
    pub offset: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupsExportFileCloseHandleParams {
    pub file_handle: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagesListParams {
    pub account_id: String,
    pub conversation_id: String,
    pub before: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagesSearchParams {
    pub account_id: String,
    pub query: String,
    pub cursor: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageGetTextParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageText {
    pub message_id: String,
    pub text: String,
    pub text_bytes: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagesSendTextParams {
    pub account_id: String,
    pub conversation_id: Option<String>,
    pub kind: Option<String>,
    pub peer_key: Option<String>,
    pub peer_title: Option<String>,
    pub text: String,
    pub client_request_id: String,
    pub quote_message_id: Option<String>,
    pub previews: Option<Vec<SendTextPreviewParams>>,
    pub mentions: Option<Vec<SendTextMentionParams>>,
    /// §4.62: optional outbound text-style ranges; entry shape validated by
    /// `SendTextStyleParams` (unknown keys fail deserialization).
    pub text_style: Option<Vec<SendTextStyleParams>>,
}

/// One outbound @mention entry (contract revision 1.34, §4.29): the shape
/// mirrors the §4.20 receive projection — `number` resolves contacts-first
/// (ACI/UUID passthrough), and `start`/`length` are UTF-16 code-unit offsets
/// the caller indexes against its own body (the official BodyRange
/// semantics). A malformed entry fails params deserialization with
/// INVALID_REQUEST before anything else runs.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SendTextMentionParams {
    pub number: String,
    pub start: u32,
    pub length: u32,
}

/// One outbound text-style range (contract revision 1.69, §4.62): the shape
/// mirrors the §4.20 receive projection — `style` must be one of the five
/// protocol-renderable upstream enum values (enforced against the
/// deserialized string in `text_styles_to_upstream`, which owns the error
/// body), and `start`/`length` are UTF-16 code-unit offsets the upstream
/// indexes against the caller's own body (the official BodyRange semantics —
/// identical to the §4.29 mention offsets). A malformed entry fails params
/// deserialization with INVALID_REQUEST before anything else runs.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SendTextStyleParams {
    pub style: String,
    pub start: u32,
    pub length: u32,
}

/// Retry one definitively-failed outgoing text (contract 1.31): addressed by
/// the ORIGINAL send's clientRequestId so both an in-session optimistic row
/// and a reloaded store row hit the same persisted record. No optional forms:
/// a retry has exactly one target.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesRetryTextParams {
    pub account_id: String,
    pub conversation_id: String,
    pub client_request_id: String,
}

/// Outbound link preview (contract §5.10 v1.26). signal-cli's JSON-RPC send
/// carries exactly one preview per message (`previewUrl`/`previewTitle`/
/// `previewDescription`/`previewImage`, verified against the pinned
/// distribution), so only the first entry is used and extras are ignored.
/// The image travels as an RFC 2397 data URI — the same byte-in-memory
/// pattern as `messages.attachments.send`; no caller-controlled path ever
/// reaches upstream.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SendTextPreviewParams {
    pub url: String,
    pub title: String,
    pub description: Option<String>,
    pub image_data_uri: Option<String>,
}

/// Normalized, validated preview passed down to `dispatch_send` and projected
/// 1:1 onto the upstream JSON-RPC keys (`previewUrl`/`previewTitle`/
/// `previewDescription`/`previewImage`).
#[derive(Debug, Clone)]
pub struct UpstreamSendPreview {
    pub url: String,
    pub title: String,
    pub description: Option<String>,
    pub image_data_uri: Option<String>,
}

/// A validated mention passed down to `dispatch_send` and projected 1:1 onto
/// the upstream `mentions` entry keys (`number`/`start`/`length`). Only
/// entries the connector could plausibly resolve — a UUID-shaped ACI, the
/// linked account's own number, or a digit-suffix match of the cached
/// contacts — survive this far; everything else was dropped at validation
/// (the official behavior for unresolvable mention addresses).
#[derive(Debug, Clone)]
pub struct UpstreamSendMention {
    pub number: String,
    pub start: u32,
    pub length: u32,
}

/// A validated text-style range passed down to `dispatch_send` and projected
/// 1:1 onto the upstream `textStyle` entry keys (`style`/`start`/`length`,
/// contract revision 1.69, §4.62 — the pinned `TextStyle.java` enum names).
/// Every entry here passed the style whitelist.
#[derive(Debug, Clone)]
pub struct UpstreamSendTextStyle {
    pub style: String,
    pub start: u32,
    pub length: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendAttachmentParams {
    pub account_id: String,
    pub conversation_id: Option<String>,
    pub kind: Option<String>,
    pub peer_key: Option<String>,
    pub peer_title: Option<String>,
    pub client_request_id: String,
    /// §4.12 singular attachment payload. Mutually exclusive with the §4.59
    /// plural `attachments` array: either form, never both (INVALID_REQUEST).
    /// Required on the singular form.
    pub data_base64: Option<String>,
    /// §4.12 singular declared raw size. Mutually exclusive with the §4.59
    /// plural `attachments` array. Required on the singular form.
    pub size_bytes: Option<u64>,
    /// §4.12 singular display filename. Only legal on the singular form.
    pub filename: Option<String>,
    /// §4.12 singular media type. Only legal on the singular form.
    pub content_type: Option<String>,
    pub text: Option<String>,
    pub quote_message_id: Option<String>,
    /// §4.30 voice note marker: true maps the attachment to the official
    /// VOICE_MESSAGE flag via the `;voice=true` data-URI parameter. Requires
    /// `audio/*` contentType, validated before the pending row exists.
    /// Singular-form only: a plural §4.59 send with voiceNote is
    /// INVALID_REQUEST (a voice note is always a single-attachment message —
    /// the official sender never produces a multi-attachment voice message).
    #[serde(default)]
    pub voice_note: bool,
    /// §4.36 view-once marker: true sends the attachment as an official
    /// view-once message (upstream `send` carries `viewOnce`). Requires
    /// `image/*` or `video/*` contentType, an empty caption, no quote, and no
    /// voiceNote — validated before the pending row exists. Singular-form
    /// only: a plural §4.59 send with viewOnce is INVALID_REQUEST (official
    /// view-once media is a single unadorned medium).
    #[serde(default)]
    pub view_once: bool,
    /// §4.59 plural attachment payloads (contract revision 1.66): one
    /// upstream `send` carries every entry's data URI in its array-typed
    /// `attachments` parameter — the official forward face merges a multi-
    /// attachment message into one upstream message. Mutually exclusive with
    /// the singular `dataBase64`/`sizeBytes`/`filename`/`contentType` keys;
    /// 1..=32 entries (the engine's per-message ceiling); the entries'
    /// combined raw budget equals the singular 100 MiB ceiling so the whole
    /// request still fits one host frame. voiceNote/viewOnce are rejected on
    /// this form.
    #[serde(default)]
    pub attachments: Option<Vec<MessagesSendAttachmentEntry>>,
}

/// One §4.59 plural attachment entry (contract revision 1.66): the §4.12
/// singular budget family per entry — standard padded base64 whose decoded
/// size must equal `sizeBytes`, optional display filename (same path-
/// separator/control-character rejection) and optional `type/subtype` media
/// type. No per-entry voice flag: the `;voice=true` data-URI parameter is a
/// singular-form face only.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendAttachmentEntry {
    pub data_base64: String,
    pub size_bytes: u64,
    pub filename: Option<String>,
    pub content_type: Option<String>,
}

/// `messages.sendSticker` image payload (contract revision 1.35, §4.31): the
/// §4.12 attachment budget family minus filename — a sticker has no display
/// name — with the sticker-specific `image/*` content-type rule enforced in
/// the prepare step.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendStickerImage {
    pub data_base64: String,
    pub size_bytes: u64,
    pub content_type: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// `messages.sendSticker` params (contract revision 1.35, §4.31): §4.12-style
/// addressing (`conversationId` or `kind`+`peerKey`, exactly one form) plus
/// the official `DataMessage.sticker` identity — hex pack id, standard base64
/// pack key, pack-local sticker id, optional emoji label — and the body-less
/// image payload. The upstream call carries no `message` key.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendStickerParams {
    pub account_id: String,
    pub conversation_id: Option<String>,
    pub kind: Option<String>,
    pub peer_key: Option<String>,
    pub peer_title: Option<String>,
    pub client_request_id: String,
    pub pack_id: String,
    pub pack_key: String,
    pub sticker_id: u32,
    pub emoji: Option<String>,
    pub image: MessagesSendStickerImage,
    /// Optional quote target (contract revision 1.37, §4.35) — resolved
    /// through the same ladder as the text face before the pending row.
    pub quote_message_id: Option<String>,
}

/// `stickerPacks.getManifest` params (contract revision 1.36, §4.32): no
/// account context — pack browsing is anonymous CDN traffic engine-side.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StickerPackManifestParams {
    pub pack_id: String,
    pub pack_key: String,
}

/// `stickerPacks.getImage` params (contract revision 1.36, §4.32): same pack
/// identity plus the pack-local sticker id.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StickerPackImageParams {
    pub pack_id: String,
    pub pack_key: String,
    pub sticker_id: u32,
}

/// `conversations.getPinned` params (contract revision 1.36, §4.33).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationsGetPinnedParams {
    pub account_id: String,
}

/// `conversations.setPinned` params (contract revision 1.36, §4.33): the
/// three-value kind enum mirrors the upstream storage reference taxonomy;
/// `pinned` is required (absent fails params deserialization).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationsSetPinnedParams {
    pub account_id: String,
    pub conversation_id: String,
    pub kind: String,
    pub pinned: bool,
}

/// `conversations.setExpireTimer` params (contract revision 1.42, §4.41):
/// `expireSeconds` is required — `0` turns the timer off, there is no
/// toggle form. The u32 type fails negative, fractional and over-range
/// values at params deserialization; the shared engine `int` face is the
/// ceiling the schema mirrors.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationsSetExpireTimerParams {
    pub account_id: String,
    pub conversation_id: String,
    pub expire_seconds: u32,
}

/// Upper bound the connector accepts for a set timer: both upstream faces
/// consume a signed 32-bit integer (`setExpirationTimer.expirationInSeconds`
/// validates within u32 on the engine face, `updateContact.expiration` is a
/// Java `int` on the signal-cli face), so the common denominator is
/// `i32::MAX` seconds — about 68 years, far past any real timer and the value
/// the schema `maximum` mirrors.
pub const MAX_EXPIRE_TIMER_SECONDS: u32 = i32::MAX as u32;

/// `conversations.setExpireTimer` result (contract revision 1.42, §4.41):
/// the timer value the upstream accepted — `0` for the off form.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationsSetExpireTimerResult {
    pub expire_timer_seconds: u64,
}

/// `stickerPacks.getSyncs` params (contract revision 1.37, §4.34).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StickerPackGetSyncsParams {
    pub account_id: String,
}

/// `stickerPacks.setSync` params (contract revision 1.37, §4.34):
/// `installed` is required (absent fails params deserialization); `packKey`
/// is required by the service when `installed` is true, and ignored —
/// forwarded as null — otherwise; `position` carries the cloud ordering for
/// an installed pack. The u32 type already fails non-integer/out-of-range
/// values at params deserialization.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StickerPackSetSyncParams {
    pub account_id: String,
    pub pack_id: String,
    pub pack_key: Option<String>,
    pub installed: bool,
    pub position: Option<u32>,
}

/// `accounts.updateConfiguration` params (contract revision 1.46, §4.45):
/// every settings key is optional — at least one must be present, which the
/// service enforces (the schema cannot express "at least one of"); only the
/// provided keys are forwarded upstream.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountsUpdateConfigurationParams {
    pub account_id: String,
    pub read_receipts: Option<bool>,
    pub typing_indicators: Option<bool>,
    pub phone_number_sharing_mode: Option<String>,
    pub unlisted_phone_number: Option<bool>,
}

/// `accounts.setProfile` params (contract revision 1.46, §4.45): the text
/// face is a full write upstream (an omitted key clears the field), so only
/// `givenName` is meaningful to omit-guard — the optional text keys pass
/// through as-is. `avatar` is the tri-state [`AvatarUpdate`]: captured as
/// `Option<Value>` because absent and explicit null must stay distinct.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountsSetProfileParams {
    pub account_id: String,
    pub given_name: String,
    pub family_name: Option<String>,
    pub about: Option<String>,
    pub emoji: Option<String>,
    pub avatar: Option<Value>,
}

/// `accounts.setExpireTimer` params (contract revision 1.46, §4.45):
/// `expireSeconds` is required — `0` turns the account-wide default off. The
/// u32 type fails negative, fractional and over-range values at params
/// deserialization; the engine face and the official `AccountRecord` field
/// are both uint32.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountsSetExpireTimerParams {
    pub account_id: String,
    pub expire_seconds: u32,
}

/// `conversations.getBlocked` params (contract revision 1.46, §4.45).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationsGetBlockedParams {
    pub account_id: String,
}

/// `accounts.setUsername` params (contract revision 1.59, §4.55): `username`
/// and `deleteUsername` are mutually exclusive — the schema keeps both
/// optional (it cannot express XOR), the service ladder enforces
/// exactly-one before any upstream call. This mirrors the pinned signal-cli
/// `updateAccount` mutually exclusive group (UpdateAccountCommand.java:41-46).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountsSetUsernameParams {
    pub account_id: String,
    pub username: Option<String>,
    #[serde(default)]
    pub delete_username: bool,
}

/// `accounts.getUsername` params (contract revision 1.59, §4.55).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountsGetUsernameParams {
    pub account_id: String,
}

/// `username.lookup` params (contract revision 1.59, §4.55): usernames or
/// username-link URLs — the upstream resolves both (`getUserStatus --username`
/// accepts "the recipient username or username link"). The entries are
/// caller-provided labels echoed verbatim in the result projection.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UsernameLookupParams {
    pub account_id: String,
    pub usernames: Vec<String>,
}

/// `conversations.getAvatar` params (contract revision 1.48, §4.47): the
/// conversation id is the shared 1..256 bound — the engine classifies the
/// reference shape itself (ServiceId UUID / phone number / 32-byte base64
/// group master key), so the connector only bounds the string and resolves
/// the local row.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationsGetAvatarParams {
    pub account_id: String,
    pub conversation_id: String,
}

/// `conversations.setBlocked` params (contract revision 1.48, §4.47): the
/// three-value kind enum mirrors the upstream storage reference taxonomy;
/// `blocked` is required (absent fails params deserialization).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConversationsSetBlockedParams {
    pub account_id: String,
    pub conversation_id: String,
    pub kind: String,
    pub blocked: bool,
}

/// `conversations.acceptRequest` / `conversations.declineRequest` params
/// (contract revision 1.63, §4.56): the addressing pair only — the request
/// kind (accept/decline) is carried by the method choice, mirroring the
/// official desktop's one-button-per-action message-request row.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageRequestParams {
    pub account_id: String,
    pub conversation_id: String,
}

/// `identities.get` params (contract revision 1.68, §4.61): the same
/// addressing pair as the §4.47/§4.56 faces — the identity is addressed by
/// the conversation it belongs to, the way the official details row does.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentitiesGetParams {
    pub account_id: String,
    pub conversation_id: String,
}

/// `identities.verify` params (contract revision 1.68, §4.61): the
/// addressing pair plus the safety number string exactly as the desktop
/// displayed it (spaced 60-digit groups on the modern face) — the digits
/// travel back as the verification confirmation, the official
/// "Mark as verified" semantics. The wire bound mirrors
/// `IDENTITY_SAFETY_NUMBER_MAX_CHARS`; the grammar is the upstream's.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentitiesVerifyParams {
    pub account_id: String,
    pub conversation_id: String,
    pub safety_number: String,
}

/// One projected identity row (contract revision 1.68, §4.61): the pinned
/// upstream `JsonIdentity` fields verbatim (ListIdentitiesCommand.java:88-96)
/// — the formatted safety number string (12 spaced 5-digit groups,
/// Util.java:52-67), the hex fingerprint, the base64 scannable payload when
/// the upstream supplied one, the three-value trust level
/// (UNTRUSTED | TRUSTED_UNVERIFIED | TRUSTED_VERIFIED, TrustLevel.java:5-8),
/// and the add timestamp. `number`/`uuid` are optional exactly as the pinned
/// shape declares: a UUID-only identity row carries a null number and the
/// reverse. The desktop derives the official `isVerified` presentation as
/// `trustLevel === 'TRUSTED_VERIFIED'`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    pub fingerprint: String,
    pub safety_number: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scannable_safety_number: Option<String>,
    pub trust_level: String,
    pub added_timestamp: u64,
}

/// The `identities.get` result (contract revision 1.68, §4.61): the first
/// projected identity row, or `identity: null` when the pinned upstream
/// answered an empty list — the honest no-identity-yet state (the official
/// desktop's SafetyNumberNotReady face; never folded into an error).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityRead {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<IdentityView>,
}

/// The §4.61 result face of `identities.verify`: `{status}` — the pinned
/// upstream `trust` answers an empty result object on success (the command
/// writes no JSON output, TrustCommand.handleCommand), so the outcome verb
/// is the honest whole of what the connector knows.
#[derive(Debug, serde::Serialize)]
pub struct IdentityVerifyOutcome {
    pub status: &'static str,
}

/// The §4.56 result face: `{status}` — the pinned upstream's
/// `sendMessageRequestResponse` answers an empty result object on success,
/// so the outcome verb is the honest whole of what the connector knows.
#[derive(Debug, serde::Serialize)]
pub struct MessageRequestOutcome {
    pub status: &'static str,
}

/// `messages.reportSpam` params (contract revision 1.49, §4.48, capability
/// report-spam): the addressing pair plus the rows to report. The array
/// bounds (1–64 entries of 1–128 characters) fail at params
/// schema mirrors both.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesReportSpamParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_ids: Vec<String>,
}

/// One per-id report failure (contract revision 1.49, §4.48): the caller's
/// message id plus the engine's server status code when the server answered
/// (absent on transport failures — never invented as 0).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportSpamFailure {
    pub message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u64>,
}

/// The `messages.reportSpam` result: the engine `{reported, failures}`
/// projection verbatim, keyed by the caller's message ids.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReportSpamResult {
    pub reported: u64,
    pub failures: Vec<ReportSpamFailure>,
}

/// `messages.reportSpam` constants (contract revision 1.49, §4.48): the
/// KT UI batch bound — one screen's selection, far below the engine's own
/// 1000-per-batch upstream ceiling — and the per-id address bound (the
/// opaqueId family).
pub const REPORT_SPAM_MESSAGE_IDS_LIMIT: usize = 64;

/// The prepared `messages.reportSpam` call (contract revision 1.49, §4.48):
/// the upstream payload plus the ordered id↔serverGuid pairs the result's
/// per-id failures project back onto. Guid-less rows stay out of the
/// upstream list and reappear as failures keyed by their message id.
#[derive(Debug)]
pub struct PreparedReportSpam {
    pub upstream: Value,
    pub pairs: Vec<(String, String)>,
}

/// `calls.sendSignal` params (contract revision 1.53, §4.49): `type` is the
/// five-value RingRTC frame-family enum, `callId` a positive u32. `payload`
/// is the opaque signaling material the connector forwards verbatim —
/// mandatory for the modern faces (offer/answer/ice/opaque all carry
/// RingRTC-serialized bytes) and optional for the legacy frames (busy/hangup
/// ride the official proto bare-frame family `CallMessage{busy{id}}` /
/// `CallMessage{hangup{...}}` with no opaque field — RingRTC 2.72.1 emits
/// them opaque-less, verified in the P1 device experiment). `urgency`
/// (contract 1.53) is the optional urgency pass-through (`droppable` |
/// `handleImmediately`, absent = the engine's droppable default). The
/// engine validates the transport face and the RingRTC layer parses the
/// content; the connector parses neither.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CallsSendSignalParams {
    pub account_id: String,
    pub conversation_id: String,
    #[serde(rename = "type")]
    pub signal_type: String,
    pub call_id: u32,
    pub payload: Option<String>,
    pub urgency: Option<String>,
}

/// `calls.getState` params (contract revision 1.51, §4.49): account-scoped
/// read over the connector's (near-empty) call-state registry.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CallsGetStateParams {
    pub account_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesEditParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub text: String,
    pub client_request_id: Option<String>,
}

#[derive(Debug)]
pub struct PreparedEdit {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub text: String,
    pub params: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesRemoteDeleteParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub operation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendReactionParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub emoji: String,
    #[serde(default)]
    pub remove: bool,
    pub operation_id: Option<String>,
}

/// messages.sendPinMessage params (contract 1.33): the §4.6 reaction
/// addressing triple verbatim plus the optional timed-pin duration. The u32
/// bound is the fail-closed range check — a negative, fractional, or
/// oversized value fails params deserialization with INVALID_REQUEST, and an
/// absent/null value is the official forever pin.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendPinMessageParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub pin_duration_seconds: Option<u32>,
    pub operation_id: Option<String>,
}

/// messages.sendUnpinMessage params (contract 1.33): same addressing, no
/// duration — the official unpinMessage carries only the target identity.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendUnpinMessageParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub operation_id: Option<String>,
}

/// messages.sendAdminDelete params (contract 1.33): same addressing, group
/// conversations only.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendAdminDeleteParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub operation_id: Option<String>,
}

/// messages.markRead / messages.markViewed params (contract revision 1.34):
/// the conversation plus an optional bounded `messageIds` list — absent means
/// every incoming row of the conversation (ascending `sentAt`, bounded 512);
/// explicit ids beyond 512 are truncated, not rejected. No operationId: the
/// request never mutates local state and its outcome never triggers a retry,
/// so there is nothing to correlate.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesMarkReadParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_ids: Option<Vec<String>>,
}

/// messages.markViewed params: identical shape to
/// [`MessagesMarkReadParams`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesMarkViewedParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_ids: Option<Vec<String>>,
}

/// messages.sendViewOnceOpen params (contract revision 1.38, §4.36): the bare
/// engine `sendViewOnceOpen {account, senderAci, timestamp}` passthrough.
/// Receipt-class semantics — the result is `{status: sent|unknown}` and any
/// failure degrades without retry. Outside the desktop flow:
/// `messages.markViewOnceOpened` orchestrates both legs.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesSendViewOnceOpenParams {
    pub account_id: String,
    pub sender_aci: String,
    pub timestamp: u64,
}

/// messages.markViewOnceOpened params (contract revision 1.38, §4.37): the
/// desktop's single trigger point for the view-once flow — the connector
/// burns the row bytes and media locally, emits `message.viewOnceOpened`
/// once, and best-effort orchestrates the official upstream pair (the VIEWED
/// receipt to the author, then the view-once open sync).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesMarkViewOnceOpenedParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
}

/// messages.attachments.open params (ADR 0002): the message-addressing triple
/// plus the attachment id from the message's received metadata. There is no
/// caller-declared sizeBytes: the file size is measured locally and reported
/// back, never trusted from the wire.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesAttachmentsOpenParams {
    pub account_id: String,
    pub conversation_id: String,
    pub message_id: String,
    pub attachment_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesAttachmentsReadChunkParams {
    pub media_handle: String,
    pub offset: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagesAttachmentsCloseHandleParams {
    pub media_handle: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactsSyncParams {
    pub account_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContactsListParams {
    pub account_id: String,
    pub query: Option<String>,
    pub cursor: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GroupsGetParams {
    pub account_id: String,
    pub group_key: String,
}

/// groups.update params (contract revision 1.45, §4.44, capability
/// group-management): every change key is individually optional — an absent
/// key is "no change" upstream, an all-absent change set is refused by the
/// service (`resetLink: false` counts as absent). Member arrays carry the
/// §4.39 roster bounds; the schema mirrors every bound this struct's
/// deserialization plus the service checks enforce.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GroupsUpdateParams {
    pub account_id: String,
    pub conversation_id: String,
    pub group_name: Option<String>,
    pub group_description: Option<String>,
    pub add_members: Option<Vec<String>>,
    pub remove_members: Option<Vec<String>>,
    pub promote_admins: Option<Vec<String>>,
    pub demote_admins: Option<Vec<String>>,
    pub ban_members: Option<Vec<String>>,
    pub unban_members: Option<Vec<String>>,
    pub link_state: Option<String>,
    pub reset_link: Option<bool>,
    pub permission_add_member: Option<String>,
    pub permission_edit_details: Option<String>,
    pub permission_send_messages: Option<String>,
    pub member_label: Option<String>,
    pub member_label_emoji: Option<String>,
    pub operation_id: Option<String>,
}

/// groups.quit params (contract revision 1.45, §4.44, capability
/// group-management): the addressing pair plus the optional audit id.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GroupsQuitParams {
    pub account_id: String,
    pub conversation_id: String,
    pub operation_id: Option<String>,
}

/// groups.terminate params (contract revision 1.52, §4.50, capability
/// group-terminate): the addressing pair plus the optional audit id — the
/// groups.quit shape verbatim; the action itself carries no parameters, so
/// the change set is empty by construction.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GroupsTerminateParams {
    pub account_id: String,
    pub conversation_id: String,
    pub operation_id: Option<String>,
}

/// groups.approveJoinRequests params (contract revision 1.58, §4.53,
/// capability group-join-approve): the addressing pair, the candidate member
/// set (1-1001 entries of 1-128 characters, the §4.39 roster bounds — the
/// §4.44 member-array discipline: an empty array carries no change upstream
/// and is refused by shape), the approve bit (true = approve the join
/// requests, false = refuse them), and the optional audit id.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GroupsApproveJoinRequestsParams {
    pub account_id: String,
    pub conversation_id: String,
    pub members: Vec<String>,
    pub approve: bool,
    pub operation_id: Option<String>,
}

/// groups.revokeInvites (contract revision 1.64, §4.57, capability
/// group-invite-revoke): the addressing pair, the candidate member set
/// (1-1001 entries of 1-128 characters, the §4.39 roster bounds — the
/// §4.44 member-array discipline: an empty array carries no change upstream
/// and is refused by shape), and the optional audit id. No approve bit: the
/// method's only direction is revocation (the official surface's
/// REVOKE_INVITE stage — `PendingInvites.dom.tsx` L57-61), and a
/// re-invite belongs to the add-member face, not to this one.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GroupsRevokeInvitesParams {
    pub account_id: String,
    pub conversation_id: String,
    pub members: Vec<String>,
    pub operation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContactsSetLocalAliasParams {
    pub account_id: String,
    pub peer_key: String,
    pub alias: String,
    pub operation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PresenceSetTypingMessageParams {
    pub account_id: String,
    pub conversation_id: String,
    pub stop: Option<bool>,
    pub operation_id: Option<String>,
}

/// Upstream addressing for one resolved conversation (pinned 0.14.7 JSON-RPC
/// shape): a group conversation addresses `groupId`, every other kind the
/// `recipient` array — the same targeting `send`, `sendTyping`,
/// `remoteDelete`, and `sendReaction` share.
fn set_upstream_target(params: &mut Value, conversation: &ConversationRow) {
    if conversation.kind == "group" {
        params["groupId"] = json!(conversation.peer_key);
    } else {
        params["recipient"] = json!([conversation.peer_key]);
    }
}

fn validate_device_name(device_name: &str) -> Result<(), ServiceError> {
    if device_name.is_empty() || device_name.len() > MAX_DEVICE_NAME_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "deviceName must contain between 1 and 64 bytes",
            false,
        )));
    }
    Ok(())
}

/// Per-engine account ceiling rejection (optimization-plan §6.4 M3.3): the
/// message carries the group dimension and the ceiling; it never carries an
/// account number. Not retryable — freeing capacity needs an explicit
/// `accounts.deleteLocalData`.
pub fn account_limit_error(proxy_group: &str) -> ServiceError {
    ServiceError::Api(ApiError::new(
        "ACCOUNT_LIMIT_REACHED",
        format!(
            "proxy group '{proxy_group}' already holds {MAX_ACCOUNTS_PER_ENGINE} accounts, \
             the per-engine ceiling; delete an account before linking another"
        ),
        false,
    ))
}

/// contacts.setLocalAlias bound (§4.9): a non-empty, short display name.
fn validate_alias(alias: &str) -> Result<(), ServiceError> {
    if alias.is_empty() || alias.len() > MAX_ALIAS_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "alias must contain between 1 and 128 bytes",
            false,
        )));
    }
    Ok(())
}

pub(crate) fn validate_account_delete_operation_id(operation_id: &str) -> Result<(), ServiceError> {
    validate_opaque_id(operation_id, "operationId")
}

fn validate_text(text: &str) -> Result<(), ServiceError> {
    if text.is_empty() || text.len() > MAX_TEXT_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "text must contain between 1 and 65536 bytes",
            false,
        )));
    }
    Ok(())
}

/// Bounds for the outbound link preview (contract §5.10 v1.26). The URL cap
/// follows the de-facto upstream limit; title/description caps mirror the
/// official desktop renderer's truncation headroom; the image cap bounds the
/// data URI to ~1 MiB of binary (4/3 base64 inflation included).
const MAX_PREVIEW_URL_BYTES: usize = 2048;
const MAX_PREVIEW_TITLE_BYTES: usize = 1024;
const MAX_PREVIEW_DESCRIPTION_BYTES: usize = 4096;
const MAX_PREVIEW_IMAGE_DATA_URI_CHARS: usize = 1_572_864;

/// At most one preview per send upstream (signal-cli builds `List.of(one)`);
/// extras are ignored, `None`/empty list means no preview.
fn previews_to_upstream(
    previews: Option<Vec<SendTextPreviewParams>>,
    text: &str,
) -> Result<Option<UpstreamSendPreview>, ServiceError> {
    previews
        .and_then(|list| list.into_iter().next())
        .map(|preview| normalize_send_preview(preview, text))
        .transpose()
}

/// The ACI/UUID wire shape (contract revision 1.34): 36 chars, hyphens at the
/// canonical positions, hex elsewhere. Only this shape passes the mention
/// gate without a contacts lookup — the engine's ServiceId parse accepts
/// exactly it for bare-UUID entries.
fn is_uuid_shaped(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        match index {
            8 | 13 | 18 | 23 => {
                if *byte != b'-' {
                    return false;
                }
            }
            _ => {
                if !byte.is_ascii_hexdigit() {
                    return false;
                }
            }
        }
    }
    true
}

/// The ASCII digits of an address — the engine `resolve_author_aci` matching
/// key (numbers compare as digit-suffix, ignoring formatting).
fn address_digits(value: &str) -> String {
    value.chars().filter(char::is_ascii_digit).collect()
}

/// Validate and normalize the caller-supplied link preview into the exact
/// shape the upstream JSON-RPC `send` expects. Enforced here — not at the
/// engine — because signal-cli validates "the same url must also appear in
/// the message body" only via its CLI help text, and a late upstream failure
/// would surface as an unknown-outcome-shaped error instead of a
/// deterministic INVALID_REQUEST.
fn normalize_send_preview(
    preview: SendTextPreviewParams,
    text: &str,
) -> Result<UpstreamSendPreview, ServiceError> {
    let invalid = |why: &str| {
        Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            why,
            false,
        )))
    };
    let url = preview.url.trim();
    if url.is_empty() || url.len() > MAX_PREVIEW_URL_BYTES {
        return invalid("preview url must contain between 1 and 2048 bytes");
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return invalid("preview url must be an absolute http(s) url");
    }
    // signal-cli: "the same url must also appear in the message body".
    if !text.contains(url) {
        return invalid("preview url must appear in the message text");
    }
    let title = preview.title.trim();
    if title.is_empty() || title.len() > MAX_PREVIEW_TITLE_BYTES {
        return invalid("preview title must contain between 1 and 1024 bytes");
    }
    let description = preview
        .description
        .map(|d| d.trim().to_string())
        .filter(|d| !d.is_empty());
    if let Some(description) = &description
        && description.len() > MAX_PREVIEW_DESCRIPTION_BYTES
    {
        return invalid("preview description must contain at most 4096 bytes");
    }
    if let Some(image) = &preview.image_data_uri {
        if !image.starts_with("data:image/") {
            return invalid("preview image must be a data:image/ rfc2397 data uri");
        }
        if image.len() > MAX_PREVIEW_IMAGE_DATA_URI_CHARS {
            return invalid("preview image data uri exceeds 1.5M chars");
        }
    }
    Ok(UpstreamSendPreview {
        url: url.to_string(),
        title: title.to_string(),
        description,
        image_data_uri: preview.image_data_uri,
    })
}

/// The reaction must be exactly one unicode grapheme cluster of at most 32
/// UTF-8 bytes — the pinned signal-cli requirement ("should be a single
/// unicode grapheme cluster", SendReactionCommand --emoji). Grapheme clusters
/// keep multi-codepoint emoji (ZWJ sequences, skin-tone modifiers, flags)
/// valid while rejecting multi-emoji strings.
fn validate_emoji(emoji: &str) -> Result<(), ServiceError> {
    if emoji.is_empty() || emoji.len() > MAX_EMOJI_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "emoji must contain between 1 and 32 bytes",
            false,
        )));
    }
    if emoji.graphemes(true).count() != 1 {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "emoji must be a single unicode grapheme cluster",
            false,
        )));
    }
    Ok(())
}

/// username.lookup single-entry cap (contract revision 1.59, §4.55): a bound
/// against fat frames — a full username is ≤32 chars and a username link is
/// far below 64.
pub const USERNAME_LOOKUP_ENTRY_MAX_CHARS: usize = 64;

/// The local shape gate for `accounts.setUsername` (contract revision 1.59,
/// §4.55): the official desktop username grammar (`ts/util/Username.dom.ts`
/// `isUsernameValid`) mirrored fail-closed — nickname
/// `[a-zA-Z_][a-zA-Z0-9_]*` 3..32 characters, an optional discriminator
/// `.N` that is all digits, never empty, never `00`-leading, and a
/// `0`-leading form only as exactly two digits. The numeric range above the
/// shape (the libsignal discriminator bounds) stays upstream authority on
/// purpose: the desktop's own validator checks exactly this shape and lets
/// the server answer the rest. The server reserve/confirm is the authority:
/// a shape-valid username can still be taken, which surfaces as
/// UPSTREAM_ERROR.
fn validate_username_shape(username: &str) -> Result<(), ServiceError> {
    let invalid = |reason: &str| ServiceError::Api(ApiError::new("INVALID_REQUEST", reason, false));
    let (nickname, discriminator) = match username.split_once('.') {
        Some((nickname, discriminator)) => (nickname, Some(discriminator)),
        None => (username, None),
    };
    // The desktop grammar's 3..32 window applies to the nickname itself; the
    // whole string is bounded by the schema wire bound (maxLength 32).
    let nickname_length = nickname.chars().count();
    if !(USERNAME_MIN_CHARS..=USERNAME_MAX_CHARS).contains(&nickname_length) {
        return Err(invalid(
            "username nickname must contain between 3 and 32 characters",
        ));
    }
    let mut nickname_chars = nickname.chars();
    match nickname_chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => {
            return Err(invalid(
                "username nickname must start with a letter or underscore",
            ));
        }
    }
    for ch in nickname_chars {
        if !(ch.is_ascii_alphanumeric() || ch == '_') {
            return Err(invalid(
                "username nickname must contain only letters, digits, and underscores",
            ));
        }
    }
    if let Some(discriminator) = discriminator
        && (discriminator.is_empty()
            || !discriminator.bytes().all(|byte| byte.is_ascii_digit())
            || discriminator.starts_with("00")
            || (discriminator.starts_with('0') && discriminator.len() != 2))
    {
        return Err(invalid(
            "username discriminator must be all digits without leading zeros (a single leading zero is allowed only as exactly two digits)",
        ));
    }
    Ok(())
}

/// One message.statusChanged event for a record whose status just moved.
fn status_changed_event(record: &MessageRecord) -> HostSideEvent {
    HostSideEvent::MessageStatusChanged {
        account_id: record.account_id.clone(),
        message_id: record.id.clone(),
        status: record.status,
        delivered_at: record.delivered_at,
        read_at: record.read_at,
    }
}

/// Emit a status event only for a real transition: a replayed terminal write
/// (store reports `false`) or a vanished row produces nothing, so the host
/// never sees a duplicate or phantom status change.
fn status_change_events(updated: Option<(MessageRecord, bool)>) -> Vec<HostSideEvent> {
    match updated {
        Some((record, true)) => vec![status_changed_event(&record)],
        _ => Vec::new(),
    }
}

fn project_message_for_host(mut message: MessageRecord) -> MessageRecord {
    let Some(text) = message.text.as_deref() else {
        return message;
    };
    if text.len() <= MAX_HOST_TEXT_PREVIEW_BYTES {
        return message;
    }
    message.text = Some(truncate_utf8_bytes(text, MAX_HOST_TEXT_PREVIEW_BYTES));
    message.text_truncated = true;
    message
}

fn validate_opaque_id(value: &str, field: &str) -> Result<(), ServiceError> {
    if value.is_empty() || value.len() > MAX_OPAQUE_ID_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            format!("{field} must contain between 1 and {MAX_OPAQUE_ID_BYTES} bytes"),
            false,
        )));
    }
    Ok(())
}

/// Opaque-id validation for faces the supervisor serves directly (the §4.46
/// backup file handles): same ladder as the service-internal callers.
pub fn validate_backup_handle(value: &str) -> Result<(), ServiceError> {
    validate_opaque_id(value, "fileHandle")
}

/// signal-cli `AttachmentStore.sanitizeId` parity (ADR 0002 security gate 3).
/// Upstream resolves an attachment id as
/// `new File(attachmentsPath, sanitizeId(id))` where `sanitizeId` replaces
/// every character outside `[A-Za-z0-9_.-]` with `_`; the connector applies
/// the identical transform before any path join so it can predict the exact
/// file the id names. On top of the transform — which alone would silently
/// launder hostile shapes into harmless-looking names — the shapes that only
/// make sense as traversal are refused outright: any id containing a path
/// separator (`/`, `\`) and, after sanitizing, the empty id, `.` and `..`.
/// Every refusal answers INVALID_REQUEST before the filesystem is touched.
fn sanitize_attachment_id(attachment_id: &str) -> Result<String, ServiceError> {
    if attachment_id.is_empty() || attachment_id.len() > MAX_ATTACHMENT_ID_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachmentId must contain between 1 and 128 bytes",
            false,
        )));
    }
    if attachment_id.contains('/') || attachment_id.contains('\\') {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachmentId must be a bare attachment file name",
            false,
        )));
    }
    let sanitized: String = attachment_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachmentId does not name an attachment file",
            false,
        )));
    }
    Ok(sanitized)
}

/// The upstream base64 must decode to exactly the size the caller declared
/// (contract revision 1.8): anything else means the host budgeted for a
/// different payload. Encoding length is checked first so a hostile upstream
/// response cannot inflate memory before the byte budget is confirmed.
fn validate_attachment_payload(
    base64_data: &str,
    expected_size_bytes: u64,
) -> Result<(), ServiceError> {
    if base64_data.is_empty() {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment payload is empty",
            false,
        )));
    }
    if base64_data.len() > MAX_ATTACHMENT_BASE64_CHARS {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment exceeds the 104857600-byte limit",
            false,
        )));
    }
    let padding = base64_data.len() - base64_data.trim_end_matches('=').len();
    let decoded_len = (base64_data.len() / 4 * 3).saturating_sub(padding);
    if decoded_len as u64 != expected_size_bytes {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment size does not match the declared sizeBytes",
            false,
        )));
    }
    Ok(())
}

/// Validate an attachment send payload (contract revision 1.13,
/// implementation-plan §4.12): the same base64/size discipline as
/// [`validate_attachment_payload`] plus the standard-alphabet shape check —
/// the connector re-encodes the payload into a data URI itself, so a payload
/// that is not canonical standard base64 would corrupt the data URI.
fn validate_attachment_send_payload(
    base64_data: &str,
    expected_size_bytes: u64,
) -> Result<(), ServiceError> {
    validate_attachment_payload(base64_data, expected_size_bytes)?;
    if !base64_data.len().is_multiple_of(4) {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment payload is not canonical base64",
            false,
        )));
    }
    let mut decoded = Vec::with_capacity(expected_size_bytes as usize);
    if base64_decode_to_vec(base64_data, &mut decoded).is_err()
        || decoded.len() as u64 != expected_size_bytes
    {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment payload is not valid base64 or does not match sizeBytes",
            false,
        )));
    }
    Ok(())
}

/// Encode standard padded base64 — the same canonical form
/// `build_attachment_data_uri` re-encodes to and `base64_decode_to_vec`
/// accepts. The chunked media delivery (ADR 0002) uses it per 256 KiB chunk:
/// 349_528 characters worst case, bounded and frame-safe. Public for the
/// §4.46 test suite (the backup chunk encoder shares the alphabet; the
/// function itself stays out of the host surface).
pub(crate) fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let byte1 = chunk.first().copied().unwrap_or_default() as u32;
        let byte2 = chunk.get(1).copied().unwrap_or_default() as u32;
        let byte3 = chunk.get(2).copied().unwrap_or_default() as u32;
        let triple = (byte1 << 16) | (byte2 << 8) | byte3;
        encoded.push(ALPHABET[(triple >> 18) as usize & 0x3F] as char);
        encoded.push(ALPHABET[(triple >> 12) as usize & 0x3F] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(triple >> 6) as usize & 0x3F] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[(triple) as usize & 0x3F] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

/// Decode standard base64 (with padding) into `out`. Returns Err on any
/// non-canonical input; the implementation mirrors the alphabet upstream's
/// java.util.Base64 accepts.
pub(crate) fn base64_decode_to_vec(input: &str, out: &mut Vec<u8>) -> Result<(), ()> {
    const INVALID: u8 = 0xFF;
    fn value(byte: u8) -> u8 {
        match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => INVALID,
        }
    }
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(());
    }
    let padding = bytes.iter().rev().take_while(|&&b| b == b'=').count();
    if padding > 2 {
        return Err(());
    }
    let data_end = bytes.len() - padding;
    // Every remaining byte before padding must be a data character; '=' only
    // allowed in the final quad (already ensured by take_while from the end).
    out.clear();
    out.reserve(data_end / 4 * 3 + 3);
    let mut quad = [0u8; 4];
    let mut quad_len = 0;
    for &byte in &bytes[..data_end] {
        let v = value(byte);
        if v == INVALID {
            return Err(());
        }
        quad[quad_len] = v;
        quad_len += 1;
        if quad_len == 4 {
            out.push((quad[0] << 2) | (quad[1] >> 4));
            out.push((quad[1] << 4) | (quad[2] >> 2));
            out.push((quad[2] << 6) | quad[3]);
            quad_len = 0;
        }
    }
    // Final partial quad: 2 chars -> 1 byte, 3 chars -> 2 bytes; leftover
    // bits must be zero (canonical form).
    match quad_len {
        0 => {}
        2 => {
            if quad[1] & 0x0F != 0 {
                return Err(());
            }
            out.push((quad[0] << 2) | (quad[1] >> 4));
        }
        3 => {
            if quad[2] & 0x03 != 0 {
                return Err(());
            }
            out.push((quad[0] << 2) | (quad[1] >> 4));
            out.push((quad[1] << 4) | (quad[2] >> 2));
        }
        _ => return Err(()),
    }
    Ok(())
}

/// Validate the caller-supplied display filename/contentType bound
/// (1–128 bytes, no control characters). Path separators are rejected for
/// filename so a descriptor can never masquerade as a path fragment.
fn validate_attachment_descriptor(value: Option<&str>, field: &str) -> Result<(), ServiceError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty() || value.len() > MAX_ATTACHMENT_FILENAME_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            format!("{field} must contain between 1 and 128 bytes"),
            false,
        )));
    }
    if value.chars().any(char::is_control) || value.contains('/') || value.contains('\\') {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            format!("{field} must not contain path separators or control characters"),
            false,
        )));
    }
    Ok(())
}

/// Validate an optional RFC 2045 media type (`type/subtype`, both non-empty
/// token shapes). The pinned upstream parses the data URI header loosely, but
/// a malformed content type upstream would surface as UPSTREAM_ERROR after a
/// pending row exists — reject it deterministically here instead.
fn validate_attachment_content_type(value: Option<&str>) -> Result<(), ServiceError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty() || value.len() > MAX_ATTACHMENT_CONTENT_TYPE_BYTES {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "contentType must contain between 1 and 128 bytes",
            false,
        )));
    }
    let valid_token = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
    };
    let (main, sub) = value.split_once('/').ok_or_else(|| {
        ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "contentType must be 'type/subtype'",
            false,
        ))
    })?;
    if !valid_token(main) || !valid_token(sub) {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "contentType contains invalid characters",
            false,
        )));
    }
    Ok(())
}

/// messages.sendSticker pack identity bounds (contract revision 1.35, §4.31).
/// The official pack id serializes hex (even-length — each byte is two hex
/// chars) and the pack key standard base64 (`Bytes.fromBase64`); both pass
/// through verbatim after shape validation, mirroring the official face.
fn validate_sticker_pack_id(pack_id: &str) -> Result<(), ServiceError> {
    let count = pack_id.chars().count();
    if count == 0
        || count > MAX_STICKER_PACK_ID_CHARS
        || !count.is_multiple_of(2)
        || !pack_id.chars().all(|c| c.is_ascii_hexdigit())
    {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "packId must be an even-length hex string of 1-64 characters",
            false,
        )));
    }
    Ok(())
}

fn validate_sticker_pack_key(pack_key: &str) -> Result<(), ServiceError> {
    let count = pack_key.chars().count();
    if count == 0 || count > MAX_STICKER_PACK_KEY_CHARS {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "packKey must contain between 1 and 128 characters",
            false,
        )));
    }
    let mut decoded = Vec::new();
    if base64_decode_to_vec(pack_key, &mut decoded).is_err() {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "packKey must be standard base64",
            false,
        )));
    }
    Ok(())
}

/// Sticker pack browsing pack identity bounds (contract revision 1.36,
/// §4.32). Deliberately stricter than the 1.35 send projection: the browse
/// face aligns with the official `isPackIdValid` — exactly 32 hex characters
/// (`Stickers.preload.ts`) — and the pack key must decode to exactly 32
/// bytes (the HKDF `"Sticker Pack"` input the engine feeds
/// `Crypto.node.ts`-style). Both validate shape only; the connector never
/// interprets the key material.
fn validate_sticker_browse_pack_id(pack_id: &str) -> Result<(), ServiceError> {
    let count = pack_id.chars().count();
    if count != STICKER_PACK_BROWSE_ID_HEX_CHARS || !pack_id.chars().all(|c| c.is_ascii_hexdigit())
    {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "packId must be exactly 32 hex characters",
            false,
        )));
    }
    Ok(())
}

fn validate_sticker_browse_pack_key(pack_key: &str) -> Result<(), ServiceError> {
    let count = pack_key.chars().count();
    // 32 bytes are exactly 44 padded base64 characters; the count guard
    // rejects absurd sizes before the decode, the decode decides validity.
    if count == 0 || count > MAX_STICKER_PACK_KEY_CHARS {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "packKey must contain between 1 and 128 characters",
            false,
        )));
    }
    let mut decoded = Vec::new();
    match base64_decode_to_vec(pack_key, &mut decoded) {
        Ok(()) if decoded.len() == STICKER_PACK_BROWSE_KEY_BYTES => Ok(()),
        _ => Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "packKey must be standard base64 decoding to exactly 32 bytes",
            false,
        ))),
    }
}

/// `conversations.setPinned` bounds (contract revision 1.36, §4.33): the
/// kind enum is the upstream reference taxonomy and the conversation id
/// bound is wider than the local opaqueId family because the host may
/// address a conversation not yet materialized locally.
fn validate_pinned_kind(kind: &str) -> Result<(), ServiceError> {
    if !matches!(kind, "contact" | "group" | "legacyGroup") {
        return Err(ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "kind must be 'contact', 'group', or 'legacyGroup'",
            false,
        )));
    }
    Ok(())
}

/// Project the engine's pinned list onto the wire shape with the KT entry
/// cap (§4.33): order is cloud order, entries beyond 128 are truncated —
/// the connector adds no ordering of its own.
pub(crate) fn pinned_conversations_result(entries: &[Value]) -> PinnedConversations {
    let pinned = entries
        .iter()
        .take(MAX_PINNED_CONVERSATIONS)
        .filter_map(|entry| {
            let conversation_id = entry.get("conversationId").and_then(Value::as_str)?;
            let kind = entry.get("kind").and_then(Value::as_str)?;
            Some(PinnedConversation {
                conversation_id: conversation_id.to_string(),
                kind: kind.to_string(),
            })
        })
        .collect();
    PinnedConversations { pinned }
}

/// Project the engine's sticker-pack record list onto the wire shape with the
/// KT entry cap (§4.34): order is cloud order, entries beyond 256 are
/// truncated — the connector adds no ordering of its own. `packId` is the
/// identity field (missing it drops the entry); the optional fields project
/// leniently beside it, and an out-of-u32 `position` degrades to absent
/// rather than discarding an otherwise-identified record.
pub(crate) fn sticker_pack_syncs_result(entries: &[Value]) -> StickerPackSyncs {
    let packs = entries
        .iter()
        .take(MAX_STICKER_PACK_SYNCS)
        .filter_map(|entry| {
            let pack_id = entry.get("packId").and_then(Value::as_str)?;
            Some(StickerPackSync {
                pack_id: pack_id.to_string(),
                pack_key: entry
                    .get("packKey")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                position: entry
                    .get("position")
                    .and_then(Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok()),
                deleted_at_timestamp_ms: entry.get("deletedAtTimestampMs").and_then(Value::as_u64),
            })
        })
        .collect();
    StickerPackSyncs { packs }
}

/// Project the engine's blocked list onto the wire shape with the KT entry
/// cap (§4.45): order is storage order, entries beyond 256 are truncated —
/// the connector adds no ordering of its own. Entries missing identity or
/// kind drop, mirroring the pinned projection (§4.33).
pub(crate) fn blocked_conversations_result(entries: &[Value]) -> BlockedConversations {
    let blocked = entries
        .iter()
        .take(MAX_BLOCKED_CONVERSATIONS)
        .filter_map(|entry| {
            let conversation_id = entry.get("conversationId").and_then(Value::as_str)?;
            let kind = entry.get("kind").and_then(Value::as_str)?;
            Some(BlockedConversation {
                conversation_id: conversation_id.to_string(),
                kind: kind.to_string(),
            })
        })
        .collect();
    BlockedConversations { blocked }
}

/// Project the engine's `listIdentities` answer onto the wire shape (contract
/// revision 1.68, §4.61): the pinned upstream writes one JSON array of
/// `JsonIdentity` records (ListIdentitiesCommand.java:68-84); the read is
/// narrowed to one recipient upstream, so the first record is the whole
/// answer and an empty array is the honest `identity: null` (the official
/// SafetyNumberNotReady face — a state, not an error). Fields the pinned
/// record declares nullable (`number`, `uuid`, `scannableSafetyNumber`)
/// pass through as absent; a record missing its non-nullable identity core
/// (fingerprint, safetyNumber, trustLevel, addedTimestamp) is a protocol
/// defect, not a no-identity answer.
pub(crate) fn identity_from_engine(result: &Value) -> Result<IdentityRead, ServiceError> {
    let empty: [Value; 0] = [];
    let entries = result
        .as_array()
        .map_or(&empty[..], |array| array.as_slice());
    let Some(entry) = entries.first() else {
        return Ok(IdentityRead { identity: None });
    };
    let fingerprint = entry
        .get("fingerprint")
        .and_then(Value::as_str)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    let safety_number = entry
        .get("safetyNumber")
        .and_then(Value::as_str)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    let trust_level = entry
        .get("trustLevel")
        .and_then(Value::as_str)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    let added_timestamp = entry
        .get("addedTimestamp")
        .and_then(Value::as_u64)
        .ok_or(ServiceError::Engine(EngineError::Protocol))?;
    Ok(IdentityRead {
        identity: Some(IdentityView {
            number: entry
                .get("number")
                .and_then(Value::as_str)
                .map(str::to_string),
            uuid: entry
                .get("uuid")
                .and_then(Value::as_str)
                .map(str::to_string),
            fingerprint: fingerprint.to_string(),
            safety_number: safety_number.to_string(),
            scannable_safety_number: entry
                .get("scannableSafetyNumber")
                .and_then(Value::as_str)
                .map(str::to_string),
            trust_level: trust_level.to_string(),
            added_timestamp,
        }),
    })
}

/// `accounts.setProfile` avatar gate (contract revision 1.46, §4.45): the
/// data URI must be the image family, canonical padded base64, non-empty and
/// within [`MAX_AVATAR_BYTES`] decoded — the engine's stated avatar budget
/// (AvatarHelper-scale), checked here so the host gets the deterministic
/// `INVALID_REQUEST` before any upstream call instead of the engine's
/// post-dial `-32603`. The validated URI forwards verbatim; the connector
/// never decodes image bytes beyond the budget check and never persists them.
fn validate_avatar_data_uri(data_uri: &str) -> Result<String, ServiceError> {
    let invalid = || {
        ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "avatar must be a data:image/*;base64, data URI within 10485760 decoded bytes",
            false,
        ))
    };
    let rest = data_uri.strip_prefix("data:image/").ok_or_else(invalid)?;
    let (subtype, payload) = rest.split_once(";base64,").ok_or_else(invalid)?;
    if subtype.is_empty() || subtype.contains(',') {
        return Err(invalid());
    }
    if payload.is_empty() || payload.len() % 4 != 0 {
        return Err(invalid());
    }
    let mut decoded = Vec::with_capacity(payload.len() / 4 * 3);
    if base64_decode_to_vec(payload, &mut decoded).is_err()
        || decoded.is_empty()
        || decoded.len() > MAX_AVATAR_BYTES
    {
        return Err(invalid());
    }
    Ok(data_uri.to_string())
}

/// Build the RFC 2397 data URI the pinned upstream accepts
/// (AttachmentHelper: `data:<mime>;filename=<name>;base64,<payload>`). The
/// payload is re-encoded from the already-validated base64 — decoding first
/// and re-encoding keeps the URI byte-exact even if the host sent a base64
/// variant with different padding; the round-trip was verified in
/// [`validate_attachment_send_payload`]. `voice` appends the `;voice=true`
/// parameter (§4.30), which the engine maps to the official VOICE_MESSAGE
/// attachment flag — the wire shape the official desktop recorder emits.
fn build_attachment_data_uri(
    base64_data: &str,
    filename: Option<&str>,
    content_type: Option<&str>,
    voice: bool,
) -> Result<String, ServiceError> {
    let mut decoded = Vec::with_capacity(base64_data.len() / 4 * 3);
    base64_decode_to_vec(base64_data, &mut decoded).map_err(|_| {
        ServiceError::Api(ApiError::new(
            "INVALID_REQUEST",
            "attachment payload is not valid base64",
            false,
        ))
    })?;
    use std::fmt::Write as _;
    let mut uri = String::with_capacity(decoded.len() / 3 * 4 + 64);
    uri.push_str("data:");
    uri.push_str(content_type.unwrap_or("application/octet-stream"));
    if voice {
        uri.push_str(";voice=true");
    }
    if let Some(filename) = filename {
        uri.push_str(";filename=");
        // encodeURIComponent semantics for the filename parameter.
        for byte in filename.bytes() {
            let c = byte as char;
            if c.is_ascii_alphanumeric() || "-_.!~*'()".contains(c) {
                uri.push(c);
            } else {
                let _ = write!(uri, "%{byte:02X}");
            }
        }
    }
    uri.push_str(";base64,");
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in decoded.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        uri.push(ALPHABET[(b[0] >> 2) as usize] as char);
        uri.push(ALPHABET[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
        if chunk.len() > 1 {
            uri.push(ALPHABET[(((b[1] & 0x0F) << 2) | (b[2] >> 6)) as usize] as char);
        } else {
            uri.push('=');
        }
        if chunk.len() > 2 {
            uri.push(ALPHABET[(b[2] & 0x3F) as usize] as char);
        } else {
            uri.push('=');
        }
    }
    Ok(uri)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::engine::{NormalizedAttachment, NormalizedQuote, ReceiptKind};
    use crate::store::StoreKey;

    fn service() -> (TempDir, ConnectorService) {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(StoreKey::from_bytes([0x5A; 32]))).unwrap();
        (temp, ConnectorService::new(Arc::new(store)))
    }

    /// A minimal incoming `MessageRecord` for report-spam preparation tests;
    /// `server_guid` stays None until the test stamps one.
    fn expire_record_fixture(account_id: &str, conversation_id: &str, id: &str) -> MessageRecord {
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
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        }
    }

    #[test]
    fn cancelled_link_cannot_complete_or_create_an_account() {
        let (_temp, mut service) = service();
        let started = service
            .begin_link("KT".into(), "sgnl://link?test".into())
            .unwrap();
        let link_session_id = started["linkSessionId"].as_str().unwrap().to_string();

        service.cancel_link(&link_session_id).unwrap();
        let late = service.complete_link_session(
            &link_session_id,
            "+15555550100",
            crate::DEFAULT_PROXY_GROUP_ID,
        );

        assert!(matches!(late, Err(ServiceError::Api(_))));
        assert!(
            service
                .list_accounts_in_group(crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
    }

    /// Per-engine account ceiling (optimization-plan §6.4 M3.3, decision D3):
    /// the eighth account in a group links; the ninth is refused with
    /// ACCOUNT_LIMIT_REACHED naming the group, and the refusal leaves the
    /// store untouched.
    #[test]
    fn eighth_account_links_but_a_ninth_is_refused_at_the_ceiling() {
        let (_temp, mut service) = service();
        let group = crate::DEFAULT_PROXY_GROUP_ID;
        let link_next = |service: &mut ConnectorService| {
            let number = format!(
                "+1556555{:04}",
                service.list_accounts_in_group(group).unwrap().len() + 1
            );
            let started = service
                .begin_link("KT".into(), "sgnl://link?test".into())
                .unwrap();
            let id = started["linkSessionId"].as_str().unwrap().to_string();
            service.complete_link_session(&id, &number, group)
        };

        for index in 1..=8 {
            let account =
                link_next(&mut service).unwrap_or_else(|e| panic!("link {index} failed: {e}"));
            assert_eq!(account.proxy_group, group);
        }
        assert_eq!(service.list_accounts_in_group(group).unwrap().len(), 8);
        assert!(service.group_at_account_ceiling(group).unwrap());

        let refused = link_next(&mut service).unwrap_err();
        let ServiceError::Api(api) = refused else {
            panic!("expected an api error at the ceiling");
        };
        assert_eq!(api.code, "ACCOUNT_LIMIT_REACHED");
        assert!(!api.retryable);
        assert!(
            api.message.contains(group),
            "message must name the group: {}",
            api.message
        );
        assert_eq!(service.list_accounts_in_group(group).unwrap().len(), 8);
    }

    /// The ceiling refuses additions, not updates: at a full group, finishing
    /// a link for a number already bound there completes as an upsert instead
    /// of an ACCOUNT_LIMIT_REACHED (store-level guard, M3.3).
    #[test]
    fn relinking_an_existing_number_at_the_ceiling_still_completes() {
        let (_temp, mut service) = service();
        let group = "team-a";
        for index in 1..=8 {
            let started = service
                .begin_link("KT".into(), "sgnl://link?test".into())
                .unwrap();
            let id = started["linkSessionId"].as_str().unwrap().to_string();
            service
                .complete_link_session(&id, &format!("+1556555{index:04}"), group)
                .unwrap();
        }

        let started = service
            .begin_link("KT".into(), "sgnl://link?test".into())
            .unwrap();
        let id = started["linkSessionId"].as_str().unwrap().to_string();
        let relinked = service
            .complete_link_session(&id, "+15565550008", group)
            .expect("relink of an existing number is an update, not an addition");
        assert_eq!(relinked.proxy_group, group);
        assert_eq!(service.list_accounts_in_group(group).unwrap().len(), 8);
    }

    #[test]
    fn active_link_is_rejected_before_requesting_another_upstream_uri() {
        let (_temp, mut service) = service();
        service
            .begin_link("KT".into(), "sgnl://link?test".into())
            .unwrap();

        assert!(matches!(
            service.ensure_link_available(),
            Err(ServiceError::Api(_))
        ));
    }

    /// An expired session answers the definite LINK_EXPIRED (retryable=false,
    /// same class as every other definite finish failure) and is consumed by
    /// that answer; only afterwards does the id read back as LINK_NOT_FOUND.
    #[test]
    fn expired_finish_answers_link_expired_once_then_reports_not_found() {
        let (_temp, mut service) = service();
        let link_session_id = service.plant_link_session_for_test("KT-Expired", Duration::ZERO);

        let error = service.peek_link_for_finish(&link_session_id).unwrap_err();
        let ServiceError::Api(api) = error else {
            panic!("expected an api error for the expired finish");
        };
        assert_eq!(api.code, "LINK_EXPIRED");
        assert!(!api.retryable);

        let again = service.peek_link_for_finish(&link_session_id).unwrap_err();
        let ServiceError::Api(api) = again else {
            panic!("expected an api error for the consumed session");
        };
        assert_eq!(api.code, "LINK_NOT_FOUND");

        // begin_link over an expired session must also succeed (expiry frees
        // the slot), keeping one deterministic terminal state per session.
        service
            .begin_link("KT-Again".into(), "sgnl://link?next".into())
            .expect("an expired session must not block a fresh link");
    }

    #[test]
    fn deleting_an_account_does_not_cancel_another_session_link() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let started = service
            .begin_link("KT-B".into(), "sgnl://link?second".into())
            .unwrap();
        let link_session_id = started["linkSessionId"].as_str().unwrap().to_string();

        assert!(
            service
                .store_ref()
                .delete_account_cascade(&account.id)
                .unwrap()
        );
        assert!(
            !service
                .store_ref()
                .delete_account_cascade(&account.id)
                .unwrap()
        );
        assert!(service.cancel_link(&link_session_id).is_ok());
    }

    /// Contract 1.21: marking an unlinked device persists `device_unlinked`
    /// and surfaces an account.changed event; unknown numbers are a no-op.
    #[test]
    fn mark_account_device_unlinked_persists_and_emits_account_changed() {
        let (_temp, service) = service();
        let group = crate::DEFAULT_PROXY_GROUP_ID;
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], group)
            .unwrap()
            .remove(0);

        let events = service
            .mark_account_device_unlinked("+15555550100")
            .unwrap();
        assert_eq!(events.len(), 1);
        let HostSideEvent::AccountChanged(changed) = &events[0] else {
            panic!("expected an account.changed event");
        };
        assert_eq!(changed.id, account.id);
        assert_eq!(changed.state, "device_unlinked");

        let listed = service
            .list_accounts_in_group(group)
            .unwrap()
            .into_iter()
            .find(|item| item.id == account.id)
            .unwrap();
        assert_eq!(listed.state, "device_unlinked");

        // A number with no account row is a silent no-op.
        assert!(
            service
                .mark_account_device_unlinked("+15555559999")
                .unwrap()
                .is_empty()
        );
    }

    /// The wire error for a rejected credential is ACCOUNT_UNLINKED
    /// (retryable=false) — distinct from the generic retryable upstream
    /// error, so hosts can distinguish "device was unlinked" from noise.
    #[test]
    fn unauthorized_engine_errors_map_to_account_unlinked() {
        let api = ServiceError::Engine(EngineError::Unauthorized).into_api();
        assert_eq!(api.code, "ACCOUNT_UNLINKED");
        assert!(!api.retryable);
    }

    /// Structured engine error mapping (contract 1.36, §4.32/§4.33): each
    /// allowlisted engine code maps to the same-named connector code with
    /// the contract-fixed retryability; the mapping is total over the
    /// engine.rs allowlist, so no allowlisted code can fall through to the
    /// generic answer.
    #[test]
    fn structured_engine_errors_map_to_same_named_connector_codes() {
        let deterministic = [
            "STICKER_PACK_KEY_INVALID",
            "STICKER_IMAGE_TOO_LARGE",
            "CONVERSATION_NOT_RESOLVED",
            // §4.47: the honest no-avatar answer and the deterministic
            // over-cap refusal are answers, not failures.
            "AVATAR_UNAVAILABLE",
            "AVATAR_TOO_LARGE",
        ];
        for code in deterministic {
            let api =
                ServiceError::Engine(EngineError::UpstreamCode { code, data: None }).into_api();
            assert_eq!(api.code, code, "{code}");
            assert!(!api.retryable, "{code}");
        }
        let retryable = [
            "STICKER_PACK_FETCH_FAILED",
            "STICKER_PACK_MALFORMED",
            "STORAGE_UNAVAILABLE",
            "STORAGE_WRITE_FAILED",
            "STORAGE_READ_FAILED",
            // §4.47: a network/download/decrypt failure may succeed on a
            // later attempt.
            "AVATAR_FETCH_FAILED",
        ];
        for code in retryable {
            let api =
                ServiceError::Engine(EngineError::UpstreamCode { code, data: None }).into_api();
            assert_eq!(api.code, code, "{code}");
            assert!(api.retryable, "{code}");
        }
        // The two allowlists must stay in lockstep: every code the engine
        // layer can emit must map to itself here.
        for code in crate::engine::ENGINE_STRUCTURED_ERROR_CODES_FOR_TESTS {
            let api =
                ServiceError::Engine(EngineError::UpstreamCode { code, data: None }).into_api();
            assert_eq!(api.code, *code, "unmapped engine code {code}");
        }
    }

    #[test]
    fn profile_upload_failed_carries_engine_data_verbatim() {
        // Contract 1.50: the partial-success data object rides through and
        // drives the retryable bit (the engine's truth, not the connector's).
        let api = ServiceError::Engine(EngineError::UpstreamCode {
            code: "PROFILE_UPLOAD_FAILED",
            data: Some(json!({ "retryable": true, "textSaved": true })),
        })
        .into_api();
        assert_eq!(api.code, "PROFILE_UPLOAD_FAILED");
        assert!(api.retryable);
        assert_eq!(
            api.data,
            Some(json!({ "retryable": true, "textSaved": true }))
        );

        // textSaved=false keeps the payload (the desktop's plain-error
        // branch reads it) — retryable re-read from the data object.
        let api = ServiceError::Engine(EngineError::UpstreamCode {
            code: "PROFILE_UPLOAD_FAILED",
            data: Some(json!({ "retryable": true, "textSaved": false })),
        })
        .into_api();
        assert_eq!(
            api.data,
            Some(json!({ "retryable": true, "textSaved": false }))
        );

        // A data-less older-engine face: the fixed retryable=true face, no
        // data member.
        let api = ServiceError::Engine(EngineError::UpstreamCode {
            code: "PROFILE_UPLOAD_FAILED",
            data: None,
        })
        .into_api();
        assert_eq!(api.code, "PROFILE_UPLOAD_FAILED");
        assert!(api.retryable);
        assert!(api.data.is_none());

        // Any other code never gains a data member even when the engine
        // sent one — the payload face is PROFILE_UPLOAD_FAILED-only.
        let api = ServiceError::Engine(EngineError::UpstreamCode {
            code: "STICKER_PACK_FETCH_FAILED",
            data: Some(json!({ "secret": 1 })),
        })
        .into_api();
        assert_eq!(api.code, "STICKER_PACK_FETCH_FAILED");
        assert!(api.data.is_none());
    }

    #[test]
    fn text_limit_is_enforced_by_utf8_bytes() {
        assert!(validate_text(&"a".repeat(MAX_TEXT_BYTES)).is_ok());
        assert!(validate_text(&"a".repeat(MAX_TEXT_BYTES + 1)).is_err());

        assert!(validate_text(&"界".repeat(21_845)).is_ok());
        assert!(validate_text(&"界".repeat(21_846)).is_err());

        assert!(validate_text(&"😀".repeat(16_384)).is_ok());
        assert!(validate_text(&format!("{}a", "😀".repeat(16_384))).is_err());
        assert!(validate_text("").is_err());
    }

    #[test]
    fn inbound_long_text_is_projected_and_fetched_on_demand() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let full_text = "界".repeat(4_000);
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(10),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    source_uuid: None,
                    peer_name: Some("Peer".into()),
                    group_id: None,
                    text: Some(full_text.clone()),
                    text_bytes: None,
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let projected = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        assert!(projected.text.as_ref().unwrap().len() <= MAX_HOST_TEXT_PREVIEW_BYTES);
        assert!(projected.text_truncated);
        assert!(projected.text_retrievable);

        let fetched = service
            .get_message_text(&account.id, &projected.conversation_id, &projected.id)
            .unwrap();
        assert_eq!(fetched.text, full_text);
        assert_eq!(fetched.text_bytes, 12_000);

        let other_account = service
            .sync_accounts_from_numbers(
                &["+15555550100".into(), "+15555550102".into()],
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.id != account.id)
            .unwrap();
        assert!(matches!(
            service.get_message_text(&other_account.id, &projected.conversation_id, &projected.id,),
            Err(ServiceError::Store(StoreError::MessageNotFound))
        ));
        assert!(matches!(
            service.get_message_text(&"x".repeat(129), "conversation", "message"),
            Err(ServiceError::Api(error)) if error.code == "INVALID_REQUEST"
        ));
    }

    #[test]
    fn inbound_text_above_receive_ceiling_keeps_only_an_explicit_preview() {
        let (_temp, mut service) = service();
        service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let oversized = "a".repeat(MAX_INBOUND_TEXT_BYTES + 1);
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(11),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    source_uuid: None,
                    peer_name: None,
                    group_id: None,
                    text: Some(oversized),
                    text_bytes: None,
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let projected = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            projected.text.as_ref().unwrap().len(),
            MAX_HOST_TEXT_PREVIEW_BYTES
        );
        assert_eq!(
            projected.text_bytes,
            Some((MAX_INBOUND_TEXT_BYTES + 1) as u32)
        );
        assert!(projected.text_truncated);
        assert!(!projected.text_retrievable);
        assert!(matches!(
            service.get_message_text(
                &projected.account_id,
                &projected.conversation_id,
                &projected.id,
            ),
            Err(ServiceError::Api(error)) if error.code == "CAPABILITY_UNAVAILABLE"
        ));
    }

    #[test]
    fn send_text_projects_first_preview_onto_upstream_params() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let dispatch = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "look at https://example.com/a now",
                "req-preview-ok",
                None,
                Some(vec![
                    SendTextPreviewParams {
                        url: "https://example.com/a".into(),
                        title: "Example".into(),
                        description: Some("A page".into()),
                        image_data_uri: Some("data:image/jpeg;base64,QUJD".into()),
                    },
                    // extras are ignored upstream (List.of(one) on the pinned
                    // signal-cli) — the connector only forwards the first.
                    SendTextPreviewParams {
                        url: "https://example.com/b".into(),
                        title: "Second".into(),
                        description: None,
                        image_data_uri: None,
                    },
                ]),
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { params, .. } => params,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        assert_eq!(dispatch["previewUrl"], json!("https://example.com/a"));
        assert_eq!(dispatch["previewTitle"], json!("Example"));
        assert_eq!(dispatch["previewDescription"], json!("A page"));
        assert_eq!(
            dispatch["previewImage"],
            json!("data:image/jpeg;base64,QUJD")
        );
    }

    #[test]
    fn send_text_preview_rejects_deterministic_shape_violations() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let preview = |url: &str, title: &str| SendTextPreviewParams {
            url: url.into(),
            title: title.into(),
            description: None,
            image_data_uri: None,
        };
        // signal-cli requires the preview url to appear in the message body —
        // enforced here so the failure is a deterministic INVALID_REQUEST
        // instead of a late upstream send error.
        let url_absent = service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "no link in here",
                "req-preview-absent",
                None,
                Some(vec![preview("https://example.com/a", "Example")]),
                None,
                None,
            )
            .unwrap_err();
        assert!(matches!(url_absent, ServiceError::Api(error) if error.code == "INVALID_REQUEST"));
        // A preview image is a data:image/ URI only — never a caller path.
        let path_image = service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "see https://example.com/a",
                "req-preview-path",
                None,
                Some(vec![SendTextPreviewParams {
                    url: "https://example.com/a".into(),
                    title: "Example".into(),
                    description: None,
                    image_data_uri: Some("/etc/passwd".into()),
                }]),
                None,
                None,
            )
            .unwrap_err();
        assert!(matches!(path_image, ServiceError::Api(error) if error.code == "INVALID_REQUEST"));
        // Title is mandatory upstream.
        let empty_title = service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "see https://example.com/a",
                "req-preview-title",
                None,
                Some(vec![preview("https://example.com/a", "   ")]),
                None,
                None,
            )
            .unwrap_err();
        assert!(matches!(empty_title, ServiceError::Api(error) if error.code == "INVALID_REQUEST"));
        // A rejected preview leaves no pending row behind.
        assert!(
            service
                .store_ref()
                .message_by_client_request(&account.id, "req-preview-absent")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn retry_text_rearms_the_same_failed_row_and_guards_states() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();

        // First send: prepare inserts the pending row, then it fails for real.
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "to retry",
                "retry-req-1",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service.complete_send_failed(&pending_id).unwrap();

        // Retry re-dispatches the SAME row — no second row is inserted.
        let params = match service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-1")
            .unwrap()
        {
            PreparedSend::Dispatch {
                pending_id: retry_pending_id,
                params,
                ..
            } => {
                assert_eq!(retry_pending_id, pending_id);
                params
            }
            PreparedSend::Existing(_) => panic!("retry must re-dispatch the same row"),
        };
        assert_eq!(params["message"], "to retry");
        let row = service
            .store_ref()
            .message_by_client_request(&account.id, "retry-req-1")
            .unwrap()
            .unwrap();
        assert_eq!(row.id, pending_id);
        assert_eq!(row.status, "pending");

        // Settlement completes the SAME row; the history stays one row.
        let (sent, _) = service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 777)
            .unwrap();
        assert_eq!(sent.status, "sent");
        let rows = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, pending_id);

        // A settled row answers RETRY_NOT_ALLOWED.
        let sent_retry = service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-1")
            .unwrap_err();
        assert!(
            matches!(sent_retry, ServiceError::Api(ref error) if error.code == "RETRY_NOT_ALLOWED")
        );

        // An unknown-outcome row is never retryable (no automatic or blind
        // resend of an unsettled wire outcome).
        let unknown_pending = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "unknown outcome",
                "retry-req-2",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service.complete_send_unknown(&unknown_pending).unwrap();
        let unknown_retry = service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-2")
            .unwrap_err();
        assert!(
            matches!(unknown_retry, ServiceError::Api(ref error) if error.code == "RETRY_NOT_ALLOWED")
        );

        // A row already in flight answers RETRY_IN_FLIGHT (double click).
        service.complete_send_failed(&unknown_pending).unwrap();
        service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-2")
            .unwrap();
        let inflight = service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-2")
            .unwrap_err();
        assert!(
            matches!(inflight, ServiceError::Api(ref error) if error.code == "RETRY_IN_FLIGHT")
        );

        // Unknown clientRequestId -> MESSAGE_NOT_FOUND; a row addressed from
        // another conversation answers INVALID_REQUEST.
        let missing = service
            .prepare_retry_text(&account.id, &conversation.id, "retry-req-absent")
            .unwrap_err();
        assert!(matches!(
            missing,
            ServiceError::Store(StoreError::MessageNotFound)
        ));
        let other = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550103", "Peer2")
            .unwrap();
        let cross = service
            .prepare_retry_text(&account.id, &other.id, "retry-req-1")
            .unwrap_err();
        assert!(matches!(cross, ServiceError::Api(ref error) if error.code == "INVALID_REQUEST"));
    }

    /// Contract 1.69 outbound text styles (§4.62): more than 64 entries fail
    /// closed INVALID_REQUEST (non-retryable, no silent truncation — a caller
    /// bug must surface, not vanish), an unknown style vocabulary fails the
    /// whole request, a valid list projects 1:1 onto the engine `send`
    /// `textStyle` entry keys (style/start/length), an empty list leaves the
    /// upstream params without a `textStyle` key at all (byte-identical
    /// legacy wire), a styled pending row persists its ranges so the sender's
    /// bubble keeps its formatting across restarts, and a retry rebuilds the
    /// send without a `textStyle` key (documented deviation, §4.29 mentions
    /// precedent — the ranges are not persisted for retry rebuilds).
    #[test]
    fn send_text_styles_validate_project_persist_and_retry_bare() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let style = |style: &str, start: u32, length: u32| SendTextStyleParams {
            style: style.to_string(),
            start,
            length,
        };
        let dispatch = |prepared: PreparedSend| match prepared {
            PreparedSend::Dispatch { params, .. } => params,
            PreparedSend::Existing(_) => panic!("a fresh client request must dispatch"),
        };

        // Over-cap: deterministic INVALID_REQUEST, never a silent truncation.
        let over_cap = service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "styled",
                "req-style-cap",
                None,
                None,
                None,
                Some((0..65).map(|index| style("BOLD", index, 1)).collect()),
            )
            .unwrap_err();
        assert!(
            matches!(&over_cap, ServiceError::Api(error) if error.code == "INVALID_REQUEST" && !error.retryable)
        );
        // A rejected style list leaves no pending row behind.
        assert!(
            service
                .store_ref()
                .message_by_client_request(&account.id, "req-style-cap")
                .unwrap()
                .is_none()
        );

        // Unknown style vocabulary: fail closed, no row.
        let unknown_style = service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "styled",
                "req-style-unknown",
                None,
                None,
                None,
                Some(vec![style("BLINK", 0, 2)]),
            )
            .unwrap_err();
        assert!(
            matches!(&unknown_style, ServiceError::Api(error) if error.code == "INVALID_REQUEST" && !error.retryable)
        );
        assert!(
            service
                .store_ref()
                .message_by_client_request(&account.id, "req-style-unknown")
                .unwrap()
                .is_none()
        );

        // Valid list projects 1:1 onto the engine textStyle entry keys.
        let projected = dispatch(
            service
                .prepare_send_text(
                    &account.id,
                    &conversation.id,
                    "styled body",
                    "req-style-ok",
                    None,
                    None,
                    None,
                    Some(vec![
                        style("BOLD", 0, 6),
                        style("SPOILER", 7, 4),
                        style("MONOSPACE", 7, 4),
                    ]),
                )
                .unwrap(),
        );
        let entries = projected["textStyle"].as_array().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0]["style"], "BOLD");
        assert_eq!(entries[0]["start"], 0);
        assert_eq!(entries[0]["length"], 6);
        assert_eq!(entries[1]["style"], "SPOILER");
        assert_eq!(entries[2]["style"], "MONOSPACE");

        // The styled pending row persists its ranges (sender echo across
        // restarts), and an empty list leaves no textStyle key at all.
        let stored = service
            .store_ref()
            .message_by_client_request(&account.id, "req-style-ok")
            .unwrap()
            .unwrap();
        let rich = stored.rich.as_ref().expect("styled row carries rich");
        assert_eq!(rich.text_styles.len(), 3);
        assert_eq!(rich.text_styles[0].style, "BOLD");
        assert!(!rich.view_once, "no view-once marker on a styled send");

        let bare = dispatch(
            service
                .prepare_send_text(
                    &account.id,
                    &conversation.id,
                    "plain body",
                    "req-style-empty",
                    None,
                    None,
                    None,
                    Some(Vec::new()),
                )
                .unwrap(),
        );
        assert!(bare.get("textStyle").is_none());

        // Retry rebuilds the send without a textStyle key (documented
        // deviation): the failed styled row rearms and re-dispatches plain.
        service.complete_send_failed(&stored.id).unwrap();
        let retry_params = match service
            .prepare_retry_text(&account.id, &conversation.id, "req-style-ok")
            .unwrap()
        {
            PreparedSend::Dispatch { params, .. } => params,
            PreparedSend::Existing(_) => panic!("retry must re-dispatch the same row"),
        };
        assert_eq!(retry_params["message"], "styled body");
        assert!(retry_params.get("textStyle").is_none());
    }

    #[test]
    fn outgoing_sync_reconciles_by_signal_timestamp_and_client_request_id() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "same text is not identity",
                "client-request-exact",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        let sync_receive = NormalizedReceive {
            timestamp: Some(99),
            content_kind: "syncMessage",
            direction: "outgoing",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: Some("same text is not identity".into()),
            text_bytes: Some(25),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: None,
            server_guid: None,
        };
        service
            .ingest_receive(sync_receive.clone(), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(
            service
                .list_messages(&account.id, &conversation.id, 10, None)
                .unwrap()
                .items
                .len(),
            2
        );

        let (sent, _) = service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 99)
            .unwrap();
        assert_eq!(
            sent.client_request_id.as_deref(),
            Some("client-request-exact")
        );
        let rows = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, pending_id);

        assert!(
            service
                .ingest_receive(sync_receive, crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            service
                .list_messages(&account.id, &conversation.id, 10, None)
                .unwrap()
                .items
                .len(),
            1
        );
    }

    /// Regression: the pending row can be gone when an upstream send completes
    /// (the account's rows were deleted in a race). The message may already be
    /// delivered, so the completion must surface a final, non-retryable state —
    /// never INTERNAL_ERROR with retryable=true on a mutating operation.
    #[test]
    fn missing_pending_row_at_send_completion_is_final_not_retryable() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "in flight",
                "req-race",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };

        // The deletion race: the account rows disappear while the upstream
        // send is still in flight.
        assert!(
            service
                .store_ref()
                .delete_account_cascade(&account.id)
                .unwrap()
        );

        let error = service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 100)
            .unwrap_err();
        let ServiceError::Api(api) = error else {
            panic!("missing pending row must be an API error, got {error:?}");
        };
        assert_eq!(api.code, "SEND_OUTCOME_UNKNOWN");
        assert!(!api.retryable);

        // A pending id that never existed lands on the same non-retryable path.
        let error =
            match service.complete_send_success("absent", &account.id, &conversation.id, 100) {
                Err(ServiceError::Api(api)) => api,
                other => panic!("missing pending row must fail, got {other:?}"),
            };
        assert_eq!(error.code, "SEND_OUTCOME_UNKNOWN");
        assert!(!error.retryable);
    }

    #[test]
    fn signal_identity_keeps_group_senders_distinct_and_dedupes_replay() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let receive = |source: &str, text: &str| NormalizedReceive {
            timestamp: Some(200),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some(source.into()),
            source_uuid: None,
            peer_name: None,
            group_id: Some("group-one".into()),
            text: Some(text.into()),
            text_bytes: Some(text.len() as u32),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: None,
            server_guid: None,
        };

        let first = receive("peer-a", "first");
        assert!(
            !service
                .ingest_receive(first.clone(), crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
        assert!(
            !service
                .ingest_receive(receive("peer-b", "second"), crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
        assert!(
            service
                .ingest_receive(first, crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );

        let conversation = service
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        assert_eq!(
            service
                .list_messages(&account.id, &conversation.id, 10, None)
                .unwrap()
                .items
                .len(),
            2
        );
    }

    /// Contract 1.27: an incoming message captures the envelope `sourceName`
    /// on the row (`senderName` on the wire record) and the conversation
    /// summary carries the last-message direction plus that author name —
    /// the metadata the desktop list needs for the official author-prefix
    /// preview. Outgoing rows stay name-less (the author is self).
    #[test]
    fn incoming_receive_captures_sender_name_into_summary_projection() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let receive = NormalizedReceive {
            timestamp: Some(200),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: Some("林菲菲".into()),
            group_id: Some("group-one".into()),
            text: Some("你好".into()),
            text_bytes: Some(6),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: None,
            server_guid: None,
        };
        assert!(
            !service
                .ingest_receive(receive.clone(), crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );

        let conversation = service
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        let summary = service
            .store
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_message_direction, Some("incoming"));
        assert_eq!(summary.last_message_author_name.as_deref(), Some("林菲菲"));
        assert!(summary.last_message_reactions.is_empty());

        let row = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        assert_eq!(row.sender_name.as_deref(), Some("林菲菲"));
        let wire = serde_json::to_value(&row).unwrap();
        assert_eq!(wire["senderName"], "林菲菲");

        // The connector's own outgoing rows are the account itself: no name
        // is captured and the summary flips to outgoing with no author.
        let reply = NormalizedReceive {
            direction: "outgoing",
            timestamp: Some(300),
            content_kind: "syncMessage",
            source: Some("group-one".into()),
            text: Some("好的".into()),
            text_bytes: Some(6),
            server_guid: None,
            ..receive
        };
        assert!(
            !service
                .ingest_receive(reply, crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
        let summary = service
            .store
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_message_direction, Some("outgoing"));
        assert_eq!(summary.last_message_author_name, None);
    }

    #[test]
    fn receive_without_signal_timestamp_has_no_persistence_side_effects() {
        let (_temp, mut service) = service();
        let receive = NormalizedReceive {
            timestamp: None,
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: Some("unstable identity".into()),
            text_bytes: Some(17),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: None,
            server_guid: None,
        };

        assert!(
            service
                .ingest_receive(receive, crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
        assert!(
            service
                .list_accounts_in_group(crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn send_by_peer_creates_conversation_once_and_reuses_it() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        let first = match service
            .prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "contact",
                    peer_key: "+15555550109",
                    peer_title: Some("New Peer"),
                },
                "hello peer",
                "peer-req-1",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch {
                conversation_id,
                params,
                ..
            } => {
                assert_eq!(params["recipient"], json!(["+15555550109"]));
                conversation_id
            }
            PreparedSend::Existing(_) => panic!("first peer send must dispatch"),
        };
        // The conversation appears with the peer title only alongside this send.
        let conversations = service.list_conversations(&account.id, 10, None).unwrap();
        assert_eq!(conversations.items.len(), 1);
        assert_eq!(conversations.items[0].id, first);
        assert_eq!(conversations.items[0].title, "New Peer");

        // A new client request to the same peer reuses the same conversation.
        let second = match service
            .prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "direct",
                    peer_key: "+15555550109",
                    peer_title: None,
                },
                "hello again",
                "peer-req-2",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch {
                conversation_id, ..
            } => conversation_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        assert_eq!(second, first);
        assert_eq!(
            service
                .list_conversations(&account.id, 10, None)
                .unwrap()
                .items
                .len(),
            1
        );

        // A repeated client request is idempotent regardless of the target form.
        assert!(matches!(
            service
                .prepare_send_text_to_peer(
                    &account.id,
                    &PeerTarget {
                        kind: "contact",
                        peer_key: "+15555550109",
                        peer_title: None,
                    },
                    "hello peer",
                    "peer-req-1",
                    None,
                    None,
                    None,
                    None
                )
                .unwrap(),
            PreparedSend::Existing(_)
        ));
        assert!(matches!(
            service
                .prepare_send_text(
                    &account.id,
                    &first,
                    "hello peer",
                    "peer-req-1",
                    None,
                    None,
                    None,
                    None
                )
                .unwrap(),
            PreparedSend::Existing(_)
        ));
    }

    #[test]
    fn send_by_peer_validates_target_and_falls_back_to_masked_title() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        assert!(matches!(
            service.prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "channel",
                    peer_key: "+15555550109",
                    peer_title: None,
                },
                "text",
                "req-bad-kind",
                None, None,
             None, None),
            Err(ServiceError::Api(error)) if error.code == "INVALID_REQUEST"
        ));
        assert!(matches!(
            service.prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "contact",
                    peer_key: "",
                    peer_title: None,
                },
                "text",
                "req-empty-peer",
                None, None,
             None, None),
            Err(ServiceError::Api(error)) if error.code == "INVALID_REQUEST"
        ));
        assert!(matches!(
            service.prepare_send_text_to_peer(
                "absent-account",
                &PeerTarget {
                    kind: "contact",
                    peer_key: "+15555550109",
                    peer_title: None,
                },
                "text",
                "req-absent",
                None,
                None,
                None,
                None
            ),
            Err(ServiceError::Store(StoreError::AccountNotFound))
        ));

        match service
            .prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "group",
                    peer_key: "Z3JvdXAtaWQ=",
                    peer_title: None,
                },
                "group hello",
                "req-group",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { params, .. } => {
                assert_eq!(params["groupId"], json!("Z3JvdXAtaWQ="));
            }
            PreparedSend::Existing(_) => panic!("group peer send must dispatch"),
        }
        let group = service
            .store_ref()
            .conversation_by_peer(&account.id, "group", "Z3JvdXAtaWQ=")
            .unwrap()
            .unwrap();
        assert_eq!(
            service.store_ref().conversation_title(&group.id).unwrap(),
            Some("group".to_string())
        );

        match service
            .prepare_send_text_to_peer(
                &account.id,
                &PeerTarget {
                    kind: "direct",
                    peer_key: "+15555550110",
                    peer_title: None,
                },
                "masked hello",
                "req-masked",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch {
                conversation_id, ..
            } => {
                let title = service
                    .store_ref()
                    .conversation_title(&conversation_id)
                    .unwrap()
                    .unwrap();
                assert!(title.contains("***"));
                assert!(!title.contains("555555"));
            }
            PreparedSend::Existing(_) => panic!("direct peer send must dispatch"),
        }
    }

    /// Phase 2 (docs/optimization-plan.md): quoteMessageId is delivered
    /// upstream. A quote of an incoming direct-chat message resolves to
    /// signal-cli's quoteTimestamp (the envelope timestamp, which is the
    /// protocol identity) and quoteAuthor (the peer number).
    #[test]
    fn quote_of_incoming_direct_message_becomes_upstream_quote_params() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(777),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    source_uuid: None,
                    peer_name: None,
                    group_id: None,
                    text: Some("quoted text".into()),
                    text_bytes: Some(11),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let quoted = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();

        match service
            .prepare_send_text(
                &account.id,
                &quoted.conversation_id,
                "reply with quote",
                "req-quote-1",
                Some(&quoted.id),
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { params, .. } => {
                assert_eq!(params["quoteTimestamp"], json!(777));
                assert_eq!(params["quoteAuthor"], json!("+15555550101"));
                assert_eq!(params["recipient"], json!(["+15555550101"]));
                assert_eq!(params["account"], json!("+15555550100"));
            }
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        }

        // The local record keeps the opaque quoteMessageId for the renderer.
        let stored = service
            .store_ref()
            .message_by_client_request(&account.id, "req-quote-1")
            .unwrap()
            .unwrap();
        assert_eq!(stored.quote_message_id.as_deref(), Some(quoted.id.as_str()));
    }

    /// A quote of our own completed send resolves the author to the linked
    /// account and the timestamp to the upstream-assigned one.
    #[test]
    fn quote_of_own_sent_message_uses_account_number_and_upstream_timestamp() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "original",
                "req-original",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 4242)
            .unwrap();

        match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "quote own",
                "req-quote-own",
                Some(&pending_id),
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { params, .. } => {
                assert_eq!(params["quoteTimestamp"], json!(4242));
                assert_eq!(params["quoteAuthor"], json!("+15555550100"));
            }
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        }
    }

    /// Deterministic rejections at validation time: unknown target, own
    /// not-yet-sent message, group message (member address is not persisted),
    /// and a message from another conversation. None of them may leave a
    /// pending row behind — the same clientRequestId must re-validate as new.
    #[test]
    fn unresolvable_quotes_are_rejected_before_any_pending_row_exists() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();

        // Unknown quote target -> MESSAGE_NOT_FOUND, non-retryable.
        let api = service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "reply",
                "req-quote-missing",
                Some("no-such-message"),
                None,
                None,
                None,
            )
            .unwrap_err()
            .into_api();
        assert_eq!(api.code, "MESSAGE_NOT_FOUND");
        assert!(!api.retryable);

        // The rejection left no pending row: the same clientRequestId is a
        // fresh dispatch, not an idempotent replay of a stuck pending record.
        assert!(matches!(
            service
                .prepare_send_text(
                    &account.id,
                    &conversation.id,
                    "reply",
                    "req-quote-missing",
                    None,
                    None,
                    None,
                    None
                )
                .unwrap(),
            PreparedSend::Dispatch { .. }
        ));

        // Quoting our own still-pending message would misquote upstream (its
        // sent_at is a local clock value until the send completes).
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "in flight",
                "req-pending",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        assert!(matches!(
            service.prepare_send_text(
                &account.id,
                &conversation.id,
                "quote pending",
                "req-quote-pending",
                Some(&pending_id), None,
             None, None),
            Err(ServiceError::Api(error)) if error.code == "INVALID_REQUEST" && !error.retryable
        ));

        // A group incoming message's author address is not persisted (only a
        // local sender hash), so the quote cannot be constructed upstream.
        let group_events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(900),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550105".into()),
                    source_uuid: None,
                    peer_name: None,
                    group_id: Some("group-one".into()),
                    text: Some("group text".into()),
                    text_bytes: Some(10),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let group_message = group_events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        assert!(matches!(
            service.prepare_send_text(
                &account.id,
                &group_message.conversation_id,
                "quote group",
                "req-quote-group",
                Some(&group_message.id), None,
             None, None),
            Err(ServiceError::Api(error)) if error.code == "INVALID_REQUEST" && !error.retryable
        ));

        // A message from another conversation is not a valid quote target here.
        assert!(matches!(
            service.prepare_send_text(
                &account.id,
                &conversation.id,
                "cross quote",
                "req-quote-cross",
                Some(&group_message.id),
                None,
                None,
                None
            ),
            Err(ServiceError::Store(StoreError::MessageNotFound))
        ));
    }

    /// messages.remoteDelete resolves the local row to the upstream addressing:
    /// only an own outgoing `sent` row qualifies, its upstream-assigned
    /// `sent_at` becomes `targetTimestamp`, and the conversation's kind
    /// selects `recipient` (direct) vs `groupId` (group) — the same addressing
    /// shape as `send` (docs/remote-delete-l2-plan.md §3.2).
    #[test]
    fn remote_delete_maps_sent_outgoing_rows_to_upstream_params() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        // Direct conversation: recipient = [peer_key].
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let direct_id = match service
            .prepare_send_text(
                &account.id,
                &direct.id,
                "delete me",
                "req-rd-1",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&direct_id, &account.id, &direct.id, 777)
            .unwrap();
        let prepared = service
            .prepare_remote_delete(&account.id, &direct.id, &direct_id)
            .unwrap();
        assert_eq!(prepared.params["account"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(777));
        assert_eq!(prepared.params["recipient"], json!(["+15555550101"]));
        assert!(prepared.params.get("groupId").is_none());
        assert_eq!(prepared.account_id, account.id);
        assert_eq!(prepared.conversation_id, direct.id);
        assert_eq!(prepared.message_id, direct_id);

        // Group conversation: groupId = peer_key, no recipient.
        service
            .sync_accounts_from_numbers(&["+15555550101".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "Z3JvdXAtaWQ=", "group")
            .unwrap();
        let group_message_id = match service
            .prepare_send_text(
                &account.id,
                &group.id,
                "group delete",
                "req-rd-2",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&group_message_id, &account.id, &group.id, 888)
            .unwrap();
        let prepared = service
            .prepare_remote_delete(&account.id, &group.id, &group_message_id)
            .unwrap();
        assert_eq!(prepared.params["targetTimestamp"], json!(888));
        assert_eq!(prepared.params["groupId"], json!("Z3JvdXAtaWQ="));
        assert!(prepared.params.get("recipient").is_none());
    }

    /// The eligibility guard is deterministic and local: pending, failed and
    /// unknown rows (their sent_at is a local clock value, not a protocol
    /// identity) and incoming rows all answer MESSAGE_NOT_FOUND before any
    /// upstream call — the same rule as quote resolution.
    #[test]
    fn remote_delete_rejects_rows_without_a_protocol_identity() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "in flight",
                "req-rd-p",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        for status in ["pending", "failed", "unknown"] {
            service
                .store_ref()
                .update_message_status(&pending_id, status, None)
                .unwrap();
            assert!(
                service
                    .prepare_remote_delete(&account.id, &conversation.id, &pending_id)
                    .is_err(),
                "a {status} row must not be deletable"
            );
        }

        // The incoming row of the same conversation is not deletable either.
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(50),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    source_uuid: None,
                    peer_name: None,
                    group_id: None,
                    text: Some("peer text".into()),
                    text_bytes: Some(9),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let incoming = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        let incoming_error = service
            .prepare_remote_delete(&account.id, &conversation.id, &incoming.id)
            .unwrap_err();
        assert_eq!(incoming_error.into_api().code, "MESSAGE_NOT_FOUND");

        // A never-existing row, conversation, or account answer their own
        // deterministic codes.
        assert_eq!(
            service
                .prepare_remote_delete(&account.id, &conversation.id, "absent-message")
                .unwrap_err()
                .into_api()
                .code,
            "MESSAGE_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_remote_delete(&account.id, "absent-conversation", "anything")
                .unwrap_err()
                .into_api()
                .code,
            "CONVERSATION_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_remote_delete("absent-account", "absent-conversation", "anything")
                .unwrap_err()
                .into_api()
                .code,
            "ACCOUNT_NOT_FOUND"
        );
    }

    /// messages.sendReaction resolves the local row to the upstream
    /// addressing: the target author follows the row direction — the linked
    /// account's own number for a `sent` outgoing row, the peer for an
    /// incoming direct row — and the conversation's kind selects `recipient`
    /// vs `groupId` (docs/remote-delete-l2-plan.md §3.2 shape, contract
    /// revision 1.7). remove passes through as an explicit boolean.
    #[test]
    fn send_reaction_maps_rows_to_the_direction_derived_target_author() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();

        // Outgoing `sent` row: targetAuthor is the linked account itself.
        let sent_id = match service
            .prepare_send_text(
                &account.id,
                &direct.id,
                "react to me",
                "req-sr-1",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&sent_id, &account.id, &direct.id, 777)
            .unwrap();
        let prepared = service
            .prepare_send_reaction(&account.id, &direct.id, &sent_id, "👍", false)
            .unwrap();
        assert_eq!(prepared.params["account"], json!("+15555550100"));
        assert_eq!(prepared.params["emoji"], json!("👍"));
        assert_eq!(prepared.params["remove"], json!(false));
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(777));
        assert_eq!(prepared.params["recipient"], json!(["+15555550101"]));
        assert!(prepared.params.get("groupId").is_none());
        assert_eq!(prepared.account_id, account.id);
        assert_eq!(prepared.conversation_id, direct.id);
        assert_eq!(prepared.message_id, sent_id);

        // Incoming row (direct chat): targetAuthor is the peer and the
        // envelope timestamp is the protocol identity.
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(50),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    source_uuid: None,
                    peer_name: None,
                    group_id: None,
                    text: Some("peer text".into()),
                    text_bytes: Some(9),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let incoming = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        let prepared = service
            .prepare_send_reaction(&account.id, &direct.id, &incoming.id, "🎉", true)
            .unwrap();
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550101"));
        assert_eq!(prepared.params["targetTimestamp"], json!(50));
        assert_eq!(prepared.params["remove"], json!(true));

        // Group conversation: groupId addressing, no recipient.
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "Z3JvdXAtaWQ=", "group")
            .unwrap();
        let group_message_id = match service
            .prepare_send_text(
                &account.id,
                &group.id,
                "group react",
                "req-sr-2",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&group_message_id, &account.id, &group.id, 888)
            .unwrap();
        let prepared = service
            .prepare_send_reaction(&account.id, &group.id, &group_message_id, "👍", false)
            .unwrap();
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(888));
        assert_eq!(prepared.params["groupId"], json!("Z3JvdXAtaWQ="));
        assert!(prepared.params.get("recipient").is_none());
    }

    /// The eligibility guard is deterministic and local: rows without a
    /// resolvable protocol identity (missing, pending/failed/unknown, system)
    /// answer MESSAGE_NOT_FOUND, and a group incoming row's author address is
    /// not persisted at all — a deterministic INVALID_REQUEST, never a
    /// mis-addressed upstream reaction (quote-resolution precedent).
    #[test]
    fn send_reaction_rejects_rows_without_a_resolvable_target_author() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "in flight",
                "req-sr-p",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        for status in ["pending", "failed", "unknown"] {
            service
                .store_ref()
                .update_message_status(&pending_id, status, None)
                .unwrap();
            let error = service
                .prepare_send_reaction(&account.id, &conversation.id, &pending_id, "👍", false)
                .unwrap_err();
            assert_eq!(
                error.into_api().code,
                "MESSAGE_NOT_FOUND",
                "a {status} row must not be reactable"
            );
        }

        // A group incoming message's author address is not persisted (only a
        // local sender hash), so the reaction cannot be addressed upstream.
        service
            .sync_accounts_from_numbers(&["+15555550101".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-one", "group")
            .unwrap();
        let group_events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(900),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550105".into()),
                    source_uuid: None,
                    peer_name: None,
                    group_id: Some("group-one".into()),
                    text: Some("group text".into()),
                    text_bytes: Some(10),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let group_message = group_events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            service
                .prepare_send_reaction(&account.id, &group.id, &group_message.id, "👍", false)
                .unwrap_err()
                .into_api()
                .code,
            "INVALID_REQUEST"
        );

        // A never-existing row, conversation, or account answer their own
        // deterministic codes.
        assert_eq!(
            service
                .prepare_send_reaction(&account.id, &conversation.id, "absent-message", "👍", false)
                .unwrap_err()
                .into_api()
                .code,
            "MESSAGE_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_send_reaction(&account.id, "absent-conversation", "anything", "👍", false)
                .unwrap_err()
                .into_api()
                .code,
            "CONVERSATION_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_send_reaction(
                    "absent-account",
                    "absent-conversation",
                    "anything",
                    "👍",
                    false
                )
                .unwrap_err()
                .into_api()
                .code,
            "ACCOUNT_NOT_FOUND"
        );
    }

    /// Receipt-advanced outgoing rows stay addressable: delivery/read
    /// receipts only advance `status` while `update_message_status` preserves
    /// the upstream `sent_at`, so a delivered or read row reacts with the
    /// same protocol identity it had at `sent` (official behavior — reacting
    /// to an already-delivered message is the common case).
    #[test]
    fn send_reaction_accepts_receipt_advanced_outgoing_rows() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let message_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "delivered then read",
                "req-sr-advance",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&message_id, &account.id, &conversation.id, 777)
            .unwrap();
        for status in ["sent", "delivered", "read"] {
            service
                .store_ref()
                .update_message_status(&message_id, status, None)
                .unwrap();
            let prepared = service
                .prepare_send_reaction(&account.id, &conversation.id, &message_id, "👍", false)
                .unwrap();
            assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
            assert_eq!(prepared.params["targetTimestamp"], json!(777));
        }
    }

    /// 上行确认后的 complete 落库：own 回应以 account 为 actor 持久化，pill
    /// 带 mine=true、actor 标 self；remove=true 撤回后 pill 消失。app 的乐观
    /// 窗口结束后读侧仍一致（supervisor 侧另推 conversation.changed）。
    #[test]
    fn complete_send_reaction_persists_own_pill_and_removal() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let message_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "reaction target",
                "req-sr-complete",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&message_id, &account.id, &conversation.id, 888)
            .unwrap();
        let prepared = service
            .prepare_send_reaction(&account.id, &conversation.id, &message_id, "👍", false)
            .unwrap();
        assert!(
            service
                .complete_send_reaction(
                    &prepared.account_id,
                    &prepared.conversation_id,
                    &prepared.emoji,
                    prepared.remove,
                    prepared.target_sent_at,
                )
                .unwrap()
                .is_some()
        );
        let reactions = service
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &message_id)
            .unwrap()
            .unwrap()
            .reactions;
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0].emoji, "👍");
        assert!(reactions[0].mine);
        assert!(reactions[0].actors.iter().all(|actor| actor.is_self));

        let prepared = service
            .prepare_send_reaction(&account.id, &conversation.id, &message_id, "👍", true)
            .unwrap();
        service
            .complete_send_reaction(
                &prepared.account_id,
                &prepared.conversation_id,
                &prepared.emoji,
                prepared.remove,
                prepared.target_sent_at,
            )
            .unwrap();
        let reactions = service
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &message_id)
            .unwrap()
            .unwrap()
            .reactions;
        assert!(reactions.is_empty());
    }

    /// One pin-family prepare callable under a uniform signature, so the
    /// guard loop drives all three methods over the same failing row.
    type PinPrepare<'a> =
        Box<dyn Fn(&ConnectorService, &str) -> Result<PreparedPin, ServiceError> + 'a>;

    /// Contract 1.33 pin family addressing: the §4.6 reaction model reused
    /// verbatim — an addressable outgoing row targets the linked account
    /// itself, a direct-chat incoming row targets the peer, groups map to
    /// `groupId` addressing, and `pinDurationSeconds` rides the upstream
    /// params only when the caller supplied one (absent = the official
    /// forever pin, the key is not sent).
    #[test]
    fn pin_family_maps_rows_with_the_reaction_addressing_model() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let sent_id = match service
            .prepare_send_text(
                &account.id,
                &direct.id,
                "pin me",
                "req-pin-1",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&sent_id, &account.id, &direct.id, 777)
            .unwrap();

        // Pin with a duration: every upstream key carries the reaction
        // addressing plus the passthrough `pinDurationSeconds`.
        let prepared = service
            .prepare_send_pin_message(&account.id, &direct.id, &sent_id, Some(3600))
            .unwrap();
        assert_eq!(prepared.params["account"], json!("+15555550100"));
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(777));
        assert_eq!(prepared.params["pinDurationSeconds"], json!(3600));
        assert_eq!(prepared.params["recipient"], json!(["+15555550101"]));
        assert!(prepared.params.get("groupId").is_none());
        assert_eq!(prepared.target_author, "+15555550100");
        assert_eq!(prepared.target_sent_at, 777);
        assert_eq!(prepared.pin_duration_seconds, Some(3600));

        // Pin without a duration: the key stays absent (forever pin).
        let prepared = service
            .prepare_send_pin_message(&account.id, &direct.id, &sent_id, None)
            .unwrap();
        assert!(prepared.params.get("pinDurationSeconds").is_none());
        assert_eq!(prepared.pin_duration_seconds, None);

        // Unpin: the same addressing, no duration field at all.
        let prepared = service
            .prepare_send_unpin_message(&account.id, &direct.id, &sent_id)
            .unwrap();
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(777));
        assert!(prepared.params.get("pinDurationSeconds").is_none());
        assert_eq!(prepared.pin_duration_seconds, None);

        // Incoming direct row: the target author is the peer and the
        // envelope timestamp is the protocol identity.
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(50),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    source_uuid: None,
                    peer_name: None,
                    group_id: None,
                    text: Some("peer pin target".into()),
                    text_bytes: Some(15),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let incoming = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        let prepared = service
            .prepare_send_pin_message(&account.id, &direct.id, &incoming.id, None)
            .unwrap();
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550101"));
        assert_eq!(prepared.params["targetTimestamp"], json!(50));

        // Group conversation: groupId addressing for the pin and the admin
        // delete, no recipient array.
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "Z3JvdXAtaWQ=", "group")
            .unwrap();
        let group_message_id = match service
            .prepare_send_text(
                &account.id,
                &group.id,
                "group pin target",
                "req-pin-group",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&group_message_id, &account.id, &group.id, 888)
            .unwrap();
        let prepared = service
            .prepare_send_pin_message(&account.id, &group.id, &group_message_id, None)
            .unwrap();
        assert_eq!(prepared.params["groupId"], json!("Z3JvdXAtaWQ="));
        assert!(prepared.params.get("recipient").is_none());
        let prepared = service
            .prepare_send_admin_delete(&account.id, &group.id, &group_message_id)
            .unwrap();
        assert_eq!(prepared.params["groupId"], json!("Z3JvdXAtaWQ="));
        assert_eq!(prepared.params["targetAuthor"], json!("+15555550100"));
        assert_eq!(prepared.params["targetTimestamp"], json!(888));
        assert!(prepared.params.get("recipient").is_none());
        assert!(prepared.params.get("pinDurationSeconds").is_none());
    }

    /// The pin family eligibility guard is the reaction guard: rows without a
    /// resolvable protocol identity answer MESSAGE_NOT_FOUND, a group
    /// incoming row's author is not resolvable (INVALID_REQUEST), and a
    /// direct chat has no admin concept — sendAdminDelete fails closed
    /// INVALID_REQUEST before any upstream call.
    #[test]
    fn pin_family_rejects_unaddressable_and_out_of_scope_targets() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "in flight",
                "req-pin-pending",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        for status in ["pending", "failed", "unknown"] {
            service
                .store_ref()
                .update_message_status(&pending_id, status, None)
                .unwrap();
            let prepares: Vec<PinPrepare> = vec![
                Box::new(|service, id| {
                    service.prepare_send_pin_message(&account.id, &conversation.id, id, None)
                }),
                Box::new(|service, id| {
                    service.prepare_send_unpin_message(&account.id, &conversation.id, id)
                }),
                Box::new(|service, id| {
                    service.prepare_send_admin_delete(&account.id, &conversation.id, id)
                }),
            ];
            for prepare in prepares {
                let error = prepare(&service, &pending_id).unwrap_err();
                assert_eq!(
                    error.into_api().code,
                    "MESSAGE_NOT_FOUND",
                    "a {status} row must not be pin-family addressable"
                );
            }
        }

        // A group incoming message's author address is not persisted, so no
        // pin-family method can be addressed upstream.
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-one", "group")
            .unwrap();
        let group_events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(900),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550105".into()),
                    source_uuid: None,
                    peer_name: None,
                    group_id: Some("group-one".into()),
                    text: Some("group text".into()),
                    text_bytes: Some(10),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let group_message = group_events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .unwrap();
        let prepares: Vec<PinPrepare> = vec![
            Box::new(|service, id| {
                service.prepare_send_pin_message(&account.id, &group.id, id, None)
            }),
            Box::new(|service, id| service.prepare_send_unpin_message(&account.id, &group.id, id)),
            Box::new(|service, id| service.prepare_send_admin_delete(&account.id, &group.id, id)),
        ];
        for prepare in prepares {
            assert_eq!(
                prepare(&service, &group_message.id)
                    .unwrap_err()
                    .into_api()
                    .code,
                "INVALID_REQUEST"
            );
        }

        // A direct chat has no admin: sendAdminDelete answers INVALID_REQUEST
        // even on a perfectly addressable row.
        let sent_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "direct row",
                "req-pin-direct",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&sent_id, &account.id, &conversation.id, 410)
            .unwrap();
        assert_eq!(
            service
                .prepare_send_admin_delete(&account.id, &conversation.id, &sent_id)
                .unwrap_err()
                .into_api()
                .code,
            "INVALID_REQUEST"
        );

        // Missing rows, conversations, and accounts answer their own
        // deterministic codes.
        assert_eq!(
            service
                .prepare_send_pin_message(&account.id, &conversation.id, "absent-message", None)
                .unwrap_err()
                .into_api()
                .code,
            "MESSAGE_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_send_unpin_message(&account.id, "absent-conversation", "anything")
                .unwrap_err()
                .into_api()
                .code,
            "CONVERSATION_NOT_FOUND"
        );
        assert_eq!(
            service
                .prepare_send_admin_delete("absent-account", "absent-conversation", "anything")
                .unwrap_err()
                .into_api()
                .code,
            "ACCOUNT_NOT_FOUND"
        );
    }

    /// 上行确认后的 complete 落库：pin 写入每会话 pinned 状态（新 pin 替换
    /// 旧 pin，timed pin 带 connector 时钟 expiry，forever pin 无
    /// expiresAt），摘要以 camelCase `pinnedMessage` 投影；不匹配的 unpin
    /// 不动新 pin（返回 None），匹配的 unpin 清除并返回刷新摘要。
    #[test]
    fn complete_pin_unpin_write_replace_and_clear_the_pinned_state() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let own_row = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "pin target one",
                "req-pin-c1",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&own_row, &account.id, &conversation.id, 888)
            .unwrap();

        // Timed pin on the own row: the summary projects the pin bar with an
        // expiry computed from the duration.
        let summary = service
            .complete_send_pin_message(
                &account.id,
                &conversation.id,
                "+15555550100",
                888,
                Some(3600),
            )
            .unwrap()
            .unwrap();
        let wire = serde_json::to_value(&summary).unwrap();
        let pinned = &wire["pinnedMessage"];
        assert_eq!(pinned["messageId"], json!(own_row));
        assert_eq!(pinned["targetAuthor"], json!("+15555550100"));
        assert_eq!(pinned["targetSentTimestamp"], json!(888));
        assert!(pinned["pinnedAt"].is_u64());
        assert!(
            pinned["expiresAt"].is_u64(),
            "a timed pin carries expiresAt"
        );
        assert_eq!(
            summary
                .pinned_message
                .as_ref()
                .unwrap()
                .target_sent_timestamp,
            888
        );

        // A newer pin replaces the older one: pin the peer row (forever) and
        // the summary follows.
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(50),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    source_uuid: None,
                    peer_name: None,
                    group_id: None,
                    text: Some("peer pin target".into()),
                    text_bytes: Some(15),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let incoming_id = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message.id.clone()),
                _ => None,
            })
            .unwrap();
        let summary = service
            .complete_send_pin_message(&account.id, &conversation.id, "+15555550101", 50, None)
            .unwrap()
            .unwrap();
        let pinned = summary.pinned_message.as_ref().unwrap();
        assert_eq!(pinned.message_id, incoming_id);
        assert_eq!(pinned.expires_at, None, "a forever pin carries no expiry");
        let wire = serde_json::to_value(&summary).unwrap();
        assert!(wire["pinnedMessage"].get("expiresAt").is_none());

        // A stale unpin naming the replaced (author, timestamp) stays
        // silent: complete answers None and the newer pin survives.
        assert!(
            service
                .complete_send_unpin_message(&account.id, &conversation.id, "+15555550100", 888)
                .unwrap()
                .is_none()
        );
        let pinned = service
            .store_ref()
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap()
            .pinned_message
            .unwrap();
        assert_eq!(pinned.target_sent_timestamp, 50);

        // The matching unpin clears the state and returns the refreshed
        // summary (pinnedMessage absent again).
        let summary = service
            .complete_send_unpin_message(&account.id, &conversation.id, "+15555550101", 50)
            .unwrap()
            .unwrap();
        assert!(summary.pinned_message.is_none());
    }

    /// 收件面（contract 1.33）：peer 的 pinMessage 更新每会话 pinned 状态并
    /// 推 conversation.changed（摘要带 pinnedMessage）；不匹配的 unpinMessage
    /// 静默；匹配的清除；adminDelete 落行级 adminDeleted 墓碑（status 不动、
    /// body 保留）——若被删行正是 pinned 行则一并清 pin。
    #[test]
    fn inbound_pin_family_updates_pinned_state_and_admin_delete_marks_rows() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let message_id = seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);

        // Peer pins our message with a timed duration: conversation.changed
        // carries the pinned summary.
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::PinMessage {
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                        duration_seconds: Some(300),
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("an inbound pin answers conversation.changed");
        let pinned = changed.pinned_message.as_ref().unwrap();
        assert_eq!(pinned.message_id, message_id);
        assert_eq!(pinned.target_author, "+15555550100");
        assert_eq!(pinned.target_sent_timestamp, 400);
        assert!(pinned.expires_at.is_some(), "the timed pin carries expiry");

        // An unpin naming another message stays silent (no event) and leaves
        // the pin alone.
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::UnpinMessage {
                        target_author: "+15555550100".into(),
                        target_timestamp: 999,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(events.is_empty(), "a mismatched unpin must be silent");
        assert!(
            service
                .store_ref()
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .pinned_message
                .is_some()
        );

        // The matching unpin clears the state and notifies once.
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::UnpinMessage {
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("a matching unpin answers conversation.changed");
        assert!(changed.pinned_message.is_none());

        // Re-pin (forever), then a group admin deletes the pinned row: the
        // row flips its additive tombstone (status ladder untouched, body
        // kept) and the pinned state clears with it.
        service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::PinMessage {
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                        duration_seconds: None,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::AdminDelete {
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let upserted = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(record) => Some(record),
                _ => None,
            })
            .expect("an admin delete answers message.upserted");
        assert_eq!(upserted.id, message_id);
        assert!(upserted.admin_deleted, "the tombstone flips");
        assert_eq!(upserted.status, "sent", "the status ladder is untouched");
        assert_eq!(upserted.text.as_deref(), Some("original"));
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("clearing the pinned row refreshes the conversation");
        assert!(changed.pinned_message.is_none());

        // A replayed adminDelete flips nothing: no events, state unchanged.
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::AdminDelete {
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(events.is_empty(), "a replayed admin delete must be silent");
    }

    /// The emoji guard enforces the pinned signal-cli contract locally:
    /// exactly one unicode grapheme cluster, 1-32 UTF-8 bytes. Multi-codepoint
    /// clusters (ZWJ family emoji, skin-tone modifiers) stay valid.
    #[test]
    fn emoji_validation_accepts_single_clusters_and_rejects_the_rest() {
        assert!(validate_emoji("👍").is_ok());
        assert!(validate_emoji("❤️").is_ok());
        assert!(validate_emoji("👍🏽").is_ok());
        assert!(validate_emoji("👨‍👩‍👧‍👦").is_ok());
        // One 32-byte ZWJ cluster: the byte bound, still a single grapheme.
        assert!(validate_emoji("👨‍👩‍👧‍👦‍👍").is_ok());

        for bad in [
            "",
            "ab",
            "👍👍",
            &"x".repeat(33),
            "👨‍👩‍👧‍👦‍👍🏽",             // 36 bytes in one cluster: over the bound
            &"👍".repeat(17), // 68 bytes: over the bound
        ] {
            assert!(
                validate_emoji(bad).is_err(),
                "emoji {bad:?} must be rejected"
            );
        }
    }

    /// message.statusChanged is produced on real transitions only: sent after
    /// completion, unknown/failed on terminal resolution — and never twice for
    /// a replayed write or a vanished row.
    #[test]
    fn status_changed_events_fire_once_per_real_transition() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let status_events = |events: &[HostSideEvent]| {
            events
                .iter()
                .filter_map(|event| match event {
                    HostSideEvent::MessageStatusChanged {
                        account_id,
                        message_id,
                        status,
                        ..
                    } => Some((account_id.clone(), message_id.clone(), *status)),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };

        let pending_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "status flow",
                "req-status",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };

        let (_, events) = service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 55)
            .unwrap();
        assert_eq!(
            status_events(&events),
            vec![(account.id.clone(), pending_id.clone(), "sent")]
        );

        // A replayed completion reports no transition -> no duplicate event.
        let (_, replayed) = service
            .complete_send_success(&pending_id, &account.id, &conversation.id, 55)
            .unwrap();
        assert!(status_events(&replayed).is_empty());

        // Terminal failure paths emit exactly once as well.
        let failed_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "will fail",
                "req-fail",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        let failed = service.complete_send_failed(&failed_id).unwrap();
        assert_eq!(
            status_events(&failed),
            vec![(account.id.clone(), failed_id.clone(), "failed")]
        );
        assert!(service.complete_send_failed(&failed_id).unwrap().is_empty());

        let unknown_id = match service
            .prepare_send_text(
                &account.id,
                &conversation.id,
                "will vanish",
                "req-unknown",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        let unknown = service.complete_send_unknown(&unknown_id).unwrap();
        assert_eq!(
            status_events(&unknown),
            vec![(account.id.clone(), unknown_id.clone(), "unknown")]
        );
        assert!(
            service
                .complete_send_unknown(&unknown_id)
                .unwrap()
                .is_empty()
        );

        // A row that never existed produces no phantom event.
        assert!(service.complete_send_unknown("absent").unwrap().is_empty());
    }

    /// The returned payload must be standard padded base64 decoding to
    /// exactly the declared size (contract revision 1.8): the encoded length
    /// is bounded before any allocation, and the padding arithmetic matches
    /// the sizes java.util.Base64 emits (implementation-plan §4.7).
    #[test]
    fn validate_attachment_payload_checks_declared_size_exactly() {
        // 3n and 3n+1 / 3n+2 byte payloads with their padded encodings.
        for (decoded, encoded) in [
            (0_u64, ""), // rejected: empty is never a valid attachment
            (24, "Zml4dHVyZSBhdHRhY2htZW50IGJ5dGVz"),
            (1, "YQ=="),
            (2, "YWI="),
            (3, "YWJj"),
            (
                // Largest 3-divisible size at the bound, encoded as unpadded
                // 4-char groups (holds for any MAX modulo 3).
                (MAX_ATTACHMENT_BYTES - MAX_ATTACHMENT_BYTES % 3) as u64,
                &"QUJD".repeat(MAX_ATTACHMENT_BYTES / 3),
            ),
        ] {
            let outcome = validate_attachment_payload(encoded, decoded);
            assert_eq!(
                outcome.is_ok(),
                decoded > 0,
                "decoded {decoded} via {encoded}"
            );
        }

        // A mismatch between the declared and the actual size is rejected.
        let error =
            validate_attachment_payload("Zml4dHVyZSBhdHRhY2htZW50IGJ5dGVz", 23).unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // An encoded length over the contract bound is rejected even before
        // the size arithmetic runs.
        let oversized = "A".repeat(MAX_ATTACHMENT_BASE64_CHARS + 4);
        let error = validate_attachment_payload(&oversized, 24).unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
    }

    /// Attachment send validation (contract revision 1.13,
    /// implementation-plan §4.12): canonical-base64 shape, declared-size
    /// equality, descriptor bounds, and the type/subtype content-type shape
    /// are all enforced deterministically before any row is written.
    #[test]
    fn attachment_send_payload_and_descriptors_are_validated_before_any_row() {
        let encoded = "Zml4dHVyZSBhdHRhY2htZW50IGJ5dGVz";
        assert!(validate_attachment_send_payload(encoded, 24).is_ok());

        // Non-canonical inputs: declared-size mismatch, whitespace, missing
        // padding (wrong length modulo), URL-safe alphabet, non-zero
        // leftover bits.
        assert!(validate_attachment_send_payload(encoded, 23).is_err());
        assert!(validate_attachment_send_payload("Zml4dHVyZSBhdHRhY2htZW50IGJ5dGVz ", 24).is_err());
        assert!(validate_attachment_send_payload("A", 1).is_err());
        assert!(validate_attachment_send_payload("YQ", 1).is_err());
        assert!(validate_attachment_send_payload("_w==", 1).is_err());
        assert!(validate_attachment_send_payload("YR==", 1).is_err());

        // Filename/contentType descriptor bounds.
        assert!(validate_attachment_descriptor(Some("report.pdf"), "filename").is_ok());
        assert!(validate_attachment_descriptor(None, "filename").is_ok());
        assert!(validate_attachment_descriptor(Some(""), "filename").is_err());
        assert!(validate_attachment_descriptor(Some("../etc/passwd"), "filename").is_err());
        assert!(validate_attachment_descriptor(Some("a\\b"), "filename").is_err());
        assert!(validate_attachment_descriptor(Some("a\nb"), "filename").is_err());
        assert!(validate_attachment_descriptor(Some(&"界".repeat(43)), "filename").is_err());

        assert!(validate_attachment_content_type(Some("image/png")).is_ok());
        assert!(validate_attachment_content_type(None).is_ok());
        assert!(validate_attachment_content_type(Some("image")).is_err());
        assert!(validate_attachment_content_type(Some("image/png;x=y")).is_err());
        assert!(validate_attachment_content_type(Some("imag e/png")).is_err());
    }

    /// The data URI is the only upstream-facing attachment form (§4.12): the
    /// validated base64 round-trips byte-exact, the filename is
    /// percent-encoded, and a missing contentType falls back to
    /// application/octet-stream.
    #[test]
    fn attachment_data_uri_round_trips_and_encodes_the_descriptor() {
        let payload = b"attachment bytes \xe2\x9c\x93";
        // Standard base64 of the payload, built locally to stay independent.
        let mut b64 = String::new();
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for chunk in payload.chunks(3) {
            let b = [
                chunk[0],
                *chunk.get(1).unwrap_or(&0),
                *chunk.get(2).unwrap_or(&0),
            ];
            b64.push(ALPHABET[(b[0] >> 2) as usize] as char);
            b64.push(ALPHABET[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
            if chunk.len() > 1 {
                b64.push(ALPHABET[(((b[1] & 0x0F) << 2) | (b[2] >> 6)) as usize] as char);
            }
            if chunk.len() > 2 {
                b64.push(ALPHABET[(b[2] & 0x3F) as usize] as char);
            }
        }
        let pad = (3 - payload.len() % 3) % 3;
        for _ in 0..pad {
            b64.push('=');
        }

        assert!(validate_attachment_send_payload(&b64, payload.len() as u64).is_ok());
        let uri =
            build_attachment_data_uri(&b64, Some("notes 下载.txt"), Some("text/plain"), false)
                .unwrap();
        assert!(uri.starts_with("data:text/plain;filename=notes%20%E4%B8%8B%E8%BD%BD.txt;base64,"));
        let payload_part = uri.rsplit(";base64,").next().unwrap();
        let mut decoded = Vec::new();
        base64_decode_to_vec(payload_part, &mut decoded).unwrap();
        assert_eq!(decoded, payload);

        // Default content type when none given.
        let uri = build_attachment_data_uri(&b64, None, None, false).unwrap();
        assert!(uri.starts_with("data:application/octet-stream;base64,"));

        // §4.30: the voice flag inserts the `;voice=true` parameter after the
        // mime type, before the filename and base64 payload markers.
        let uri =
            build_attachment_data_uri(&b64, Some("voice.ogg"), Some("audio/ogg"), true).unwrap();
        assert!(uri.starts_with("data:audio/ogg;voice=true;filename=voice.ogg;base64,"));
    }

    /// prepare_send_attachment (§4.12): validation failures leave no pending
    /// row (the same clientRequestId stays fresh), the prepared upstream
    /// params carry the data URI under `attachments`, and an unknown
    /// clientRequestId replay returns the same existing row (idempotency).
    #[test]
    fn prepare_send_attachment_builds_data_uri_params_and_is_idempotent() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let _events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(10),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    source_uuid: None,
                    peer_name: Some("Peer".into()),
                    group_id: None,
                    text: Some("hello".into()),
                    text_bytes: None,
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let conversation_id = service
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0)
            .id;

        let payload = b"attachment bytes";
        let b64 = "YXR0YWNobWVudCBieXRlcw=="; // base64 of "attachment bytes"
        assert_eq!(payload.len(), 16);

        // A rejected request (size mismatch) leaves no row behind: the same
        // clientRequestId is reusable afterwards.
        let error = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-attach-1",
                b64,
                15,
                Some("notes.txt"),
                Some("text/plain"),
                Some("see attachment"),
                None,
                false,
                false,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        assert!(
            service
                .store
                .message_by_client_request(&account.id, "req-attach-1")
                .unwrap()
                .is_none()
        );

        let prepared = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-attach-1",
                b64,
                16,
                Some("notes.txt"),
                Some("text/plain"),
                Some("see attachment"),
                None,
                false,
                false,
            )
            .unwrap();
        let PreparedSend::Dispatch {
            pending_id, params, ..
        } = &prepared
        else {
            panic!("expected a dispatch");
        };
        assert_eq!(params["message"], "see attachment");
        let attachments = params["attachments"].as_array().unwrap();
        assert_eq!(attachments.len(), 1);
        let uri = attachments[0].as_str().unwrap();
        assert_eq!(
            uri,
            "data:text/plain;filename=notes.txt;base64,YXR0YWNobWVudCBieXRlcw=="
        );
        // Addressing rides the shared upstream-target shape.
        assert!(params.get("recipient").is_some() || params.get("groupId").is_some());

        // The pending row exists once; replaying the same clientRequestId
        // returns the existing row instead of a second dispatch.
        let replay = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-attach-1",
                b64,
                16,
                Some("notes.txt"),
                Some("text/plain"),
                Some("see attachment"),
                None,
                false,
                false,
            )
            .unwrap();
        // The stored row carries the metadata-only descriptor so the desktop
        // renders the outgoing attachment from its first tick.
        let PreparedSend::Existing(row) = &replay else {
            panic!("expected the replayed existing row");
        };
        assert_eq!(row.attachments.len(), 1);
        assert_eq!(row.attachments[0].filename.as_deref(), Some("notes.txt"));
        assert_eq!(
            row.attachments[0].content_type.as_deref(),
            Some("text/plain")
        );
        assert_eq!(row.attachments[0].size, Some(16));
        assert!(!row.attachments[0].is_voice_note);
        assert_eq!(row.text.as_deref(), Some("see attachment"));
        let _ = pending_id;

        // The captioned attachment keeps a text ending — no noun (contract 1.22).
        let summary = service
            .store
            .conversation_summary(&account.id, &conversation_id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_message_kind, None);

        // A caption-less attachment previews as the filename — never an
        // empty preview the conversation list renders as blank.
        let prepared = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-attach-3",
                b64,
                16,
                Some("report.pdf"),
                Some("application/pdf"),
                None,
                None,
                false,
                false,
            )
            .unwrap();
        assert!(matches!(prepared, PreparedSend::Dispatch { .. }));
        let summary = service
            .store
            .conversation_summary(&account.id, &conversation_id)
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_message_preview.as_deref(), Some("report.pdf"));
        // Caption-less attachment ending: the generic file noun (contract 1.22).
        assert_eq!(summary.last_message_kind, Some("file"));

        // Unknown account and conversation answer their deterministic errors.
        let error = service
            .prepare_send_attachment(
                "no-such-account",
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-attach-2",
                b64,
                16,
                None,
                None,
                None,
                None,
                false,
                false,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "ACCOUNT_NOT_FOUND");

        // §4.30: a voice-note send rides an audio attachment — the data URI
        // gains the ;voice=true parameter and the descriptor records the flag
        // so the sender's own bubble renders the voice UI across restarts.
        let prepared = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-voice-1",
                b64,
                16,
                None,
                Some("audio/ogg"),
                None,
                None,
                true,
                false,
            )
            .unwrap();
        let PreparedSend::Dispatch { params, .. } = &prepared else {
            panic!("expected a dispatch");
        };
        let uri = params["attachments"][0].as_str().unwrap();
        assert_eq!(
            uri,
            "data:audio/ogg;voice=true;base64,YXR0YWNobWVudCBieXRlcw=="
        );
        let row = service
            .store
            .message_by_client_request(&account.id, "req-voice-1")
            .unwrap()
            .expect("the pending voice row exists");
        assert!(row.attachments[0].is_voice_note);
        assert_eq!(
            row.attachments[0].content_type.as_deref(),
            Some("audio/ogg")
        );

        // A voice flag on a non-audio attachment rejects deterministically
        // before the pending row exists: the same clientRequestId stays fresh.
        let error = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation_id),
                "req-voice-2",
                b64,
                16,
                None,
                Some("image/png"),
                None,
                None,
                true,
                false,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        assert!(
            service
                .store
                .message_by_client_request(&account.id, "req-voice-2")
                .unwrap()
                .is_none()
        );
    }

    /// Contract 1.66 (§4.59): the plural form sends ONE upstream message
    /// whose array-typed `attachments` carries every entry in order — the
    /// official forward face. The pending row carries one descriptor per
    /// entry, in array order; the caption is message-level; settlement is
    /// the §4.12 chain. Rejections (empty array, past the 32-entry upstream
    /// ceiling, combined budget over the §4.12 100 MiB message budget,
    /// per-entry payload/filename violations) all answer INVALID_REQUEST
    /// before the pending row exists, so the same clientRequestId stays a
    /// fresh request. A single-element array is the singular form's twin:
    /// same upstream wire shape, same idempotency chain.
    #[test]
    fn prepare_send_attachment_multi_merges_entries_into_one_upstream_message() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let b64_16 = "YXR0YWNobWVudCBieXRlcw=="; // "attachment bytes", 16 raw
        let b64_5 = "aGVsbG8="; // "hello", 5 raw
        let entries = |items: &[(&str, u64, Option<&str>, Option<&str>)]| {
            items
                .iter()
                .map(
                    |(data, size, filename, content_type)| MessagesSendAttachmentEntry {
                        data_base64: (*data).to_string(),
                        size_bytes: *size,
                        filename: filename.map(str::to_string),
                        content_type: content_type.map(str::to_string),
                    },
                )
                .collect::<Vec<_>>()
        };

        // The plural send: one dispatch, two ordered entries, one caption.
        let prepared = service
            .prepare_send_attachment_multi(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-multi-1",
                &entries(&[
                    (b64_16, 16, Some("notes.txt"), Some("text/plain")),
                    (b64_5, 5, Some("clip.ogg"), Some("audio/ogg")),
                ]),
                Some("two files"),
                None,
            )
            .unwrap();
        let PreparedSend::Dispatch { params, .. } = &prepared else {
            panic!("expected a dispatch");
        };
        assert_eq!(params["message"], "two files");
        let attachments = params["attachments"].as_array().unwrap();
        assert_eq!(attachments.len(), 2);
        assert_eq!(
            attachments[0].as_str().unwrap(),
            "data:text/plain;filename=notes.txt;base64,YXR0YWNobWVudCBieXRlcw=="
        );
        assert_eq!(
            attachments[1].as_str().unwrap(),
            "data:audio/ogg;filename=clip.ogg;base64,aGVsbG8="
        );
        assert!(params.get("viewOnce").is_none());
        // The pending row carries one descriptor per entry, in array order.
        let row = service
            .store
            .message_by_client_request(&account.id, "req-multi-1")
            .unwrap()
            .expect("the pending multi row exists");
        assert_eq!(row.attachments.len(), 2);
        assert_eq!(row.attachments[0].filename.as_deref(), Some("notes.txt"));
        assert_eq!(row.attachments[1].filename.as_deref(), Some("clip.ogg"));
        assert_eq!(row.attachments[1].size, Some(5));
        assert!(row.attachments.iter().all(|entry| !entry.is_voice_note));

        // Idempotency: replaying the clientRequestId returns the existing
        // row instead of a second dispatch.
        let replay = service
            .prepare_send_attachment_multi(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-multi-1",
                &entries(&[(b64_16, 16, Some("notes.txt"), Some("text/plain"))]),
                Some("two files"),
                None,
            )
            .unwrap();
        assert!(matches!(replay, PreparedSend::Existing(_)));

        // A single-element array is the singular form's twin: the same data
        // URI the §4.12 singular send composes for the same payload.
        let single = service
            .prepare_send_attachment_multi(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-multi-single",
                &entries(&[(b64_16, 16, Some("notes.txt"), Some("text/plain"))]),
                Some("see attachment"),
                None,
            )
            .unwrap();
        let PreparedSend::Dispatch { params, .. } = &single else {
            panic!("expected a dispatch");
        };
        assert_eq!(params["message"], "see attachment");
        assert_eq!(
            params["attachments"][0].as_str().unwrap(),
            "data:text/plain;filename=notes.txt;base64,YXR0YWNobWVudCBieXRlcw=="
        );

        // Empty array: INVALID_REQUEST, nothing behind.
        let error = service
            .prepare_send_attachment_multi(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-multi-empty",
                &entries(&[]),
                None,
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        assert!(
            service
                .store
                .message_by_client_request(&account.id, "req-multi-empty")
                .unwrap()
                .is_none()
        );

        // Past the 32-entry upstream ceiling: INVALID_REQUEST (mirrored
        // locally instead of the engine's late internal error), nothing
        // behind.
        let oversized: Vec<MessagesSendAttachmentEntry> = (0..33)
            .map(|_| MessagesSendAttachmentEntry {
                data_base64: b64_5.to_string(),
                size_bytes: 5,
                filename: Some("a.txt".into()),
                content_type: Some("text/plain".into()),
            })
            .collect();
        let error = service
            .prepare_send_attachment_multi(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-multi-cap",
                &oversized,
                None,
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        assert!(
            service
                .store
                .message_by_client_request(&account.id, "req-multi-cap")
                .unwrap()
                .is_none()
        );

        // Combined budget: the entries' declared sizes together exceed the
        // §4.12 100 MiB message budget even though each entry is within it.
        // The total check runs before per-entry payload validation, so the
        // rejection needs no megabyte payloads.
        let heavy: Vec<MessagesSendAttachmentEntry> = (0..2)
            .map(|_| MessagesSendAttachmentEntry {
                data_base64: b64_5.to_string(),
                size_bytes: 60_000_000,
                filename: Some("big.bin".into()),
                content_type: Some("application/octet-stream".into()),
            })
            .collect();
        let error = service
            .prepare_send_attachment_multi(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-multi-budget",
                &heavy,
                None,
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        assert!(
            service
                .store
                .message_by_client_request(&account.id, "req-multi-budget")
                .unwrap()
                .is_none()
        );

        // Per-entry filename discipline: a path separator rejects the whole
        // request before the pending row exists.
        let bad_name = entries(&[(b64_16, 16, Some("../escape.txt"), Some("text/plain"))]);
        let error = service
            .prepare_send_attachment_multi(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-multi-path",
                &bad_name,
                None,
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        assert!(
            service
                .store
                .message_by_client_request(&account.id, "req-multi-path")
                .unwrap()
                .is_none()
        );
    }

    /// §4.59 host-side form discipline: the plural and singular payload forms
    /// are mutually exclusive and voiceNote/viewOnce stay singular-form
    /// faces. The host answers INVALID_REQUEST deterministically before any
    /// prepare work; a plural request whose entries violate the §4.12 family
    /// rejects inside prepare with the row-absent guarantee.
    #[test]
    fn messages_send_attachment_params_reject_mixed_forms_at_deserialization_time() {
        let base = json!({
            "accountId": "acc-1",
            "conversationId": "conv-1",
            "clientRequestId": "req-form-1",
        });
        // A plural form alone deserializes (the host then enforces the
        // singular-key absence deterministically).
        let plural_only = serde_json::from_value::<MessagesSendAttachmentParams>(json!({
            "accountId": "acc-1",
            "conversationId": "conv-1",
            "clientRequestId": "req-form-2",
            "attachments": [
                { "dataBase64": "aGVsbG8=", "sizeBytes": 5 }
            ]
        }))
        .unwrap();
        assert_eq!(plural_only.attachments.as_ref().unwrap().len(), 1);
        assert!(plural_only.data_base64.is_none());
        assert!(!plural_only.voice_note);
        let _ = base;

        // Any singular key beside the array fails the host's deterministic
        // form match (simulated at the deserialization boundary: the shape
        // deserializes — both key sets are declared — so the mutual
        // exclusion lives in the host, exactly like the addressing rule).
        let mixed = serde_json::from_value::<MessagesSendAttachmentParams>(json!({
            "accountId": "acc-1",
            "conversationId": "conv-1",
            "clientRequestId": "req-form-3",
            "dataBase64": "aGVsbG8=",
            "sizeBytes": 5,
            "attachments": [
                { "dataBase64": "aGVsbG8=", "sizeBytes": 5 }
            ]
        }))
        .unwrap();
        assert!(mixed.data_base64.is_some() && mixed.attachments.is_some());

        // An unknown key still fails closed at deserialization.
        assert!(
            serde_json::from_value::<MessagesSendAttachmentParams>(json!({
                "accountId": "acc-1",
                "conversationId": "conv-1",
                "clientRequestId": "req-form-4",
                "attachments": [],
                "bogus": true
            }))
            .is_err()
        );
    }

    /// Contract 1.38 (§4.36): the `viewOnce` flag demands the official
    /// view-once shape — an image/* or video/* contentType, no caption, no
    /// quote, no voiceNote — and every violation rejects before a pending row
    /// exists. A passing send carries the upstream `viewOnce` key and marks
    /// its pending row's rich record so the sender's bubble renders the
    /// view-once UI across restarts.
    #[test]
    fn prepare_send_attachment_view_once_validates_and_marks_the_row() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let b64 = "iVBORw0KGgo="; // canonical base64 of the 8 PNG magic bytes

        // Every violation answers INVALID_REQUEST and leaves no pending row.
        type Reject = (
            &'static str,
            &'static str,
            Option<&'static str>,
            Option<&'static str>,
            bool,
        );
        let rejects: &[Reject] = &[
            ("not media", "text/plain", None, None, false),
            ("voice mix", "audio/ogg", None, None, true),
            ("quote mix", "image/png", None, Some("m-quote"), false),
            ("caption mix", "image/png", Some("caption"), None, false),
        ];
        for (label, content_type, caption, quote, voice) in rejects {
            let error = service
                .prepare_send_attachment(
                    &account.id,
                    &AttachmentSendTarget::Conversation(&conversation.id),
                    "req-view",
                    b64,
                    8,
                    None,
                    Some(content_type),
                    *caption,
                    *quote,
                    *voice,
                    true,
                )
                .unwrap_err();
            assert_eq!(error.into_api().code, "INVALID_REQUEST", "{label}");
            assert!(
                service
                    .store
                    .message_by_client_request(&account.id, "req-view")
                    .unwrap()
                    .is_none(),
                "{label}"
            );
        }

        // The passing shape dispatches with the upstream flag and persists
        // the view-once marker on its own pending row.
        let prepared = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-view-ok",
                b64,
                8,
                None,
                Some("image/png"),
                None,
                None,
                false,
                true,
            )
            .unwrap();
        let PreparedSend::Dispatch { params, .. } = &prepared else {
            panic!("expected a dispatch");
        };
        assert_eq!(params["viewOnce"], serde_json::json!(true));
        let row = service
            .store
            .message_by_client_request(&account.id, "req-view-ok")
            .unwrap()
            .expect("the pending view-once row exists");
        let rich = row.rich.expect("the view-once marker persists");
        assert!(rich.view_once);
        assert!(rich.view_once_opened_at.is_none());

        // A plain send (the flag false) keeps the upstream key absent — the
        // pre-1.38 wire shape stays byte-identical.
        let prepared = service
            .prepare_send_attachment(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-view-off",
                b64,
                8,
                None,
                Some("image/png"),
                None,
                None,
                false,
                false,
            )
            .unwrap();
        let PreparedSend::Dispatch { params, .. } = &prepared else {
            panic!("expected a dispatch");
        };
        assert!(params.get("viewOnce").is_none());
        let row = service
            .store
            .message_by_client_request(&account.id, "req-view-off")
            .unwrap()
            .expect("the plain pending row exists");
        assert!(row.rich.is_none());
    }

    /// Contract 1.38 (§4.37): `messages.markViewOnceOpened` refuses
    /// non-view-once rows, burns the first call (one event, the fan-out plan
    /// names the incoming direct row's author on both legs), answers a replay
    /// with no event and no fan-out, sends the outgoing row's open sync to
    /// the account itself, and skips both legs for an unresolvable group
    /// author while the burn still ran.
    #[test]
    fn prepare_mark_view_once_opened_burns_and_plans_fanout() {
        use crate::engine::{NormalizedAttachment, NormalizedRich};
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let group = service
            .store
            .ensure_conversation(&account.id, "group", "group-one", "group")
            .unwrap();
        let base = |id: &str, conversation_id: &str, direction: &'static str| MessageRecord {
            id: id.into(),
            account_id: account.id.clone(),
            conversation_id: conversation_id.into(),
            direction,
            sender_id: if direction == "outgoing" {
                account.id.clone()
            } else {
                "peer-hash".into()
            },
            sender_name: None,
            mentions_self: false,
            sent_at: 99,
            received_at: None,
            text: Some("vanishing media".into()),
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
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            admin_deleted: false,
            server_guid: None,
        };
        for record in [
            base("m-view-in", &conversation.id, "incoming"),
            base("m-view-out", &conversation.id, "outgoing"),
            base("m-view-group", &group.id, "incoming"),
            // Same shapes without the marker: deterministic refusals.
            {
                let mut plain = base("m-plain", &conversation.id, "incoming");
                plain.rich = None;
                plain
            },
        ] {
            service
                .store
                .insert_message(&record, None, Some("vanishing"), true)
                .unwrap();
        }
        let mark = |message_id: &str| crate::service::MessagesMarkViewOnceOpenedParams {
            account_id: account.id.clone(),
            conversation_id: conversation.id.clone(),
            message_id: message_id.into(),
        };

        // A row that is not a view-once message answers INVALID_REQUEST and
        // stays untouched.
        let error = service
            .prepare_mark_view_once_opened(&mark("m-plain"))
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        let row = service
            .store
            .message_by_id(&account.id, &conversation.id, "m-plain")
            .unwrap()
            .expect("row");
        assert_eq!(row.text.as_deref(), Some("vanishing media"));

        // The incoming direct row: one event, both fan-out legs address the
        // author, the timestamp is the burned row's Signal timestamp.
        let prepared = service
            .prepare_mark_view_once_opened(&mark("m-view-in"))
            .unwrap();
        assert_eq!(prepared.events.len(), 1);
        assert_eq!(prepared.account_signal, "+15555550100");
        assert_eq!(prepared.timestamp, 99);
        assert_eq!(prepared.receipt_recipient.as_deref(), Some("+15555550101"));
        assert_eq!(prepared.open_sender_aci.as_deref(), Some("+15555550101"));
        let opened_at = match &prepared.events[0] {
            HostSideEvent::MessageViewOnceOpened { opened_at, .. } => *opened_at,
            other => panic!("unexpected event {other:?}"),
        };
        let row = service
            .store
            .message_by_id(&account.id, &conversation.id, "m-view-in")
            .unwrap()
            .expect("row");
        assert_eq!(row.text, None);
        assert_eq!(row.rich.expect("rich").view_once_opened_at, Some(opened_at));
        assert_eq!(row.attachments.len(), 1);

        // The replay is a trivial no-op: no event, and both fan-out legs skip
        // (the supervisor answers plain `sent` without touching upstream).
        let replay = service
            .prepare_mark_view_once_opened(&mark("m-view-in"))
            .unwrap();
        assert!(replay.events.is_empty());
        assert!(replay.receipt_recipient.is_none());
        assert!(replay.open_sender_aci.is_none());

        // The outgoing row burns too; its open sync names our own account
        // (the engine resolves the registered number to the own ACI) and no
        // receipt leg exists — we are the author.
        let prepared = service
            .prepare_mark_view_once_opened(&mark("m-view-out"))
            .unwrap();
        assert_eq!(prepared.events.len(), 1);
        assert!(prepared.receipt_recipient.is_none());
        assert_eq!(prepared.open_sender_aci.as_deref(), Some("+15555550100"));

        // A group row's author is unresolvable (§4.28): both legs skip, the
        // burn still ran.
        let prepared = service
            .prepare_mark_view_once_opened(&crate::service::MessagesMarkViewOnceOpenedParams {
                account_id: account.id.clone(),
                conversation_id: group.id.clone(),
                message_id: "m-view-group".into(),
            })
            .unwrap();
        assert_eq!(prepared.events.len(), 1);
        assert!(prepared.receipt_recipient.is_none());
        assert!(prepared.open_sender_aci.is_none());
        let row = service
            .store
            .message_by_id(&account.id, &group.id, "m-view-group")
            .unwrap()
            .expect("row");
        assert_eq!(row.text, None);
    }

    /// prepare_send_sticker (contract revision 1.35, §4.31): bounds failures
    /// reject before the pending row exists, the happy path dispatches a
    /// body-less upstream send whose only payload keys are account, the
    /// sticker object and the shared upstream target, and the pending row
    /// carries the pack identity plus the metadata-only image descriptor.
    #[test]
    fn prepare_send_sticker_validates_bounds_and_builds_sticker_params() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let image_b64 = "iVBORw0KGgo="; // canonical base64 of the 8 PNG magic bytes
        let pack_key = "AAAAAAAAAAAAAAAAAAAAAA=="; // standard base64 of 16 zero bytes

        // Every bounds failure answers INVALID_REQUEST and leaves no pending
        // row: the same clientRequestId stays a fresh request.
        let rejects = &[
            (
                "odd hex length",
                "abc",
                pack_key,
                Some("🎉"),
                "image/webp",
                "packId must be an even-length hex string of 1-64 characters",
            ),
            (
                "non-hex pack id",
                "abcdefgh",
                pack_key,
                None,
                "image/webp",
                "packId must be an even-length hex string of 1-64 characters",
            ),
            (
                "oversized pack id",
                "ab".repeat(33).leak(),
                pack_key,
                None,
                "image/webp",
                "packId must be an even-length hex string of 1-64 characters",
            ),
            (
                "non-base64 pack key",
                "abcdef01",
                "not base64!!",
                None,
                "image/webp",
                "packKey must be standard base64",
            ),
            (
                "oversized pack key",
                "abcdef01",
                "QQ".repeat(65).leak(),
                None,
                "image/webp",
                "packKey must contain between 1 and 128 characters",
            ),
            (
                "oversized emoji",
                "abcdef01",
                pack_key,
                Some(&"😀".repeat(33)),
                "image/webp",
                "emoji must contain between 1 and 32 characters",
            ),
            (
                "video content type",
                "abcdef01",
                pack_key,
                None,
                "video/mp4",
                "sticker image contentType must be image/*",
            ),
        ];
        for (index, (name, pack_id, pack_key, emoji, content_type, message)) in
            rejects.iter().enumerate()
        {
            let error = service
                .prepare_send_sticker(
                    &account.id,
                    &AttachmentSendTarget::Conversation(&conversation.id),
                    "req-sticker-invalid",
                    pack_id,
                    pack_key,
                    7,
                    *emoji,
                    &MessagesSendStickerImage {
                        data_base64: image_b64.to_string(),
                        size_bytes: 8,
                        content_type: (*content_type).to_string(),
                        width: Some(512),
                        height: None,
                    },
                    None,
                )
                .unwrap_err();
            let api = error.into_api();
            assert_eq!(api.code, "INVALID_REQUEST", "{name}");
            assert_eq!(api.message, *message, "{name}");
            assert!(
                service
                    .store
                    .message_by_client_request(&account.id, "req-sticker-invalid")
                    .unwrap()
                    .is_none(),
                "{name} must not leave a pending row"
            );
            let _ = index;
        }

        // A payload that fails the §4.12 family (size mismatch) rejects the
        // same way.
        let error = service
            .prepare_send_sticker(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-sticker-invalid",
                "abcdef01",
                pack_key,
                7,
                None,
                &MessagesSendStickerImage {
                    data_base64: image_b64.to_string(),
                    size_bytes: 9,
                    content_type: "image/webp".to_string(),
                    width: None,
                    height: None,
                },
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        assert!(
            service
                .store
                .message_by_client_request(&account.id, "req-sticker-invalid")
                .unwrap()
                .is_none()
        );

        // Happy path: exactly-one addressing, emoji/width keys present only
        // when supplied, and no `message` / `attachments` key upstream.
        let prepared = service
            .prepare_send_sticker(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-sticker-1",
                "abcdef01",
                pack_key,
                7,
                Some("🎉"),
                &MessagesSendStickerImage {
                    data_base64: image_b64.to_string(),
                    size_bytes: 8,
                    content_type: "image/webp".to_string(),
                    width: Some(512),
                    height: Some(512),
                },
                None,
            )
            .unwrap();
        let PreparedSend::Dispatch {
            pending_id,
            account_id,
            conversation_id,
            params,
            pending_sent_at,
        } = &prepared
        else {
            panic!("expected a dispatch");
        };
        assert_eq!(account_id, &account.id);
        assert_eq!(conversation_id, &conversation.id);
        assert_eq!(params["account"], "+15555550100");
        assert!(params.get("message").is_none());
        assert!(params.get("attachments").is_none());
        assert_eq!(
            params["recipient"],
            json!(["+15555550101"]),
            "direct chats address the recipient array"
        );
        let sticker = &params["sticker"];
        assert_eq!(sticker["packId"], "abcdef01");
        assert_eq!(sticker["packKey"], pack_key);
        assert_eq!(sticker["stickerId"], 7);
        assert_eq!(sticker["emoji"], "🎉");
        assert_eq!(sticker["width"], 512);
        assert_eq!(sticker["height"], 512);
        assert_eq!(
            sticker["image"], "data:image/webp;base64,iVBORw0KGgo=",
            "the image re-encodes as a data URI without a filename"
        );
        assert!(*pending_sent_at > 0);
        let _ = pending_id;

        // The pending row records the pack identity beside a metadata-only
        // image descriptor — no body text.
        let row = service
            .store
            .message_by_client_request(&account.id, "req-sticker-1")
            .unwrap()
            .expect("the pending sticker row exists");
        assert_eq!(row.status, "pending");
        assert!(row.text.is_none());
        assert_eq!(row.attachments.len(), 1);
        let descriptor = &row.attachments[0];
        assert_eq!(descriptor.content_type.as_deref(), Some("image/webp"));
        assert_eq!(descriptor.filename, None);
        assert_eq!(descriptor.size, Some(8));
        assert_eq!(descriptor.width, Some(512));
        assert_eq!(descriptor.height, Some(512));
        assert!(!descriptor.is_voice_note);
        let sticker_meta = row.sticker.expect("the row carries the pack identity");
        assert_eq!(sticker_meta.pack_id, "abcdef01");
        assert_eq!(sticker_meta.pack_key, pack_key);
        assert_eq!(sticker_meta.sticker_id, 7);
        assert_eq!(sticker_meta.emoji.as_deref(), Some("🎉"));

        // Replaying the same clientRequestId returns the existing row.
        let replay = service
            .prepare_send_sticker(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "req-sticker-1",
                "abcdef01",
                pack_key,
                7,
                Some("🎉"),
                &MessagesSendStickerImage {
                    data_base64: image_b64.to_string(),
                    size_bytes: 8,
                    content_type: "image/webp".to_string(),
                    width: Some(512),
                    height: Some(512),
                },
                None,
            )
            .unwrap();
        assert!(matches!(replay, PreparedSend::Existing(_)));

        // Peer addressing progressive-fills a group skeleton and addresses
        // groupId upstream; emoji/width/height keys stay absent when unsupplied.
        let prepared = service
            .prepare_send_sticker(
                &account.id,
                &AttachmentSendTarget::Peer(PeerTarget {
                    kind: "group",
                    peer_key: "ZmFrZS1ncm91cC0x",
                    peer_title: None,
                }),
                "req-sticker-2",
                "ABCDEF01",
                pack_key,
                0,
                None,
                &MessagesSendStickerImage {
                    data_base64: image_b64.to_string(),
                    size_bytes: 8,
                    content_type: "image/png".to_string(),
                    width: None,
                    height: None,
                },
                None,
            )
            .unwrap();
        let PreparedSend::Dispatch { params, .. } = &prepared else {
            panic!("expected a dispatch");
        };
        assert!(params.get("recipient").is_none());
        assert_eq!(params["groupId"], "ZmFrZS1ncm91cC0x");
        assert!(params["sticker"].get("emoji").is_none());
        assert!(params["sticker"].get("width").is_none());
        assert!(params["sticker"].get("height").is_none());
    }

    /// Sticker pack browsing bounds (contract revision 1.36, §4.32): the
    /// browse-face pack identity is strictly the official 32-hex/32-byte
    /// shape — a 1.35-legal 8-hex pack id or 16-byte key is rejected here.
    #[test]
    fn prepare_sticker_pack_browse_validates_bounds() {
        let (_temp, service) = service();
        let good_id = "abcdef0123456789abcdef0123456789";
        // Standard padded base64 of exactly 32 zero bytes.
        let good_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

        // Happy shapes validate.
        service
            .prepare_sticker_pack_manifest(&StickerPackManifestParams {
                pack_id: good_id.to_string(),
                pack_key: good_key.to_string(),
            })
            .unwrap();
        service
            .prepare_sticker_pack_image(&StickerPackImageParams {
                pack_id: good_id.to_string(),
                pack_key: good_key.to_string(),
                sticker_id: 7,
            })
            .unwrap();

        // Each parameter fails with at least one representative.
        let rejects: &[(&str, &str, &str)] = &[
            // packId: the 1.35 even-hex face is NOT enough on the browse face.
            ("8-hex pack id (1.35 shape)", "abcdef01", good_key),
            ("31 hex chars", &"a".repeat(31), good_key),
            ("33 hex chars", &"a".repeat(33), good_key),
            ("non-hex id", &"g".repeat(32), good_key),
            // packKey: decodes to 16 bytes, not the required 32.
            ("16-byte key", "AAAAAAAAAAAAAAAAAAAAAA==", good_key),
            ("non-base64 key", good_id, "not base64!!"),
            ("31-byte key", &"QQ".repeat(31), good_key),
        ];
        for (name, pack_id, pack_key) in rejects {
            let error = service
                .prepare_sticker_pack_manifest(&StickerPackManifestParams {
                    pack_id: (*pack_id).to_string(),
                    pack_key: (*pack_key).to_string(),
                })
                .unwrap_err();
            assert_eq!(error.into_api().code, "INVALID_REQUEST", "{name}");
            let error = service
                .prepare_sticker_pack_image(&StickerPackImageParams {
                    pack_id: (*pack_id).to_string(),
                    pack_key: (*pack_key).to_string(),
                    sticker_id: 7,
                })
                .unwrap_err();
            assert_eq!(error.into_api().code, "INVALID_REQUEST", "{name}");
        }
    }

    /// Conversation pin sync bounds (contract revision 1.36, §4.33): the
    /// kind enum, the conversationId bound, and the required `pinned`
    /// boolean; a resolved conversation projects the upstream reference
    /// (peer key), never the host's opaque local id, and an unknown
    /// conversation answers CONVERSATION_NOT_FOUND before any upstream
    /// call.
    #[test]
    fn prepare_conversations_pinned_resolves_and_validates() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);

        // Happy path: the upstream params carry the resolved peer key.
        let params = service
            .prepare_conversations_set_pinned(&account.id, &conversation.id, "contact", true)
            .unwrap();
        assert_eq!(params["account"], "+15555550100");
        assert_eq!(params["conversationId"], "+15555550101");
        assert_eq!(params["kind"], "contact");
        assert_eq!(params["pinned"], true);

        let get_params = service
            .prepare_conversations_get_pinned(&account.id)
            .unwrap();
        assert_eq!(get_params["account"], "+15555550100");

        // kind enum: only the three upstream values pass.
        for kind in ["group", "legacyGroup"] {
            service
                .prepare_conversations_set_pinned(&account.id, &conversation.id, kind, false)
                .unwrap();
        }
        let error = service
            .prepare_conversations_set_pinned(&account.id, &conversation.id, "story", true)
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // conversationId bound: empty and >256 chars reject; the 256-char
        // boundary value passes validation shape (then fails resolution).
        let error = service
            .prepare_conversations_set_pinned(&account.id, "", "contact", true)
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        let error = service
            .prepare_conversations_set_pinned(&account.id, &"x".repeat(257), "contact", true)
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        let error = service
            .prepare_conversations_set_pinned(&account.id, &"x".repeat(256), "contact", true)
            .unwrap_err();
        assert_eq!(error.into_api().code, "CONVERSATION_NOT_FOUND");

        // Unknown conversation answers before any upstream call.
        let error = service
            .prepare_conversations_set_pinned(&account.id, "no-such-conversation", "contact", true)
            .unwrap_err();
        assert_eq!(error.into_api().code, "CONVERSATION_NOT_FOUND");

        // Unknown account answers ACCOUNT_NOT_FOUND on both faces.
        let error = service
            .prepare_conversations_get_pinned("no-such-account")
            .unwrap_err();
        assert_eq!(error.into_api().code, "ACCOUNT_NOT_FOUND");
    }

    /// The pinned-list projection (contract revision 1.36, §4.33): cloud
    /// order preserved, the 128-entry KT cap truncates, and entries missing
    /// identity fields are dropped rather than half-carried.
    #[test]
    fn pinned_conversations_result_caps_and_projects() {
        let entries: Vec<Value> = (0..150)
            .map(|index| {
                json!({
                    "conversationId": format!("conv-{index}"),
                    "kind": if index % 3 == 0 { "contact" } else if index % 3 == 1 { "group" } else { "legacyGroup" },
                })
            })
            .collect();
        let result = pinned_conversations_result(&entries);
        assert_eq!(result.pinned.len(), MAX_PINNED_CONVERSATIONS);
        assert_eq!(result.pinned[0].conversation_id, "conv-0");
        assert_eq!(result.pinned[127].conversation_id, "conv-127");
        assert_eq!(result.pinned[0].kind, "contact");

        // Malformed entries drop instead of failing the whole read.
        let mixed = vec![
            json!({"conversationId": "conv-a", "kind": "contact"}),
            json!({"kind": "group"}),
            json!({"conversationId": "conv-b"}),
            json!({"conversationId": "conv-c", "kind": "legacyGroup"}),
        ];
        let result = pinned_conversations_result(&mixed);
        assert_eq!(result.pinned.len(), 2);
        assert_eq!(result.pinned[1].kind, "legacyGroup");
    }

    /// Sticker pack sync bounds (contract revision 1.37, §4.34): the pack id
    /// is the §4.32 32-hex shape; installed=true requires the 32-byte pack
    /// key; installed=false ignores packKey/position — the upstream write is
    /// exactly the tombstone regardless of what the caller supplied.
    #[test]
    fn prepare_sticker_pack_sync_validates_bounds() {
        let (_temp, mut service) = service();
        let (account, _conversation) = linked_account_and_conversation(&mut service);
        let good_id = "abcdef0123456789abcdef0123456789";
        // Standard padded base64 of exactly 32 zero bytes.
        let good_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

        // Happy install: the upstream account is the resolved signal account
        // and the key/position ride through verbatim.
        let params = service
            .prepare_sticker_pack_set_sync(&account.id, good_id, Some(good_key), true, Some(3))
            .unwrap();
        assert_eq!(params["account"], "+15555550100");
        assert_eq!(params["packId"], good_id);
        assert_eq!(params["packKey"], good_key);
        assert_eq!(params["installed"], true);
        assert_eq!(params["position"], 3);

        // Uninstall: supplied key/position are dropped — the wire carries
        // exactly the tombstone shape (nulls) no matter what the caller sent.
        let params = service
            .prepare_sticker_pack_set_sync(&account.id, good_id, Some(good_key), false, Some(3))
            .unwrap();
        assert_eq!(params["installed"], false);
        assert!(params["packKey"].is_null());
        assert!(params["position"].is_null());

        let get_params = service.prepare_sticker_pack_get_syncs(&account.id).unwrap();
        assert_eq!(get_params["account"], "+15555550100");

        // installed=true without a key: deterministic local rejection.
        let error = service
            .prepare_sticker_pack_set_sync(&account.id, good_id, None, true, Some(3))
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // A key decoding to 16 bytes fails the install (the 1.35 send-face
        // shape is not enough here).
        let error = service
            .prepare_sticker_pack_set_sync(
                &account.id,
                good_id,
                Some("AAAAAAAAAAAAAAAAAAAAAA=="),
                true,
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // packId: the browse-face 32-hex rule on both faces.
        let error = service
            .prepare_sticker_pack_set_sync(&account.id, "abcdef01", Some(good_key), true, None)
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // Unknown account answers ACCOUNT_NOT_FOUND on both faces.
        let error = service
            .prepare_sticker_pack_get_syncs("no-such-account")
            .unwrap_err();
        assert_eq!(error.into_api().code, "ACCOUNT_NOT_FOUND");
        let error = service
            .prepare_sticker_pack_set_sync("no-such-account", good_id, Some(good_key), true, None)
            .unwrap_err();
        assert_eq!(error.into_api().code, "ACCOUNT_NOT_FOUND");
    }

    /// The pack-record projection (contract revision 1.37, §4.34): cloud
    /// order preserved, the 256-entry KT cap truncates, and entries missing
    /// packId drop rather than half-carried; the optional fields project
    /// leniently (an out-of-u32 position degrades to absent instead of
    /// discarding an otherwise-identified record).
    #[test]
    fn sticker_pack_syncs_result_caps_and_projects() {
        let entries: Vec<Value> = (0..300)
            .map(|index| {
                json!({
                    "packId": format!("{index:032x}"),
                    "packKey": if index % 2 == 0 {
                        Value::Null
                    } else {
                        json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
                    },
                    "position": if index % 3 == 0 { Value::Null } else { json!(index) },
                    "deletedAtTimestampMs": if index % 2 == 0 {
                        json!(1727000000000u64 + index)
                    } else {
                        Value::Null
                    },
                })
            })
            .collect();
        let result = sticker_pack_syncs_result(&entries);
        assert_eq!(result.packs.len(), MAX_STICKER_PACK_SYNCS);
        assert_eq!(result.packs[0].pack_id, format!("{:032x}", 0));
        assert_eq!(result.packs[255].pack_id, format!("{:032x}", 255));
        assert_eq!(result.packs[1].position, Some(1));
        assert!(result.packs[0].deleted_at_timestamp_ms.is_some());
        assert!(result.packs[1].deleted_at_timestamp_ms.is_none());

        // Missing identity drops the entry; a position beyond u32 degrades
        // to absent; a tombstone carries the timestamp without key/position.
        let mixed = vec![
            json!({
                "packId": "a".repeat(32),
                "packKey": Value::Null,
                "position": 4294967296u64,
                "deletedAtTimestampMs": Value::Null,
            }),
            json!({"packKey": "orphan"}),
            json!({
                "packId": "b".repeat(32),
                "packKey": Value::Null,
                "position": Value::Null,
                "deletedAtTimestampMs": json!(1727000000000u64),
            }),
        ];
        let result = sticker_pack_syncs_result(&mixed);
        assert_eq!(result.packs.len(), 2);
        assert_eq!(result.packs[0].pack_id, "a".repeat(32));
        assert_eq!(result.packs[0].position, None);
        assert_eq!(result.packs[1].pack_id, "b".repeat(32));
        assert_eq!(result.packs[1].deleted_at_timestamp_ms, Some(1727000000000));
    }

    /// Account configuration bounds (contract revision 1.46, §4.45): at
    /// least one writable key is required (zero keys fail closed locally —
    /// the engine would answer -32603), the sharing-mode enum is the official
    /// two-value proto set, and only the provided keys forward upstream so
    /// the partial-write semantics survive the hop.
    #[test]
    fn prepare_account_configuration_validates_and_forwards_only_provided_keys() {
        let (_temp, mut service) = service();
        let (account, _conversation) = linked_account_and_conversation(&mut service);
        let params =
            |read_receipts: Option<bool>, sharing: Option<&str>, unlisted: Option<bool>| {
                AccountsUpdateConfigurationParams {
                    account_id: account.id.clone(),
                    read_receipts,
                    typing_indicators: None,
                    phone_number_sharing_mode: sharing.map(str::to_string),
                    unlisted_phone_number: unlisted,
                }
            };

        // Happy partial write: exactly the provided keys ride the upstream
        // payload beside the resolved account.
        let upstream = service
            .prepare_accounts_update_configuration(&params(Some(true), Some("nobody"), None))
            .unwrap();
        assert_eq!(upstream["account"], "+15555550100");
        assert_eq!(upstream["readReceipts"], true);
        assert_eq!(upstream["phoneNumberSharingMode"], "nobody");
        assert!(upstream.get("typingIndicators").is_none());
        assert!(upstream.get("unlistedPhoneNumber").is_none());

        // Zero writable keys: deterministic local refusal, not a silent no-op.
        let error = service
            .prepare_accounts_update_configuration(&params(None, None, None))
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // The old CONTACTS_ONLY value is gone from the official proto.
        let error = service
            .prepare_accounts_update_configuration(&params(None, Some("contactsOnly"), None))
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // Unknown account answers ACCOUNT_NOT_FOUND before any upstream call.
        let mut unknown = params(Some(true), None, None);
        unknown.account_id = "no-such-account".to_string();
        let error = service
            .prepare_accounts_update_configuration(&unknown)
            .unwrap_err();
        assert_eq!(error.into_api().code, "ACCOUNT_NOT_FOUND");
    }

    /// Profile bounds (contract revision 1.46, §4.45): the official desktop
    /// editor budgets (name components 128 bytes, bio 512, emoji 32), the
    /// avatar tri-state keeps absent/null/string distinct on the wire, and
    /// the data-URI gate refuses non-image families, non-canonical base64 and
    /// over-budget payloads before any upstream call.
    #[test]
    fn prepare_account_profile_validates_bounds_and_avatar_tri_state() {
        let (_temp, mut service) = service();
        let (account, _conversation) = linked_account_and_conversation(&mut service);
        let params = |given_name: &str,
                      family_name: Option<&str>,
                      about: Option<&str>,
                      emoji: Option<&str>,
                      avatar: Option<Value>| AccountsSetProfileParams {
            account_id: account.id.clone(),
            given_name: given_name.to_string(),
            family_name: family_name.map(str::to_string),
            about: about.map(str::to_string),
            emoji: emoji.map(str::to_string),
            avatar,
        };

        // Minimal write: only givenName forwards (the upstream text face is a
        // full write — absent keys are the caller's clear instruction, the
        // connector adds nothing).
        let upstream = service
            .prepare_accounts_set_profile(&params("Li", None, None, None, None))
            .unwrap();
        assert_eq!(upstream["account"], "+15555550100");
        assert_eq!(upstream["givenName"], "Li");
        assert!(upstream.get("familyName").is_none());
        assert!(upstream.get("avatar").is_none());

        // Full write: text keys forward verbatim; an empty optional clears
        // upstream (official full-write semantics — empty is a legal value).
        let upstream = service
            .prepare_accounts_set_profile(&params(
                "Li",
                Some(""),
                Some("hello"),
                Some("🎉"),
                Some(Value::Null),
            ))
            .unwrap();
        assert_eq!(upstream["familyName"], "");
        assert_eq!(upstream["about"], "hello");
        assert_eq!(upstream["emoji"], "🎉");
        assert!(upstream["avatar"].is_null());

        // Replace: a legal data URI forwards verbatim.
        let upstream = service
            .prepare_accounts_set_profile(&params(
                "Li",
                None,
                None,
                None,
                Some(json!("data:image/png;base64,YQ==")),
            ))
            .unwrap();
        assert_eq!(upstream["avatar"], "data:image/png;base64,YQ==");

        // Text bounds: the official editor budgets, enforced deterministically.
        for (request_id, profile) in [
            ("empty-given-name", params("", None, None, None, None)),
            (
                "given-name-over-128-bytes",
                params(&"A".repeat(129), None, None, None, None),
            ),
            (
                "family-name-over-128-bytes",
                params("Li", Some(&"A".repeat(129)), None, None, None),
            ),
            (
                "about-over-512-bytes",
                params("Li", None, Some(&"A".repeat(513)), None, None),
            ),
            (
                "emoji-over-32-bytes",
                params("Li", None, None, Some(&"A".repeat(33)), None),
            ),
        ] {
            let error = service.prepare_accounts_set_profile(&profile).unwrap_err();
            assert_eq!(error.into_api().code, "INVALID_REQUEST", "{request_id}");
        }

        // Avatar gate: non-string, non-image family, missing base64 marker,
        // empty payload, non-canonical payload, over-budget decode.
        for avatar in [
            json!(7),
            json!("data:text/html;base64,YQ=="),
            json!("data:image/png,YQ=="),
            json!("data:image/png;base64,"),
            json!("data:image/png;base64,YQ"),
            json!(format!(
                "data:image/png;base64,{}",
                "QUJD".repeat(MAX_AVATAR_BYTES / 3 + 1)
            )),
        ] {
            let error = service
                .prepare_accounts_set_profile(&params("Li", None, None, None, Some(avatar)))
                .unwrap_err();
            assert_eq!(error.into_api().code, "INVALID_REQUEST");
        }

        // The exact 10 MiB decoded budget passes; unknown account errors first-class.
        let at_bound = format!(
            "data:image/png;base64,{}",
            "QUJD".repeat(MAX_AVATAR_BYTES / 3)
        );
        assert_eq!(
            (MAX_AVATAR_BYTES / 3) * 3,
            MAX_AVATAR_BYTES - MAX_AVATAR_BYTES % 3
        );
        let upstream = service
            .prepare_accounts_set_profile(&params("Li", None, None, None, Some(json!(at_bound))))
            .unwrap();
        assert!(
            upstream["avatar"]
                .as_str()
                .unwrap()
                .starts_with("data:image/png;base64,")
        );

        let error = service
            .prepare_conversations_get_blocked("no-such-account")
            .unwrap_err();
        assert_eq!(error.into_api().code, "ACCOUNT_NOT_FOUND");
    }

    /// Universal-timer forward (contract revision 1.46, §4.45): the resolved
    /// account plus the engine-face `expirationInSeconds` key; 0 forwards as
    /// the legal off value, the u32 face is params-shaped so no extra ladder
    /// exists beyond opaque-id/account checks.
    #[test]
    fn prepare_account_expire_timer_forwards_the_engine_face() {
        let (_temp, mut service) = service();
        let (account, _conversation) = linked_account_and_conversation(&mut service);

        let upstream = service
            .prepare_accounts_set_expire_timer(&account.id, 0)
            .unwrap();
        assert_eq!(upstream["account"], "+15555550100");
        assert_eq!(upstream["expirationInSeconds"], 0);

        let upstream = service
            .prepare_accounts_set_expire_timer(&account.id, 86400)
            .unwrap();
        assert_eq!(upstream["expirationInSeconds"], 86400);

        let error = service
            .prepare_accounts_set_expire_timer("no-such-account", 86400)
            .unwrap_err();
        assert_eq!(error.into_api().code, "ACCOUNT_NOT_FOUND");

        let blocked_params = service
            .prepare_conversations_get_blocked(&account.id)
            .unwrap();
        assert_eq!(blocked_params["account"], "+15555550100");
    }

    /// The blocked-list projection (contract revision 1.46, §4.45): storage
    /// order preserved, the 256-entry KT cap truncates, and entries missing
    /// identity or kind drop rather than half-carried — the §4.33 pinned
    /// projection discipline with the wider cap.
    #[test]
    fn blocked_conversations_result_caps_and_projects() {
        let entries: Vec<Value> = (0..300)
            .map(|index| {
                json!({
                    "conversationId": format!("conv-{index}"),
                    "kind": if index % 3 == 0 { "contact" } else if index % 3 == 1 { "group" } else { "legacyGroup" },
                })
            })
            .collect();
        let result = blocked_conversations_result(&entries);
        assert_eq!(result.blocked.len(), MAX_BLOCKED_CONVERSATIONS);
        assert_eq!(result.blocked[0].conversation_id, "conv-0");
        assert_eq!(result.blocked[255].conversation_id, "conv-255");
        assert_eq!(result.blocked[0].kind, "contact");

        // Malformed entries drop instead of failing the whole read.
        let mixed = vec![
            json!({"conversationId": "conv-a", "kind": "contact"}),
            json!({"kind": "group"}),
            json!({"conversationId": "conv-b"}),
            json!({"conversationId": "conv-c", "kind": "legacyGroup"}),
        ];
        let result = blocked_conversations_result(&mixed);
        assert_eq!(result.blocked.len(), 2);
        assert_eq!(result.blocked[1].kind, "legacyGroup");
    }

    /// Avatar face (contract revision 1.48, §4.47): the conversation
    /// resolves through the shared ladder and forwards the row's peer
    /// reference; the blocked write forwards the resolved reference, the
    /// kind enum, and the boolean verbatim.
    #[test]
    fn prepare_avatar_and_set_blocked_forward_resolved_references() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);

        // getAvatar: peer reference, never the opaque local id; bounds first.
        let upstream = service
            .prepare_conversations_get_avatar(&account.id, &conversation.id)
            .unwrap();
        assert_eq!(upstream["account"], "+15555550100");
        assert_eq!(upstream["conversationId"], conversation.peer_key);

        for (request_id, account_id, conversation_id) in [
            ("avatar-empty-conversation", account.id.as_str(), ""),
            (
                "avatar-over-bound-conversation",
                account.id.as_str(),
                &"A".repeat(257),
            ),
            ("avatar-unknown-account", "no-such-account", "conv"),
            (
                "avatar-unknown-conversation",
                account.id.as_str(),
                "no-such-conversation",
            ),
        ] {
            let error = service
                .prepare_conversations_get_avatar(account_id, conversation_id)
                .unwrap_err();
            assert_eq!(
                error.into_api().code,
                if request_id == "avatar-unknown-account" {
                    "ACCOUNT_NOT_FOUND"
                } else if request_id == "avatar-unknown-conversation" {
                    "CONVERSATION_NOT_FOUND"
                } else {
                    "INVALID_REQUEST"
                },
                "{request_id}"
            );
        }

        // setBlocked: the resolved reference plus the kind enum and boolean,
        // the §4.33 pin-write wire shape with `blocked` in `pinned`'s place.
        let upstream = service
            .prepare_conversations_set_blocked(&account.id, &conversation.id, "contact", true)
            .unwrap();
        assert_eq!(upstream["account"], "+15555550100");
        assert_eq!(upstream["conversationId"], conversation.peer_key);
        assert_eq!(upstream["kind"], "contact");
        assert_eq!(upstream["blocked"], true);

        let upstream = service
            .prepare_conversations_set_blocked(&account.id, &conversation.id, "group", false)
            .unwrap();
        assert_eq!(upstream["blocked"], false);
        assert_eq!(upstream["kind"], "group");

        // Fail-closed ladder: illegal kind, unknown conversation, empty id.
        let error = service
            .prepare_conversations_set_blocked(&account.id, &conversation.id, "story", true)
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        let error = service
            .prepare_conversations_set_blocked(&account.id, "no-such-conversation", "contact", true)
            .unwrap_err();
        assert_eq!(error.into_api().code, "CONVERSATION_NOT_FOUND");
        let error = service
            .prepare_conversations_set_blocked(&account.id, "", "contact", true)
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
    }

    /// The message-request faces (contract revision 1.63, §4.56): the direct
    /// ladder forwards the resolved peer reference as `recipient`; a group
    /// conversation forwards the raw group master key as `groupId`; bounds
    /// and resolution fail closed in wire order. The `type` stamp is the
    /// supervisor's (accept/delete), never the prepare's.
    #[test]
    fn prepare_message_request_faces_route_direct_and_group() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);

        // Direct: peer reference, never the opaque local id; the same ladder
        // as the §4.47 faces.
        let upstream = service
            .prepare_conversations_message_request(&account.id, &conversation.id)
            .unwrap();
        assert_eq!(upstream["account"], "+15555550100");
        assert_eq!(upstream["recipient"], conversation.peer_key);
        assert!(upstream.get("groupId").is_none());
        assert!(upstream.get("type").is_none());

        // Bounds and resolution fail closed in wire order.
        for (request_id, account_id, conversation_id, expected) in [
            (
                "empty-conversation",
                account.id.as_str(),
                "",
                "INVALID_REQUEST",
            ),
            (
                "over-bound-conversation",
                account.id.as_str(),
                &"A".repeat(257),
                "INVALID_REQUEST",
            ),
            (
                "unknown-account",
                "no-such-account",
                "conv",
                "ACCOUNT_NOT_FOUND",
            ),
            (
                "unknown-conversation",
                account.id.as_str(),
                "no-such-conversation",
                "CONVERSATION_NOT_FOUND",
            ),
        ] {
            let error = service
                .prepare_conversations_message_request(account_id, conversation_id)
                .unwrap_err();
            assert_eq!(error.into_api().code, expected, "{request_id}");
        }

        // Group: the raw group master key rides as `groupId` (the
        // conversations row's peer_key — the sync materializes it from the
        // same `listGroups` id), never a `recipient`.
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "abc123GROUP", "Group")
            .unwrap();
        let upstream = service
            .prepare_conversations_message_request(&account.id, &group.id)
            .unwrap();
        assert_eq!(upstream["groupId"], "abc123GROUP");
        assert!(upstream.get("recipient").is_none());
    }

    /// The safety-number faces (contract revision 1.68, §4.61): the read
    /// narrows the pinned listIdentities call to the resolved peer reference
    /// (`number` key); the verify carries it as `recipient` beside the echoed
    /// verification digits (`verifiedSafetyNumber` — never the testing-only
    /// trustAllKnownKeys mode). The 1:1 gate fails group/legacy rows and the
    /// §4.42 self row (empty peer reference) closed before any upstream call;
    /// bounds and resolution fail closed in wire order.
    #[test]
    fn prepare_identity_faces_gate_one_to_one_and_forward_peer_reference() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);

        // Read: the peer reference rides the upstream `number` key verbatim.
        let upstream = service
            .prepare_identities_get(&account.id, &conversation.id)
            .unwrap();
        assert_eq!(upstream["account"], "+15555550100");
        assert_eq!(upstream["number"], conversation.peer_key);

        // Verify: the same reference as `recipient` plus the echoed digits;
        // trustAllKnownKeys is never part of the payload.
        let upstream = service
            .prepare_identities_verify(&account.id, &conversation.id, "12345 67890")
            .unwrap();
        assert_eq!(upstream["recipient"], conversation.peer_key);
        assert_eq!(upstream["verifiedSafetyNumber"], "12345 67890");
        assert!(upstream.get("trustAllKnownKeys").is_none());

        // The verification string bound fails closed before any upstream
        // resolution (empty, and one past the 256-char wire bound).
        for (request_id, safety_number) in [
            ("empty-digits", ""),
            ("over-bound-digits", &"1".repeat(257)[..]),
        ] {
            let error = service
                .prepare_identities_verify(&account.id, &conversation.id, safety_number)
                .unwrap_err();
            assert_eq!(error.into_api().code, "INVALID_REQUEST", "{request_id}");
        }

        // Group and legacy rows are the 1:1 gate's fail-closed face (the
        // official details row gates on !isGroup); the §4.42 self row's empty
        // peer reference fails closed the same way.
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "abc123GROUP", "Group")
            .unwrap();
        let error = service
            .prepare_identities_get(&account.id, &group.id)
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        let legacy = service
            .store_ref()
            .ensure_conversation(&account.id, "legacyGroup", "abc123LEGACY", "Legacy")
            .unwrap();
        let error = service
            .prepare_identities_verify(&account.id, &legacy.id, "12345 67890")
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        let self_row = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "", "Note to Self")
            .unwrap();
        let error = service
            .prepare_identities_get(&account.id, &self_row.id)
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // Bounds and resolution fail closed in wire order (shared ladder).
        for (request_id, account_id, conversation_id, expected) in [
            (
                "empty-conversation",
                account.id.as_str(),
                "",
                "INVALID_REQUEST",
            ),
            (
                "unknown-account",
                "no-such-account",
                "conv",
                "ACCOUNT_NOT_FOUND",
            ),
            (
                "unknown-conversation",
                account.id.as_str(),
                "no-such-conversation",
                "CONVERSATION_NOT_FOUND",
            ),
        ] {
            let error = service
                .prepare_identities_get(account_id, conversation_id)
                .unwrap_err();
            assert_eq!(error.into_api().code, expected, "{request_id}");
        }
    }

    /// The safety-number read projection (contract revision 1.68, §4.61):
    /// the pinned JsonIdentity fields pass through verbatim with the nullable
    /// trio (number/uuid/scannableSafetyNumber) optional; an empty upstream
    /// array is the honest `identity: null` (never an error); a record
    /// missing its non-nullable core is a protocol defect, not a no-identity
    /// answer.
    #[test]
    fn identity_projection_maps_pinned_record_and_empty_list() {
        // Full record: every field round-trips, camelCase on the wire.
        let read = identity_from_engine(&json!([{
            "number": "+15555550103",
            "uuid": "99999999-9999-9999-9999-999999999999",
            "fingerprint": "ab".repeat(32),
            "safetyNumber": "12345 67890",
            "scannableSafetyNumber": "AQIDBAUGBwgJCgsMDQ4PEA==",
            "trustLevel": "TRUSTED_VERIFIED",
            "addedTimestamp": 1727000000000_u64,
        }]))
        .unwrap();
        let identity = read.identity.as_ref().unwrap();
        assert_eq!(identity.number.as_deref(), Some("+15555550103"));
        assert_eq!(identity.trust_level, "TRUSTED_VERIFIED");
        assert_eq!(identity.added_timestamp, 1727000000000);
        let wire = serde_json::to_value(&read).unwrap();
        assert_eq!(wire["identity"]["safetyNumber"], "12345 67890");
        assert_eq!(wire["identity"]["trustLevel"], "TRUSTED_VERIFIED");
        assert_eq!(
            wire["identity"]["scannableSafetyNumber"],
            "AQIDBAUGBwgJCgsMDQ4PEA=="
        );

        // Nullable trio: a UUID-only record carries no number key on the
        // wire; a record without the scannable payload omits the key.
        let read = identity_from_engine(&json!([{
            "number": serde_json::Value::Null,
            "uuid": "99999999-9999-9999-9999-999999999999",
            "fingerprint": "ab".repeat(32),
            "safetyNumber": "12345 67890",
            "scannableSafetyNumber": serde_json::Value::Null,
            "trustLevel": "TRUSTED_UNVERIFIED",
            "addedTimestamp": 1727000000000_u64,
        }]))
        .unwrap();
        let wire = serde_json::to_value(&read).unwrap();
        assert!(wire["identity"].get("number").is_none());
        assert!(wire["identity"].get("scannableSafetyNumber").is_none());
        assert_eq!(
            wire["identity"]["uuid"],
            "99999999-9999-9999-9999-999999999999"
        );

        // Empty list: the honest no-identity-yet state.
        let read = identity_from_engine(&json!([])).unwrap();
        assert!(read.identity.is_none());
        let wire = serde_json::to_value(&read).unwrap();
        assert!(wire.get("identity").is_none());

        // Malformed record: the non-nullable core missing is a protocol
        // defect, folded by the caller into the structured upstream family.
        let error = identity_from_engine(&json!([{
            "number": "+15555550103",
            "fingerprint": "ab".repeat(32),
        }]))
        .unwrap_err();
        assert!(matches!(error, ServiceError::Engine(EngineError::Protocol)));
    }

    /// The avatar result projection (contract revision 1.48, §4.47):
    /// dataUri/byteSize pass through, the coarse result-face bound holds,
    /// and a shape-violating engine answer is a protocol error — nothing
    /// half-carried ships to the host.
    #[test]
    fn avatar_result_projection_holds_the_face_bound() {
        let payload = "aGVsbG8="; // "hello", 5 bytes
        let ok = json!({
            "dataUri": format!("data:image/png;base64,{payload}"),
            "size": 5,
        });
        let download = crate::supervisor::avatar_from_engine_for_tests(&ok).unwrap();
        assert_eq!(download.byte_size, 5);
        assert!(download.data_uri.starts_with("data:image/png;base64,"));

        // byteSize disagreeing with the decoded count: protocol error.
        let mismatch = json!({
            "dataUri": format!("data:image/png;base64,{payload}"),
            "size": 4,
        });
        assert_eq!(
            crate::supervisor::avatar_from_engine_for_tests(&mismatch)
                .unwrap_err()
                .into_api()
                .code,
            "UPSTREAM_PROTOCOL_ERROR"
        );

        // Missing fields / non-base64 payload: protocol errors.
        for broken in [
            json!({"size": 5}),
            json!({"dataUri": format!("data:image/png;base64,{payload}")}),
            json!({"dataUri": "data:image/png;base64,!!!", "size": 5}),
        ] {
            assert_eq!(
                crate::supervisor::avatar_from_engine_for_tests(&broken)
                    .unwrap_err()
                    .into_api()
                    .code,
                "UPSTREAM_PROTOCOL_ERROR",
                "{broken}"
            );
        }
    }

    /// Call signaling (contract revision 1.53, §4.49): the sendSignal shape
    /// gate (type enum / positive callId / payload mandatory for the modern
    /// faces and optional for the legacy family / urgency enum), the resolved
    /// direct peer reference as the upstream `recipient`, the legacy bare-frame
    /// routing, and the group-shape refusal.
    #[test]
    fn prepare_call_signal_gates_and_forwards_the_envelope() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);

        // The happy path: resolved number as recipient, payload verbatim.
        let upstream = service
            .prepare_calls_send_signal(
                &account.id,
                &conversation.id,
                "offer",
                7,
                Some("c2RwLW9mZmVy"),
                None,
            )
            .unwrap();
        assert_eq!(upstream["account"], "+15555550100");
        assert_eq!(upstream["recipient"], conversation.peer_key);
        assert_eq!(upstream["data"], "c2RwLW9mZmVy");
        assert!(
            upstream.get("urgency").is_none(),
            "urgency absent = key absent"
        );

        // The type enum: every legal value passes with a payload, an unknown
        // one and a wrong-typed one fail closed.
        for legal in CALL_SIGNAL_TYPES {
            service
                .prepare_calls_send_signal(
                    &account.id,
                    &conversation.id,
                    legal,
                    7,
                    Some("eA=="),
                    None,
                )
                .unwrap();
        }
        for illegal in ["offerx", "", "OFFER", "sdp"] {
            let error = service
                .prepare_calls_send_signal(
                    &account.id,
                    &conversation.id,
                    illegal,
                    7,
                    Some("eA=="),
                    None,
                )
                .unwrap_err();
            assert_eq!(error.into_api().code, "INVALID_REQUEST", "{illegal}");
        }

        // The callId face: 0 is the no-call marker and is rejected; the u32
        // ceiling is legal.
        let error = service
            .prepare_calls_send_signal(
                &account.id,
                &conversation.id,
                "offer",
                0,
                Some("eA=="),
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");
        service
            .prepare_calls_send_signal(
                &account.id,
                &conversation.id,
                "hangup",
                u32::MAX,
                Some("eA=="),
                None,
            )
            .unwrap();

        // The payload face: empty, wrong type, and over-budget all reject
        // before any resolution.
        for (label, call_id, payload) in [
            ("payload-empty", 7u32, ""),
            ("payload-over-bound", 7u32, &"A".repeat(3 * 1024 * 1024 + 1)),
        ] {
            let error = service
                .prepare_calls_send_signal(
                    &account.id,
                    &conversation.id,
                    "offer",
                    call_id,
                    Some(payload),
                    None,
                )
                .unwrap_err();
            assert_eq!(error.into_api().code, "INVALID_REQUEST", "{label}");
        }

        // The legacy face (1.53): busy/hangup without a payload route the
        // engine's `legacy` bare-frame family — official proto shapes, no
        // invented bytes, no `data` key. A modern type without a payload
        // still refuses (offer/answer/ice carry RingRTC bytes by protocol).
        let legacy_busy = service
            .prepare_calls_send_signal(&account.id, &conversation.id, "busy", 7, None, None)
            .unwrap();
        assert_eq!(legacy_busy["legacy"], json!({"kind": "busy", "id": 7}));
        assert!(
            legacy_busy.get("data").is_none(),
            "legacy face carries no data"
        );
        let legacy_hangup = service
            .prepare_calls_send_signal(&account.id, &conversation.id, "hangup", 9, None, None)
            .unwrap();
        assert_eq!(legacy_hangup["legacy"], json!({"kind": "hangup", "id": 9}));
        let error = service
            .prepare_calls_send_signal(&account.id, &conversation.id, "offer", 7, None, None)
            .unwrap_err();
        assert_eq!(
            error.into_api().code,
            "INVALID_REQUEST",
            "modern type demands payload"
        );
        let error = service
            .prepare_calls_send_signal(&account.id, &conversation.id, "ice", 7, None, None)
            .unwrap_err();
        assert_eq!(
            error.into_api().code,
            "INVALID_REQUEST",
            "ice demands payload"
        );

        // The urgency face (1.53): the two-value enum rides through, absence
        // stays absent (the engine's droppable default), anything else
        // refuses before any upstream call.
        let urgent = service
            .prepare_calls_send_signal(
                &account.id,
                &conversation.id,
                "offer",
                7,
                Some("eA=="),
                Some("handleImmediately"),
            )
            .unwrap();
        assert_eq!(urgent["urgency"], "handleImmediately");
        let droppable = service
            .prepare_calls_send_signal(
                &account.id,
                &conversation.id,
                "busy",
                7,
                None,
                Some("droppable"),
            )
            .unwrap();
        assert_eq!(droppable["urgency"], "droppable");
        assert_eq!(droppable["legacy"], json!({"kind": "busy", "id": 7}));
        let error = service
            .prepare_calls_send_signal(
                &account.id,
                &conversation.id,
                "offer",
                7,
                Some("eA=="),
                Some("urgent"),
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // The local ladder: unknown account and conversation refuse before
        // any upstream shape is built.
        let error = service
            .prepare_calls_send_signal(
                "no-such-account",
                &conversation.id,
                "offer",
                7,
                Some("eA=="),
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "ACCOUNT_NOT_FOUND");
        let error = service
            .prepare_calls_send_signal(
                &account.id,
                "no-such-conversation",
                "offer",
                7,
                Some("eA=="),
                None,
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "CONVERSATION_NOT_FOUND");

        // The 1:1-only face: a group conversation fails closed locally.
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "Z3JvdXAtaWQ=", "group")
            .unwrap();
        let error = service
            .prepare_calls_send_signal(&account.id, &group.id, "offer", 7, Some("eA=="), None)
            .unwrap_err();
        assert_eq!(error.into_api().code, "INVALID_REQUEST");

        // The state face: the honest empty registry, idempotent across
        // repeats, and an unknown account still refuses.
        let state = service.prepare_calls_get_state(&account.id).unwrap();
        assert!(state.calls.is_empty());
        assert_eq!(state.projection, "connector-pipe");
        let again = service.prepare_calls_get_state(&account.id).unwrap();
        assert_eq!(again.calls.len(), state.calls.len());
        let error = service
            .prepare_calls_get_state("no-such-account")
            .unwrap_err();
        assert_eq!(error.into_api().code, "ACCOUNT_NOT_FOUND");
    }

    /// The inbound call event (contract revision 1.50, §4.48): a Call
    /// control receive routes by the shared conversation ladder and emits
    /// exactly one transient CallIncoming event carrying the payload
    /// verbatim — no row, no store write, nothing else on the lane.
    #[test]
    fn call_control_receive_emits_one_transient_event() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);

        let receive = crate::engine::NormalizedReceive {
            timestamp: Some(1727000000000),
            content_kind: "control",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".to_string()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: None,
            text_bytes: None,
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            control: Some(crate::engine::ControlReceive::Call {
                call_id: 7,
                signal_type: "offer",
                payload: json!({ "offer": { "id": 7, "opaque": "c2Rw" } }),
            }),
            expire_seconds: None,
            expire_start_at: None,
            server_guid: None,
        };
        let events = service
            .ingest_receive(receive, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(events.len(), 1, "{events:?}");
        let HostSideEvent::CallIncoming {
            account_id,
            conversation_id,
            call_id,
            signal_type,
            payload,
        } = &events[0]
        else {
            panic!("expected a CallIncoming event: {events:?}");
        };
        assert_eq!(account_id, &account.id);
        assert_eq!(conversation_id, &conversation.id);
        assert_eq!(*call_id, 7);
        assert_eq!(*signal_type, "offer");
        assert_eq!(payload["offer"]["opaque"], "c2Rw");

        // Nothing persisted: no row, no store write, nothing to self-heal.
        let listed = service
            .store_ref()
            .list_messages(&account.id, &conversation.id, 50, None)
            .unwrap();
        assert!(listed.items.is_empty(), "no row may be created");
    }

    /// The §4.35 sticker-quote face: the quote resolves through the same
    /// ladder as `dispatch_send` (upstream quoteTimestamp/quoteAuthor beside
    /// the sticker object, the pending row records the target), an unknown
    /// target answers MESSAGE_NOT_FOUND with no row left behind, and a
    /// replay of an inserted row returns it without re-resolving the quote.
    #[test]
    fn prepare_send_sticker_resolves_quote_through_the_shared_ladder() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let quoted_id = seed_outgoing_sent(&mut service, &account.id, &conversation.id, 777);
        let image = MessagesSendStickerImage {
            data_base64: "iVBORw0KGgo=".to_string(),
            size_bytes: 8,
            content_type: "image/webp".to_string(),
            width: None,
            height: None,
        };

        // Happy path: the quote keys ride the send top level beside the
        // sticker object; the pending row records the quoted target.
        let prepared = service
            .prepare_send_sticker(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "sticker-quote-1",
                "abcdef01",
                "AAAAAAAAAAAAAAAAAAAAAA==",
                7,
                None,
                &image,
                Some(&quoted_id),
            )
            .unwrap();
        let PreparedSend::Dispatch { params, .. } = &prepared else {
            panic!("expected a dispatch");
        };
        assert_eq!(params["quoteTimestamp"], 777);
        assert_eq!(params["quoteAuthor"], "+15555550100");
        let row = service
            .store
            .message_by_client_request(&account.id, "sticker-quote-1")
            .unwrap()
            .expect("the pending row exists");
        assert_eq!(row.quote_message_id.as_deref(), Some(quoted_id.as_str()));

        // Unknown target: MESSAGE_NOT_FOUND and nothing behind — the same
        // clientRequestId stays a fresh request.
        let error = service
            .prepare_send_sticker(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "sticker-quote-2",
                "abcdef01",
                "AAAAAAAAAAAAAAAAAAAAAA==",
                7,
                None,
                &image,
                Some("no-such-row"),
            )
            .unwrap_err();
        assert_eq!(error.into_api().code, "MESSAGE_NOT_FOUND");
        assert!(
            service
                .store
                .message_by_client_request(&account.id, "sticker-quote-2")
                .unwrap()
                .is_none()
        );

        // Replay: the inserted row returns as Existing without re-resolving
        // — the quote validation ran once, before the row was inserted.
        let replay = service
            .prepare_send_sticker(
                &account.id,
                &AttachmentSendTarget::Conversation(&conversation.id),
                "sticker-quote-1",
                "abcdef01",
                "AAAAAAAAAAAAAAAAAAAAAA==",
                7,
                None,
                &image,
                Some("no-such-row"),
            )
            .unwrap();
        assert!(matches!(replay, PreparedSend::Existing(_)));
    }

    /// groups.get (contract revision 1.9, implementation-plan §4.8): a pure
    /// cache projection. Synced groups answer with their stored title and
    /// memberCount; anything else — a contact peer key, an unsynced group, a
    /// missing account, a malformed key — answers its deterministic error
    /// without any upstream call.
    #[test]
    fn get_group_projects_the_cached_group_row() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "group",
                "ZmFrZS1ncm91cC0x",
                "Fixture Group",
                Some("{\"memberCount\":2}"),
                1725300000,
            )
            .unwrap();
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "contact",
                "+15555550101",
                "Alice Contact",
                None,
                1725300000,
            )
            .unwrap();

        let group = service.get_group(&account.id, "ZmFrZS1ncm91cC0x").unwrap();
        assert_eq!(group.peer_key, "ZmFrZS1ncm91cC0x");
        assert_eq!(group.title, "Fixture Group");
        assert_eq!(group.member_count, Some(2));
        assert_eq!(group.synced_at, 1725300000);
        // A pre-1.40 row (memberCount only, contract 1.9 shape) keeps the
        // §4.8 response: no members key.
        assert_eq!(group.members, None);
        // Contract 1.53 (§4.50 appendix): a row without the captured
        // terminated flag (pre-1.53 shape, engines without the upstream
        // face) answers the default false.
        assert!(!group.is_terminated);

        // Contract 1.40 (§4.39): a synced roster projects identities, the
        // read-time contacts names, the number-based self marker, and the
        // upstream admin mark. The wire shape carries only the informative
        // keys: unknown name, foreign uuid, non-self, non-admin all collapse
        // to absent; entries without an address drop individually.
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "group",
                "cm9zdGVyLWdyb3Vw",
                "Roster Group",
                Some(
                    json!({
                        "memberCount": 5,
                        "members": [
                            {"id": "+15555550100"},
                            {"id": "+15555550101", "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10", "admin": true},
                            {"id": "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1"},
                            {"uuid": "no-address-entry"},
                            "not-an-object",
                        ],
                    })
                    .to_string()
                    .as_str(),
                ),
                1725300001,
            )
            .unwrap();
        let roster = service.get_group(&account.id, "cm9zdGVyLWdyb3Vw").unwrap();
        assert_eq!(roster.member_count, Some(5));
        let members = roster.members.unwrap();
        assert_eq!(members.len(), 3);
        let wire = serde_json::to_value(&members).unwrap();
        // Self member: id is the linked account's number; no contacts row
        // exists for self, so the name stays absent.
        assert_eq!(
            wire[0],
            json!({"id": "+15555550100", "self": true}),
            "self member projects the number-based marker"
        );
        // Alice: contacts-cache name at read time, upstream uuid and admin
        // mark captured by the sync.
        assert_eq!(
            wire[1],
            json!({
                "id": "+15555550101",
                "uuid": "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10",
                "name": "Alice Contact",
                "admin": true,
            })
        );
        // An ACI-keyed member the cache does not know: bare identity only.
        assert_eq!(
            wire[2],
            json!({"id": "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1"})
        );

        // The roster cap holds even for a hostile oversized capture: the
        // projection returns at most the official group-size ceiling.
        let flood: Vec<Value> = (0..MAX_GROUP_MEMBERS + 50)
            .map(|index| json!({"id": format!("+1555555{index:04}")}))
            .collect();
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "group",
                "Zmxvb2QtZ3JvdXA",
                "Flood Group",
                Some(
                    json!({"memberCount": flood.len(), "members": flood})
                        .to_string()
                        .as_str(),
                ),
                1725300002,
            )
            .unwrap();
        let flooded = service.get_group(&account.id, "Zmxvb2QtZ3JvdXA").unwrap();
        assert_eq!(
            flooded.members.as_ref().map(Vec::len),
            Some(MAX_GROUP_MEMBERS)
        );
        assert_eq!(flooded.member_count, Some((MAX_GROUP_MEMBERS + 50) as u64));

        // Contract 1.67 (§4.60): the captured label pair projects as
        // absent-when-none keys — present verbatim for labelled members,
        // absent for label-less members, and dropped (not truncated) for
        // values past the server budgets the capture enforced.
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "group",
                "bGFiZWwtZ3JvdXA",
                "Label Group",
                Some(
                    json!({
                        "memberCount": 4,
                        "members": [
                            {"id": "+15555550100", "labelEmoji": "🛠️", "label": "Builders"},
                            {"id": "+15555550101"},
                            {"id": "+15555550102", "label": "x".repeat(512)},
                            {"id": "+15555550103", "label": "x".repeat(513)},
                        ],
                    })
                    .to_string()
                    .as_str(),
                ),
                1725300003,
            )
            .unwrap();
        let labelled = service.get_group(&account.id, "bGFiZWwtZ3JvdXA").unwrap();
        let labelled_wire = serde_json::to_value(labelled.members.unwrap()).unwrap();
        assert_eq!(
            labelled_wire[0],
            json!({"id": "+15555550100", "self": true, "labelEmoji": "🛠️", "label": "Builders"}),
        );
        assert_eq!(
            labelled_wire[1],
            json!({"id": "+15555550101", "name": "Alice Contact"})
        );
        assert_eq!(labelled_wire[2]["label"], json!("x".repeat(512)));
        assert!(labelled_wire[2].get("labelEmoji").is_none());
        assert_eq!(labelled_wire[3], json!({"id": "+15555550103"}));

        // A group row without a captured member list omits memberCount.
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "group",
                "Z3JvdXAtbm8tbWVtYmVycw",
                "No Members",
                None,
                7,
            )
            .unwrap();
        let bare = service
            .get_group(&account.id, "Z3JvdXAtbm8tbWVtYmVycw")
            .unwrap();
        assert_eq!(bare.title, "No Members");
        assert_eq!(bare.member_count, None);
        assert_eq!(bare.synced_at, 7);

        // Contract 1.53 (§4.50 appendix): the terminated flag the sync
        // captured projects as a plain bool — true only when the upstream
        // JsonGroup carried it, alongside the still-absent members list.
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "group",
                "dGVybWluYXRlZC1ncm91cA",
                "Ended Group",
                Some(r#"{"memberCount":4,"isTerminated":true}"#),
                1725300003,
            )
            .unwrap();
        let ended = service
            .get_group(&account.id, "dGVybWluYXRlZC1ncm91cA")
            .unwrap();
        assert!(ended.is_terminated);
        assert_eq!(ended.member_count, Some(4));
        assert_eq!(ended.members, None);
        // The wire shape carries the plain bool even when false.
        let wire = serde_json::to_value(&ended).unwrap();
        assert_eq!(wire["isTerminated"], json!(true));
        let plain = serde_json::to_value(&group).unwrap();
        assert_eq!(plain["isTerminated"], json!(false));

        // Contract 1.62 (§4.53 appendix): the live requesting-members
        // projection. None (the pre-1.62 face, engine stopped / refused)
        // keeps the key absent — byte-identical to the cached row face.
        let quiet = service.get_group(&account.id, "ZmFrZS1ncm91cC0x").unwrap();
        assert_eq!(quiet.requesting_members, None);
        let quiet_wire = serde_json::to_value(&quiet).unwrap();
        assert!(quiet_wire.get("requestingMembers").is_none());

        // The live face: the engine's raw JsonGroupMemberAddress entries
        // project with the first-wins ladder id, the upstream uuid (only
        // when it differs from the id), and the read-time contacts-cache
        // name. Malformed entries drop individually; the cap holds.
        let with_requesting = store_get_group_with_requesting(
            service.store_ref(),
            &account.id,
            "ZmFrZS1ncm91cC0x",
            Some(&[
                json!({"number": "+15555550101", "uuid": "9f0fd6a2-1111-4000-8000-000000000001"}),
                json!({"uuid": "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1"}),
                json!({"number": "+15555550102", "uuid": "+15555550102"}),
                json!("not-an-object"),
                json!({}),
            ]),
            None,
        )
        .unwrap();
        let requesting = with_requesting.requesting_members.unwrap();
        let requesting_wire = serde_json::to_value(&requesting).unwrap();
        assert_eq!(
            requesting.len(),
            3,
            "malformed entries drop: {requesting_wire}"
        );
        assert_eq!(
            requesting_wire[0],
            json!({
                "id": "+15555550101",
                "uuid": "9f0fd6a2-1111-4000-8000-000000000001",
                "name": "Alice Contact",
            }),
            "ladder id + upstream uuid + read-time contacts-cache name"
        );
        assert_eq!(
            requesting_wire[1],
            json!({"id": "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1"}),
            "an unknown ACI-keyed candidate keeps the bare identity, uuid never equals id"
        );
        assert_eq!(
            requesting_wire[2],
            json!({"id": "+15555550102"}),
            "the ladder collapses a uuid that repeats the number"
        );

        // An empty live answer (nobody requesting / upstream face missing)
        // collapses to the absent key, not an empty array.
        let empty = store_get_group_with_requesting(
            service.store_ref(),
            &account.id,
            "ZmFrZS1ncm91cC0x",
            Some(&[]),
            None,
        )
        .unwrap();
        assert_eq!(empty.requesting_members, None);

        // The cap holds for a hostile oversized live answer.
        let flood: Vec<Value> = (0..MAX_GROUP_MEMBERS + 50)
            .map(|index| json!({"number": format!("+1555555{index:04}")}))
            .collect();
        let flooded = store_get_group_with_requesting(
            service.store_ref(),
            &account.id,
            "ZmFrZS1ncm91cC0x",
            Some(&flood),
            None,
        )
        .unwrap();
        assert_eq!(
            flooded.requesting_members.as_ref().map(Vec::len),
            Some(MAX_GROUP_MEMBERS)
        );

        // A contact peer key is not a group row: deterministic GROUP_NOT_FOUND.
        let contact = service.get_group(&account.id, "+15555550101").unwrap_err();
        assert_eq!(contact.into_api().code, "GROUP_NOT_FOUND");

        // An unsynced group id is a GROUP_NOT_FOUND, never a listGroups call.
        let unsynced = service
            .get_group(&account.id, "bm90LWEtbWVtYmVy")
            .unwrap_err();
        assert_eq!(unsynced.into_api().code, "GROUP_NOT_FOUND");

        let absent_account = service
            .get_group("no-such-account", "ZmFrZS1ncm91cC0x")
            .unwrap_err();
        assert_eq!(absent_account.into_api().code, "ACCOUNT_NOT_FOUND");

        let malformed = service.get_group(&account.id, "").unwrap_err();
        assert_eq!(malformed.into_api().code, "INVALID_REQUEST");
    }

    /// The live pending-invite projection (contract revision 1.64, §4.57):
    /// the engine's raw JsonGroupMemberAddress entries project with the
    /// first-wins ladder id, the upstream uuid (only when it differs from
    /// the id), and the read-time contacts-cache name; malformed entries
    /// drop individually; the cap holds; absent on None/empty; no inviter
    /// metadata ever rides a row (the recorded upstream projection
    /// difference).
    #[test]
    fn get_group_projects_the_live_pending_members() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "group",
                "ZmFrZS1ncm91cC0x",
                "Fixture Group",
                Some("{\"memberCount\":2}"),
                1725300000,
            )
            .unwrap();
        service
            .store_ref()
            .upsert_contact(
                &account.id,
                "contact",
                "+15555550101",
                "Alice Contact",
                None,
                1725300000,
            )
            .unwrap();

        // The live face: the same projection ladder the requesting face
        // runs, over the pending array.
        let with_pending = store_get_group_with_requesting(
            service.store_ref(),
            &account.id,
            "ZmFrZS1ncm91cC0x",
            None,
            Some(&[
                json!({"number": "+15555550101", "uuid": "9f0fd6a2-1111-4000-8000-000000000001"}),
                json!({"uuid": "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1"}),
                json!({"number": "+15555550102", "uuid": "+15555550102"}),
                json!("not-an-object"),
                json!({}),
            ]),
        )
        .unwrap();
        let pending = with_pending.pending_members.unwrap();
        let pending_wire = serde_json::to_value(&pending).unwrap();
        assert_eq!(pending.len(), 3, "malformed entries drop: {pending_wire}");
        assert_eq!(
            pending_wire[0],
            json!({
                "id": "+15555550101",
                "uuid": "9f0fd6a2-1111-4000-8000-000000000001",
                "name": "Alice Contact",
            }),
            "ladder id + upstream uuid + read-time contacts-cache name"
        );
        assert_eq!(
            pending_wire[1],
            json!({"id": "d43e8ea0-3f6c-4a57-8a5c-51f83f8e5df1"}),
            "an unknown ACI-keyed invitee keeps the bare identity, uuid never equals id"
        );
        assert_eq!(
            pending_wire[2],
            json!({"id": "+15555550102"}),
            "the ladder collapses a uuid that repeats the number"
        );
        assert!(
            pending_wire
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row.get("addedByUserId").is_none()),
            "no inviter metadata rides the projection (upstream has none)"
        );

        // An empty live answer (nobody pending / upstream face missing)
        // collapses to the absent key, not an empty array.
        let empty = store_get_group_with_requesting(
            service.store_ref(),
            &account.id,
            "ZmFrZS1ncm91cC0x",
            None,
            Some(&[]),
        )
        .unwrap();
        assert_eq!(empty.pending_members, None);

        // The cap holds for a hostile oversized live answer.
        let flood: Vec<Value> = (0..MAX_GROUP_MEMBERS + 50)
            .map(|index| json!({"number": format!("+1555555{index:04}")}))
            .collect();
        let flooded = store_get_group_with_requesting(
            service.store_ref(),
            &account.id,
            "ZmFrZS1ncm91cC0x",
            None,
            Some(&flood),
        )
        .unwrap();
        assert_eq!(
            flooded.pending_members.as_ref().map(Vec::len),
            Some(MAX_GROUP_MEMBERS)
        );
    }

    /// contacts.setLocalAlias prepare (contract revision 1.10, §4.9): the
    /// upstream params are exactly `account` + a single-string `recipient` +
    /// `name`; the peer must already be known (cached contact row or direct
    /// conversation peer); a bogus peer, group key, or alias answers
    /// INVALID_REQUEST without any upstream call.
    #[test]
    fn prepare_set_local_alias_builds_upstream_params_and_resolves_the_peer() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        // Known via the direct conversation peer.
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        // Known via the cached contacts row.
        service
            .store_ref()
            .upsert_contact(&account.id, "contact", "+15555550102", "Bob", None, 9)
            .unwrap();

        let via_conversation = service
            .prepare_set_local_alias(&account.id, &direct.peer_key, "Alice K.")
            .unwrap();
        assert_eq!(
            via_conversation,
            json!({
                "account": "+15555550100",
                "recipient": "+15555550101",
                "name": "Alice K."
            })
        );

        let via_contact_row = service
            .prepare_set_local_alias(&account.id, "+15555550102", "Bob B.")
            .unwrap();
        assert_eq!(via_contact_row["recipient"], "+15555550102");
        assert_eq!(via_contact_row["name"], "Bob B.");

        // A group key is not a contact: rejected before any upstream call.
        let group_key = service
            .prepare_set_local_alias(&account.id, "ZmFrZS1ncm91cC0x", "G")
            .unwrap_err();
        assert_eq!(group_key.into_api().code, "INVALID_REQUEST");

        let unknown_peer = service
            .prepare_set_local_alias(&account.id, "+15555559999", "Nobody")
            .unwrap_err();
        assert_eq!(unknown_peer.into_api().code, "INVALID_REQUEST");

        let absent_account = service
            .prepare_set_local_alias("no-such-account", "+15555550101", "A")
            .unwrap_err();
        assert_eq!(absent_account.into_api().code, "ACCOUNT_NOT_FOUND");

        // Shape bounds: empty and oversized aliases.
        let empty = service
            .prepare_set_local_alias(&account.id, "+15555550101", "")
            .unwrap_err();
        assert_eq!(empty.into_api().code, "INVALID_REQUEST");
        let oversized = service
            .prepare_set_local_alias(&account.id, "+15555550101", &"x".repeat(129))
            .unwrap_err();
        assert_eq!(oversized.into_api().code, "INVALID_REQUEST");
    }

    /// presence.setTypingMessage prepare (contract revision 1.11, §4.10):
    /// the upstream params are exactly `account` + the explicit `stop`
    /// boolean + group/peer addressing; a missing conversation answers
    /// CONVERSATION_NOT_FOUND without any upstream call.
    #[test]
    fn prepare_set_typing_message_builds_upstream_params_from_the_conversation() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "ZmFrZS1ncm91cC0x", "G")
            .unwrap();

        let start = service
            .prepare_set_typing_message(&account.id, &direct.id, false)
            .unwrap();
        assert_eq!(
            start,
            json!({
                "account": "+15555550100",
                "stop": false,
                "recipient": ["+15555550101"]
            })
        );

        let stop = service
            .prepare_set_typing_message(&account.id, &direct.id, true)
            .unwrap();
        assert_eq!(
            stop,
            json!({
                "account": "+15555550100",
                "stop": true,
                "recipient": ["+15555550101"]
            })
        );

        let group_start = service
            .prepare_set_typing_message(&account.id, &group.id, false)
            .unwrap();
        assert_eq!(
            group_start,
            json!({
                "account": "+15555550100",
                "stop": false,
                "groupId": "ZmFrZS1ncm91cC0x"
            })
        );

        let missing = service
            .prepare_set_typing_message(&account.id, "no-such-conversation", false)
            .unwrap_err();
        assert_eq!(missing.into_api().code, "CONVERSATION_NOT_FOUND");

        let absent_account = service
            .prepare_set_typing_message("no-such-account", &direct.id, false)
            .unwrap_err();
        assert_eq!(absent_account.into_api().code, "ACCOUNT_NOT_FOUND");
    }

    /// Builds a control-carrying receive addressed to this account/peer.
    fn control_receive(account: &str, source: &str, control: ControlReceive) -> NormalizedReceive {
        NormalizedReceive {
            timestamp: Some(500),
            content_kind: "control",
            direction: "control",
            account_present: true,
            account: Some(account.into()),
            source: Some(source.into()),
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: None,
            text_bytes: None,
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: Some(control),
            server_guid: None,
        }
    }

    /// Same as [`control_receive`] with the envelope display name a real
    /// signal-cli receive carries (contract 1.27 actor-name capture).
    fn control_receive_named(
        account: &str,
        source: &str,
        peer_name: &str,
        control: ControlReceive,
    ) -> NormalizedReceive {
        let mut receive = control_receive(account, source, control);
        receive.peer_name = Some(peer_name.into());
        receive
    }

    fn linked_account_and_conversation(
        service: &mut ConnectorService,
    ) -> (crate::store::AccountSummary, crate::store::ConversationRow) {
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        (account, conversation)
    }

    fn seed_outgoing_sent(
        service: &mut ConnectorService,
        account_id: &str,
        conversation_id: &str,
        sent_at: u64,
    ) -> String {
        let prepared = match service
            .prepare_send_text(
                account_id,
                conversation_id,
                "original",
                "req-ctl",
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&prepared, account_id, conversation_id, sent_at)
            .unwrap();
        prepared
    }

    /// Contract 1.34: `prepare_receipt` (markRead/markViewed shared body) —
    /// a direct conversation fans one group out to its peer with ascending
    /// deduplicated timestamps, explicit ids are filtered to incoming rows in
    /// receipt order, group conversations answer zero groups (local sender
    /// hashes cannot be resolved to an author), a 600-id list is truncated to
    /// the 512 cap instead of rejected, and an empty conversation is the
    /// trivial no-op.
    #[test]
    fn prepare_receipt_groups_direct_timestamps_and_skips_groups() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let seed = |id: &str, sent_at: u64, direction: &'static str, conv: &str| {
            let record = MessageRecord {
                id: id.into(),
                account_id: account.id.clone(),
                conversation_id: conv.to_string(),
                direction,
                sender_id: if direction == "incoming" {
                    "peer-hash".into()
                } else {
                    account.id.clone()
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
                sticker: None,
                expire_seconds: None,
                expire_start_at: None,
                admin_deleted: false,
                server_guid: None,
            };
            assert!(
                service
                    .store_ref()
                    .insert_message(&record, None, Some("body"), true)
                    .unwrap()
            );
        };
        seed("m3", 30, "incoming", &conversation.id);
        seed("out-1", 40, "outgoing", &conversation.id);
        seed("m1", 10, "incoming", &conversation.id);
        seed("m2", 20, "incoming", &conversation.id);

        let absent = service
            .prepare_mark_read(&account.id, &conversation.id, None)
            .unwrap();
        assert_eq!(absent.account_id, account.id);
        assert_eq!(absent.conversation_id, conversation.id);
        assert_eq!(absent.account_signal, "+15555550100");
        assert_eq!(absent.groups.len(), 1);
        assert_eq!(absent.groups[0].recipient, "+15555550101");
        assert_eq!(absent.groups[0].timestamps, [10, 20, 30]);

        let explicit = service
            .prepare_mark_viewed(
                &account.id,
                &conversation.id,
                Some(vec![
                    "m3".into(),
                    "out-1".into(),
                    "m3".into(),
                    "missing".into(),
                ]),
            )
            .unwrap();
        assert_eq!(explicit.groups.len(), 1);
        assert_eq!(explicit.groups[0].timestamps, [30]);

        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "ZmFrZS1ncm91cC0x", "Group")
            .unwrap();
        seed("g1", 50, "incoming", &group.id);
        let group_receipts = service
            .prepare_mark_read(&account.id, &group.id, None)
            .unwrap();
        assert!(
            group_receipts.groups.is_empty(),
            "group rows carry no resolvable author"
        );

        let truncated = service
            .prepare_mark_read(
                &account.id,
                &conversation.id,
                Some((0..600).map(|index| format!("junk-{index}")).collect()),
            )
            .unwrap();
        assert!(truncated.groups.is_empty(), "truncation drops unknown ids");

        let empty_conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550102", "Quiet")
            .unwrap();
        let empty = service
            .prepare_mark_viewed(&account.id, &empty_conversation.id, None)
            .unwrap();
        assert!(empty.groups.is_empty());
    }

    /// Contract 1.34 outbound mentions: more than 64 entries are capped, an
    /// empty number fails the whole request INVALID_REQUEST, a UUID-shaped
    /// number passes through without a lookup, the linked account's own
    /// number and digit-suffix contact matches survive, an unresolvable
    /// number drops its entry, and an all-dropped list leaves the upstream
    /// params without a `mentions` key at all.
    #[test]
    fn send_text_mentions_are_capped_resolved_and_dropped_locally() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        service
            .store_ref()
            .upsert_synced_contacts(
                &account.id,
                &[SyncedContact {
                    kind: "contact",
                    peer_key: "+15555550101",
                    title: "Peer",
                    extra: None,
                }],
                1,
            )
            .unwrap();
        let uuid = |index: usize| format!("00000000-0000-4000-8000-{index:012x}");
        let mention = |number: &str, start: u32, length: u32| SendTextMentionParams {
            number: number.to_string(),
            start,
            length,
        };
        let dispatch = |prepared: PreparedSend| match prepared {
            PreparedSend::Dispatch { params, .. } => params,
            PreparedSend::Existing(_) => panic!("a fresh client request must dispatch"),
        };
        let send = |service: &mut ConnectorService,
                    client_request_id: &str,
                    mentions: Vec<SendTextMentionParams>| {
            service.prepare_send_text(
                &account.id,
                &conversation.id,
                "hi @all",
                client_request_id,
                None,
                None,
                Some(mentions),
                None,
            )
        };

        let capped = dispatch(
            send(
                &mut service,
                "req-cap",
                (0..70).map(|index| mention(&uuid(index), 0, 2)).collect(),
            )
            .unwrap(),
        );
        assert_eq!(capped["mentions"].as_array().unwrap().len(), 64);

        let error = match send(&mut service, "req-empty", vec![mention("  ", 0, 2)]).unwrap_err() {
            ServiceError::Api(api) => api,
            other => panic!("an empty mention number must fail closed, got {other:?}"),
        };
        assert_eq!(error.code, "INVALID_REQUEST");

        let resolved = dispatch(
            send(
                &mut service,
                "req-mixed",
                vec![
                    mention("+19990000001", 0, 2),
                    mention(&uuid(1), 3, 4),
                    mention("+15555550101", 7, 8),
                    mention("+15555550100", 9, 10),
                ],
            )
            .unwrap(),
        );
        let entries = resolved["mentions"].as_array().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0]["number"], uuid(1));
        assert_eq!(entries[0]["start"], 3);
        assert_eq!(entries[0]["length"], 4);
        assert_eq!(entries[1]["number"], "+15555550101");
        assert_eq!(entries[2]["number"], "+15555550100");

        let all_dropped = dispatch(
            send(
                &mut service,
                "req-dropped",
                vec![mention("+19990000001", 0, 2), mention("+19990000002", 3, 4)],
            )
            .unwrap(),
        );
        assert!(all_dropped.get("mentions").is_none());
    }

    /// A peer reaction upserts a message_events row keyed on the reacting
    /// actor, answers conversation.changed (never message.upserted), and a
    /// repeat of the same reaction stays idempotent. The envelope display
    /// name rides along (contract 1.27) and surfaces on the pill's per-actor
    /// detail.
    #[test]
    fn inbound_reaction_persists_an_actor_keyed_event_and_notifies_once() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let message_id = seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);

        let receive = control_receive_named(
            "+15555550100",
            "+15555550101",
            "林菲菲",
            ControlReceive::Reaction {
                emoji: "👍".into(),
                target_author: "+15555550100".into(),
                target_timestamp: 400,
                remove: false,
            },
        );
        let events = service
            .ingest_receive(receive, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert!(
            events
                .iter()
                .all(|event| matches!(event, HostSideEvent::ConversationChanged(_))),
            "reaction answers conversation.changed only: {events:?}"
        );

        let reactions = service
            .store_ref()
            .list_reaction_events(&account.id, &conversation.id, 10)
            .unwrap();
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0].emoji, "👍");
        assert_eq!(reactions[0].target_timestamp, 400);
        assert!(!reactions[0].removed);
        assert_ne!(reactions[0].actor_id, account.id, "actor is the peer");

        // The pill projection carries the actor with the captured name.
        let record = service
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &message_id)
            .unwrap()
            .unwrap();
        assert_eq!(record.reactions.len(), 1);
        assert_eq!(record.reactions[0].actors.len(), 1);
        assert!(!record.reactions[0].actors[0].is_self);
        assert_eq!(
            record.reactions[0].actors[0].name.as_deref(),
            Some("林菲菲")
        );

        let repeat = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::Reaction {
                        emoji: "👍".into(),
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                        remove: false,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(!repeat.is_empty(), "the summary still changes for the host");
        assert_eq!(
            service
                .store_ref()
                .list_reaction_events(&account.id, &conversation.id, 10)
                .unwrap()
                .len(),
            1,
            "a repeated reaction upserts in place"
        );
    }

    /// A reaction removal flips the stored row in place instead of adding a
    /// second event.
    #[test]
    fn inbound_reaction_removal_flips_the_existing_row() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);
        let reaction = |remove: bool| {
            control_receive(
                "+15555550100",
                "+15555550101",
                ControlReceive::Reaction {
                    emoji: "🎉".into(),
                    target_author: "+15555550100".into(),
                    target_timestamp: 400,
                    remove,
                },
            )
        };
        service
            .ingest_receive(reaction(false), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        service
            .ingest_receive(reaction(true), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let reactions = service
            .store_ref()
            .list_reaction_events(&account.id, &conversation.id, 10)
            .unwrap();
        assert_eq!(reactions.len(), 1);
        assert!(reactions[0].removed);
    }

    /// A reaction addressed to a conversation that does not exist locally is
    /// dropped without error and without a message_events row.
    #[test]
    fn inbound_reaction_without_a_local_conversation_is_dropped() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550999",
                    ControlReceive::Reaction {
                        emoji: "👍".into(),
                        target_author: "+15555550100".into(),
                        target_timestamp: 400,
                        remove: false,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(events.is_empty());
        assert_eq!(
            service
                .store_ref()
                .list_reaction_events(&account.id, "no-such-conversation", 10)
                .unwrap()
                .len(),
            0
        );
    }

    /// A peer remote delete marks the original incoming row
    /// `remote-deleted` and answers message.statusChanged — never a row
    /// insert.
    #[test]
    fn inbound_remote_delete_marks_the_row_and_answers_status_changed() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let incoming = NormalizedReceive {
            timestamp: Some(300),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: Some("to be deleted".into()),
            text_bytes: Some(13),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: None,
            server_guid: None,
        };
        service
            .ingest_receive(incoming, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::RemoteDelete {
                        target_timestamp: 300,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], HostSideEvent::MessageStatusChanged { status, .. } if *status == "remote-deleted")
        );

        let messages = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        assert_eq!(messages.len(), 1, "no duplicate row is created");
        assert_eq!(messages[0].status, "remote-deleted");
        assert_eq!(messages[0].sent_at, 300);
    }

    /// A peer's envelope-level edit (direction "incoming" + control Edit)
    /// updates the original row in place: same id, new body, editedAt set —
    /// and never inserts a second message row.
    #[test]
    fn inbound_peer_edit_updates_the_row_in_place() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let original = NormalizedReceive {
            timestamp: Some(310),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: Some("before edit".into()),
            text_bytes: Some(11),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: None,
            server_guid: None,
        };
        service
            .ingest_receive(original, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let mut edit = control_receive(
            "+15555550100",
            "+15555550101",
            ControlReceive::Edit {
                target_timestamp: 310,
            },
        );
        edit.direction = "incoming";
        edit.content_kind = "editMessage";
        edit.text = Some("after edit".into());
        edit.text_bytes = Some(10);
        let events = service
            .ingest_receive(edit, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(events.len(), 1);
        let HostSideEvent::MessageUpserted(record) = &events[0] else {
            panic!("expected message.upserted, got {:?}", events[0]);
        };
        assert_eq!(record.text.as_deref(), Some("after edit"));
        assert!(record.edited_at.is_some());

        let messages = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        assert_eq!(messages.len(), 1, "edit must not insert a second row");
        assert_eq!(messages[0].id, record.id);
        assert_eq!(messages[0].text.as_deref(), Some("after edit"));
        assert!(messages[0].edited_at.is_some());
    }

    /// Our own multi-device edit echo (direction "outgoing" + control Edit)
    /// retargets the phone-sent row through its upstream timestamp.
    #[test]
    fn sync_outgoing_edit_retargets_the_sent_row() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 420);

        let mut edit = control_receive(
            "+15555550100",
            "+15555550101",
            ControlReceive::Edit {
                target_timestamp: 420,
            },
        );
        edit.direction = "outgoing";
        edit.text = Some("edited on phone".into());
        edit.text_bytes = Some(15);
        let events = service
            .ingest_receive(edit, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(events.len(), 1);
        let HostSideEvent::MessageUpserted(record) = &events[0] else {
            panic!("expected message.upserted, got {:?}", events[0]);
        };
        assert_eq!(record.text.as_deref(), Some("edited on phone"));

        let row = service
            .store_ref()
            .message_by_id(&account.id, &conversation.id, &record.id)
            .unwrap()
            .unwrap();
        assert_eq!(row.sent_at, 420, "protocol identity is untouched");
        assert!(row.edited_at.is_some());
    }

    /// Typing START passes through as conversation.typing; an immediate
    /// second indicator for the same conversation is rate-limited away.
    #[test]
    fn typing_start_emits_once_and_an_immediate_repeat_is_rate_limited() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let start = || {
            control_receive(
                "+15555550100",
                "+15555550101",
                ControlReceive::Typing {
                    action: "START".into(),
                },
            )
        };
        let first = service
            .ingest_receive(start(), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert!(matches!(
            &first[0],
            HostSideEvent::ConversationTyping { action, .. } if action == "START"
        ));
        assert!(
            service
                .ingest_receive(start(), crate::DEFAULT_PROXY_GROUP_ID)
                .unwrap()
                .is_empty(),
            "a second indicator inside the window must be swallowed"
        );
        // STOP is control-plane state too, but the rate limiter is
        // conversation-scoped, not action-scoped — it is swallowed as well.
        assert!(
            service
                .ingest_receive(
                    control_receive(
                        "+15555550100",
                        "+15555550101",
                        ControlReceive::Typing {
                            action: "STOP".into()
                        },
                    ),
                    crate::DEFAULT_PROXY_GROUP_ID,
                )
                .unwrap()
                .is_empty()
        );
        let _ = (&account.id, &conversation.id);
    }

    /// Peer receipts upgrade earlier outgoing rows monotonically
    /// (sent → delivered → read): a lower tier never downgrades a higher
    /// one, incoming rows stay put, and only real transitions emit
    /// message.statusChanged.
    #[test]
    fn inbound_receipts_upgrade_outgoing_rows_monotonically() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);
        // An incoming row the receipts must never touch.
        service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(401),
                    content_kind: "dataMessage",
                    direction: "incoming",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: Some("+15555550101".into()),
                    source_uuid: None,
                    peer_name: None,
                    group_id: None,
                    text: Some("incoming stays".into()),
                    text_bytes: Some(14),
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: None,
                    server_guid: None,
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let messages_before = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        let incoming_before = messages_before
            .iter()
            .find(|row| row.sent_at == 401)
            .map(|row| row.status)
            .unwrap();

        let receipt = |kind| {
            control_receive(
                "+15555550100",
                "+15555550101",
                ControlReceive::Receipt {
                    kind,
                    timestamps: vec![400],
                    when: Some(1234),
                },
            )
        };
        let delivered = service
            .ingest_receive(
                receipt(ReceiptKind::Delivered),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert_eq!(delivered.len(), 1);
        assert!(matches!(
            &delivered[0],
            HostSideEvent::MessageStatusChanged { status, .. } if *status == "delivered"
        ));

        let read = service
            .ingest_receive(receipt(ReceiptKind::Read), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(read.len(), 1);
        assert!(matches!(
            &read[0],
            HostSideEvent::MessageStatusChanged { status, .. } if *status == "read"
        ));

        // A late delivery receipt must not downgrade nor re-emit.
        assert!(
            service
                .ingest_receive(
                    receipt(ReceiptKind::Delivered),
                    crate::DEFAULT_PROXY_GROUP_ID,
                )
                .unwrap()
                .is_empty()
        );

        let messages = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items;
        let outgoing = messages.iter().find(|row| row.sent_at == 400).unwrap();
        assert_eq!(outgoing.status, "read");
        let incoming = messages.iter().find(|row| row.sent_at == 401).unwrap();
        assert_eq!(incoming.status, incoming_before, "incoming rows stay put");
    }

    /// Receipt envelopes carrying a `when` stamp the delivered/read timeline
    /// (contract 1.32): the first stamp wins, a replayed receipt neither
    /// re-stamps nor re-emits, and reloaded rows keep both stamps for the
    /// host projection.
    #[test]
    fn receipts_stamp_the_delivered_read_timeline_once() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);

        let receipt = |kind, when: u64| {
            control_receive(
                "+15555550100",
                "+15555550101",
                ControlReceive::Receipt {
                    kind,
                    timestamps: vec![400],
                    when: Some(when),
                },
            )
        };

        let delivered = service
            .ingest_receive(
                receipt(ReceiptKind::Delivered, 1234),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert_eq!(delivered.len(), 1);
        assert!(matches!(
            &delivered[0],
            HostSideEvent::MessageStatusChanged { status, delivered_at, read_at, .. }
                if *status == "delivered"
                    && *delivered_at == Some(1234)
                    && read_at.is_none()
        ));

        let read = service
            .ingest_receive(
                receipt(ReceiptKind::Read, 5678),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert_eq!(read.len(), 1);
        assert!(matches!(
            &read[0],
            HostSideEvent::MessageStatusChanged { status, delivered_at, read_at, .. }
                if *status == "read"
                    && *delivered_at == Some(1234)
                    && *read_at == Some(5678)
        ));

        // A replayed read neither re-stamps nor re-emits.
        assert!(
            service
                .ingest_receive(
                    receipt(ReceiptKind::Read, 9999),
                    crate::DEFAULT_PROXY_GROUP_ID,
                )
                .unwrap()
                .is_empty()
        );

        let outgoing = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .find(|row| row.sent_at == 400)
            .unwrap();
        assert_eq!(outgoing.status, "read");
        assert_eq!(outgoing.delivered_at, Some(1234));
        assert_eq!(outgoing.read_at, Some(5678));
    }

    /// A receipt envelope without its own timestamp (contract 1.32) falls
    /// back to the connector clock: the row is stamped with a positive now
    /// and the event carries exactly the stamp it just wrote.
    #[test]
    fn receipt_without_envelope_timestamp_falls_back_to_connector_clock() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 400);

        let before = crate::link::now_ms();
        let delivered = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::Receipt {
                        kind: ReceiptKind::Delivered,
                        timestamps: vec![400],
                        when: None,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let after = crate::link::now_ms();

        assert_eq!(delivered.len(), 1);
        let fallback = match &delivered[0] {
            HostSideEvent::MessageStatusChanged {
                status,
                delivered_at,
                read_at,
                ..
            } if *status == "delivered" && read_at.is_none() => {
                delivered_at.expect("fallback stamp must be present")
            }
            other => panic!("unexpected event: {other:?}"),
        };
        assert!(
            fallback >= before && fallback <= after,
            "fallback stamp {fallback} outside [{before}, {after}]"
        );

        let outgoing = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .find(|row| row.sent_at == 400)
            .unwrap();
        assert_eq!(outgoing.status, "delivered");
        assert_eq!(outgoing.delivered_at, Some(fallback));
        assert_eq!(outgoing.read_at, None);
    }

    /// Peer edits snapshot the body they replace (contract 1.32): each
    /// upserted record rides the ascending prior-body history and reloaded
    /// rows keep it (the desktop renders newest-first on its own).
    #[test]
    fn edits_snapshot_the_replaced_body_into_the_row_projection() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let original = NormalizedReceive {
            timestamp: Some(310),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: Some("v0".into()),
            text_bytes: Some(2),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: None,
            server_guid: None,
        };
        service
            .ingest_receive(original, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let edit = |body: &str| {
            let mut receive = control_receive(
                "+15555550100",
                "+15555550101",
                ControlReceive::Edit {
                    target_timestamp: 310,
                },
            );
            receive.direction = "incoming";
            receive.content_kind = "editMessage";
            receive.text = Some(body.into());
            receive.text_bytes = Some(body.len() as u32);
            receive
        };

        let first = service
            .ingest_receive(edit("v1"), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(first.len(), 1);
        let HostSideEvent::MessageUpserted(record) = &first[0] else {
            panic!("expected message.upserted, got {:?}", first[0]);
        };
        assert_eq!(record.text.as_deref(), Some("v1"));
        let bodies: Vec<&str> = record
            .edits
            .iter()
            .map(|entry| entry.body.as_str())
            .collect();
        assert_eq!(bodies, ["v0"], "the replaced body rides the upserted row");

        let second = service
            .ingest_receive(edit("v2"), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert_eq!(second.len(), 1);
        let HostSideEvent::MessageUpserted(record) = &second[0] else {
            panic!("expected message.upserted, got {:?}", second[0]);
        };
        let bodies: Vec<&str> = record
            .edits
            .iter()
            .map(|entry| entry.body.as_str())
            .collect();
        assert_eq!(
            bodies,
            ["v0", "v1"],
            "history grows ascending, oldest first"
        );

        let reloaded = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .find(|row| row.sent_at == 310)
            .unwrap();
        assert_eq!(reloaded.text.as_deref(), Some("v2"));
        let bodies: Vec<&str> = reloaded
            .edits
            .iter()
            .map(|entry| entry.body.as_str())
            .collect();
        assert_eq!(bodies, ["v0", "v1"]);
    }

    /// An edit whose receive carries no new body is dropped silently.
    #[test]
    fn inbound_edit_without_a_body_is_dropped() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        seed_outgoing_sent(&mut service, &account.id, &conversation.id, 420);
        let events = service
            .ingest_receive(
                control_receive(
                    "+15555550100",
                    "+15555550101",
                    ControlReceive::Edit {
                        target_timestamp: 420,
                    },
                ),
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(events.is_empty());
        let row = service
            .store_ref()
            .conversation_summary(&account.id, &conversation.id)
            .unwrap()
            .unwrap();
        assert_eq!(row.id, conversation.id);
    }

    /// `messages.edit` preparation: an outgoing sent row resolves to
    /// upstream editTimestamp params; settlement rewrites the body and
    /// stamps editedAt.
    #[test]
    fn prepare_edit_targets_sent_row_and_settlement_rewrites_it() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let pending = seed_outgoing_sent(&mut service, &account.id, &conversation.id, 430);

        let prepared = service
            .prepare_edit(&account.id, &conversation.id, &pending, "edited body")
            .unwrap();
        assert_eq!(
            prepared.params["editTimestamp"], 430,
            "upstream edit targets the sent row's protocol timestamp"
        );
        assert_eq!(prepared.params["message"], "edited body");

        let record = service
            .complete_edit_success(&account.id, &conversation.id, &pending, "edited body")
            .unwrap()
            .expect("settlement of a sent row returns the updated record");
        assert_eq!(record.text.as_deref(), Some("edited body"));
        assert!(record.edited_at.is_some());
        assert_eq!(record.status, "sent");

        // An incoming row is not editable by us.
        let incoming = NormalizedReceive {
            timestamp: Some(440),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: Some("peer says".into()),
            text_bytes: Some(9),
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: None,
            server_guid: None,
        };
        service
            .ingest_receive(incoming, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let incoming_id = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .find(|row| row.direction == "incoming")
            .unwrap()
            .id;
        let error = service
            .prepare_edit(&account.id, &conversation.id, &incoming_id, "nope")
            .unwrap_err();
        assert_eq!(error.into_api().code, "MESSAGE_NOT_FOUND");
    }

    /// Inbound quote + attachment metadata ride the message row end to end:
    /// ingest persists them, list_messages returns them verbatim.
    #[test]
    fn inbound_quote_and_attachment_metadata_roundtrip_through_the_store() {
        let (_temp, mut service) = service();
        let (account, conversation) = linked_account_and_conversation(&mut service);
        let receive = NormalizedReceive {
            timestamp: Some(360),
            content_kind: "dataMessage",
            direction: "incoming",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: None,
            group_id: None,
            text: Some("replying with a file".into()),
            text_bytes: Some(20),
            text_truncated: false,
            quote: Some(NormalizedQuote {
                id: 350,
                author: "+15555550100".into(),
                text: "the original".into(),
            }),
            attachments: vec![NormalizedAttachment {
                id: "att-1".into(),
                content_type: Some("image/png".into()),
                filename: Some("shot.png".into()),
                size: Some(2048),
                width: Some(64),
                height: Some(32),
                is_voice_note: false,
                locator: None,
            }],
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: None,
            server_guid: None,
        };
        service
            .ingest_receive(receive, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();

        let row = service
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap()
            .items
            .into_iter()
            .next()
            .unwrap();
        let quote = row
            .quote_snapshot
            .expect("quote snapshot survives the roundtrip");
        assert_eq!(quote.id, 350);
        assert_eq!(quote.author, "+15555550100");
        assert_eq!(quote.text, "the original");
        assert_eq!(row.attachments.len(), 1);
        assert_eq!(row.attachments[0].id, "att-1");
        assert_eq!(row.attachments[0].filename.as_deref(), Some("shot.png"));
        assert_eq!(row.attachments[0].size, Some(2048));
    }

    // ---- Media ingest (ADR 0002) -------------------------------------

    /// A service with the media backing armed over an engine-shaped data
    /// directory (`<data>/attachments/`, the single-account probe layout).
    fn media_service() -> (TempDir, ConnectorService) {
        let temp = TempDir::new().unwrap();
        let store = Store::open(temp.path(), Some(StoreKey::from_bytes([0x5A; 32]))).unwrap();
        let data_dir = temp.path().join("signal-data");
        std::fs::create_dir_all(data_dir.join("attachments")).unwrap();
        let handles = Arc::new(MediaHandleTable::new());
        (
            temp,
            ConnectorService::with_media(
                Arc::new(store),
                Some(MediaIngest::new(data_dir, handles)),
            ),
        )
    }

    /// Addressing rows for one account, mirroring the media-open test setup:
    /// account → direct conversation → one completed outgoing message.
    fn addressed_message(
        service: &ConnectorService,
        number: &str,
        peer: &str,
        request_id: &str,
    ) -> (String, String, String) {
        let account = service
            .store_ref()
            .upsert_account_from_signal(number, None, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", peer, "Peer")
            .unwrap();
        let message_id = match service
            .prepare_send_text(
                &account.id,
                &direct.id,
                "has attachment",
                request_id,
                None,
                None,
                None,
                None,
            )
            .unwrap()
        {
            PreparedSend::Dispatch { pending_id, .. } => pending_id,
            PreparedSend::Existing(_) => panic!("new client request must dispatch"),
        };
        service
            .complete_send_success(&message_id, &account.id, &direct.id, 800)
            .unwrap();
        (account.id, direct.id, message_id)
    }

    /// Without the launcher opt-in (`ConnectorService::new`), all three
    /// media methods answer the shared capability gate before any other
    /// check — the pre-PoC contract (ADR 0002 acceptance gate 5).
    #[test]
    fn media_methods_are_capability_gated_without_the_opt_in() {
        let (_temp, service) = service();
        for error in [
            service
                .open_media_handle("a", "c", "m", "att-1")
                .unwrap_err(),
            service.read_media_chunk("handle", 0).unwrap_err(),
            service.close_media_handle("handle").unwrap_err(),
        ] {
            let api = error.into_api();
            assert_eq!(api.code, "CAPABILITY_UNAVAILABLE");
            assert!(!api.retryable);
        }
    }

    /// Security gate (ADR 0002 acceptance gate 3): traversal-shaped ids —
    /// `../x`, absolute paths, backslashes, and the sanitize survivors
    /// `.` / `..` — answer INVALID_REQUEST before the filesystem is touched.
    /// The decoy file makes the check real: a naive path join would resolve
    /// it (and answer a handle or UPSTREAM_ERROR), never INVALID_REQUEST.
    #[test]
    fn media_open_rejects_traversal_ids_before_touching_the_filesystem() {
        let (_temp, service) = media_service();
        let (account_id, conversation_id, message_id) =
            addressed_message(&service, "+15555550100", "+15555550101", "req-media-trav");
        // Exactly where a naive `attachments/` join of "../escape.dat" and
        // "../../escape.dat" would land.
        std::fs::write(
            _temp.path().join("signal-data").join("escape.dat"),
            b"escaped",
        )
        .unwrap();

        for attachment_id in [
            "../escape.dat",
            "..",
            ".",
            "/etc/passwd",
            "a\\b.dat",
            "C:\\temp\\att.dat",
            "sub/../../escape.dat",
        ] {
            let error = service
                .open_media_handle(&account_id, &conversation_id, &message_id, attachment_id)
                .unwrap_err()
                .into_api();
            assert_eq!(error.code, "INVALID_REQUEST", "{attachment_id}");
            assert!(!error.retryable, "{attachment_id}");
        }
    }

    /// The happy path end to end: a downloaded file resolves, `open` reports
    /// the local size and the chunk bound, the read loop delivers the exact
    /// bytes through sequential offsets, and `closeHandle` is idempotent.
    #[test]
    fn media_open_streams_a_downloaded_file_through_sequential_chunks() {
        let (_temp, service) = media_service();
        let (account_id, conversation_id, message_id) =
            addressed_message(&service, "+15555550100", "+15555550101", "req-media-open");
        let body: Vec<u8> = (0..8192_u32).map(|i| (i % 251) as u8).collect();
        let attachment_dir = _temp.path().join("signal-data").join("attachments");
        std::fs::write(attachment_dir.join("a1b2c3.dat"), &body).unwrap();

        let view = service
            .open_media_handle(&account_id, &conversation_id, &message_id, "a1b2c3.dat")
            .unwrap();
        assert_eq!(view.size_bytes, body.len() as u64);
        assert_eq!(view.chunk_bytes, 262_144);

        let mut assembled: Vec<u8> = Vec::with_capacity(body.len());
        let mut offset = 0_u64;
        let mut chunks = 0;
        loop {
            let chunk = service
                .read_media_chunk(&view.media_handle, offset)
                .unwrap();
            let mut decoded = Vec::with_capacity(chunk.size_bytes as usize);
            base64_decode_to_vec(&chunk.data_base64, &mut decoded)
                .expect("chunk base64 must be canonical");
            assert_eq!(decoded.len() as u64, chunk.size_bytes);
            assembled.extend_from_slice(&decoded);
            offset += chunk.size_bytes;
            chunks += 1;
            if chunk.eof {
                break;
            }
        }
        assert_eq!(assembled, body);
        assert_eq!(chunks, 1, "8 KiB fits one chunk; eof must be immediate");

        assert!(
            service
                .close_media_handle(&view.media_handle)
                .unwrap()
                .released
        );
        // Idempotent close, and a closed handle is unreadable.
        assert!(
            !service
                .close_media_handle(&view.media_handle)
                .unwrap()
                .released
        );
        let error = service
            .read_media_chunk(&view.media_handle, offset)
            .unwrap_err()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST");
    }

    /// A missing file answers UPSTREAM_ERROR: not-downloaded and
    /// governor-deleted are indistinguishable (ADR 0002).
    #[test]
    fn media_open_answers_upstream_error_when_the_file_is_absent() {
        let (_temp, service) = media_service();
        let (account_id, conversation_id, message_id) = addressed_message(
            &service,
            "+15555550100",
            "+15555550101",
            "req-media-missing",
        );
        let error = service
            .open_media_handle(
                &account_id,
                &conversation_id,
                &message_id,
                "never-downloaded.dat",
            )
            .unwrap_err()
            .into_api();
        assert_eq!(error.code, "UPSTREAM_ERROR");
        assert!(error.retryable);
    }

    /// Non-sequential offsets fail closed at the service boundary with the
    /// shared INVALID_REQUEST answer.
    #[test]
    fn media_read_chunk_rejects_non_sequential_offsets() {
        let (_temp, service) = media_service();
        let (account_id, conversation_id, message_id) =
            addressed_message(&service, "+15555550100", "+15555550101", "req-media-seq");
        let attachment_dir = _temp.path().join("signal-data").join("attachments");
        std::fs::write(attachment_dir.join("seq.dat"), [0_u8; 4096]).unwrap();
        let view = service
            .open_media_handle(&account_id, &conversation_id, &message_id, "seq.dat")
            .unwrap();

        let error = service
            .read_media_chunk(&view.media_handle, 1)
            .unwrap_err()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST");
        // The first read is still at offset 0 after the refusal.
        service
            .read_media_chunk(&view.media_handle, 0)
            .expect("a refused offset must not advance the stream");
    }

    /// Releasing an account's handles (the deleteLocalData hook) must not
    /// touch another account's live streams.
    #[test]
    fn media_handles_release_per_account_without_cross_account_effects() {
        let (_temp, service) = media_service();
        let (account_a, conversation_a, message_a) =
            addressed_message(&service, "+15555550100", "+15555550191", "req-media-a");
        let (account_b, conversation_b, message_b) =
            addressed_message(&service, "+15555550101", "+15555550192", "req-media-b");
        let attachment_dir = _temp.path().join("signal-data").join("attachments");
        std::fs::write(attachment_dir.join("a.dat"), [1_u8; 64]).unwrap();
        std::fs::write(attachment_dir.join("b.dat"), [2_u8; 64]).unwrap();
        let handle_a = service
            .open_media_handle(&account_a, &conversation_a, &message_a, "a.dat")
            .unwrap()
            .media_handle;
        let handle_b = service
            .open_media_handle(&account_b, &conversation_b, &message_b, "b.dat")
            .unwrap()
            .media_handle;

        service.clear_media_handles_for_account(&account_a);

        assert_eq!(
            service
                .read_media_chunk(&handle_a, 0)
                .unwrap_err()
                .into_api()
                .code,
            "INVALID_REQUEST"
        );
        // The other account's stream is untouched: single-origin by account.
        let chunk_b = service.read_media_chunk(&handle_b, 0).unwrap();
        assert_eq!(chunk_b.size_bytes, 64);
        assert!(chunk_b.eof);
    }

    /// The encoder emits the exact canonical form the upstream
    /// java.util.Base64 produces (RFC 4648 vectors), and every encoding
    /// round-trips through the decoder that guards attachment sends.
    #[test]
    fn base64_encode_matches_the_canonical_alphabet_and_round_trips() {
        for (raw, expected) in [
            (&b""[..], ""),
            (b"f".as_slice(), "Zg=="),
            (b"fo".as_slice(), "Zm8="),
            (b"foo".as_slice(), "Zm9v"),
            (b"foob".as_slice(), "Zm9vYg=="),
            (b"fooba".as_slice(), "Zm9vYmE="),
            (b"foobar".as_slice(), "Zm9vYmFy"),
        ] {
            assert_eq!(base64_encode(raw), expected, "{raw:?}");
        }
        // A full chunk's encoding is bounded exactly as the contract claims.
        assert_eq!(base64_encode(&[0_u8; 262_144]).len(), 349_528);
        // Random round-trip through the independent decoder.
        let mut value = [0_u8; 1000];
        use rand::RngCore;
        rand::rng().fill_bytes(&mut value);
        let mut decoded = Vec::new();
        base64_decode_to_vec(&base64_encode(&value), &mut decoded).unwrap();
        assert_eq!(decoded, value);
    }

    // ---- Contract 1.42 (§4.41); group route re-cut by contract 1.45 (§4.44) ----

    /// The upstream payload follows the caller's engine mode verbatim —
    /// the engine face's `setExpirationTimer` (seconds) vs the signal-cli
    /// face's `updateContact` (expiration, int seconds) — after bounds,
    /// account, and conversation resolution. A direct conversation carries
    /// the direct route; a group routes `updateGroup {account, groupId,
    /// expiration}` in every mode since the engine batch landed
    /// `updateGroup {expiration}` (2026-10-06 dated note; engine main @
    /// 216a4c7), an unknown conversation answers CONVERSATION_NOT_FOUND, and
    /// an out-of-face value answers INVALID_REQUEST.
    #[test]
    fn set_expire_timer_preparation_shapes_upstream_by_mode() {
        use crate::engine::SignalCliMode;
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-id-1", "group")
            .unwrap();

        let (engine_method, engine_payload, seconds) = service
            .prepare_conversations_set_expire_timer(
                &account.id,
                &direct.id,
                86_400,
                SignalCliMode::KtEngine,
            )
            .unwrap();
        assert_eq!(engine_method, "setExpirationTimer");
        assert_eq!(seconds, 86_400);
        assert_eq!(
            engine_payload,
            json!({
                "account": "+15555550100",
                "recipient": "+15555550101",
                "expirationInSeconds": 86_400,
            })
        );

        let (signal_cli, signal_cli_payload, _) = service
            .prepare_conversations_set_expire_timer(
                &account.id,
                &direct.id,
                604_800,
                SignalCliMode::Jvm,
            )
            .unwrap();
        assert_eq!(signal_cli, "updateContact");
        assert_eq!(
            signal_cli_payload,
            json!({
                "account": "+15555550100",
                "recipient": "+15555550101",
                "expiration": 604_800,
            })
        );

        // Group route (§4.44): the signal-cli JVM face travels by updateGroup.
        let (group_method, group_payload, group_seconds) = service
            .prepare_conversations_set_expire_timer(
                &account.id,
                &group.id,
                3600,
                SignalCliMode::Jvm,
            )
            .unwrap();
        assert_eq!(group_method, "updateGroup");
        assert_eq!(group_seconds, 3600);
        assert_eq!(
            group_payload,
            json!({
                "account": "+15555550100",
                "groupId": "group-id-1",
                "expiration": 3600,
            })
        );

        // Native shares the JVM face one-to-one (the §4.32-§4.34 swap
        // boundary): same method, same payload.
        let (native_method, native_payload, _) = service
            .prepare_conversations_set_expire_timer(
                &account.id,
                &group.id,
                3600,
                SignalCliMode::Native,
            )
            .unwrap();
        assert_eq!(native_method, "updateGroup");
        assert_eq!(native_payload, group_payload);

        // ... and the kt-engine face converges on the same group payload —
        // the engine-batch gate is lifted (2026-10-06 dated note), the one
        // updateGroup shape in every mode.
        let (kt_group_method, kt_group_payload, kt_group_seconds) = service
            .prepare_conversations_set_expire_timer(
                &account.id,
                &group.id,
                60,
                SignalCliMode::KtEngine,
            )
            .unwrap();
        assert_eq!(kt_group_method, "updateGroup");
        assert_eq!(kt_group_seconds, 60);
        assert_eq!(
            kt_group_payload,
            json!({
                "account": "+15555550100",
                "groupId": "group-id-1",
                "expiration": 60,
            })
        );

        let missing = service
            .prepare_conversations_set_expire_timer(
                &account.id,
                "conv-absent",
                60,
                SignalCliMode::KtEngine,
            )
            .unwrap_err();
        assert!(matches!(
            missing,
            ServiceError::Store(StoreError::ConversationNotFound)
        ));

        let oversized = service
            .prepare_conversations_set_expire_timer(
                &account.id,
                &direct.id,
                MAX_EXPIRE_TIMER_SECONDS + 1,
                SignalCliMode::KtEngine,
            )
            .unwrap_err();
        assert!(matches!(oversized, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
    }

    /// A timer notice mirrors the conversation's new timer state before the
    /// row lands (the conversation.changed summary already carries it), the
    /// notice row itself never sweeps (its expire columns stay empty), and a
    /// regular row carries the envelope-stated timer verbatim. The close
    /// form — no stated value — reads the state off.
    #[test]
    fn ingest_timer_change_mirrors_state_and_lands_a_nonsweeping_row() {
        let (_temp, mut service) = service();
        let timer_change = NormalizedReceive {
            timestamp: Some(1727000000000),
            content_kind: "dataMessage",
            direction: "system",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: Some("Peer".into()),
            group_id: None,
            text: Some("已更新消息定时消失：86400 秒".into()),
            text_bytes: None,
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: Some(crate::engine::NormalizedRich {
                is_expiration_update: true,
                ..Default::default()
            }),
            sticker: None,
            expire_seconds: Some(86_400),
            expire_start_at: None,
            control: None,
            server_guid: None,
        };
        let events = service
            .ingest_receive(timer_change.clone(), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let upserted = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .expect("timer notice row");
        assert_eq!(upserted.direction, "system");
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("state mirror event");
        assert_eq!(changed.expire_timer_seconds, Some(86_400));

        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .list_conversations(&account.id, 10, None)
            .unwrap()
            .items
            .remove(0);
        let rows = service
            .store_ref()
            .list_messages(&account.id, &conversation.id, 10, None)
            .unwrap();
        assert_eq!(rows.items.len(), 1);
        assert_eq!(rows.items[0].expire_seconds, None);
        assert_eq!(rows.items[0].expire_start_at, None);

        // The close form mirrors the off state and lands the closed copy (a
        // distinct protocol timestamp: signal identity dedup would otherwise
        // swallow the second notice).
        let close = NormalizedReceive {
            timestamp: Some(1727000000001),
            text: Some("已更新消息定时消失：已关闭".into()),
            expire_seconds: None,
            server_guid: None,
            ..timer_change
        };
        let events = service
            .ingest_receive(close, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("close mirror event");
        assert_eq!(changed.expire_timer_seconds, Some(0));
    }

    // ---- Contract 1.45 (§4.44): group management write face ----

    /// Every change family maps to its upstream `updateGroup` key verbatim
    /// (the connector-facing names are the caller's; the upstream names are
    /// the wire's), member arrays keep the roster bounds, and the refusal
    /// ladder answers before any upstream call: empty change sets, explicit
    /// empty member arrays, oversized strings and rosters, bad enums, direct
    /// addressing, unknown conversations, a malformed operationId.
    #[test]
    fn groups_update_preparation_maps_change_families_and_refuses_gaps() {
        use crate::engine::SignalCliMode;
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-id-1", "group")
            .unwrap();
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let base = |conversation_id: &str| GroupsUpdateParams {
            account_id: account.id.clone(),
            conversation_id: conversation_id.into(),
            group_name: None,
            group_description: None,
            add_members: None,
            remove_members: None,
            promote_admins: None,
            demote_admins: None,
            ban_members: None,
            unban_members: None,
            link_state: None,
            reset_link: None,
            permission_add_member: None,
            permission_edit_details: None,
            permission_send_messages: None,
            member_label: None,
            member_label_emoji: None,
            operation_id: None,
        };

        // One payload per change family, verbatim key mapping.
        assert_eq!(
            service
                .prepare_groups_update(
                    &GroupsUpdateParams {
                        group_name: Some("New Name".into()),
                        ..base(&group.id)
                    },
                    SignalCliMode::Jvm
                )
                .unwrap(),
            json!({
                "account": "+15555550100",
                "groupId": "group-id-1",
                "name": "New Name",
            })
        );
        assert_eq!(
            service
                .prepare_groups_update(
                    &GroupsUpdateParams {
                        group_description: Some("A description".into()),
                        ..base(&group.id)
                    },
                    SignalCliMode::Jvm
                )
                .unwrap(),
            json!({
                "account": "+15555550100",
                "groupId": "group-id-1",
                "description": "A description",
            })
        );
        for (connector_key, upstream_key, members) in [
            ("addMembers", "members", vec!["+15555550101"]),
            ("removeMembers", "removeMembers", vec!["+15555550102"]),
            ("promoteAdmins", "admins", vec!["+15555550101"]),
            ("demoteAdmins", "removeAdmins", vec!["+15555550101"]),
            ("banMembers", "bans", vec!["+15555550102"]),
            ("unbanMembers", "unbans", vec!["+15555550102"]),
        ] {
            let members: Vec<String> = members.into_iter().map(str::to_string).collect();
            let params = GroupsUpdateParams {
                add_members: (connector_key == "addMembers").then(|| members.clone()),
                remove_members: (connector_key == "removeMembers").then(|| members.clone()),
                promote_admins: (connector_key == "promoteAdmins").then(|| members.clone()),
                demote_admins: (connector_key == "demoteAdmins").then(|| members.clone()),
                ban_members: (connector_key == "banMembers").then(|| members.clone()),
                unban_members: (connector_key == "unbanMembers").then(|| members.clone()),
                ..base(&group.id)
            };
            let mut expected = json!({
                "account": "+15555550100",
                "groupId": "group-id-1",
            });
            expected[upstream_key] = json!(members);
            assert_eq!(
                service
                    .prepare_groups_update(&params, SignalCliMode::Jvm)
                    .unwrap(),
                expected,
                "family {connector_key} must land on upstream key {upstream_key}"
            );
        }

        // Link state: the connector's enabled-without-approval maps to the
        // upstream "enabled" choice; the other two pass verbatim.
        for (connector, upstream) in [
            ("enabled-with-approval", "enabled-with-approval"),
            ("enabled-without-approval", "enabled"),
            ("disabled", "disabled"),
        ] {
            assert_eq!(
                service
                    .prepare_groups_update(
                        &GroupsUpdateParams {
                            link_state: Some(connector.into()),
                            ..base(&group.id)
                        },
                        SignalCliMode::Jvm
                    )
                    .unwrap(),
                json!({
                    "account": "+15555550100",
                    "groupId": "group-id-1",
                    "link": upstream,
                })
            );
        }
        // resetLink: only the true form carries a change.
        assert_eq!(
            service
                .prepare_groups_update(
                    &GroupsUpdateParams {
                        reset_link: Some(true),
                        ..base(&group.id)
                    },
                    SignalCliMode::Jvm
                )
                .unwrap(),
            json!({
                "account": "+15555550100",
                "groupId": "group-id-1",
                "resetLink": true,
            })
        );
        for (value, key) in [
            ("every-member", "setPermissionAddMember"),
            ("only-admins", "setPermissionEditDetails"),
            ("only-admins", "setPermissionSendMessages"),
        ] {
            let params = GroupsUpdateParams {
                permission_add_member: (key == "setPermissionAddMember").then(|| value.into()),
                permission_edit_details: (key == "setPermissionEditDetails").then(|| value.into()),
                permission_send_messages: (key == "setPermissionSendMessages")
                    .then(|| value.into()),
                ..base(&group.id)
            };
            assert_eq!(
                service
                    .prepare_groups_update(&params, SignalCliMode::Jvm)
                    .unwrap(),
                json!({
                    "account": "+15555550100",
                    "groupId": "group-id-1",
                    key: value,
                })
            );
        }
        assert_eq!(
            service
                .prepare_groups_update(
                    &GroupsUpdateParams {
                        member_label: Some("Builders".into()),
                        member_label_emoji: Some("🛠️".into()),
                        ..base(&group.id)
                    },
                    SignalCliMode::Jvm
                )
                .unwrap(),
            json!({
                "account": "+15555550100",
                "groupId": "group-id-1",
                "memberLabel": "Builders",
                "memberLabelEmoji": "🛠️",
            })
        );

        // The refusal ladder — each before any upstream call.
        let refused = |params: GroupsUpdateParams| {
            let error = service
                .prepare_groups_update(&params, SignalCliMode::Jvm)
                .unwrap_err();
            assert!(
                matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"),
                "expected INVALID_REQUEST"
            );
        };
        refused(GroupsUpdateParams {
            group_name: Some(String::new()),
            ..base(&group.id)
        });
        refused(GroupsUpdateParams {
            group_name: None,
            ..base(&group.id)
        }); // no change at all
        refused(GroupsUpdateParams {
            reset_link: Some(false),
            ..base(&group.id)
        }); // false is absence, not a change
        refused(GroupsUpdateParams {
            add_members: Some(Vec::new()),
            ..base(&group.id)
        }); // explicit empty member array
        refused(GroupsUpdateParams {
            add_members: Some(vec!["+15555550101".into(); MAX_GROUP_MEMBERS + 1]),
            ..base(&group.id)
        });
        refused(GroupsUpdateParams {
            add_members: Some(vec!["x".repeat(MAX_GROUP_MEMBER_ID_CHARS + 1)]),
            ..base(&group.id)
        });
        refused(GroupsUpdateParams {
            group_name: Some("長".repeat(GROUP_NAME_MAX_BYTES / 3 + 1)),
            ..base(&group.id)
        });
        refused(GroupsUpdateParams {
            group_description: Some("d".repeat(GROUP_DESCRIPTION_MAX_BYTES + 1)),
            ..base(&group.id)
        });
        refused(GroupsUpdateParams {
            member_label: Some("".into()),
            ..base(&group.id)
        });
        refused(GroupsUpdateParams {
            member_label_emoji: Some("👍👍".into()),
            ..base(&group.id)
        });
        refused(GroupsUpdateParams {
            link_state: Some("open".into()),
            ..base(&group.id)
        });
        refused(GroupsUpdateParams {
            permission_send_messages: Some("nobody".into()),
            ..base(&group.id)
        });
        refused(GroupsUpdateParams {
            operation_id: Some("".into()),
            ..base(&group.id)
        });
        // Direct addressing and unknown conversations answer their ladder
        // steps, not the group payload.
        refused(GroupsUpdateParams {
            conversation_id: direct.id.clone(),
            group_name: Some("New Name".into()),
            ..base(&group.id)
        });
        let missing = service
            .prepare_groups_update(
                &GroupsUpdateParams {
                    conversation_id: "conv-absent".into(),
                    group_name: Some("New Name".into()),
                    ..base(&group.id)
                },
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(
            missing,
            ServiceError::Store(StoreError::ConversationNotFound)
        ));
    }

    /// The kt-engine face has no group-management methods: a fully valid
    /// change set still answers CAPABILITY_UNAVAILABLE (retryable=false)
    /// after the local validation, while an invalid one keeps its
    /// INVALID_REQUEST — the honest degradation, never a silent rewrite.
    #[test]
    fn groups_update_and_quit_refuse_the_engine_face_after_validation() {
        use crate::engine::SignalCliMode;
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-id-1", "group")
            .unwrap();
        let named = GroupsUpdateParams {
            account_id: account.id.clone(),
            conversation_id: group.id.clone(),
            group_name: Some("New Name".into()),
            group_description: None,
            add_members: None,
            remove_members: None,
            promote_admins: None,
            demote_admins: None,
            ban_members: None,
            unban_members: None,
            link_state: None,
            reset_link: None,
            permission_add_member: None,
            permission_edit_details: None,
            permission_send_messages: None,
            member_label: None,
            member_label_emoji: None,
            operation_id: None,
        };
        let error = service
            .prepare_groups_update(&named, SignalCliMode::KtEngine)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api)
                if api.code == "CAPABILITY_UNAVAILABLE" && !api.retryable));
        // Validation outranks the capability gate.
        let invalid = GroupsUpdateParams {
            group_name: None,
            ..named
        };
        let error = service
            .prepare_groups_update(&invalid, SignalCliMode::KtEngine)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));

        let quit = GroupsQuitParams {
            account_id: account.id.clone(),
            conversation_id: group.id.clone(),
            operation_id: None,
        };
        let error = service
            .prepare_groups_quit(&quit, SignalCliMode::KtEngine)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api)
                if api.code == "CAPABILITY_UNAVAILABLE" && !api.retryable));
    }

    /// groups.quit resolves the group and answers the bare upstream payload;
    /// direct addressing and unknown conversations answer their ladder steps.
    #[test]
    fn groups_quit_preparation_addresses_the_group_and_refuses_non_groups() {
        use crate::engine::SignalCliMode;
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-id-1", "group")
            .unwrap();
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let quit = |conversation_id: String| GroupsQuitParams {
            account_id: account.id.clone(),
            conversation_id,
            operation_id: None,
        };
        assert_eq!(
            service
                .prepare_groups_quit(&quit(group.id.clone()), SignalCliMode::Jvm)
                .unwrap(),
            json!({
                "account": "+15555550100",
                "groupId": "group-id-1",
            })
        );
        let error = service
            .prepare_groups_quit(&quit(direct.id.clone()), SignalCliMode::Jvm)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        let missing = service
            .prepare_groups_quit(&quit("conv-absent".into()), SignalCliMode::Jvm)
            .unwrap_err();
        assert!(matches!(
            missing,
            ServiceError::Store(StoreError::ConversationNotFound)
        ));
    }

    // ---- Contract 1.59 (§4.55): username faces ----

    /// accounts.setUsername enforces the XOR ladder before the capability
    /// gate, mirrors the desktop grammar fail-closed, and builds the
    /// `updateAccount` payload per mode.
    #[test]
    fn set_username_preparation_enforces_xor_shape_and_mode() {
        use crate::engine::SignalCliMode;
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let params = |username: Option<String>, delete: bool| AccountsSetUsernameParams {
            account_id: account.id.clone(),
            username,
            delete_username: delete,
        };

        // XOR: both present, both absent.
        let error = service
            .prepare_accounts_set_username(
                &params(Some("kt_test.01".into()), true),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        let error = service
            .prepare_accounts_set_username(&params(None, false), SignalCliMode::Jvm)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));

        // Shape gate: grammar violations answered locally.
        for bad in [
            "ab",          // nickname below the 3-char window
            "1kt.test",    // nickname starts with a digit
            "kt test",     // space in nickname
            "kt.test.x",   // second dot breaks the discriminator grammar
            "kt_test.00",  // 00-leading discriminator
            "kt_test.0",   // 0-leading single digit
            "kt_test.012", // 0-leading three digits
            "kt_test.",    // empty discriminator
        ] {
            let error = service
                .prepare_accounts_set_username(&params(Some(bad.into()), false), SignalCliMode::Jvm)
                .unwrap_err();
            assert!(
                matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"),
                "expected INVALID_REQUEST for {bad}"
            );
        }
        // Whole-string wire bound (38): the grammar walk never sees it.
        let error = service
            .prepare_accounts_set_username(&params(Some("a".repeat(39)), false), SignalCliMode::Jvm)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        // Two dots: the split keeps the second dot inside the discriminator —
        // also a grammar face.
        let error = service
            .prepare_accounts_set_username(
                &params(Some("kt.te.st.1".into()), false),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));

        // Shape-valid names pass: full username, bare nickname, boundary
        // discriminators (a single leading zero as exactly two digits; the
        // digit range above the shape stays upstream authority).
        for good in [
            "kt_test.01".to_string(),
            "KtAgentUser".to_string(),
            "a".repeat(32), // maximal nickname, no discriminator
            "kt_test.10".to_string(),
            "kt_test.99".to_string(),
            "kt_test.65536".to_string(),
        ] {
            service
                .prepare_accounts_set_username(
                    &params(Some(good.clone()), false),
                    SignalCliMode::Jvm,
                )
                .unwrap_or_else(|error| panic!("expected {good} to pass: {error:?}"));
        }

        // KtEngine debt: validation outranks the capability gate.
        let error = service
            .prepare_accounts_set_username(
                &params(Some("kt_test.01".into()), false),
                SignalCliMode::KtEngine,
            )
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api)
                if api.code == "CAPABILITY_UNAVAILABLE" && !api.retryable));
        let error = service
            .prepare_accounts_set_username(&params(None, true), SignalCliMode::KtEngine)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api)
                if api.code == "CAPABILITY_UNAVAILABLE" && !api.retryable));

        // JVM payloads.
        assert_eq!(
            service
                .prepare_accounts_set_username(
                    &params(Some("kt_test.01".into()), false),
                    SignalCliMode::Jvm
                )
                .unwrap(),
            json!({"account": "+15555550100", "username": "kt_test.01"})
        );
        assert_eq!(
            service
                .prepare_accounts_set_username(&params(None, true), SignalCliMode::Jvm)
                .unwrap(),
            json!({"account": "+15555550100", "deleteUsername": true})
        );

        // Unknown account answers before everything else.
        let error = service
            .prepare_accounts_set_username(
                &AccountsSetUsernameParams {
                    account_id: "acct-absent".into(),
                    username: Some("kt_test.01".into()),
                    delete_username: false,
                },
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::Store(StoreError::AccountNotFound)
        ));
    }

    /// accounts.getUsername answers CAPABILITY_UNAVAILABLE in both modes
    /// after account resolution; unknown accounts answer first.
    #[test]
    fn get_username_preparation_answers_honest_capability_debt() {
        use crate::engine::SignalCliMode;
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        for mode in [SignalCliMode::Jvm, SignalCliMode::KtEngine] {
            let error = service
                .prepare_accounts_get_username(&account.id)
                .unwrap_err();
            assert!(matches!(error, ServiceError::Api(ref api)
                    if api.code == "CAPABILITY_UNAVAILABLE" && !api.retryable));
            let _ = mode;
        }
        let error = service
            .prepare_accounts_get_username("acct-absent")
            .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::Store(StoreError::AccountNotFound)
        ));
    }

    /// username.lookup bounds the batch, forwards entries verbatim, and
    /// answers the kt-engine debt after validation.
    #[test]
    fn username_lookup_preparation_bounds_batch_and_gates_mode() {
        use crate::engine::SignalCliMode;
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let params = |usernames: Vec<String>| UsernameLookupParams {
            account_id: account.id.clone(),
            usernames,
        };

        // Empty and over-cap batches.
        let error = service
            .prepare_username_lookup(&params(vec![]), SignalCliMode::Jvm)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        let over = vec!["kt.user".to_string(); USERNAME_LOOKUP_MAX_ENTRIES + 1];
        let error = service
            .prepare_username_lookup(&params(over), SignalCliMode::Jvm)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        // Entry bound: 65 chars.
        let error = service
            .prepare_username_lookup(&params(vec!["k".repeat(65)]), SignalCliMode::Jvm)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));

        // KtEngine debt after validation.
        let error = service
            .prepare_username_lookup(&params(vec!["kt_test.01".into()]), SignalCliMode::KtEngine)
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api)
                if api.code == "CAPABILITY_UNAVAILABLE" && !api.retryable));

        // JVM payload: entries forwarded verbatim.
        assert_eq!(
            service
                .prepare_username_lookup(
                    &params(vec!["kt_test.01".into(), "https://signal.me/#eu".into()]),
                    SignalCliMode::Jvm
                )
                .unwrap(),
            json!({
                "account": "+15555550100",
                "usernames": ["kt_test.01", "https://signal.me/#eu"],
            })
        );
    }

    /// The username shape gate is exactly the desktop grammar: a focused
    /// rule-by-rule pass.
    #[test]
    fn validate_username_shape_mirrors_the_desktop_grammar() {
        for good in [
            "abc",           // minimal nickname
            &"a".repeat(32), // maximal nickname
            "kt_test",       // underscore nickname, bare (no discriminator)
            "_leading",      // underscore first
            "kt_test.01",    // discriminator, two-digit 0-leading form
            "kt_test.10",
            "kt_test.65536", // full digit run (range stays upstream authority)
            "a_b.99",
        ] {
            validate_username_shape(good)
                .unwrap_or_else(|error| panic!("expected {good} to pass: {error:?}"));
        }
        for bad in [
            "ab",             // below the 3-char nickname window
            &"a".repeat(33),  // above the 32-char nickname window
            "kt test",        // space
            "kt-Test",        // dash
            "1kt",            // leading digit
            "kt.",            // nickname 2 chars AND empty discriminator
            "kt..1",          // nickname 2 chars AND empty first discriminator
            "kt_test.01.2",   // dot inside the discriminator
            "kt_test.00",     // double zero
            "kt_test.000",    // triple zero
            "kt_test.0",      // 0-leading single digit (only exactly two digits allowed)
            "kt_test.012",    // 0-leading three digits
            "kt_test.65536x", // trailing letter inside the discriminator
        ] {
            assert!(
                validate_username_shape(bad).is_err(),
                "expected {bad} to fail"
            );
        }
    }

    // ---- Contract 1.52 (§4.50): group termination face ----

    /// prepare_groups_terminate resolves the group and answers the bare
    /// upstream payload (the pinned 0.14.8 `terminateGroup` dests); the
    /// refusal ladder answers before any upstream call — bad ids, an unknown
    /// conversation, and direct addressing. No mode gate: the face exists on
    /// every engine mode's shared jsonRpc surface.
    #[test]
    fn groups_terminate_preparation_addresses_the_group_and_refuses_non_groups() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-id-1", "group")
            .unwrap();
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let terminate = |conversation_id: String| GroupsTerminateParams {
            account_id: account.id.clone(),
            conversation_id,
            operation_id: None,
        };
        assert_eq!(
            service
                .prepare_groups_terminate(&terminate(group.id.clone()))
                .unwrap(),
            json!({
                "account": "+15555550100",
                "groupId": "group-id-1",
            })
        );
        // The operationId is validated by shape and never persisted; a legal
        // one changes nothing on the wire.
        assert_eq!(
            service
                .prepare_groups_terminate(&GroupsTerminateParams {
                    operation_id: Some("term-op-1".into()),
                    ..terminate(group.id.clone())
                })
                .unwrap(),
            json!({
                "account": "+15555550100",
                "groupId": "group-id-1",
            })
        );
        let error = service
            .prepare_groups_terminate(&terminate(direct.id.clone()))
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        let missing = service
            .prepare_groups_terminate(&terminate("conv-absent".into()))
            .unwrap_err();
        assert!(matches!(
            missing,
            ServiceError::Store(StoreError::ConversationNotFound)
        ));
        let bad_operation = service
            .prepare_groups_terminate(&GroupsTerminateParams {
                operation_id: Some("x".repeat(MAX_OPAQUE_ID_BYTES + 1)),
                ..terminate(group.id.clone())
            })
            .unwrap_err();
        assert!(
            matches!(bad_operation, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"),
            "an over-bound operationId is a shape refusal: {bad_operation:?}"
        );
    }

    // ---- Contract 1.58 (§4.53): group join-request approval face ----

    /// prepare_groups_approve_join_requests validates the candidate set and
    /// resolves the group row: the happy path answers (peerKey, deduped
    /// members, approve) with duplicates collapsed and order normalized;
    /// the refusal ladder answers before any engine call — empty members,
    /// oversized member arrays, per-entry shape violations, bad ids, an
    /// unknown conversation, direct addressing, and an over-bound audit id.
    /// The kt-engine face answers CAPABILITY_UNAVAILABLE after the local
    /// validation completed (the engine debt precedent — the fold must not
    /// disguise "cannot approve" as "nobody is requesting").
    #[test]
    fn groups_approve_join_requests_preparation_folds_shape_and_refuses_gaps() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-id-1", "group")
            .unwrap();
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let params = |conversation_id: String, members: Vec<String>, approve: bool| {
            GroupsApproveJoinRequestsParams {
                account_id: account.id.clone(),
                conversation_id,
                members,
                approve,
                operation_id: None,
            }
        };

        // The happy path: both decision directions and duplicate collapse.
        assert_eq!(
            service
                .prepare_groups_approve_join_requests(
                    &params(group.id.clone(), vec!["+15555550101".into()], true),
                    SignalCliMode::Jvm,
                )
                .unwrap(),
            (
                "group-id-1".to_string(),
                vec!["+15555550101".to_string()],
                true
            )
        );
        assert_eq!(
            service
                .prepare_groups_approve_join_requests(
                    &params(
                        group.id.clone(),
                        vec![
                            "+15555550102".into(),
                            "+15555550101".into(),
                            "+15555550102".into()
                        ],
                        false,
                    ),
                    SignalCliMode::Jvm,
                )
                .unwrap(),
            (
                "group-id-1".to_string(),
                vec!["+15555550101".to_string(), "+15555550102".to_string()],
                false
            )
        );

        // The refusal ladder, each before any engine call.
        let empty = service
            .prepare_groups_approve_join_requests(
                &params(group.id.clone(), vec![], true),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(empty, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        let direct_addressed = service
            .prepare_groups_approve_join_requests(
                &params(direct.id.clone(), vec!["+15555550102".into()], true),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(
            matches!(direct_addressed, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"),
            "a direct conversation is a shape refusal: {direct_addressed:?}"
        );
        let missing = service
            .prepare_groups_approve_join_requests(
                &params("conv-absent".into(), vec!["+15555550102".into()], true),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(
            missing,
            ServiceError::Store(StoreError::ConversationNotFound)
        ));
        let bad_shape = service
            .prepare_groups_approve_join_requests(
                &params(group.id.clone(), vec![String::new()], true),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(bad_shape, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        let oversized = service
            .prepare_groups_approve_join_requests(
                &params(
                    group.id.clone(),
                    (0..=MAX_GROUP_MEMBERS)
                        .map(|index| format!("+15555050{index:03}"))
                        .collect(),
                    true,
                ),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(oversized, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));

        // The kt-engine face: the honest capability gap after local
        // validation (CAPABILITY_UNAVAILABLE, retryable=false).
        let engine_mode = service
            .prepare_groups_approve_join_requests(
                &params(group.id.clone(), vec!["+15555550101".into()], true),
                SignalCliMode::KtEngine,
            )
            .unwrap_err();
        assert!(
            matches!(engine_mode, ServiceError::Api(ref api) if api.code == "CAPABILITY_UNAVAILABLE" && !api.retryable),
            "the engine debt answers the capability gap: {engine_mode:?}"
        );
        // The engine gate sits after local validation: a shape violation
        // still answers INVALID_REQUEST on the engine face.
        let engine_mode_shape = service
            .prepare_groups_approve_join_requests(
                &params(group.id.clone(), vec![], true),
                SignalCliMode::KtEngine,
            )
            .unwrap_err();
        assert!(
            matches!(engine_mode_shape, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST")
        );
    }

    /// prepare_groups_revoke_invites validates the candidate set and
    /// resolves the group row: the happy path answers (peerKey, deduped
    /// members) with duplicates collapsed and order normalized; the refusal
    /// ladder answers before any engine call — empty members, oversized
    /// member arrays, per-entry shape violations, bad ids, an unknown
    /// conversation, and direct addressing. The kt-engine face answers
    /// CAPABILITY_UNAVAILABLE after the local validation completed (the
    /// engine debt precedent — the fold must not disguise "cannot revoke"
    /// as "nobody is pending").
    #[test]
    fn groups_revoke_invites_preparation_folds_shape_and_refuses_gaps() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-id-1", "group")
            .unwrap();
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550101", "Peer")
            .unwrap();
        let params = |conversation_id: String, members: Vec<String>| GroupsRevokeInvitesParams {
            account_id: account.id.clone(),
            conversation_id,
            members,
            operation_id: None,
        };

        // The happy path: duplicate collapse and order normalization.
        assert_eq!(
            service
                .prepare_groups_revoke_invites(
                    &params(
                        group.id.clone(),
                        vec![
                            "+15555550102".into(),
                            "+15555550101".into(),
                            "+15555550102".into()
                        ],
                    ),
                    SignalCliMode::Jvm,
                )
                .unwrap(),
            (
                "group-id-1".to_string(),
                vec!["+15555550101".to_string(), "+15555550102".to_string()]
            )
        );

        // The refusal ladder, each before any engine call.
        let empty = service
            .prepare_groups_revoke_invites(&params(group.id.clone(), vec![]), SignalCliMode::Jvm)
            .unwrap_err();
        assert!(matches!(empty, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        let direct_addressed = service
            .prepare_groups_revoke_invites(
                &params(direct.id.clone(), vec!["+15555550102".into()]),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(
            matches!(direct_addressed, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"),
            "a direct conversation is a shape refusal: {direct_addressed:?}"
        );
        let missing = service
            .prepare_groups_revoke_invites(
                &params("conv-absent".into(), vec!["+15555550102".into()]),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(
            missing,
            ServiceError::Store(StoreError::ConversationNotFound)
        ));
        let bad_shape = service
            .prepare_groups_revoke_invites(
                &params(group.id.clone(), vec![String::new()]),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(bad_shape, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        let oversized = service
            .prepare_groups_revoke_invites(
                &params(
                    group.id.clone(),
                    (0..=MAX_GROUP_MEMBERS)
                        .map(|index| format!("+15555050{index:03}"))
                        .collect(),
                ),
                SignalCliMode::Jvm,
            )
            .unwrap_err();
        assert!(matches!(oversized, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));

        // The kt-engine face: the honest capability gap after local
        // validation (CAPABILITY_UNAVAILABLE, retryable=false).
        let engine_mode = service
            .prepare_groups_revoke_invites(
                &params(group.id.clone(), vec!["+15555550101".into()]),
                SignalCliMode::KtEngine,
            )
            .unwrap_err();
        assert!(
            matches!(engine_mode, ServiceError::Api(ref api) if api.code == "CAPABILITY_UNAVAILABLE" && !api.retryable),
            "the engine debt answers the capability gap: {engine_mode:?}"
        );
        // The engine gate sits after local validation: a shape violation
        // still answers INVALID_REQUEST on the engine face.
        let engine_mode_shape = service
            .prepare_groups_revoke_invites(
                &params(group.id.clone(), vec![]),
                SignalCliMode::KtEngine,
            )
            .unwrap_err();
        assert!(
            matches!(engine_mode_shape, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST")
        );
    }

    // ---- Contract 1.49 (§4.48): report spam face ----

    /// prepare_report_spam resolves the direct conversation's rows to the
    /// engine payload: only rows carrying a persisted serverGuid join
    /// `serverGuids` (order preserved, caller's id paired for the result
    /// projection); the refusal ladder answers before any upstream call —
    /// empty/oversized id lists, bad ids, unknown conversations, group
    /// addressing, a numeric peer with no recorded ACI mapping — while a
    /// UUID-shaped peer key addresses the report directly.
    #[test]
    fn report_spam_preparation_resolves_guid_rows_and_refuses_gaps() {
        let (_temp, service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);

        // A UUID-shaped peer key: the ACI itself, no mapping needed.
        let aci = "0b7fca57-1234-4d0e-9b0f-4f6c1f8a2e10";
        let direct = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", aci, "Peer")
            .unwrap();
        let mut guided = expire_record_fixture(&account.id, &direct.id, "m1");
        guided.server_guid = Some("guid-1".into());
        service
            .store_ref()
            .insert_message(&guided, None, None, true)
            .unwrap();
        let plain = expire_record_fixture(&account.id, &direct.id, "m2");
        service
            .store_ref()
            .insert_message(&plain, None, None, true)
            .unwrap();

        let params = |conversation_id: String, message_ids: Vec<String>| MessagesReportSpamParams {
            account_id: account.id.clone(),
            conversation_id,
            message_ids,
        };

        let prepared = service
            .prepare_report_spam(&params(
                direct.id.clone(),
                vec!["m1".into(), "m2".into(), "missing".into()],
            ))
            .unwrap();
        assert_eq!(
            prepared.upstream,
            json!({
                "account": "+15555550100",
                "targetAci": aci,
                "serverGuids": ["guid-1"],
            })
        );
        assert_eq!(
            prepared.pairs,
            vec![("m1".to_string(), "guid-1".to_string())]
        );

        // Empty and oversized id lists.
        let error = service
            .prepare_report_spam(&params(direct.id.clone(), Vec::new()))
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        let error = service
            .prepare_report_spam(&params(
                direct.id.clone(),
                (0..REPORT_SPAM_MESSAGE_IDS_LIMIT + 1)
                    .map(|index| format!("m{index}"))
                    .collect(),
            ))
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        // Per-id bounds: an empty entry and an over-budget id.
        let error = service
            .prepare_report_spam(&params(direct.id.clone(), vec!["".into()]))
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));
        let error = service
            .prepare_report_spam(&params(
                direct.id.clone(),
                vec!["x".repeat(MAX_OPAQUE_ID_BYTES + 1)],
            ))
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));

        // A numeric peer without a recorded ACI mapping refuses before any
        // upstream call — the report would mis-address upstream.
        let numeric = service
            .store_ref()
            .ensure_conversation(&account.id, "direct", "+15555550102", "Numeric")
            .unwrap();
        let error = service
            .prepare_report_spam(&params(numeric.id.clone(), vec!["m1".into()]))
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api)
                if api.code == "INVALID_REQUEST" && !api.retryable));

        // The recorded ACI↔peer-key mapping resolves the numeric peer.
        service
            .store_ref()
            .upsert_peer_identity(
                &account.id,
                "33333333-3333-3333-3333-333333333333",
                "+15555550102",
            )
            .unwrap();
        let prepared = service
            .prepare_report_spam(&params(numeric.id.clone(), vec!["m1".into()]))
            .unwrap();
        assert_eq!(
            prepared.upstream["targetAci"],
            json!("33333333-3333-3333-3333-333333333333")
        );

        // A group conversation refuses before any upstream call.
        let group = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-id-1", "group")
            .unwrap();
        let error = service
            .prepare_report_spam(&params(group.id.clone(), vec!["m1".into()]))
            .unwrap_err();
        assert!(matches!(error, ServiceError::Api(ref api) if api.code == "INVALID_REQUEST"));

        // Unknown conversations answer the ladder step.
        let error = service
            .prepare_report_spam(&params("conv-absent".into(), vec!["m1".into()]))
            .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::Store(StoreError::ConversationNotFound)
        ));
    }

    /// A group-update row mirrors the engine's post-change title into the
    /// conversation row (the summary riding conversation.changed already
    /// carries it), lands as a system row, and leaves the title untouched
    /// when the update carried no name material. A body-carrying update keeps
    /// its ordinary message row and still mirrors.
    #[test]
    fn ingest_group_update_mirrors_title_and_lands_a_system_row() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-id-1", "group")
            .unwrap();
        let group_update = NormalizedReceive {
            timestamp: Some(1727000000000),
            content_kind: "dataMessage",
            direction: "system",
            account_present: true,
            account: Some("+15555550100".into()),
            source: Some("+15555550101".into()),
            source_uuid: None,
            peer_name: Some("Peer".into()),
            group_id: Some("group-id-1".into()),
            text: Some("群聊信息已更新".into()),
            text_bytes: None,
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: Some(crate::engine::NormalizedRich {
                is_group_update: true,
                group_name: Some("Fixture Group".into()),
                ..Default::default()
            }),
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: None,
            server_guid: None,
        };
        let events = service
            .ingest_receive(group_update.clone(), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let upserted = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .expect("group update row");
        assert_eq!(upserted.direction, "system");
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("title mirror event");
        assert_eq!(changed.title, "Fixture Group");
        assert_eq!(
            service
                .store_ref()
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .title,
            "Fixture Group"
        );

        // An update with no name material lands its row and converges nothing.
        let nameless = NormalizedReceive {
            timestamp: Some(1727000000001),
            rich: Some(crate::engine::NormalizedRich {
                is_group_update: true,
                ..Default::default()
            }),
            server_guid: None,
            ..group_update.clone()
        };
        let events = service
            .ingest_receive(nameless, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, HostSideEvent::MessageUpserted(_)))
        );
        assert_eq!(
            service
                .store_ref()
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .title,
            "Fixture Group"
        );

        // A body-carrying update is an ordinary incoming row — and still
        // mirrors the title.
        let with_body = NormalizedReceive {
            timestamp: Some(1727000000002),
            direction: "incoming",
            text: Some("renamed us".into()),
            rich: Some(crate::engine::NormalizedRich {
                is_group_update: true,
                group_name: Some("Renamed Group".into()),
                ..Default::default()
            }),
            server_guid: None,
            ..group_update
        };
        let events = service
            .ingest_receive(with_body, crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let upserted = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::MessageUpserted(message) => Some(message),
                _ => None,
            })
            .expect("body row");
        assert_eq!(upserted.direction, "incoming");
        assert_eq!(
            service
                .store_ref()
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .title,
            "Renamed Group"
        );
    }

    /// The engine's groupUpdated projection (2026-10-06 engine batch) is
    /// state convergence, not chat content: the mirror lands with the
    /// conversation.changed summary carrying the new timer (0 = off included),
    /// no message row and no system row exists, and a same-value event changes
    /// nothing but still re-pushes the summary so a lagging window converges.
    #[test]
    fn ingest_group_timer_update_mirrors_state_without_a_row() {
        let (_temp, mut service) = service();
        let account = service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap()
            .remove(0);
        let conversation = service
            .store_ref()
            .ensure_conversation(&account.id, "group", "group-id-1", "group")
            .unwrap();

        let group_timer = NormalizedReceive {
            timestamp: Some(1727000000000),
            content_kind: "control",
            direction: "control",
            account_present: true,
            account: Some("+15555550100".into()),
            source: None,
            source_uuid: None,
            server_guid: None,
            peer_name: None,
            group_id: Some("group-id-1".into()),
            text: None,
            text_bytes: None,
            text_truncated: false,
            quote: None,
            attachments: Vec::new(),
            rich: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            control: Some(ControlReceive::GroupTimerUpdate {
                expire_timer_seconds: 3600,
            }),
        };
        let events = service
            .ingest_receive(group_timer.clone(), crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, HostSideEvent::MessageUpserted(_))),
            "a timer notification is not a message row"
        );
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("the mirror rides conversation.changed");
        assert_eq!(changed.id, conversation.id);
        assert_eq!(changed.expire_timer_seconds, Some(3600));
        assert_eq!(
            service
                .store_ref()
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .expire_timer_seconds,
            Some(3600)
        );

        // The off form stores 0 literally (the §4.41 setter discipline), never
        // NULL, and the summary reports the off state.
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    control: Some(ControlReceive::GroupTimerUpdate {
                        expire_timer_seconds: 0,
                    }),
                    ..group_timer.clone()
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        let changed = events
            .iter()
            .find_map(|event| match event {
                HostSideEvent::ConversationChanged(summary) => Some(summary),
                _ => None,
            })
            .expect("the off form notifies too");
        assert_eq!(changed.expire_timer_seconds, Some(0));
        assert_eq!(
            service
                .store_ref()
                .conversation_summary(&account.id, &conversation.id)
                .unwrap()
                .unwrap()
                .expire_timer_seconds,
            Some(0)
        );

        // A same-value event changes nothing but still re-pushes the summary.
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    control: Some(ControlReceive::GroupTimerUpdate {
                        expire_timer_seconds: 0,
                    }),
                    ..group_timer
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, HostSideEvent::ConversationChanged(_))),
            "the same-value event still re-pushes the summary"
        );
    }

    /// A groupUpdated naming a group nothing local resolves is warn-only
    /// dropped — better to converge late than to invent a row (宁缺勿错). The
    /// resolvable twin of this drop path is the mirror test above.
    #[test]
    fn ingest_group_timer_update_for_an_unknown_group_drops() {
        let (_temp, mut service) = service();
        service
            .sync_accounts_from_numbers(&["+15555550100".into()], crate::DEFAULT_PROXY_GROUP_ID)
            .unwrap();
        let events = service
            .ingest_receive(
                NormalizedReceive {
                    timestamp: Some(1727000000000),
                    content_kind: "control",
                    direction: "control",
                    account_present: true,
                    account: Some("+15555550100".into()),
                    source: None,
                    source_uuid: None,
                    server_guid: None,
                    peer_name: None,
                    group_id: Some("dW5rbm93bi1ncm91cA==".into()),
                    text: None,
                    text_bytes: None,
                    text_truncated: false,
                    quote: None,
                    attachments: Vec::new(),
                    rich: None,
                    sticker: None,
                    expire_seconds: None,
                    expire_start_at: None,
                    control: Some(ControlReceive::GroupTimerUpdate {
                        expire_timer_seconds: 3600,
                    }),
                },
                crate::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap();
        assert!(events.is_empty(), "an unknown group converges nothing");
    }
}
