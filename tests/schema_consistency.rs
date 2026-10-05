// SPDX-License-Identifier: AGPL-3.0-only

//! Phase 0 safety net (docs/optimization-plan.md, 2026-08-25): machine-checked
//! consistency between `schemas/connector-api-v1.schema.json` and the code, so
//! contract drift fails the test suite instead of shipping silently.
//!
//! Code-side sets are extracted mechanically where a stable source exists:
//!
//! - methods: `PHASE2_CAPABILITIES` (src/lib.rs:35) plus `"handshake"`, which is
//!   handled before capability dispatch (src/host.rs:307). Every schema method
//!   must also appear as a dispatch literal in src/host.rs.
//! - events: every `HostEvent::new("<name>", ...)` literal in src/ (the wire
//!   serializer is the emission boundary).
//! - error codes: every `ApiError::new("<CODE>", ...)` literal in src/.
//! - numeric bounds: every schema length/numeric constraint the code also
//!   enforces through a named constant (optimization-plan A8; this is the
//!   gate that keeps A5-class drift — schema 128 vs code 256 — from
//!   recurring). Bounds without a natural code-constant or schema
//!   representation are inventoried with reasons on the bounds test below
//!   (optimization-plan §6.4 M3.4).
//!
//! Known drift is encoded in the explicit allowlists below, each with a reason
//! and the phase that resolves it. Any drift outside an allowlist fails.

#![cfg(unix)]

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use serde_json::Value;

use kt_signal_connector::PHASE2_CAPABILITIES;
use kt_signal_connector::protocol::MAX_REQUEST_ID_BYTES;
use kt_signal_connector::service::{
    GROUP_DESCRIPTION_MAX_BYTES, GROUP_MEMBER_LABEL_MAX_BYTES, GROUP_NAME_MAX_BYTES,
    MARK_RECEIPT_MESSAGE_IDS_LIMIT, MAX_ALIAS_BYTES, MAX_ATTACHMENT_BYTES,
    MAX_ATTACHMENT_CONTENT_TYPE_BYTES, MAX_ATTACHMENT_FILENAME_BYTES, MAX_ATTACHMENT_ID_BYTES,
    MAX_DEVICE_NAME_BYTES, MAX_EMOJI_BYTES, MAX_GROUP_MEMBER_ID_CHARS, MAX_GROUP_MEMBERS,
    MAX_OPAQUE_ID_BYTES, MAX_PIN_CONVERSATION_ID_CHARS, MAX_SEND_MENTIONS, MAX_STICKER_EMOJI_CHARS,
    MAX_STICKER_PACK_ID_CHARS, MAX_STICKER_PACK_KEY_CHARS, MAX_TEXT_BYTES,
    STICKER_PACK_BROWSE_ID_HEX_CHARS, STICKER_PACK_SYNC_KEY_B64_CHARS,
};
use kt_signal_connector::store::MAX_PAGE_LIMIT;

/// Events the code emits but the schema event enum does not declare. Currently
/// none: `runtime.resourcePressure` (emitted by the RSS sampler bridge,
/// src/host.rs) was registered in the schema by optimization-plan Phase 1,
/// recording existing behavior rather than changing the protocol.
const EVENT_CODE_ONLY_ALLOWLIST: &[&str] = &[];

/// Events the schema declares but the code never emits. Currently none:
/// `message.statusChanged` gained a producer in optimization-plan Phase 2
/// (send completion paths in src/service.rs) and is emitted like every other
/// event, so it is covered by the plain set diff.
const EVENT_SCHEMA_ONLY_ALLOWLIST: &[&str] = &[];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn schema() -> Value {
    let path = repo_root().join("schemas/connector-api-v1.schema.json");
    let raw = fs::read_to_string(&path).expect("schema file must be readable");
    serde_json::from_str(&raw).expect("schema file must be valid JSON")
}

fn schema_enum(pointer: &str) -> BTreeSet<String> {
    schema()
        .pointer(pointer)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("schema must declare an enum at {pointer}"))
        .iter()
        .map(|value| {
            value
                .as_str()
                .expect("schema enum entries must be strings")
                .to_string()
        })
        .collect()
}

fn schema_methods() -> BTreeSet<String> {
    schema_enum("/$defs/request/properties/method/enum")
}

fn schema_events() -> BTreeSet<String> {
    schema_enum("/$defs/event/properties/event/enum")
}

fn schema_error_codes() -> BTreeSet<String> {
    schema_enum("/$defs/error/properties/code/enum")
}

/// All Rust sources under src/, as (relative path, content) pairs. Test-only
/// `#[cfg(test)]` modules are part of the crate's code and are scanned too:
/// a literal that only a unit test uses is still a defined code/event name.
fn sources() -> Vec<(String, String)> {
    let src = repo_root().join("src");
    let mut entries: Vec<(String, String)> = fs::read_dir(&src)
        .expect("src/ must be readable")
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .map(|path| {
            (
                path.file_name().unwrap().to_string_lossy().into_owned(),
                fs::read_to_string(&path).unwrap(),
            )
        })
        .collect();
    entries.sort();
    assert!(!entries.is_empty(), "src/ must contain Rust sources");
    entries
}

/// Extract the string literal in the first argument position of every
/// `Constructor::new("literal", ...)` call site, tolerating line breaks
/// between the opening parenthesis and the literal.
fn first_string_literals(sources: &[(String, String)], constructor: &str) -> BTreeSet<String> {
    let marker = format!("{constructor}::new(");
    let mut found = BTreeSet::new();
    for (_name, content) in sources {
        let mut rest = content.as_str();
        while let Some(at) = rest.find(&marker) {
            rest = &rest[at + marker.len()..];
            let trimmed = rest.trim_start();
            let Some(literal) = trimmed.strip_prefix('"') else {
                continue;
            };
            let Some(end) = literal.find('"') else {
                continue;
            };
            found.insert(literal[..end].to_string());
            rest = &literal[end..];
        }
    }
    found
}

fn code_events() -> BTreeSet<String> {
    first_string_literals(&sources(), "HostEvent")
}

fn code_error_codes() -> BTreeSet<String> {
    first_string_literals(&sources(), "ApiError")
        .into_iter()
        .filter(|literal| {
            !literal.is_empty()
                && literal
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        })
        .collect()
}

#[test]
fn schema_methods_match_code_methods() {
    let schema_methods = schema_methods();

    // Code-side method list: the advertised capability list (src/lib.rs:32-35)
    // plus "handshake", which is answered before capability dispatch
    // (src/host.rs:307).
    let mut code_methods: BTreeSet<String> = PHASE2_CAPABILITIES
        .iter()
        .map(|method| (*method).to_string())
        .collect();
    code_methods.insert("handshake".to_string());

    let schema_only: Vec<_> = schema_methods.difference(&code_methods).collect();
    let code_only: Vec<_> = code_methods.difference(&schema_methods).collect();
    assert!(
        schema_only.is_empty() && code_only.is_empty(),
        "method drift: schema-only {schema_only:?}, code-only {code_only:?}"
    );

    // Every schema method must be dispatched by the host: it appears as a
    // string literal in src/host.rs.
    let host = sources()
        .into_iter()
        .find(|(name, _)| name == "host.rs")
        .expect("src/host.rs must exist")
        .1;
    for method in &schema_methods {
        assert!(
            host.contains(&format!("\"{method}\"")),
            "schema method {method} has no dispatch literal in src/host.rs"
        );
    }
}

#[test]
fn schema_error_codes_match_code_error_codes() {
    let schema_codes = schema_error_codes();
    let code_codes = code_error_codes();
    assert!(
        !code_codes.is_empty(),
        "error-code scan found no ApiError::new literals; the scanner is broken"
    );

    let schema_only: Vec<_> = schema_codes.difference(&code_codes).collect();
    let code_only: Vec<_> = code_codes.difference(&schema_codes).collect();
    assert!(
        schema_only.is_empty() && code_only.is_empty(),
        "error-code drift: schema-only {schema_only:?}, code-only {code_only:?}"
    );
}

#[test]
fn schema_events_match_emitted_events_within_allowlist() {
    let schema_events = schema_events();
    let emitted = code_events();
    assert!(
        !emitted.is_empty(),
        "event scan found no HostEvent::new literals; the scanner is broken"
    );

    let allowlisted_code_only: BTreeSet<&str> = EVENT_CODE_ONLY_ALLOWLIST.iter().copied().collect();
    let allowlisted_schema_only: BTreeSet<&str> =
        EVENT_SCHEMA_ONLY_ALLOWLIST.iter().copied().collect();

    let code_only: BTreeSet<&str> = emitted
        .iter()
        .map(String::as_str)
        .filter(|event| !schema_events.contains(*event))
        .collect();
    let schema_only: BTreeSet<&str> = schema_events
        .iter()
        .map(String::as_str)
        .filter(|event| !emitted.contains(*event))
        .collect();

    assert_eq!(
        code_only, allowlisted_code_only,
        "code emits events outside the schema and the allowlist (or an allowlisted \
         drift was fixed — remove its entry): {code_only:?}"
    );
    assert_eq!(
        schema_only, allowlisted_schema_only,
        "schema declares events the code never emits, outside the allowlist (or an \
         allowlisted drift was fixed — remove its entry): {schema_only:?}"
    );
}

/// The integer a schema constraint declares at `pointer` under `key`
/// (`maxLength`, `maximum`, ...). A missing constraint panics: every bound
/// listed below is part of the host contract and must stay declared.
fn schema_bound(pointer: &str, key: &str) -> u64 {
    schema()
        .pointer(pointer)
        .and_then(|value| value.get(key))
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("schema must declare {key} at {pointer}"))
}

/// Numeric bounds the schema declares and the code enforces through a named
/// constant must be equal (A8). The schema is the host-facing contract, the
/// constants are the connector's enforcement; either side drifting changes
/// what hosts may send versus what the connector accepts. This is the gate
/// that would have caught A5 (schema attachmentId maxLength 128 while the
/// code enforced 256).
///
/// Pairs cover every code constant that enforces a schema constraint. Every
/// remaining numeric bound was re-evaluated item by item (optimization-plan
/// §6.4 M3.4) and stays outside the diff for a stated reason — either the
/// code enforces it by a different mechanism than a named constant, or it
/// has no schema representation at all:
///
/// Schema-only bounds without an enforcing code constant:
/// - cursor/before `maxLength` 256: cursors fail closed by decode and key
///   binding (`Store::decode_*_cursor`), never by length; a length constant
///   would not be the enforcement, so pairing it would be fabricated.
/// - error message `maxLength` 256: messages are short code-authored
///   literals; no constant governs them.
/// - proxyGroupId `maxLength` 128: unknown ids fail closed by launch-plan
///   lookup (`PROXY_GROUP_NOT_FOUND`); launcher ids additionally obey the
///   tighter 32-byte grammar at startup (src/groups.rs), which is a
///   launcher-input rule, not a wire-bound constant.
/// - `minLength`/`minimum` 1 and pid/rssBytes representation bounds:
///   inherent shape constraints enforced structurally (required fields,
///   u64/u32 types); no named constant exists to pair.
///
/// Code-only bounds without a schema representation (internal capacity —
/// gated by behavior tests per optimization-plan §6.4 M3.5, not by pairs):
/// - receive queue budgets (256 items / 2 MiB) and internal channel
///   capacities: in-process queues, not wire contracts.
/// - host admission (128 global / 32 per account / 8 MiB aggregate) and
///   per-method dispatch lanes: connection-local admission control.
/// - inbound text projections (4 KiB preview / 128 KiB persisted body),
///   retention (2000 rows/conversation, 256 delete operations): store-side
///   bounds.
/// - RSS pressure policy (512/420 MiB over 3/2 samples, src/resource.rs):
///   operational thresholds pinned by
///   `default_rss_policy_matches_the_documented_budget`.
/// - proxy-group ceiling 8 (src/groups.rs) and the per-engine account
///   ceiling 8 (M3.3, `MAX_ACCOUNTS_PER_ENGINE`): launcher/store policy with
///   no natural schema numeric hook; pinned by
///   `group_count_is_hard_capped_at_eight_including_default` and
///   `eighth_account_links_but_a_ninth_is_refused_at_the_ceiling`.
/// - sticker browse result bounds (contract 1.36, §4.32):
///   `MAX_STICKER_MANIFEST_TITLE_CHARS` / `MAX_STICKER_MANIFEST_AUTHOR_CHARS`
///   (256) and `MAX_STICKER_MANIFEST_ENTRIES` (1024), plus
///   `MAX_STICKER_IMAGE_BASE64_CHARS` (409600) — the schema's `result` is a
///   free-form object, so these live on the engine-result projection
///   (`manifest_from_engine` / `image_from_engine`) and are pinned by the
///   §4.32 integration test.
/// - the pinned-list entry cap (contract 1.36, §4.33,
///   `MAX_PINNED_CONVERSATIONS` = 128): same free-form-result situation,
///   enforced in `pinned_conversations_result` and pinned by the §4.33
///   integration test.
/// - the sticker-pack sync list cap (contract 1.37, §4.34,
///   `MAX_STICKER_PACK_SYNCS` = 256): same free-form-result situation,
///   enforced in `sticker_pack_syncs_result` and pinned by the §4.34
///   integration test.
/// - the group roster cap (contract 1.40, §4.39, `MAX_GROUP_MEMBERS` = 1001)
///   and the roster address cap (`MAX_GROUP_MEMBER_ID_CHARS` = 128): on the
///   §4.39 sync/read faces the schema's `result` is a free-form object, so
///   the bounds live on the sync capture (`group_extra_json`,
///   src/supervisor.rs) and the read projection (`project_group_roster`,
///   src/service.rs), pinned by the §4.39 unit and integration tests; the
///   §4.44 group-management write face (contract 1.45) mirrors both caps in
///   the schema and is pinned by the pairs below.
#[test]
fn schema_numeric_bounds_match_code_constants() {
    let pairs: &[(&str, &str, u64)] = &[
        ("/$defs/requestId", "maxLength", MAX_REQUEST_ID_BYTES as u64),
        ("/$defs/opaqueId", "maxLength", MAX_OPAQUE_ID_BYTES as u64),
        (
            "/$defs/linkStartParams/properties/deviceName",
            "maxLength",
            MAX_DEVICE_NAME_BYTES as u64,
        ),
        (
            "/$defs/messagesSendTextParams/properties/text",
            "maxLength",
            MAX_TEXT_BYTES as u64,
        ),
        (
            "/$defs/messagesSendTextParams/properties/peerKey",
            "maxLength",
            MAX_OPAQUE_ID_BYTES as u64,
        ),
        (
            "/$defs/messagesSendTextParams/properties/peerTitle",
            "maxLength",
            MAX_OPAQUE_ID_BYTES as u64,
        ),
        (
            "/$defs/contactsListParams/properties/query",
            "maxLength",
            MAX_OPAQUE_ID_BYTES as u64,
        ),
        (
            "/$defs/groupsGetParams/properties/groupKey",
            "maxLength",
            MAX_OPAQUE_ID_BYTES as u64,
        ),
        (
            "/$defs/contactsSetLocalAliasParams/properties/peerKey",
            "maxLength",
            MAX_OPAQUE_ID_BYTES as u64,
        ),
        (
            "/$defs/contactsSetLocalAliasParams/properties/alias",
            "maxLength",
            MAX_ALIAS_BYTES as u64,
        ),
        (
            "/$defs/messagesSendReactionParams/properties/emoji",
            "maxLength",
            MAX_EMOJI_BYTES as u64,
        ),
        (
            "/$defs/messagesAttachmentsOpenParams/properties/attachmentId",
            "maxLength",
            MAX_ATTACHMENT_ID_BYTES as u64,
        ),
        (
            "/$defs/messagesSendAttachmentParams/properties/dataBase64",
            "maxLength",
            (4 * MAX_ATTACHMENT_BYTES.div_ceil(3)) as u64,
        ),
        (
            "/$defs/messagesSendAttachmentParams/properties/sizeBytes",
            "maximum",
            MAX_ATTACHMENT_BYTES as u64,
        ),
        (
            "/$defs/messagesSendAttachmentParams/properties/filename",
            "maxLength",
            MAX_ATTACHMENT_FILENAME_BYTES as u64,
        ),
        (
            "/$defs/messagesSendAttachmentParams/properties/contentType",
            "maxLength",
            MAX_ATTACHMENT_CONTENT_TYPE_BYTES as u64,
        ),
        (
            "/$defs/messagesSendAttachmentParams/properties/text",
            "maxLength",
            MAX_TEXT_BYTES as u64,
        ),
        (
            "/$defs/messagesSendStickerParams/properties/packId",
            "maxLength",
            MAX_STICKER_PACK_ID_CHARS as u64,
        ),
        (
            "/$defs/messagesSendStickerParams/properties/packKey",
            "maxLength",
            MAX_STICKER_PACK_KEY_CHARS as u64,
        ),
        (
            "/$defs/messagesSendStickerParams/properties/emoji",
            "maxLength",
            MAX_STICKER_EMOJI_CHARS as u64,
        ),
        (
            "/$defs/messagesSendStickerParams/properties/image/properties/dataBase64",
            "maxLength",
            (4 * MAX_ATTACHMENT_BYTES.div_ceil(3)) as u64,
        ),
        (
            "/$defs/messagesSendStickerParams/properties/image/properties/sizeBytes",
            "maximum",
            MAX_ATTACHMENT_BYTES as u64,
        ),
        // Sticker pack browsing (contract 1.36, §4.32): the browse-face pack
        // id is exactly the official 32-hex form (both minLength and
        // maxLength pinned to the constant); title/author carry the KT
        // 256-char bound; the manifest entry cap is the schema maxItems.
        (
            "/$defs/stickerPackManifestParams/properties/packId",
            "maxLength",
            STICKER_PACK_BROWSE_ID_HEX_CHARS as u64,
        ),
        (
            "/$defs/stickerPackImageParams/properties/packId",
            "maxLength",
            STICKER_PACK_BROWSE_ID_HEX_CHARS as u64,
        ),
        (
            "/$defs/conversationsSetPinnedParams/properties/conversationId",
            "maxLength",
            MAX_PIN_CONVERSATION_ID_CHARS as u64,
        ),
        // Group management (contract 1.45, §4.44): the schema mirrors every
        // bound the service enforces — the addressing pair, the string
        // bounds, the emoji grapheme budget, and the roster caps on the
        // member arrays (the six arrays share one service loop, so one
        // maxItems and one items pin stand for all six).
        (
            "/$defs/groupsUpdateParams/properties/conversationId",
            "maxLength",
            MAX_PIN_CONVERSATION_ID_CHARS as u64,
        ),
        (
            "/$defs/groupsQuitParams/properties/conversationId",
            "maxLength",
            MAX_PIN_CONVERSATION_ID_CHARS as u64,
        ),
        (
            "/$defs/groupsUpdateParams/properties/groupName",
            "maxLength",
            GROUP_NAME_MAX_BYTES as u64,
        ),
        (
            "/$defs/groupsUpdateParams/properties/groupDescription",
            "maxLength",
            GROUP_DESCRIPTION_MAX_BYTES as u64,
        ),
        (
            "/$defs/groupsUpdateParams/properties/memberLabel",
            "maxLength",
            GROUP_MEMBER_LABEL_MAX_BYTES as u64,
        ),
        (
            "/$defs/groupsUpdateParams/properties/memberLabelEmoji",
            "maxLength",
            MAX_EMOJI_BYTES as u64,
        ),
        (
            "/$defs/groupsUpdateParams/properties/addMembers",
            "maxItems",
            MAX_GROUP_MEMBERS as u64,
        ),
        (
            "/$defs/groupsUpdateParams/properties/removeMembers",
            "maxItems",
            MAX_GROUP_MEMBERS as u64,
        ),
        (
            "/$defs/groupsUpdateParams/properties/addMembers/items",
            "maxLength",
            MAX_GROUP_MEMBER_ID_CHARS as u64,
        ),
        (
            "/$defs/groupsUpdateParams/properties/removeMembers/items",
            "maxLength",
            MAX_GROUP_MEMBER_ID_CHARS as u64,
        ),
        // Sticker pack sync (contract 1.37, §4.34): the pack id is the §4.32
        // browse-face 32-hex shape; the pack key when present is exactly the
        // 44-character padded base64 of 32 bytes.
        (
            "/$defs/stickerPackSetSyncParams/properties/packId",
            "maxLength",
            STICKER_PACK_BROWSE_ID_HEX_CHARS as u64,
        ),
        (
            "/$defs/stickerPackSetSyncParams/properties/packKey",
            "maxLength",
            STICKER_PACK_SYNC_KEY_B64_CHARS as u64,
        ),
        (
            "/$defs/conversationsListParams/properties/limit",
            "maximum",
            MAX_PAGE_LIMIT as u64,
        ),
        (
            "/$defs/messagesListParams/properties/limit",
            "maximum",
            MAX_PAGE_LIMIT as u64,
        ),
        (
            "/$defs/contactsListParams/properties/limit",
            "maximum",
            MAX_PAGE_LIMIT as u64,
        ),
        (
            "/$defs/messagesSendTextParams/properties/mentions",
            "maxItems",
            MAX_SEND_MENTIONS as u64,
        ),
        (
            "/$defs/messagesMarkReadParams/properties/messageIds",
            "maxItems",
            MARK_RECEIPT_MESSAGE_IDS_LIMIT as u64,
        ),
        (
            "/$defs/messagesMarkViewedParams/properties/messageIds",
            "maxItems",
            MARK_RECEIPT_MESSAGE_IDS_LIMIT as u64,
        ),
    ];
    assert!(!pairs.is_empty());
    for (pointer, key, code) in pairs {
        let declared = schema_bound(pointer, key);
        assert_eq!(
            declared, *code,
            "bound drift at {pointer}.{key}: schema says {declared}, code enforces {code}"
        );
    }
}
