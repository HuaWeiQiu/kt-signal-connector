// SPDX-License-Identifier: AGPL-3.0-only

//! Single source of truth for the host method surface (optimization-plan
//! §5.2 B1). One row per method carries every classification the connector
//! makes about it; dispatch-lane admission (`host.rs`), the metrics label
//! (`metrics.rs`), and the advertised capability list (`lib.rs`) all derive
//! from this table. Adding a method is one row here plus its host dispatch
//! arm and schema entry — not four parallel classification lists.

/// Global dispatch lane that bounds a method's concurrency
/// (`host::HostDispatchLimits`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Control,
    Read,
    Send,
}

/// One table row: `(name, lane, account-scoped mutating, metrics class)`.
///
/// `account-scoped mutating` marks the mutating methods that serialize on
/// the per-account mutex and sit behind the delete drain barrier (`host.rs`
/// `acquire`); the metrics class is a fixed label from
/// `metrics::METHOD_CLASSES`, never the raw method string.
pub type MethodRow = (&'static str, Lane, bool, &'static str);

pub const METHODS: &[MethodRow] = &[
    ("runtime.status", Lane::Control, false, "control"),
    ("runtime.start", Lane::Control, false, "control"),
    ("runtime.stop", Lane::Control, false, "control"),
    ("accounts.list", Lane::Control, false, "accounts"),
    ("accounts.deleteLocalData", Lane::Control, true, "accounts"),
    ("link.start", Lane::Control, false, "link"),
    // `link.finish` never takes a global lane: `host.rs` routes it to the
    // bounded per-group wait lane (ADR 0001 R3) before the table's lane
    // would apply, so its Control entry only documents the fallback shape.
    ("link.finish", Lane::Control, false, "link"),
    ("link.cancel", Lane::Control, false, "link"),
    ("conversations.list", Lane::Read, false, "read"),
    ("messages.list", Lane::Read, false, "read"),
    ("messages.getText", Lane::Read, false, "read"),
    // Account-wide store search (contract 1.30): a bounded read over the
    // local database only — no upstream traffic, no mutation.
    ("messages.search", Lane::Read, false, "read"),
    ("messages.sendText", Lane::Send, true, "send"),
    // Same-row retry of a definitively failed send (contract 1.31): mutating,
    // send-lane, upstream traffic — the same class as the original send.
    ("messages.retryText", Lane::Send, true, "send"),
    ("messages.attachments.send", Lane::Send, true, "send"),
    // Sticker send (contract 1.35): mutating, send-lane upstream traffic —
    // the §4.12 attachment class verbatim, same-account mutex and delete
    // barrier included; advertised through `send-sticker`.
    ("messages.sendSticker", Lane::Send, true, "send"),
    ("messages.edit", Lane::Send, true, "send"),
    ("messages.remoteDelete", Lane::Send, true, "send"),
    ("messages.sendReaction", Lane::Send, true, "send"),
    // Pin family (contract 1.33): mutating, send-lane upstream traffic — the
    // reaction class verbatim, same-account mutex and delete barrier included.
    ("messages.sendPinMessage", Lane::Send, true, "send"),
    ("messages.sendUnpinMessage", Lane::Send, true, "send"),
    ("messages.sendAdminDelete", Lane::Send, true, "send"),
    // Outbound receipt face (contract 1.34): mutating, send-lane upstream
    // traffic — the reaction class again; the receipt send failure never
    // fails the request, but the write-lane mutex and delete drain barrier
    // still serialize it against sends on the same account.
    ("messages.markRead", Lane::Send, true, "send"),
    ("messages.markViewed", Lane::Send, true, "send"),
    // View-once faces (contract 1.38): the bare open-sync passthrough and
    // the burn trigger are both receipt-class — mutating, send-lane upstream
    // traffic under the reaction-class write-lane mutex and delete barrier;
    // an upstream failure degrades {status: unknown} and is never retried.
    ("messages.sendViewOnceOpen", Lane::Send, true, "send"),
    ("messages.markViewOnceOpened", Lane::Send, true, "send"),
    // Media ingest (ADR 0002): the chunked streaming triple rides the READ
    // lane — one bounded chunk per call, no mutation, no upstream traffic.
    // The one-shot base64 reader (`messages.attachments.get`, contract 1.10
    // PoC) was retired with contract 1.20; the chunked triple is the only
    // inbound-attachment channel.
    ("messages.attachments.open", Lane::Read, false, "read"),
    ("messages.attachments.readChunk", Lane::Read, false, "read"),
    (
        "messages.attachments.closeHandle",
        Lane::Read,
        false,
        "read",
    ),
    // `contacts.sync` rides the READ lane: a slow sync must not squeeze the
    // control lane (runtime.status/start keep answering). The desktop
    // client's requestScheduler.ts keeps its own lane limits as a
    // hand-maintained mirror of host.rs CONTROL/READ/SEND_CONCURRENCY — a
    // lane or capacity change on either side must move both sides in
    // lockstep (optimization-plan §5.2 B2; desktop-side realignment is a
    // separate batch).
    ("contacts.sync", Lane::Read, true, "contacts"),
    ("contacts.list", Lane::Read, false, "contacts"),
    ("groups.get", Lane::Read, false, "contacts"),
    ("contacts.setLocalAlias", Lane::Send, true, "send"),
    ("presence.setTypingMessage", Lane::Send, true, "send"),
    // Sticker pack browsing (contract 1.36, §4.32): anonymous CDN traffic
    // through the engine — no account, no mutation, no local state, so the
    // READ lane and the read metrics class; the connector passes pack
    // identity through and never touches key material beyond the parameter.
    ("stickerPacks.getManifest", Lane::Read, false, "read"),
    ("stickerPacks.getImage", Lane::Read, false, "read"),
    // Conversation pin sync (contract 1.36, §4.33): getPinnedConversations is
    // a bounded cloud-backed read; setConversationPinned is the mutating
    // storage-service write — reaction-class write-lane mutex and delete
    // barrier, no local persistence on either side.
    ("conversations.getPinned", Lane::Read, false, "read"),
    ("conversations.setPinned", Lane::Send, true, "send"),
    // Sticker pack sync (contract 1.37, §4.34): getSyncs is a bounded
    // cloud-backed read of the account's StickerPackRecord set; setSync is
    // the mutating storage-service write — reaction-class write-lane mutex
    // and delete barrier, no local persistence on either side (unlike the
    // anonymous §4.32 browse face, these are account-scoped).
    ("stickerPacks.getSyncs", Lane::Read, false, "read"),
    ("stickerPacks.setSync", Lane::Send, true, "send"),
    // Link-time history import status (contract revision 1.39, §4.38): a
    // store-only read over the import ledger and the archive presence signal
    // — no engine call, no network, no mutation. The desktop polls it while
    // the state is `running`; there is deliberately no push event.
    ("history.importStatus", Lane::Read, false, "read"),
    // Disappearing-message timer set (contract revision 1.42, §4.41):
    // mutating, send-lane upstream traffic — the reaction class verbatim,
    // same-account mutex and delete barrier included. Direct conversations
    // route setExpirationTimer/updateContact by mode; groups route the
    // updateGroup expiration write since contract 1.45 (§4.44).
    ("conversations.setExpireTimer", Lane::Send, true, "send"),
    // Group management writes (contract revision 1.45, §4.44): mutating,
    // send-lane upstream traffic — the setExpireTimer class verbatim,
    // same-account mutex and delete barrier included; advertised through
    // `group-management`. No local row is written by the send path; group
    // state converges through the group-update receive projection.
    ("groups.update", Lane::Send, true, "send"),
    ("groups.quit", Lane::Send, true, "send"),
];

fn spec(method: &str) -> Option<&'static MethodRow> {
    METHODS.iter().find(|row| row.0 == method)
}

/// Dispatch lane for one method; an unknown method takes the control lane
/// (the pre-table fallback behavior).
pub fn lane(method: &str) -> Lane {
    spec(method).map_or(Lane::Control, |row| row.1)
}

/// Whether the method is mutating and account-scoped (per-account mutex plus
/// delete drain barrier in `host.rs`); unknown methods are not.
pub fn is_account_scoped_mutating(method: &str) -> bool {
    spec(method).is_some_and(|row| row.2)
}

/// Fixed metrics label; unknown methods classify as `"unknown"` (plan §8).
pub fn metrics_class(method: &str) -> &'static str {
    spec(method).map_or("unknown", |row| row.3)
}

/// Method names in table order; the advertised capability list
/// (`crate::PHASE2_CAPABILITIES`) derives from this, so the two cannot drift
/// apart.
pub const METHOD_NAMES: [&str; METHODS.len()] = {
    let mut names = [""; METHODS.len()];
    let mut index = 0;
    while index < METHODS.len() {
        names[index] = METHODS[index].0;
        index += 1;
    }
    names
};

/// Feature capability tags that ride the handshake `capabilities` array
/// beyond the method-name list (contract revision 1.34): `send-receipts`
/// marks the outbound receipt face — `messages.markRead` /
/// `messages.markViewed` plus the auto delivery receipt — as present, and
/// `send-sticker` (contract revision 1.35) marks `messages.sendSticker` plus
/// the sticker receive projection as present. `sticker-pack-browse` and
/// `conversation-pin-sync` (contract revision 1.36) mark the two browse/sync
/// faces of §4.32/§4.33, `sticker-pack-sync` (contract revision 1.37)
/// marks the §4.34 account-scoped pack install face, `view-once`
/// (contract revision 1.38) marks the §4.36/§4.37 view-once send, open-sync
/// and burn faces, `history-import` (contract revision 1.39) marks the
/// §4.38 link-time import status face, `disappearing-messages`
/// (contract revision 1.42) marks the §4.41 receive metadata, timer-notice
/// and bounded-sweep faces plus `conversations.setExpireTimer`, and
/// `group-management` (contract revision 1.45) marks the §4.44 group write
/// face (`groups.update` / `groups.quit` / the group route of
/// `conversations.setExpireTimer`) — desktops gate the new surface on this
/// tag so an old connector answers a clean capability gap instead of
/// `METHOD_NOT_ALLOWED`. They never join
/// [`METHOD_NAMES`]: the schema request-frame method enum and the dispatch
/// table describe real methods only.
pub const FEATURE_CAPABILITIES: &[&str] = &[
    "send-receipts",
    "send-sticker",
    "sticker-pack-browse",
    "conversation-pin-sync",
    "sticker-pack-sync",
    "view-once",
    "history-import",
    "disappearing-messages",
    "group-management",
];

#[cfg(test)]
mod tests {
    use super::*;

    /// The dispatch contract the table must keep serving (`host.rs`
    /// `acquire`): the exact read-lane and account-scoped mutating sets, and
    /// the control fallback for unknown methods.
    #[test]
    fn lane_and_mutating_sets_match_the_dispatch_contract() {
        let read_lane = [
            "conversations.list",
            "messages.list",
            "messages.getText",
            "messages.search",
            "messages.attachments.open",
            "messages.attachments.readChunk",
            "messages.attachments.closeHandle",
            "contacts.sync",
            "contacts.list",
            "groups.get",
            "stickerPacks.getManifest",
            "stickerPacks.getImage",
            "conversations.getPinned",
            "stickerPacks.getSyncs",
            "history.importStatus",
        ];
        let send_lane = [
            "messages.sendText",
            "messages.retryText",
            "messages.attachments.send",
            "messages.sendSticker",
            "messages.edit",
            "messages.remoteDelete",
            "messages.sendReaction",
            "messages.sendPinMessage",
            "messages.sendUnpinMessage",
            "messages.sendAdminDelete",
            "messages.markRead",
            "messages.markViewed",
            "messages.sendViewOnceOpen",
            "messages.markViewOnceOpened",
            "contacts.setLocalAlias",
            "presence.setTypingMessage",
            "conversations.setPinned",
            "conversations.setExpireTimer",
            "groups.update",
            "groups.quit",
            "stickerPacks.setSync",
        ];
        let mutating = [
            "messages.sendText",
            "messages.retryText",
            "messages.attachments.send",
            "messages.sendSticker",
            "messages.edit",
            "messages.remoteDelete",
            "messages.sendReaction",
            "messages.sendPinMessage",
            "messages.sendUnpinMessage",
            "messages.sendAdminDelete",
            "messages.markRead",
            "messages.markViewed",
            "messages.sendViewOnceOpen",
            "messages.markViewOnceOpened",
            "contacts.sync",
            "contacts.setLocalAlias",
            "presence.setTypingMessage",
            "accounts.deleteLocalData",
            "conversations.setPinned",
            "conversations.setExpireTimer",
            "groups.update",
            "groups.quit",
            "stickerPacks.setSync",
        ];
        for (name, row_lane, row_mutating, _) in METHODS {
            let expected_lane = if read_lane.contains(name) {
                Lane::Read
            } else if send_lane.contains(name) {
                Lane::Send
            } else {
                Lane::Control
            };
            assert_eq!(*row_lane, expected_lane, "lane for {name}");
            assert_eq!(
                *row_mutating,
                mutating.contains(name),
                "mutating classification for {name}"
            );
        }
        assert_eq!(lane("anything.else"), Lane::Control);
        assert!(!is_account_scoped_mutating("anything.else"));
    }

    /// Cross-table consistency: the capability list, the metrics labels, and
    /// the classification rows cannot drift apart.
    #[test]
    fn capabilities_and_metrics_labels_derive_from_the_table() {
        // The advertised method list is exactly the table; the handshake
        // payload appends the feature tags after it (contract revision 1.34:
        // `send-receipts`).
        assert_eq!(crate::PHASE2_CAPABILITIES, METHOD_NAMES.as_slice());
        assert_eq!(
            crate::advertised_capabilities()[..METHOD_NAMES.len()],
            METHOD_NAMES[..]
        );
        assert_eq!(
            &crate::advertised_capabilities()[METHOD_NAMES.len()..],
            FEATURE_CAPABILITIES
        );
        let names: Vec<_> = METHODS.iter().map(|row| row.0).collect();
        let mut unique = names.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(names.len(), unique.len(), "duplicate method row");
        for (_, _, _, class) in METHODS {
            assert!(
                crate::metrics::METHOD_CLASSES.contains(class),
                "metrics class {class} is not a known series"
            );
        }
        assert_eq!(metrics_class("anything.else"), "unknown");
    }
}
