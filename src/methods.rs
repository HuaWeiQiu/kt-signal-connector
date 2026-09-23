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
    ("messages.sendText", Lane::Send, true, "send"),
    ("messages.attachments.send", Lane::Send, true, "send"),
    ("messages.edit", Lane::Send, true, "send"),
    ("messages.remoteDelete", Lane::Send, true, "send"),
    ("messages.sendReaction", Lane::Send, true, "send"),
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
            "messages.attachments.open",
            "messages.attachments.readChunk",
            "messages.attachments.closeHandle",
            "contacts.sync",
            "contacts.list",
            "groups.get",
        ];
        let send_lane = [
            "messages.sendText",
            "messages.attachments.send",
            "messages.edit",
            "messages.remoteDelete",
            "messages.sendReaction",
            "contacts.setLocalAlias",
            "presence.setTypingMessage",
        ];
        let mutating = [
            "messages.sendText",
            "messages.attachments.send",
            "messages.edit",
            "messages.remoteDelete",
            "messages.sendReaction",
            "contacts.sync",
            "contacts.setLocalAlias",
            "presence.setTypingMessage",
            "accounts.deleteLocalData",
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
        assert_eq!(crate::PHASE2_CAPABILITIES, METHOD_NAMES.as_slice());
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
