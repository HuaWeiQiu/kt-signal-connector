// SPDX-License-Identifier: AGPL-3.0-only

//! Android local-backup export consumption (contract revision 1.47, §4.46):
//! the three connector-side machines behind `backups.*`.
//!
//! [`build_artifact`] projects the store into the one-JSON-object-per-line
//! artifact the engine's export task consumes (`backup-export-input.ndjson`
//! in the engine's per-account data directory, backup-export-contract §10.2).
//! Skip decisions belong here — the engine never drops a line silently, so
//! every store row or column that cannot project into the v1 line set is
//! counted per cause in [`SkipCounters`] and reported through
//! `backups.exportStatus` instead.
//!
//! The snapshot caretaker owns the lifecycle the engine contract assigns to
//! the connector (§10.6): clear `-tmp` residue, keep only the most recent
//! snapshot, and preempt the same-second collision by removing a newest
//! snapshot whose name matches the second the next export will stamp.
//!
//! [`BackupFileHandleTable`] streams the two fixed snapshot files back to the
//! desktop with the media three-chain shape (ADR 0002): unguessable handles,
//! strictly sequential chunk reads, 300 s TTL, ≤ 8 live handles per account.
//! Like the media table it is process-wide: `readChunk`/`closeHandle` carry
//! no accountId, so all groups must resolve handles from one table.
//!
//! Boundaries: no raw path, number, or key material ever crosses the host
//! boundary — handles are random tokens, and the snapshot files are the only
//! addressable bytes (a two-value `file` enum, never a path).

use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::ids::{random_id, stable_hash_id};
use crate::protocol::ApiError;
use crate::service::ServiceError;
use crate::store::Store;

/// The transitional artifact file name, beside the engine's
/// `history-import.ndjson` in the account store directory (§10.2). The
/// engine deletes it when the task leaves `running`; the connector treats it
/// as write-once and rebuilds it before every export attempt.
pub const ARTIFACT_FILE_NAME: &str = "backup-export-input.ndjson";
/// Snapshot root inside one account store directory (§10.6).
pub const SNAPSHOT_ROOT: &str = "SignalBackups";
/// Snapshot directory prefix; the suffix is UTC `yyyy-MM-dd-HH-mm-ss` and
/// lexicographic order is time order (engine export.rs).
pub const SNAPSHOT_PREFIX: &str = "signal-backup-";
/// The engine's transient write name (`-tmp`, renamed away atomically).
pub const SNAPSHOT_TMP_SUFFIX: &str = "-tmp";
/// Per-line artifact cap (§10.2, mirrored by the schema description and the
/// engine's fail-closed gate).
pub const MAX_ARTIFACT_LINE_BYTES: usize = 1_048_576;
/// Total artifact line cap (§10.2): far above the store's own retention
/// bounds — a pure runaway fuse.
pub const MAX_ARTIFACT_LINES: u64 = 4_000_000;
/// Total artifact byte cap (§10.2).
pub const MAX_ARTIFACT_TOTAL_BYTES: u64 = 2_147_483_648;
/// `chat_item.body` cap (§10.2): the store's own 128 KiB inbound body bound,
/// re-checked per exported row.
pub const MAX_ARTIFACT_BODY_BYTES: usize = 131_072;
/// Snapshot list cap, newest-first (§10.4).
pub const MAX_SNAPSHOT_ENTRIES: usize = 32;
/// Raw bytes per `backups.exportFile.readChunk` — the media chain's chunk
/// size (ADR 0002); base64 expands one chunk to 349,528 characters.
pub const CHUNK_BYTES: u64 = 262_144;
/// Handle lifetime — the media chain's TTL backstop.
pub const HANDLE_TTL: Duration = Duration::from_secs(300);
/// Live handles one account may hold at once.
pub const MAX_HANDLES_PER_ACCOUNT: usize = 8;
/// The v1 artifact projects a contact's display name into `givenName` only
/// (the Contact proto face this connector can fill); titles are bounded
/// upstream, so no extra cap is layered on here.
const GIVEN_NAME_SOURCE_BYTES: usize = 256;

/// Why one artifact build refused to start or had to abort. Never carries
/// paths, ids, or content — the wire message is the fixed literal.
#[derive(Debug)]
pub enum BuildError {
    /// The engine's `engine-state.json` registry names no store directory for
    /// this account (or an unreadable/malformed registry): fail closed
    /// instead of writing an artifact into a guessed location.
    NoEngineDirectory,
    /// A store read failed.
    Store(crate::store::StoreError),
    /// The connector's own projection exceeded an artifact cap (§10.2). The
    /// engine would fail-closed on the same artifact; refusing locally keeps
    /// the failure before any engine call.
    CapExceeded(&'static str),
    /// The artifact could not be written (missing account directory, disk
    /// full, permissions).
    Io,
}

impl From<BuildError> for ServiceError {
    fn from(error: BuildError) -> Self {
        match error {
            BuildError::NoEngineDirectory => ServiceError::Api(ApiError::new(
                "UPSTREAM_ERROR",
                "the engine has no data directory registered for this account",
                true,
            )),
            BuildError::Store(error) => ServiceError::Store(error),
            BuildError::CapExceeded(what) => ServiceError::Api(ApiError::new(
                "INVALID_REQUEST",
                format!("the backup export projection exceeds the {what} cap"),
                false,
            )),
            BuildError::Io => ServiceError::Api(ApiError::new(
                "STORAGE_WRITE_FAILED",
                "the backup export artifact could not be written",
                true,
            )),
        }
    }
}

/// Per-cause skip counts from one artifact build (§4.46). Every field is a
/// real measurement of the artifact that was just written — zeros included —
/// so the block is published only after a successful build and stays absent
/// before the first one (honest absence, never fabricated).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkipCounters {
    /// `message_events` rows (contract 1.16) — no reaction face in v1.
    pub skipped_reactions: u64,
    /// `message_edits` rows (contract 1.32) — the row body is the latest
    /// body; edit history has no v1 face.
    pub skipped_edits: u64,
    /// Messages carrying a quote — quotes have no v1 face.
    pub skipped_quotes: u64,
    /// Messages carrying attachment descriptors — bytes/metadata have no v1
    /// face; a row with a body still exports the body.
    pub skipped_attachments: u64,
    /// View-once rows — the burn-on-restore semantics have no v1 face.
    pub skipped_view_once: u64,
    /// Sticker rows — packs have no v1 face.
    pub skipped_stickers: u64,
    /// Contacts-sync skeleton peers whose address cannot project into a
    /// contact identity (§6.5): their chats and items are skipped.
    pub skipped_skeleton_chats: u64,
    /// Rows with a truncated preview body — a preview must never become the
    /// canonical restored body.
    pub skipped_body_truncated: u64,
    /// Rows skipped for every other reason: system rows, remote/admin-delete
    /// tombstones, unconfirmed outgoing rows, empty bodies, and group items
    /// whose author does not resolve to a projected contact.
    pub skipped_rows: u64,
}

impl SkipCounters {
    /// Merge one build's counters into an `exportStatus` result object. Takes
    /// `self` by value: the stored counters are `Copy`, and a fresh copy per
    /// answer cannot race a concurrent rebuild.
    pub(super) fn merge_into(self, result: &mut Value) {
        let Ok(object) = serde_json::to_value(self) else {
            return;
        };
        if let (Some(target), Some(source)) = (result.as_object_mut(), object.as_object()) {
            for (key, value) in source {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

/// The fixed v1 body of the `account` line's `profileKey`: the store does not
/// track the linked account's real profile key, so the build derives a
/// deterministic stand-in from the account address. Restoration on Android
/// re-reads the live profile from the network; this value only has to be a
/// stable, well-formed 32-byte key.
fn derived_account_profile_key(signal_account: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"kt-signal-connector/backup-export/account-profile-key");
    hasher.update([0]);
    hasher.update(signal_account.as_bytes());
    hex::encode(hasher.finalize())
}

/// Chunk base64 for the file three-chain — the connector's one canonical
/// padded encoder (the media chain's), shared so the two chunk faces cannot
/// drift. The host surface only ever sees the string; the test suite
/// round-trips through it instead of pulling a decoder dependency.
pub fn encode_chunk(data: &[u8]) -> String {
    crate::service::base64_encode(data)
}

/// One projected contact: the artifact id, both identity forms it is known
/// by, and the single-field display-name projection. `id` stays 0 through the
/// collection pass — ids allocate only after every source has contributed, in
/// canonical-key order, so the same store always builds the same artifact.
struct ContactEntry {
    id: u64,
    aci: Option<String>,
    e164: Option<String>,
    given_name: Option<String>,
    /// Every store spelling this contact is known by — the canonical forms
    /// plus the raw peer-key/roster strings: the receive path hashes whatever
    /// form the envelope carried, so every spelling needs a reverse-map entry.
    forms: Vec<String>,
}

/// The identity forms one peer key can carry: a 16-byte ACI (UUID or raw
/// hex) and/or an E.164 number.
struct PeerIdentity {
    aci: Option<String>,
    /// The E.164 spelling exactly as the store carries it (`+<digits>`); the
    /// contact line projects it as a bare u64 (§10.2).
    e164: Option<String>,
}

/// `+<digits>` (no whitespace, length over 1, value over 0) is an E.164 peer
/// key; the spelling is kept verbatim for the identity forms.
fn parse_e164(peer_key: &str) -> Option<String> {
    let digits = peer_key.strip_prefix('+')?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits
        .parse::<u64>()
        .ok()
        .filter(|value| *value != 0)
        .map(|_| peer_key.to_string())
}

/// A UUID-shaped peer key (or an already-raw 32-char hex string) is an ACI;
/// the canonical artifact form is 32 lowercase hex chars.
fn parse_aci(peer_key: &str) -> Option<String> {
    let hex_chars: String = if peer_key.len() == 36
        && peer_key.as_bytes()[8] == b'-'
        && peer_key.as_bytes()[13] == b'-'
        && peer_key.as_bytes()[18] == b'-'
        && peer_key.as_bytes()[23] == b'-'
    {
        let stripped: String = peer_key.chars().filter(|c| *c != '-').collect();
        stripped
    } else if peer_key.len() == 32 {
        peer_key.to_ascii_lowercase()
    } else {
        return None;
    };
    hex_chars
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit())
        .then(|| hex_chars.to_ascii_lowercase())
}

fn peer_identity(peer_key: &str, aci_by_peer: &HashMap<String, String>) -> PeerIdentity {
    let e164 = parse_e164(peer_key);
    let mut aci = parse_aci(peer_key);
    // The receive path can key a conversation by the E.164 while the ACI is
    // only known from a later envelope observation (`peer_identities`).
    if aci.is_none() {
        aci = aci_by_peer.get(peer_key).cloned();
    }
    PeerIdentity { aci, e164 }
}

/// The group master key behind a group peer key. The engine's group
/// conversation ids are the standard base64 of the 32-byte master key
/// (engine `decode_group_id`), so the decode mirrors the engine exactly; a
/// group id that does not decode to 32 bytes is a skeleton (§6.5).
fn parse_group_master_key(peer_key: &str) -> Option<String> {
    use crate::service::base64_decode_to_vec;
    let mut decoded = Vec::new();
    base64_decode_to_vec(peer_key.trim(), &mut decoded).ok()?;
    (decoded.len() == 32).then(|| hex::encode(decoded))
}

/// Canonical contact-map key: the ACI when known, else the E.164, else the
/// raw spelling (the skeleton case never reaches the map).
fn canonical_contact_key(identity: &PeerIdentity) -> Option<String> {
    if let Some(aci) = &identity.aci {
        return Some(format!("aci:{aci}"));
    }
    identity.e164.as_ref().map(|e164| format!("e164:{e164}"))
}

/// Merge one identity into the contact set (first-wins on every field). The
/// canonical forms and every raw spelling the caller saw ride `forms` so the
/// sender reverse map covers whichever string the receive envelope hashed.
/// `None` — no contact — is the skeleton case: the peer has neither ACI nor
/// E.164 shape, so nothing about it can project (§6.5).
fn record_contact(
    contacts: &mut BTreeMap<String, ContactEntry>,
    identity: PeerIdentity,
    given_name: Option<String>,
    raw_forms: &[&str],
) -> Option<String> {
    let key = canonical_contact_key(&identity)?;
    let entry = contacts.entry(key.clone()).or_insert_with(|| ContactEntry {
        id: 0,
        aci: None,
        e164: None,
        given_name: None,
        forms: Vec::new(),
    });
    if entry.aci.is_none() {
        entry.aci = identity.aci.clone();
    }
    if entry.e164.is_none() {
        entry.e164 = identity.e164.clone();
    }
    if entry.given_name.is_none() {
        entry.given_name =
            given_name.filter(|name| !name.is_empty() && name.len() <= GIVEN_NAME_SOURCE_BYTES);
    }
    let raw = raw_forms.iter().map(|form| (*form).to_string());
    for form in identity.aci.into_iter().chain(identity.e164).chain(raw) {
        if !form.is_empty() && !entry.forms.contains(&form) {
            entry.forms.push(form);
        }
    }
    Some(key)
}

/// Project one group roster (`extra` JSON `members` array, §4.39) into the
/// contact set so group chat items resolve their authors. The linked account's
/// own membership form is skipped — the engine declares self from the account
/// line, so the account is never a contact row. A roster entry with neither
/// ACI nor E.164 shape is simply not a contact: the row-level `skipped_rows`
/// counter catches its messages, not the roster.
fn record_roster_members(
    extra: &Option<String>,
    aci_by_peer: &HashMap<String, String>,
    signal_account: &str,
    contacts: &mut BTreeMap<String, ContactEntry>,
) {
    let Some(extra) = extra
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
    else {
        return;
    };
    let Some(members) = extra.get("members").and_then(Value::as_array) else {
        return;
    };
    for member in members {
        let Some(member) = member.as_object() else {
            continue;
        };
        let id = member.get("id").and_then(Value::as_str);
        let uuid = member.get("uuid").and_then(Value::as_str);
        if id.is_none() && uuid.is_none() {
            continue;
        }
        // One member, two spellings: `uuid` is the ACI form, `id` the
        // address the sync captured (number or raw uuid). Merge them into
        // one identity so the contact set holds one entry with both forms —
        // never two half-identified contacts.
        let raw_forms: Vec<&str> = [id, uuid].into_iter().flatten().collect();
        let base = uuid
            .filter(|uuid| !uuid.is_empty())
            .or(id)
            .unwrap_or_default();
        if raw_forms.contains(&signal_account) {
            continue;
        }
        let mut identity = peer_identity(base, aci_by_peer);
        if identity.aci.is_none() {
            if let Some(form) = raw_forms.iter().find(|form| parse_aci(form).is_some()) {
                identity.aci = parse_aci(form);
            }
        }
        if identity.e164.is_none() {
            if let Some(form) = raw_forms.iter().find(|form| parse_e164(form).is_some()) {
                identity.e164 = parse_e164(form);
            }
        }
        record_contact(contacts, identity, None, &raw_forms);
    }
}

/// A chat line's recipient reference, resolved after the id-allocation pass.
enum ChatRecipient {
    /// Note to Self: the engine's reserved self recipient (id 0, auto-declared
    /// from the account line — the store's own account cache entry never
    /// becomes a contact row).
    NoteToSelf,
    /// A projected contact, by canonical contact-map key.
    Contact(String),
    /// A group, by master key (`groups`' map key).
    Group(String),
}

/// One conversation the artifact carries a `chat` line (and chat items) for,
/// kept in conversation store-id order — that order becomes the chat id order.
struct PendingChat {
    conversation_id: String,
    recipient: ChatRecipient,
}

/// The artifact file writer: counts lines and bytes against the §10.2 caps as
/// it writes, so a runaway projection fails closed mid-build; a failed build
/// never leaves a partial file behind (`build_artifact` removes it).
struct ArtifactWriter {
    file: std::io::BufWriter<std::fs::File>,
    total_bytes: u64,
    lines: u64,
}

impl ArtifactWriter {
    fn create(path: &Path) -> Result<Self, BuildError> {
        let file = std::fs::File::create(path).map_err(|_| BuildError::Io)?;
        Ok(Self {
            file: std::io::BufWriter::new(file),
            total_bytes: 0,
            lines: 0,
        })
    }

    fn write_line(&mut self, line: &Value) -> Result<(), BuildError> {
        let mut bytes = serde_json::to_vec(line).map_err(|_| BuildError::Io)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_ARTIFACT_LINE_BYTES {
            return Err(BuildError::CapExceeded("per-line byte"));
        }
        self.total_bytes += bytes.len() as u64;
        self.lines += 1;
        if self.total_bytes > MAX_ARTIFACT_TOTAL_BYTES {
            return Err(BuildError::CapExceeded("artifact byte"));
        }
        if self.lines > MAX_ARTIFACT_LINES {
            return Err(BuildError::CapExceeded("line count"));
        }
        self.file.write_all(&bytes).map_err(|_| BuildError::Io)
    }

    fn flush(mut self) -> Result<(), BuildError> {
        self.file.flush().map_err(|_| BuildError::Io)
    }
}

/// Build the §4.46 artifact for one account from the store and return the
/// per-cause skip counters. Runs on the blocking pool — no service lock, no
/// engine lock. The artifact is written whole before the engine call; a
/// failed build removes the partial file, so the engine never sees one.
///
/// Row order follows §10.2's reference rule (a referenced line always
/// precedes its referencing line): account, contacts, groups, chats, then
/// chat items per chat in store order, then the closing `all_chat_folder`.
pub fn build_artifact(
    store: &Store,
    data_dir: &Path,
    account_id: &str,
    signal_account: &str,
) -> Result<SkipCounters, BuildError> {
    let Some(account_dir) = crate::history_import::engine_account_dir(data_dir, signal_account)
    else {
        return Err(BuildError::NoEngineDirectory);
    };
    let artifact_path = account_dir.join(ARTIFACT_FILE_NAME);

    let mut counters = SkipCounters {
        skipped_reactions: store
            .count_reaction_events(account_id)
            .map_err(BuildError::Store)?,
        skipped_edits: store
            .count_message_edits(account_id)
            .map_err(BuildError::Store)?,
        ..SkipCounters::default()
    };

    // -- Identity projection ------------------------------------------------
    let aci_by_peer: HashMap<String, String> = store
        .backup_peer_identity_rows(account_id)
        .map_err(BuildError::Store)?
        .into_iter()
        .collect();
    let mut contacts: BTreeMap<String, ContactEntry> = BTreeMap::new();
    let mut groups: BTreeMap<String, ()> = BTreeMap::new();

    for row in store
        .backup_contact_rows(account_id)
        .map_err(BuildError::Store)?
    {
        match row.kind {
            "contact" => {
                if row.peer_key == signal_account {
                    // The linked account's own cache entry (§4.43): the engine
                    // declares self from the account line, so the account is
                    // never a contact row and its chat is Note to Self.
                    continue;
                }
                let identity = peer_identity(&row.peer_key, &aci_by_peer);
                if record_contact(&mut contacts, identity, Some(row.title), &[&row.peer_key])
                    .is_none()
                {
                    counters.skipped_skeleton_chats += 1;
                }
            }
            "group" => match parse_group_master_key(&row.peer_key) {
                Some(master_key) => {
                    groups.insert(master_key, ());
                    record_roster_members(&row.extra, &aci_by_peer, signal_account, &mut contacts);
                }
                None => counters.skipped_skeleton_chats += 1,
            },
            _ => {}
        }
    }

    let mut chats: Vec<PendingChat> = Vec::new();
    for conversation in store
        .backup_conversation_rows(account_id)
        .map_err(BuildError::Store)?
    {
        let recipient = match conversation.kind {
            "direct" => {
                if conversation.peer_key == signal_account {
                    ChatRecipient::NoteToSelf
                } else {
                    let identity = peer_identity(&conversation.peer_key, &aci_by_peer);
                    match record_contact(
                        &mut contacts,
                        identity,
                        Some(conversation.title.clone()),
                        &[&conversation.peer_key],
                    ) {
                        Some(key) => ChatRecipient::Contact(key),
                        None => {
                            counters.skipped_skeleton_chats += 1;
                            continue;
                        }
                    }
                }
            }
            "group" => match parse_group_master_key(&conversation.peer_key) {
                Some(master_key) => {
                    groups.insert(master_key.clone(), ());
                    ChatRecipient::Group(master_key)
                }
                None => {
                    counters.skipped_skeleton_chats += 1;
                    continue;
                }
            },
            _ => continue,
        };
        chats.push(PendingChat {
            conversation_id: conversation.id,
            recipient,
        });
    }

    // -- Id allocation (deterministic) ---------------------------------------
    // Contacts follow canonical-key order, groups master-key order, chats
    // conversation store-id order; every id allocates only after all sources
    // have contributed, so no reference can dangle.
    let mut next_id = 1_u64;
    for entry in contacts.values_mut() {
        entry.id = next_id;
        next_id += 1;
    }
    let mut group_ids: HashMap<&str, u64> = HashMap::new();
    let mut next_id = 1_u64;
    for master_key in groups.keys() {
        group_ids.insert(master_key, next_id);
        next_id += 1;
    }

    // The sender reverse map: every registered spelling of every contact under
    // both conversation namespaces, because the receive path stamps
    // `sender_id = stable_hash_id([account_id, kind, source])` with whatever
    // spelling the envelope carried.
    let mut sender_ids: HashMap<String, u64> = HashMap::new();
    for entry in contacts.values() {
        for form in &entry.forms {
            for namespace in ["direct", "group"] {
                sender_ids.insert(stable_hash_id(&[account_id, namespace, form]), entry.id);
            }
        }
    }

    // -- Write ---------------------------------------------------------------
    let outcome = write_artifact(
        store,
        &artifact_path,
        account_id,
        signal_account,
        &contacts,
        &groups,
        &group_ids,
        &chats,
        &sender_ids,
        &mut counters,
    );
    if outcome.is_err() {
        let _ = std::fs::remove_file(&artifact_path);
    }
    outcome
}

/// The write pass: open the artifact, emit every line, flush. Split from
/// [`build_artifact`] so the error path (partial-file removal) has exactly
/// one owner.
#[allow(clippy::too_many_arguments)]
fn write_artifact(
    store: &Store,
    artifact_path: &Path,
    account_id: &str,
    signal_account: &str,
    contacts: &BTreeMap<String, ContactEntry>,
    groups: &BTreeMap<String, ()>,
    group_ids: &HashMap<&str, u64>,
    chats: &[PendingChat],
    sender_ids: &HashMap<String, u64>,
    counters: &mut SkipCounters,
) -> Result<SkipCounters, BuildError> {
    let display_name = store
        .account_summary(account_id)
        .map_err(BuildError::Store)?
        .and_then(|account| account.display_name)
        .filter(|name| !name.is_empty());
    let mut writer = ArtifactWriter::create(artifact_path)?;
    let write = write_lines(
        &mut writer,
        signal_account,
        display_name.as_deref(),
        contacts,
        groups,
        group_ids,
        chats,
        sender_ids,
        store,
        account_id,
        counters,
    );
    let flush = writer.flush();
    match (write, flush) {
        (Ok(()), Ok(())) => Ok(*counters),
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
    }
}

/// Emit the §10.2 line set in reference order.
#[allow(clippy::too_many_arguments)]
fn write_lines(
    writer: &mut ArtifactWriter,
    signal_account: &str,
    display_name: Option<&str>,
    contacts: &BTreeMap<String, ContactEntry>,
    groups: &BTreeMap<String, ()>,
    group_ids: &HashMap<&str, u64>,
    chats: &[PendingChat],
    sender_ids: &HashMap<String, u64>,
    store: &Store,
    account_id: &str,
    counters: &mut SkipCounters,
) -> Result<(), BuildError> {
    // account — the first line, exactly once (§10.2).
    let mut account_line = json!({
        "type": "account",
        "profileKey": derived_account_profile_key(signal_account),
    });
    if let Some(name) = display_name {
        account_line["givenName"] = Value::String(name.to_string());
    }
    writer.write_line(&account_line)?;

    // contacts — canonical-key order (the allocation order).
    for entry in contacts.values() {
        let mut line = json!({ "type": "contact", "id": entry.id });
        if let Some(aci) = &entry.aci {
            line["aci"] = Value::String(aci.clone());
        }
        if let Some(e164) = &entry.e164 {
            // parse_e164 validated the digits; the line form is a bare u64.
            if let Ok(number) = e164.trim_start_matches('+').parse::<u64>() {
                line["e164"] = json!(number);
            }
        }
        if let Some(name) = &entry.given_name {
            line["givenName"] = Value::String(name.clone());
        }
        writer.write_line(&line)?;
    }

    // groups — master-key order.
    for master_key in groups.keys() {
        writer.write_line(&json!({
            "type": "group",
            "id": group_ids[master_key.as_str()],
            "masterKey": master_key,
        }))?;
    }

    // chats — conversation store-id order.
    for (index, chat) in chats.iter().enumerate() {
        let recipient = match &chat.recipient {
            ChatRecipient::NoteToSelf => 0_u64,
            ChatRecipient::Contact(key) => contacts[key].id,
            ChatRecipient::Group(master_key) => group_ids[master_key.as_str()],
        };
        writer.write_line(&json!({
            "type": "chat",
            "id": index as u64 + 1,
            "recipient": recipient,
        }))?;
    }

    // chat items — per chat in chat order, messages oldest first (the store
    // read's `sent_at ASC, id ASC`).
    for (index, chat) in chats.iter().enumerate() {
        let chat_id = index as u64 + 1;
        for row in store
            .backup_message_rows(account_id, &chat.conversation_id)
            .map_err(BuildError::Store)?
        {
            // Row-level gates first: system rows and tombstones carry no
            // conversational content; an unconfirmed outgoing row must not
            // masquerade as delivered history on the restored device.
            if row.status == "system"
                || row.direction == "system"
                || row.status == "remote-deleted"
                || row.admin_deleted
            {
                counters.skipped_rows += 1;
                continue;
            }
            if row.direction == "outgoing" && !matches!(row.status, "sent" | "delivered" | "read") {
                counters.skipped_rows += 1;
                continue;
            }
            // Column counters: features with no v1 face. A row carrying one
            // still exports its body when it has one.
            if rich_has_view_once(row.rich_json.as_deref()) {
                counters.skipped_view_once += 1;
            }
            if row.sticker_json.is_some() {
                counters.skipped_stickers += 1;
            }
            if row.has_quote {
                counters.skipped_quotes += 1;
            }
            if row.attachments_json.is_some() {
                counters.skipped_attachments += 1;
            }
            let Some(body) = row.body.as_deref().filter(|body| !body.is_empty()) else {
                counters.skipped_rows += 1;
                continue;
            };
            if row.body_truncated {
                // A bounded preview must never become the canonical restored
                // body — counted here only, never in `skippedRows` too.
                counters.skipped_body_truncated += 1;
                continue;
            }
            if body.len() > MAX_ARTIFACT_BODY_BYTES {
                return Err(BuildError::CapExceeded("chat item body"));
            }
            let author = if row.direction == "outgoing" {
                0_u64
            } else if let Some(id) = sender_ids.get(&row.sender_id) {
                *id
            } else {
                // A group or direct author the projection never met: the item
                // cannot reference a contact that does not exist.
                counters.skipped_rows += 1;
                continue;
            };
            writer.write_line(&json!({
                "type": "chat_item",
                "chat": chat_id,
                "author": author,
                "dateSent": row.sent_at,
                "direction": row.direction,
                "dateReceived": row.received_at.unwrap_or(row.sent_at),
                "body": body,
            }))?;
        }
    }

    // The closing marker (§6.1): emitted so the frame set is explicit rather
    // than relying on the engine's default completion.
    writer.write_line(&json!({ "type": "all_chat_folder" }))?;
    Ok(())
}

/// Whether a row's stored rich payload carries the view-once marker (the
/// serialized `NormalizedRich` camelCase shape).
fn rich_has_view_once(rich_json: Option<&str>) -> bool {
    let Some(raw) = rich_json else {
        return false;
    };
    let Ok(rich) = serde_json::from_str::<Value>(raw) else {
        return false;
    };
    ["viewOnce", "viewOnceInvalid"]
        .iter()
        .any(|key| rich.get(key).and_then(Value::as_bool).unwrap_or(false))
}

// -------------------------------------------------------------------------
// Snapshot caretaker (§10.6): the connector owns cleanup.
// -------------------------------------------------------------------------

/// One listed snapshot: directory name and the main file's byte size
/// (a missing main reports 0, the engine's damaged-snapshot honesty).
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotEntry {
    pub name: String,
    pub main_bytes: u64,
}

fn snapshot_root(account_dir: &Path) -> PathBuf {
    account_dir.join(SNAPSHOT_ROOT)
}

fn is_snapshot_name(name: &str) -> bool {
    name.starts_with(SNAPSHOT_PREFIX) && !name.ends_with(SNAPSHOT_TMP_SUFFIX)
}

/// The snapshot directory list, newest-first (name sort), ≤
/// [`MAX_SNAPSHOT_ENTRIES`] — the same rules the engine's `exportStatus`
/// applies (§10.4), so the connector and the engine can never disagree about
/// what "the latest snapshot" is.
pub fn list_snapshots(account_dir: &Path) -> Vec<SnapshotEntry> {
    let root = snapshot_root(account_dir);
    let Ok(entries) = std::fs::read_dir(&root) else {
        return Vec::new();
    };
    let mut snapshots: Vec<SnapshotEntry> = entries
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| is_snapshot_name(name))
        .map(|name| SnapshotEntry {
            main_bytes: std::fs::metadata(root.join(&name).join("main"))
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            name,
        })
        .collect();
    snapshots.sort_by(|a, b| b.name.cmp(&a.name));
    snapshots.truncate(MAX_SNAPSHOT_ENTRIES);
    snapshots
}

/// The newest snapshot, if any.
pub fn latest_snapshot(account_dir: &Path) -> Option<SnapshotEntry> {
    list_snapshots(account_dir).into_iter().next()
}

/// UTC `yyyy-MM-dd-HH-mm-ss` — the engine's snapshot-name suffix shape
/// (export.rs `format_utc_timestamp`, civil-from-days algorithm), reproduced
/// here so the collision check names the exact second the next export will
/// stamp.
pub fn format_utc_timestamp_seconds(secs: u64) -> String {
    const SECS_PER_DAY: u64 = 86_400;
    let days = (secs / SECS_PER_DAY) as i64;
    let secs_of_day = secs % SECS_PER_DAY;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}-{hour:02}-{minute:02}-{second:02}")
}

fn current_snapshot_candidate_name() -> Option<String> {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(format!(
        "{SNAPSHOT_PREFIX}{}",
        format_utc_timestamp_seconds(secs)
    ))
}

/// Best-effort deletion: `NotFound` races are success, real failures log by
/// class and keep the pass moving (a failed collision-candidate deletion
/// surfaces honestly as the engine's `BACKUP_SNAPSHOT_EXISTS`).
fn remove_dir_best_effort(path: &Path) {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(kind = %error.kind(), "backup snapshot cleanup could not remove a directory");
        }
    }
}

/// The pre-export cleanup (§10.6): remove `-tmp` residue, keep only the most
/// recent snapshot, and also remove the newest snapshot when its name equals
/// the second the next export is about to stamp (the same-second collision
/// the engine fail-closes on — clearing it first is the connector's half of
/// the "clear and retry" contract).
pub fn prepare_snapshots_for_export(account_dir: &Path) {
    let root = snapshot_root(account_dir);
    let Ok(entries) = std::fs::read_dir(&root) else {
        return;
    };
    let mut names: Vec<String> = Vec::new();
    for entry in entries.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(SNAPSHOT_TMP_SUFFIX) {
            remove_dir_best_effort(&root.join(&name));
            continue;
        }
        if is_snapshot_name(&name) {
            names.push(name);
        }
    }
    names.sort();
    names.reverse();
    let candidate = current_snapshot_candidate_name();
    for (index, name) in names.into_iter().enumerate() {
        if index == 0 && Some(&name) != candidate.as_ref() {
            continue;
        }
        remove_dir_best_effort(&root.join(name));
    }
}

// -------------------------------------------------------------------------
// File-handle table (the media three-chain shape, ADR 0002).
// -------------------------------------------------------------------------

/// Why a handle operation failed — mapped onto wire codes by the supervisor;
/// the variants stay log-safe (no ids, no paths).
#[derive(Debug, Eq, PartialEq)]
pub enum BackupHandleError {
    /// Unknown, expired, or early-released handle — one indistinguishable
    /// answer (`INVALID_REQUEST`) so handle space cannot be probed.
    NotFound,
    /// Offsets are strictly sequential; this one is not the next byte.
    NotSequential { expected: u64 },
    /// The account already holds the per-account live-handle ceiling.
    Exhausted,
    /// The chunk read failed for a reason other than the file being gone.
    Io,
}

/// One chunk of a snapshot file stream.
#[derive(Debug, Eq, PartialEq)]
pub struct BackupChunk {
    pub data: Vec<u8>,
    pub eof: bool,
}

struct BackupHandleEntry {
    account_id: String,
    path: PathBuf,
    size_bytes: u64,
    bytes_read: u64,
    expires_at: Instant,
}

/// Process-wide table of live snapshot-file streams. One instance serves
/// every proxy group (the registry hands the same `Arc` to all of them):
/// `readChunk`/`closeHandle` carry no accountId on the wire, so the table —
/// not the routing — is the single-origin authority.
#[derive(Default)]
pub struct BackupFileHandleTable {
    entries: StdMutex<HashMap<String, BackupHandleEntry>>,
}

impl BackupFileHandleTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mint a handle for one fixed snapshot file. Refuses at the per-account
    /// ceiling; expired entries are swept lazily first.
    pub fn open(
        &self,
        account_id: &str,
        path: PathBuf,
        size_bytes: u64,
    ) -> Result<String, BackupHandleError> {
        let mut entries = self.entries.lock().expect("backup handle table mutex");
        Self::sweep_expired(&mut entries);
        let live = entries
            .values()
            .filter(|entry| entry.account_id == account_id)
            .count();
        if live >= MAX_HANDLES_PER_ACCOUNT {
            return Err(BackupHandleError::Exhausted);
        }
        let handle = random_id();
        entries.insert(
            handle.clone(),
            BackupHandleEntry {
                account_id: account_id.to_string(),
                path,
                size_bytes,
                bytes_read: 0,
                expires_at: Instant::now() + HANDLE_TTL,
            },
        );
        Ok(handle)
    }

    /// Read the next sequential chunk. A file removed under the stream
    /// (connector cleanup) terminates the stream with a short chunk and
    /// `eof` instead of an error — the caller's loop must end, not retry; any
    /// other read failure is [`BackupHandleError::Io`].
    pub fn read_chunk(
        &self,
        backup_handle: &str,
        offset: u64,
    ) -> Result<BackupChunk, BackupHandleError> {
        let mut entries = self.entries.lock().expect("backup handle table mutex");
        Self::sweep_expired(&mut entries);
        let entry = entries
            .get_mut(backup_handle)
            .ok_or(BackupHandleError::NotFound)?;
        if entry.bytes_read != offset {
            return Err(BackupHandleError::NotSequential {
                expected: entry.bytes_read,
            });
        }
        let want = CHUNK_BYTES.min(entry.size_bytes - entry.bytes_read);
        let data = match read_exact_range(&entry.path, offset, want) {
            Ok(data) => data,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(_) => return Err(BackupHandleError::Io),
        };
        let short = (data.len() as u64) < want;
        entry.bytes_read += data.len() as u64;
        let eof = short || entry.bytes_read >= entry.size_bytes;
        Ok(BackupChunk { data, eof })
    }

    /// Explicit early release; idempotent like the media chain.
    pub fn close(&self, backup_handle: &str) -> bool {
        let mut entries = self.entries.lock().expect("backup handle table mutex");
        entries.remove(backup_handle).is_some()
    }

    /// Drop every handle bound to one account — the account-delete path (a
    /// handle cannot outlive its account session); the TTL backstops the rest.
    pub fn clear_account(&self, account_id: &str) -> usize {
        let mut entries = self.entries.lock().expect("backup handle table mutex");
        let before = entries.len();
        entries.retain(|_, entry| entry.account_id != account_id);
        before - entries.len()
    }

    /// Live handles for tests.
    #[cfg(test)]
    pub fn live_count(&self) -> usize {
        self.entries
            .lock()
            .expect("backup handle table mutex")
            .len()
    }

    fn sweep_expired(entries: &mut HashMap<String, BackupHandleEntry>) {
        let now = Instant::now();
        entries.retain(|_, entry| entry.expires_at > now);
    }
}

/// Read `want` bytes starting at `offset`, opening the file for this one
/// chunk only. Returns short data when the file ends early.
fn read_exact_range(path: &Path, offset: u64, want: u64) -> std::io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut data = vec![0_u8; want as usize];
    let mut filled = 0;
    while filled < data.len() {
        match file.read(&mut data[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    data.truncate(filled);
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn e164_parsing_accepts_only_bare_plus_numbers() {
        assert_eq!(parse_e164("+15550000001").as_deref(), Some("+15550000001"));
        assert_eq!(parse_e164("+0"), None, "the zero number is not an identity");
        assert_eq!(parse_e164("15550000001"), None, "missing plus");
        assert_eq!(parse_e164("+1555 000"), None, "interior space");
        assert_eq!(parse_e164("+"), None);
        assert_eq!(
            parse_e164("+99999999999999999999999"),
            None,
            "overflow is not an identity"
        );
    }

    #[test]
    fn aci_parsing_accepts_uuid_and_raw_hex_and_canonicalizes() {
        assert_eq!(
            parse_aci("6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b").as_deref(),
            Some("6e08f0b61c2d4e5f8a9b0c1d2e3f4a5b")
        );
        assert_eq!(
            parse_aci("6E08F0B61C2D4E5F8A9B0C1D2E3F4A5B").as_deref(),
            Some("6e08f0b61c2d4e5f8a9b0c1d2e3f4a5b"),
            "hex canonicalizes to lowercase"
        );
        assert_eq!(parse_aci("6e08f0b6"), None, "wrong length");
        assert_eq!(
            parse_aci("6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5z"),
            None,
            "non-hex"
        );
    }

    #[test]
    fn group_master_key_decode_mirrors_engine_base64() {
        // "fake-group-1" is 12 bytes — a legacy/other group id, not a master
        // key: the skeleton answer.
        assert_eq!(parse_group_master_key("ZmFrZS1ncm91cC0x"), None);
        let master_key = parse_group_master_key("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=");
        assert_eq!(
            master_key.as_deref(),
            Some("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
        );
    }

    #[test]
    fn canonical_key_prefers_aci_over_e164() {
        let both = PeerIdentity {
            aci: Some("a".repeat(32)),
            e164: Some("+15550000001".to_string()),
        };
        assert_eq!(
            canonical_contact_key(&both).as_deref(),
            Some(&*format!("aci:{}", "a".repeat(32)))
        );
        let only_e164 = PeerIdentity {
            aci: None,
            e164: Some("+15550000001".to_string()),
        };
        assert_eq!(
            canonical_contact_key(&only_e164).as_deref(),
            Some("e164:+15550000001")
        );
        let skeleton = PeerIdentity {
            aci: None,
            e164: None,
        };
        assert_eq!(canonical_contact_key(&skeleton), None);
    }

    #[test]
    fn utc_timestamp_format_matches_the_engine_snapshot_shape() {
        // 2026-10-06T00:00:00Z.
        assert_eq!(
            format_utc_timestamp_seconds(1_791_244_800),
            "2026-10-06-00-00-00"
        );
        assert_eq!(format_utc_timestamp_seconds(0), "1970-01-01-00-00-00");
        // Leap-year day: 2024-02-29T12:34:56Z = 1709210096.
        assert_eq!(
            format_utc_timestamp_seconds(1_709_210_096),
            "2024-02-29-12-34-56"
        );
    }

    #[test]
    fn snapshot_listing_is_newest_first_and_ignores_tmp() {
        let temp = tempfile::TempDir::new().unwrap();
        let root = temp.path().join(SNAPSHOT_ROOT);
        for name in [
            format!("{SNAPSHOT_PREFIX}2026-01-01-00-00-00"),
            format!("{SNAPSHOT_PREFIX}2026-01-02-00-00-00"),
            format!("{SNAPSHOT_PREFIX}2026-01-03-00-00-00{SNAPSHOT_TMP_SUFFIX}"),
            "unrelated".to_string(),
        ] {
            std::fs::create_dir_all(root.join(&name)).unwrap();
            std::fs::write(root.join(&name).join("main"), b"main-bytes").unwrap();
        }
        let snapshots = list_snapshots(temp.path());
        assert_eq!(snapshots.len(), 2, "-tmp and non-snapshot entries stay out");
        assert!(snapshots[0].name > snapshots[1].name, "newest first");
        assert_eq!(snapshots[0].main_bytes, 10);
        assert_eq!(latest_snapshot(temp.path()).unwrap(), snapshots[0]);
    }

    #[test]
    fn prepare_keeps_only_the_latest_snapshot_and_clears_collision_candidate() {
        let temp = tempfile::TempDir::new().unwrap();
        let root = temp.path().join(SNAPSHOT_ROOT);
        let older = format!("{SNAPSHOT_PREFIX}2026-01-01-00-00-00");
        let newest = format!("{SNAPSHOT_PREFIX}2026-01-02-00-00-00");
        let residue = format!("{SNAPSHOT_PREFIX}2026-01-03-00-00-00{SNAPSHOT_TMP_SUFFIX}");
        for name in [&older, &newest, &residue] {
            std::fs::create_dir_all(root.join(name)).unwrap();
        }
        prepare_snapshots_for_export(temp.path());
        assert!(root.join(&newest).exists(), "the latest snapshot is kept");
        assert!(!root.join(&older).exists(), "older snapshots are cleared");
        assert!(!root.join(&residue).exists(), "-tmp residue is cleared");

        // A newest snapshot named for the current second is the collision
        // candidate the engine would fail-closed on: cleared too.
        let now = current_snapshot_candidate_name().unwrap();
        std::fs::create_dir_all(root.join(&now)).unwrap();
        prepare_snapshots_for_export(temp.path());
        assert!(!root.join(&now).exists(), "same-second candidate cleared");
    }

    #[test]
    fn handle_table_is_sequential_bounded_and_idempotent() {
        let table = BackupFileHandleTable::new();
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("main");
        std::fs::write(&file, vec![0xAB_u8; 700_000]).unwrap();

        let handle = table.open("acct", file.clone(), 700_000).unwrap();
        // Offsets strictly sequential.
        assert!(matches!(
            table.read_chunk(&handle, 10),
            Err(BackupHandleError::NotSequential { expected: 0 })
        ));
        let first = table.read_chunk(&handle, 0).unwrap();
        assert_eq!(first.data.len(), CHUNK_BYTES as usize);
        assert!(!first.eof);
        let second = table.read_chunk(&handle, CHUNK_BYTES).unwrap();
        assert_eq!(second.data.len(), CHUNK_BYTES as usize);
        assert!(!second.eof);
        let third = table.read_chunk(&handle, 2 * CHUNK_BYTES).unwrap();
        assert_eq!(third.data.len(), 700_000 - 2 * CHUNK_BYTES as usize);
        assert!(third.eof, "the stream terminates at the file end");

        // Idempotent close: the second release reports false.
        assert!(table.close(&handle));
        assert!(!table.close(&handle));
        assert!(matches!(
            table.read_chunk(&handle, 700_000),
            Err(BackupHandleError::NotFound)
        ));

        // Per-account ceiling: the ninth open for one account is refused.
        let handles: Vec<String> = (0..MAX_HANDLES_PER_ACCOUNT)
            .map(|_| table.open("acct2", file.clone(), 1).unwrap())
            .collect();
        assert!(matches!(
            table.open("acct2", file.clone(), 1),
            Err(BackupHandleError::Exhausted)
        ));
        assert_eq!(table.clear_account("acct2"), handles.len());
        assert_eq!(table.open("acct2", file, 1).unwrap().len(), 32);
    }

    #[test]
    fn short_file_terminates_the_stream_instead_of_erroring() {
        let table = BackupFileHandleTable::new();
        let temp = tempfile::TempDir::new().unwrap();
        let file = temp.path().join("main");
        std::fs::write(&file, b"tiny").unwrap();
        let handle = table.open("acct", file.clone(), 4).unwrap();
        let chunk = table.read_chunk(&handle, 0).unwrap();
        assert_eq!(chunk.data, b"tiny");
        assert!(chunk.eof);
        // The file vanishing under the stream (connector cleanup) is a short
        // chunk + eof, never an error.
        std::fs::remove_file(&file).unwrap();
        table.close(&handle);
        let handle = table.open("acct", file, 4).unwrap();
        let chunk = table.read_chunk(&handle, 0).unwrap();
        assert!(chunk.data.is_empty());
        assert!(chunk.eof);
    }

    #[test]
    fn counters_serialize_camel_case_and_merge() {
        let counters = SkipCounters {
            skipped_reactions: 1,
            skipped_body_truncated: 2,
            skipped_skeleton_chats: 3,
            ..SkipCounters::default()
        };
        let mut result = serde_json::json!({"state": "idle"});
        counters.merge_into(&mut result);
        assert_eq!(result["skippedReactions"], 1);
        assert_eq!(result["skippedBodyTruncated"], 2);
        assert_eq!(result["skippedSkeletonChats"], 3);
        assert_eq!(result["state"], "idle");
    }
}
