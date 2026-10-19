// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded inbound media ingest (ADR 0004): the two connector-side machines
//! that make lifting `--ignore-attachments` survivable.
//!
//! [`MediaGovernor`] bounds the disk pressure signal-cli's eager downloads
//! create. `[KT 适配]` (audit §9.10): the official desktop is a single-account
//! install, so its media directory has no account dimension. This connector
//! serves several accounts per engine/proxy group, so every byte it writes
//! lives under a per-account path
//! (`<data-dir>/media/<account-id>/attachments/`): quota and LRU are
//! account-local—one account's flood can never evict another account's
//! media—and a view-once burn or `deleteLocalData` only touches the addressed
//! account's subtree. Legacy signal-cli layouts stay readable (and TTL
//! governed) for upgrades.
//!
//! A pass deletes files older than the TTL and, when one account's total
//! exceeds [`MEDIA_QUOTA_BYTES`], that account's oldest-`mtime` files until it
//! is back inside its own quota. Deletion is ledger-free by design —
//! signal-cli's `AttachmentStore` is a stateless file store, so a deleted id
//! simply makes the later fetch answer `UPSTREAM_ERROR`.
//!
//! [`MediaHandleTable`] bounds the delivery side: `open` mints a random
//! 128-bit handle bound to one resolved (account, conversation, message,
//! attachment) tuple, `readChunk` streams exactly one 256 KiB chunk at a
//! strictly sequential offset, and `closeHandle` releases early. Resident
//! memory is one chunk, never the file, and no filesystem path ever crosses
//! the host boundary.
//!
//! Trigger discipline for both (ADR 0004): one pass at process start plus one
//! per [`GOVERNOR_INBOUND_MESSAGE_INTERVAL`] inbound messages — a counter, no
//! resident timer thread. Filesystem work never holds the store lock; these
//! types take no store dependency at all.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant, SystemTime};

use crate::ids::random_id;

/// How long a downloaded attachment stays fetchable (ADR 0004): seven days,
/// mirroring Signal's own server-side retention expectations. Age is the
/// file's `mtime`, which signal-cli sets at download time.
pub const MEDIA_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Total attachment bytes one account may retain (ADR 0004; `[KT 适配]`
/// audit §9.10): 2 GiB **per account**, not per engine. The official desktop
/// keeps one account per install, so its single quota is per-account by
/// construction; KT's many-accounts-per-engine shape needs the same unit.
/// A group-wide cap would have to evict across account boundaries once one
/// account filled it, which is exactly the cross-account eviction this fix
/// forbids, so the quota is enforced per bucket and nowhere else. Bounds
/// *retention*, not the instantaneous write — signal-cli downloads before the
/// connector can veto, and the blast radius of one oversized download is the
/// server's own 100 MiB per-attachment cap (ADR 0004).
pub const MEDIA_QUOTA_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Deletions one governor pass may perform (ADR 0004). A directory far over
/// quota or full of expired files walks back inside the budget over several
/// passes instead of stalling one.
pub const MAX_DELETIONS_PER_PASS: usize = 256;
/// Inbound messages that accumulate before the next governor pass (ADR 0004):
/// once per process start plus once per 200 inbound messages, whichever
/// first. A counter, never a resident timer thread.
pub const GOVERNOR_INBOUND_MESSAGE_INTERVAL: u64 = 200;
/// Raw bytes per `messages.attachments.readChunk` (ADR 0004). Base64 expands
/// one full chunk to `4 * ceil(262_144 / 3)` = 349_528 characters, far under
/// the 160 MiB host frame limit (`DEFAULT_HOST_FRAME_LIMIT`); resident memory
/// per request is one chunk, never the file.
pub const MEDIA_CHUNK_BYTES: u64 = 256 * 1024;
/// Handle lifetime (ADR 0004): 300 seconds. `closeHandle` releases early;
/// the TTL is the backstop so an abandoned renderer loop cannot pin memory
/// or an open-handle slot.
pub const MEDIA_HANDLE_TTL: Duration = Duration::from_secs(300);
/// Live handles one account may hold at once (ADR 0004). A leaking renderer
/// loop is refused at `open` instead of growing without bound.
pub const MAX_MEDIA_HANDLES_PER_ACCOUNT: usize = 8;

/// Root of the per-account media namespace (`[KT 适配]`, audit §9.10):
/// `<data-dir>/media/<account-id>/attachments/`. Deliberately its own root,
/// so the legacy signal-cli probes (`<data-dir>/attachments/` and
/// `<data-dir>/<account>/attachments/`) keep their meaning and stay readable
/// during upgrades.
const ACCOUNT_MEDIA_ROOT: &str = "media";
const ATTACHMENTS_DIR_NAME: &str = "attachments";
/// Longest account id accepted as a path component. Production ids are
/// 32-hex (`ids::random_id`); the bound keeps a hostile or corrupt id from
/// producing an overlong component.
const MAX_ACCOUNT_KEY_BYTES: usize = 64;

/// What one governor pass did. Counts and bytes only — never paths or ids
/// (AGENTS.md log discipline).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GovernorStats {
    pub deleted: usize,
    pub bytes_deleted: u64,
}

/// One proxy group's media retention machinery over its engine data
/// directory (ADR 0004). State by construction: every pass recomputes reality
/// from the filesystem, so a crash mid-pass leaves nothing to clean up.
#[derive(Clone, Debug)]
pub struct MediaGovernor {
    data_dir: PathBuf,
}

impl MediaGovernor {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// The group data directory the probes anchor at.
    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// `[KT 适配]` (audit §9.10): the account's own media directory,
    /// `<data-dir>/media/<account-id>/attachments/`. `None` means the id
    /// cannot be a path component (empty, overlong, a separator, `.` or
    /// `..`); production ids are 32-hex and always pass. Every new write is
    /// scoped here, which is what makes quota, LRU, burn, and account deletion
    /// account-local by construction.
    pub fn account_attachments_dir(&self, account_id: &str) -> Option<PathBuf> {
        let key = account_key(account_id)?;
        Some(
            self.data_dir
                .join(ACCOUNT_MEDIA_ROOT)
                .join(key)
                .join(ATTACHMENTS_DIR_NAME),
        )
    }

    /// Create (on the first media write) and return the account's media
    /// directory. The legacy probes are read/TTL compatibility only; this is
    /// the single write target.
    pub fn ensure_account_attachments_dir(&self, account_id: &str) -> std::io::Result<PathBuf> {
        let dir = self.account_attachments_dir(account_id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "media account key")
        })?;
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    /// The legacy `attachments/` directories an upgraded install may still
    /// hold (ADR 0004's original signal-cli `PathConfig` probe): the flat
    /// `<data-dir>/attachments/` (the pre-isolation connector layout) and
    /// every `<data-dir>/<account>/attachments/` (signal-cli's multi-account
    /// layout). These are **read-only compatibility surfaces**: new writes
    /// always land in [`Self::account_attachments_dir`], so a flat file
    /// cannot be attributed to one of this connector's accounts. The direct
    /// flat probe comes first, then the sorted scan—deterministic pass order.
    pub fn legacy_attachments_dirs(&self) -> Vec<PathBuf> {
        let mut found = Vec::new();
        let direct = self.data_dir.join(ATTACHMENTS_DIR_NAME);
        if direct.is_dir() {
            found.push(direct);
        }
        let Ok(entries) = std::fs::read_dir(&self.data_dir) else {
            return found;
        };
        let mut scanned = Vec::new();
        for entry in entries.flatten() {
            let candidate = entry.path().join(ATTACHMENTS_DIR_NAME);
            if entry.path().is_dir() && candidate.is_dir() {
                scanned.push(candidate);
            }
        }
        scanned.sort();
        found.extend(scanned);
        found
    }

    /// Run one bounded pass (ADR 0004): orphaned/superseded `*.preview`
    /// files first, then the 7-day TTL, then the per-account quota LRU — all
    /// sharing one [`MAX_DELETIONS_PER_PASS`] batch. Never called while
    /// holding the store lock; the caller runs it on its own task.
    ///
    /// `[KT 适配]` (audit §9.10): each account bucket is swept on its own.
    /// The LRU only ever sees the files of the bucket that is over its own
    /// quota, so one account's flood cannot select another account's bytes.
    /// The flat legacy pool is TTL-only for the same reason: its files have
    /// no provable owner, and an mtime-ordered sweep there could evict the
    /// wrong account's media.
    pub fn run_pass(&self) -> GovernorStats {
        let mut stats = GovernorStats::default();
        let mut budget = MAX_DELETIONS_PER_PASS;

        for dir in self.account_bucket_dirs() {
            sweep_dir(&dir, Some(MEDIA_QUOTA_BYTES), &mut budget, &mut stats);
            if budget == 0 {
                return stats;
            }
        }

        let flat = self.data_dir.join(ATTACHMENTS_DIR_NAME);
        let mut seen = std::collections::HashSet::new();
        for dir in self.legacy_attachments_dirs() {
            if !seen.insert(dir.clone()) {
                continue;
            }
            let quota = (dir != flat).then_some(MEDIA_QUOTA_BYTES);
            sweep_dir(&dir, quota, &mut budget, &mut stats);
            if budget == 0 {
                break;
            }
        }
        stats
    }

    /// The account buckets this governor owns, sorted for deterministic pass
    /// order: `<data-dir>/media/<account-id>/attachments/`.
    fn account_bucket_dirs(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(self.data_dir.join(ACCOUNT_MEDIA_ROOT)) else {
            return Vec::new();
        };
        let mut found: Vec<PathBuf> = entries
            .flatten()
            .filter(|entry| entry.path().is_dir())
            .map(|entry| entry.path().join(ATTACHMENTS_DIR_NAME))
            .filter(|dir| dir.is_dir())
            .collect();
        found.sort();
        found
    }

    /// Burn one view-once message's media (contract revision 1.38, §4.37;
    /// `[KT 适配]` audit §9.10): delete the attachment file and its preview
    /// from the addressed account's bucket only, so two accounts that happen
    /// to carry the same attachment id can never erase each other's bytes.
    /// Legacy pre-isolation files are deliberately not deleted here —
    /// ownership cannot be proven, and the TTL pass bounds them instead.
    /// Best-effort by design — a missing file is success (the same
    /// `NotFound`-is-success rule the retention passes follow), a real I/O
    /// failure logs by class and keeps the burn moving so the row projection
    /// and the host event never depend on the filesystem answering. Returns
    /// how many files were removed.
    pub fn delete_attachment(&self, account_id: &str, sanitized_id: &str) -> u64 {
        let Some(dir) = self.account_attachments_dir(account_id) else {
            return 0;
        };
        let mut removed = 0_u64;
        for suffix in ["", ".preview"] {
            if delete_file(&dir.join(format!("{sanitized_id}{suffix}"))) {
                removed += 1;
            }
        }
        removed
    }

    /// Delete one account's whole media subtree (the `deleteLocalData` hook,
    /// `[KT 适配]` audit §9.10). Account-scoped by construction: other
    /// accounts' buckets are never enumerated, let alone removed, and the
    /// same file name under another account stays untouched. Returns what was
    /// removed for the caller's count-only log.
    pub fn clear_account(&self, account_id: &str) -> GovernorStats {
        let Some(dir) = self.account_attachments_dir(account_id) else {
            return GovernorStats::default();
        };
        let mut stats = GovernorStats::default();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata()
                    && metadata.is_file()
                {
                    stats.deleted += 1;
                    stats.bytes_deleted += metadata.len();
                }
            }
        }
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return GovernorStats::default();
            }
            Err(error) => {
                tracing::warn!(
                    kind = %error.kind(),
                    "media governor could not clear an account directory"
                );
                return GovernorStats::default();
            }
        }
        // Drop the now-empty `<data-dir>/media/<account-id>` parent too;
        // failure means something else lives there and is left alone.
        if let Some(parent) = dir.parent() {
            let _ = std::fs::remove_dir(parent);
        }
        stats
    }
}

/// One bounded sweep of one media directory. `quota` is `Some` for a bucket
/// with a provable owner (an account's own directory, including a legacy
/// signal-cli `<account>/attachments/`) and `None` for the unattributable
/// flat legacy pool, where only the preview and TTL phases may run.
fn sweep_dir(dir: &Path, quota: Option<u64>, budget: &mut usize, stats: &mut GovernorStats) {
    let mut files = Vec::new();
    collect_regular_files(dir, &mut files);
    if files.is_empty() {
        return;
    }
    let now = SystemTime::now();
    // Everything deleted so far this sweep, so later phases neither
    // re-delete (NotFound would still report success) nor double-count.
    let mut deleted_paths: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();

    // Preview cleanup first, so stale previews never count toward the
    // quota accounting below. A preview is stale when its main file is
    // gone (true orphan) or was re-downloaded after it (superseded).
    for (path, size, mtime) in &files {
        if *budget == 0 {
            return;
        }
        let is_stale_preview = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .and_then(|name| name.strip_suffix(".preview"))
            .map(|stem| {
                let main = path.with_file_name(stem);
                match std::fs::symlink_metadata(&main) {
                    Err(_) => true,
                    Ok(main_meta) => main_meta
                        .modified()
                        .ok()
                        .is_some_and(|main_mtime| *mtime < main_mtime),
                }
            })
            .unwrap_or(false);
        if is_stale_preview && delete_file(path) {
            stats.deleted += 1;
            stats.bytes_deleted += size;
            deleted_paths.insert(path.clone());
            *budget -= 1;
        }
    }

    // 7-day TTL.
    let mut remaining: Vec<&(PathBuf, u64, SystemTime)> = Vec::with_capacity(files.len());
    for entry in &files {
        let (path, size, mtime) = entry;
        if *budget == 0 {
            break;
        }
        if deleted_paths.contains(path) {
            continue;
        }
        if now.duration_since(*mtime).is_ok_and(|age| age > MEDIA_TTL) {
            if delete_file(path) {
                stats.deleted += 1;
                stats.bytes_deleted += size;
                deleted_paths.insert(path.clone());
                *budget -= 1;
            }
            continue;
        }
        remaining.push(entry);
    }

    // Per-account quota LRU: oldest mtime first until the survivor total is
    // inside this bucket's own budget (ADR 0004 gate: ≤ quota after enough
    // passes). `remaining` holds only this directory's files, so the LRU can
    // never reach another account's media.
    let Some(quota) = quota else {
        return;
    };
    let mut total: u64 = remaining.iter().map(|(_, size, _)| *size).sum();
    if total > quota {
        remaining.sort_by_key(|(_, _, mtime)| *mtime);
        for (path, size, _) in remaining {
            if *budget == 0 || total <= quota {
                break;
            }
            if delete_file(path) {
                stats.deleted += 1;
                stats.bytes_deleted += size;
                total = total.saturating_sub(*size);
                *budget -= 1;
            }
        }
    }
}

/// `[KT 适配]`: one account id, one directory component. Account ids are
/// minted as 32-hex (`ids::random_id`), so this accepts the generated shape
/// and the fixture shapes and refuses anything that could escape the media
/// root or collide after sanitizing. Refusal fails the media face closed
/// (the stash reports a class, the download answers its IO face) instead of
/// inventing a shared fallback directory.
fn account_key(account_id: &str) -> Option<&str> {
    if account_id.is_empty()
        || account_id.len() > MAX_ACCOUNT_KEY_BYTES
        || account_id == "."
        || account_id == ".."
    {
        return None;
    }
    account_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        .then_some(account_id)
}

fn collect_regular_files(dir: &Path, out: &mut Vec<(PathBuf, u64, SystemTime)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        // A vanished directory (fresh engine, concurrent cleanup) simply
        // holds nothing to govern this pass.
        return;
    };
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        let Ok(mtime) = metadata.modified() else {
            continue;
        };
        out.push((entry.path(), metadata.len(), mtime));
    }
}

/// Best-effort deletion: `NotFound` races (the engine re-downloading under
/// the pass, a concurrent pass in a duplicated data dir) are success; real
/// failures are logged by class only and keep the pass moving.
fn delete_file(path: &Path) -> bool {
    match std::fs::remove_file(path) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => {
            tracing::warn!(kind = %error.kind(), "media governor could not delete a file");
            false
        }
    }
}

/// Why a handle operation failed. The service layer maps these onto the wire
/// error codes; nothing here carries ids or paths so the variants stay
/// log-safe.
#[derive(Debug, Eq, PartialEq)]
pub enum MediaHandleError {
    /// Unknown handle, an expired handle, or a handle released early: all
    /// three answer the caller identically (the wire code is INVALID_REQUEST),
    /// because the distinction would only help someone probing handle space.
    NotFound,
    /// `readChunk` offsets must be strictly sequential (ADR 0004): this
    /// offset is not exactly the next unread byte.
    NotSequential { expected: u64 },
    /// The `open` was refused because the account already holds the
    /// per-account live-handle ceiling.
    Exhausted,
    /// The chunk read failed (file deleted by a governor pass mid-stream,
    /// truncated, unreadable). Mapped to UPSTREAM_ERROR: indistinguishable
    /// from a not-downloaded attachment, per the existing media contract.
    Io,
}

/// Binding recorded at `open`: the resolved host addressing tuple plus the
/// local file the open resolved to. The path stays connector-side forever —
/// only the random handle crosses the wire (ADR 0004). The tuple fields are
/// the binding record of the stream (what `open` resolved, for tests and
/// future account-session enforcement); only `account_id` is read on the
/// release path, so the struct carries the dead-field allowance explicitly.
#[allow(dead_code)]
struct MediaHandleEntry {
    account_id: String,
    conversation_id: String,
    message_id: String,
    attachment_id: String,
    path: PathBuf,
    size_bytes: u64,
    /// Next byte offset `readChunk` will accept: strict sequencing state.
    bytes_read: u64,
    expires_at: Instant,
}

/// One open chunk stream: exactly what `open` returned to the host.
#[derive(Debug, Eq, PartialEq)]
pub struct MediaChunk {
    pub data: Vec<u8>,
    pub eof: bool,
}

/// Process-wide table of live chunk streams (ADR 0004). One instance serves
/// every proxy group: `readChunk`/`closeHandle` carry no accountId on the
/// wire, so the table — not the routing — is the single-origin authority.
/// Handles are unguessable random 128-bit tokens bound to one resolved
/// (account, conversation, message, attachment) tuple.
#[derive(Default)]
pub struct MediaHandleTable {
    entries: StdMutex<HashMap<String, MediaHandleEntry>>,
}

impl MediaHandleTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mint a handle for one resolved attachment. Refuses when the account
    /// already holds [`MAX_MEDIA_HANDLES_PER_ACCOUNT`] live handles; expired
    /// entries are swept lazily first so the ceiling applies to live handles
    /// only. `size_bytes` is the file size captured at open and caps every
    /// later chunk read.
    pub fn open(
        &self,
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        attachment_id: &str,
        path: PathBuf,
        size_bytes: u64,
    ) -> Result<String, MediaHandleError> {
        let mut entries = self.entries.lock().expect("media handle table mutex");
        Self::sweep_expired(&mut entries);
        let live = entries
            .values()
            .filter(|entry| entry.account_id == account_id)
            .count();
        if live >= MAX_MEDIA_HANDLES_PER_ACCOUNT {
            return Err(MediaHandleError::Exhausted);
        }
        // `random_id` is the repository's 128-bit random hex source; it names
        // nothing and resolves to nothing outside this table.
        let handle = random_id();
        entries.insert(
            handle.clone(),
            MediaHandleEntry {
                account_id: account_id.to_string(),
                conversation_id: conversation_id.to_string(),
                message_id: message_id.to_string(),
                attachment_id: attachment_id.to_string(),
                path,
                size_bytes,
                bytes_read: 0,
                expires_at: Instant::now() + MEDIA_HANDLE_TTL,
            },
        );
        Ok(handle)
    }

    /// Read the next sequential chunk of one handle's file. `offset` must be
    /// exactly the bytes already delivered on this handle (ADR 0004: no
    /// random-access scanning, so a hostile renderer cannot turn the handle
    /// into a seek oracle). The read is one bounded chunk; the file is never
    /// held open across calls.
    pub fn read_chunk(
        &self,
        media_handle: &str,
        offset: u64,
    ) -> Result<MediaChunk, MediaHandleError> {
        let mut entries = self.entries.lock().expect("media handle table mutex");
        Self::sweep_expired(&mut entries);
        let entry = entries
            .get_mut(media_handle)
            .ok_or(MediaHandleError::NotFound)?;
        if entry.bytes_read != offset {
            return Err(MediaHandleError::NotSequential {
                expected: entry.bytes_read,
            });
        }
        let want = MEDIA_CHUNK_BYTES.min(entry.size_bytes - entry.bytes_read);
        let data = read_exact_range(&entry.path, offset, want).map_err(|_| MediaHandleError::Io)?;
        let short = (data.len() as u64) < want;
        entry.bytes_read += data.len() as u64;
        // EOF when the declared size is reached, or early when the file
        // shrank underneath the stream (a governor pass raced the read):
        // reporting EOF there stops the caller from looping forever on a
        // file that will never grow back.
        let eof = short || entry.bytes_read >= entry.size_bytes;
        Ok(MediaChunk { data, eof })
    }

    /// Explicit early release (ADR 0004). Idempotent by design: a caller that
    /// closes after the TTL already reaped the handle still observes a
    /// released stream instead of a race-dependent error. Returns whether a
    /// live handle was released.
    pub fn close(&self, media_handle: &str) -> bool {
        let mut entries = self.entries.lock().expect("media handle table mutex");
        entries.remove(media_handle).is_some()
    }

    /// Drop every handle bound to one account. Runs when the account's local
    /// data is deleted (ADR 0004 gate 3: a handle cannot outlive its account
    /// session); the TTL backstops every other kind of abandonment. Returns
    /// how many handles were released.
    pub fn clear_account(&self, account_id: &str) -> usize {
        let mut entries = self.entries.lock().expect("media handle table mutex");
        let before = entries.len();
        entries.retain(|_, entry| entry.account_id != account_id);
        before - entries.len()
    }

    /// Drop every handle bound to one message (contract revision 1.38):
    /// a burned view-once message must not keep a readable stream open — the
    /// chunk reads would otherwise outlive the bytes the burn just erased.
    /// Returns how many handles were released.
    pub fn close_for_message(&self, account_id: &str, message_id: &str) -> usize {
        let mut entries = self.entries.lock().expect("media handle table mutex");
        let before = entries.len();
        entries.retain(|_, entry| entry.account_id != account_id || entry.message_id != message_id);
        before - entries.len()
    }

    /// Live handles for tests and capability introspection.
    #[cfg(test)]
    pub fn live_count(&self) -> usize {
        self.entries.lock().expect("media handle table mutex").len()
    }

    /// Lazy expiry: drop entries past their TTL so abandoned streams stop
    /// counting against the per-account ceiling. Called with the table lock
    /// already held.
    fn sweep_expired(entries: &mut HashMap<String, MediaHandleEntry>) {
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
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    /// Distinct, deterministic mtimes without a dev-dependency: `set_modified`
    /// backdates files precisely; `0` would make every entry tie in the LRU
    /// sort, so each file gets its own second.
    fn backdate(path: &Path, seconds_before_now: u64) {
        let mtime = SystemTime::now() - Duration::from_secs(seconds_before_now);
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(mtime)
            .unwrap();
    }

    fn write_file(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, bytes).unwrap();
        path
    }

    /// Quota accounting uses metadata sizes, so sparse `set_len` files keep
    /// a 2 GiB quota test off a 2 GiB write.
    fn set_sparse_size(path: &Path, size: u64) {
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(size)
            .unwrap();
    }

    /// Create and return one account's media bucket.
    fn account_bucket(governor: &MediaGovernor, account_id: &str) -> PathBuf {
        let dir = governor
            .account_attachments_dir(account_id)
            .expect("fixture account id");
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A data dir laid out like signal-cli's multi-account shape:
    /// `<data>/<account>/attachments/`.
    fn account_attachments_dir() -> (TempDir, PathBuf) {
        let temp = TempDir::new().unwrap();
        let attachments = temp.path().join("account-one").join("attachments");
        fs::create_dir_all(&attachments).unwrap();
        (temp, attachments)
    }

    #[test]
    fn legacy_probe_prefers_the_single_account_layout() {
        let temp = TempDir::new().unwrap();
        let direct = temp.path().join("attachments");
        fs::create_dir_all(&direct).unwrap();
        let governor = MediaGovernor::new(temp.path().to_path_buf());
        assert_eq!(governor.legacy_attachments_dirs(), vec![direct]);
    }

    #[test]
    fn legacy_probe_scans_one_level_for_account_subdirectories() {
        let (temp, attachments) = account_attachments_dir();
        // A second account directory joins the scan (sorted, deterministic);
        // a sibling without its own `attachments/` is ignored.
        let second = temp.path().join("account-two").join("attachments");
        fs::create_dir_all(&second).unwrap();
        fs::create_dir_all(temp.path().join("plain")).unwrap();
        let governor = MediaGovernor::new(temp.path().to_path_buf());
        assert_eq!(
            governor.legacy_attachments_dirs(),
            vec![attachments, second]
        );
    }

    #[test]
    fn ttl_deletes_expired_files_and_keeps_fresh_ones() {
        let (temp, attachments) = account_attachments_dir();
        let expired = write_file(&attachments, "expired.dat", b"old");
        let fresh = write_file(&attachments, "fresh.dat", b"new");
        backdate(&expired, 8 * 24 * 60 * 60);
        backdate(&fresh, 60);

        let stats = MediaGovernor::new(temp.path().to_path_buf()).run_pass();

        assert_eq!(stats.deleted, 1);
        assert!(!expired.exists());
        assert!(fresh.exists());
    }

    #[test]
    fn quota_overrun_deletes_oldest_mtime_first_until_inside_the_budget() {
        let (temp, attachments) = account_attachments_dir();
        // Quota accounting uses metadata sizes, so sparse set_len files keep
        // the test off a 2 GiB write.
        let oldest = write_file(&attachments, "oldest.dat", b"a");
        let middle = write_file(&attachments, "middle.dat", b"b");
        let newest = write_file(&attachments, "newest.dat", b"c");
        for (path, age, size) in [
            (&oldest, 300, MEDIA_QUOTA_BYTES / 2 + 1_000),
            (&middle, 200, MEDIA_QUOTA_BYTES / 2),
            (&newest, 100, MEDIA_QUOTA_BYTES / 2),
        ] {
            fs::File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_len(size)
                .unwrap();
            backdate(path, age);
        }

        let stats = MediaGovernor::new(temp.path().to_path_buf()).run_pass();

        // The total is quota + 1_000, so dropping only the oldest file lands
        // the survivors exactly on the quota; the LRU stops there.
        assert_eq!(stats.deleted, 1);
        assert!(!oldest.exists());
        assert!(middle.exists());
        assert!(newest.exists());
    }

    #[test]
    fn account_media_paths_are_scoped_and_refuse_unsafe_ids() {
        let temp = TempDir::new().unwrap();
        let governor = MediaGovernor::new(temp.path().to_path_buf());
        assert_eq!(
            governor.account_attachments_dir("acct-a"),
            Some(temp.path().join("media").join("acct-a").join("attachments"))
        );
        for unsafe_id in ["", ".", "..", "a/b", "a\\b", "a b", "../acct-b"] {
            assert_eq!(
                governor.account_attachments_dir(unsafe_id),
                None,
                "{unsafe_id:?} must not name a media directory"
            );
        }
        assert_eq!(governor.account_attachments_dir(&"a".repeat(65)), None);
    }

    /// Audit §9.10 rule: one account's flood never evicts another account's
    /// media. B's file is the oldest file in the whole data dir, so a
    /// group-global LRU would pick it first; the per-account sweep must leave
    /// it untouched and evict only from the over-quota account.
    #[test]
    fn a_flood_in_one_account_never_evicts_another_accounts_media() {
        let temp = TempDir::new().unwrap();
        let governor = MediaGovernor::new(temp.path().to_path_buf());
        let bucket_a = account_bucket(&governor, "acct-a");
        let bucket_b = account_bucket(&governor, "acct-b");

        let a_oldest = write_file(&bucket_a, "a-oldest.dat", b"a");
        let a_middle = write_file(&bucket_a, "a-middle.dat", b"b");
        let a_newest = write_file(&bucket_a, "a-newest.dat", b"c");
        for (path, age, size) in [
            (&a_oldest, 300, MEDIA_QUOTA_BYTES / 2 + 1_000),
            (&a_middle, 200, MEDIA_QUOTA_BYTES / 2),
            (&a_newest, 100, MEDIA_QUOTA_BYTES / 2),
        ] {
            set_sparse_size(path, size);
            backdate(path, age);
        }
        let b_file = write_file(&bucket_b, "b.dat", b"b");
        set_sparse_size(&b_file, MEDIA_QUOTA_BYTES / 2);
        backdate(&b_file, 1_000);

        let stats = governor.run_pass();

        assert_eq!(stats.deleted, 1);
        assert!(
            !a_oldest.exists(),
            "the over-quota account evicts its own oldest"
        );
        assert!(a_middle.exists());
        assert!(a_newest.exists());
        assert!(
            b_file.exists(),
            "an under-quota account's media is never selected, however old"
        );
    }

    /// The per-account quota is a boundary, not a trigger: exactly the quota
    /// survives a pass, and one byte more evicts only the oldest file of that
    /// account.
    #[test]
    fn per_account_quota_boundary_keeps_exactly_the_quota() {
        let temp = TempDir::new().unwrap();
        let governor = MediaGovernor::new(temp.path().to_path_buf());
        let bucket = account_bucket(&governor, "acct-a");
        let first = write_file(&bucket, "first.dat", b"a");
        let second = write_file(&bucket, "second.dat", b"b");
        for (path, age) in [(&first, 300), (&second, 200)] {
            set_sparse_size(path, MEDIA_QUOTA_BYTES / 2);
            backdate(path, age);
        }

        let at_quota = governor.run_pass();
        assert_eq!(at_quota.deleted, 0);
        assert!(first.exists());
        assert!(second.exists());

        let third = write_file(&bucket, "third.dat", b"c");
        set_sparse_size(&third, 1);
        backdate(&third, 100);
        let over = governor.run_pass();
        assert_eq!(over.deleted, 1);
        assert!(!first.exists());
        assert!(second.exists());
        assert!(third.exists());
    }

    /// The `deleteLocalData` hook removes one account's whole subtree and
    /// nothing else — the same file name under another account survives.
    #[test]
    fn clearing_one_account_removes_only_that_accounts_media() {
        let temp = TempDir::new().unwrap();
        let governor = MediaGovernor::new(temp.path().to_path_buf());
        let bucket_a = account_bucket(&governor, "acct-a");
        let bucket_b = account_bucket(&governor, "acct-b");
        write_file(&bucket_a, "same.dat", b"aaaa");
        write_file(&bucket_a, "other.dat", b"aa");
        let b_file = write_file(&bucket_b, "same.dat", b"bbbb");

        let stats = governor.clear_account("acct-a");

        assert_eq!(stats.deleted, 2);
        assert_eq!(stats.bytes_deleted, 6);
        assert!(!bucket_a.exists());
        assert!(b_file.exists());
        assert_eq!(fs::read(&b_file).unwrap(), b"bbbb");
    }

    /// The view-once burn is account-scoped: the same attachment id under a
    /// second account keeps its bytes and preview.
    #[test]
    fn burning_one_accounts_media_never_touches_another_accounts_same_id() {
        let temp = TempDir::new().unwrap();
        let governor = MediaGovernor::new(temp.path().to_path_buf());
        let bucket_a = account_bucket(&governor, "acct-a");
        let bucket_b = account_bucket(&governor, "acct-b");
        let a_file = write_file(&bucket_a, "same-id.dat", b"aaaa");
        let a_preview = write_file(&bucket_a, "same-id.dat.preview", b"p");
        let b_file = write_file(&bucket_b, "same-id.dat", b"bbbb");
        let b_preview = write_file(&bucket_b, "same-id.dat.preview", b"q");

        assert_eq!(governor.delete_attachment("acct-a", "same-id.dat"), 2);

        assert!(!a_file.exists());
        assert!(!a_preview.exists());
        assert!(b_file.exists());
        assert!(b_preview.exists());
        assert_eq!(governor.delete_attachment("../acct-b", "same-id.dat"), 0);
        assert!(b_file.exists());
    }

    /// The pre-isolation flat pool has no provable owner: the TTL still
    /// bounds it, but an over-quota file there is never selected by an
    /// mtime-ordered LRU (that selection is exactly what could evict another
    /// account's bytes).
    #[test]
    fn legacy_flat_pool_is_ttl_only_never_quota_evicted() {
        let temp = TempDir::new().unwrap();
        let governor = MediaGovernor::new(temp.path().to_path_buf());
        let flat = temp.path().join("attachments");
        fs::create_dir_all(&flat).unwrap();
        let huge = write_file(&flat, "huge.dat", b"h");
        set_sparse_size(&huge, MEDIA_QUOTA_BYTES * 2);
        backdate(&huge, 60);

        let fresh = governor.run_pass();
        assert_eq!(fresh.deleted, 0);
        assert!(huge.exists());

        backdate(&huge, 8 * 24 * 60 * 60);
        let expired = governor.run_pass();
        assert_eq!(expired.deleted, 1);
        assert!(!huge.exists());
    }

    #[test]
    fn a_pass_deletes_at_most_the_batch_ceiling() {
        let (temp, attachments) = account_attachments_dir();
        for index in 0..(MAX_DELETIONS_PER_PASS + 50) {
            let path = write_file(&attachments, &format!("bulk-{index}.dat"), b"x");
            backdate(&path, 8 * 24 * 60 * 60);
        }

        let first = MediaGovernor::new(temp.path().to_path_buf()).run_pass();

        assert_eq!(first.deleted, MAX_DELETIONS_PER_PASS);
        // The remainder waits for the next pass (next start or 200 inbound
        // messages) — bounded work per pass is the contract.
        let second = MediaGovernor::new(temp.path().to_path_buf()).run_pass();
        assert_eq!(second.deleted, 50);
    }

    #[test]
    fn preview_files_are_cleaned_when_orphaned_or_superseded() {
        let (temp, attachments) = account_attachments_dir();
        // Orphan: the main file is gone.
        let orphan = write_file(&attachments, "gone.dat.preview", b"p");
        backdate(&orphan, 10);
        // Superseded: the main file is newer than its preview.
        let main = write_file(&attachments, "fresh.dat", b"main");
        let stale_preview = write_file(&attachments, "fresh.dat.preview", b"p");
        backdate(&main, 10);
        backdate(&stale_preview, 60);
        // Current: the preview is newer than its main file and must survive.
        let older_main = write_file(&attachments, "current.dat", b"main");
        let current_preview = write_file(&attachments, "current.dat.preview", b"p");
        backdate(&older_main, 60);
        backdate(&current_preview, 10);

        let stats = MediaGovernor::new(temp.path().to_path_buf()).run_pass();

        assert_eq!(stats.deleted, 2);
        assert!(!orphan.exists());
        assert!(!stale_preview.exists());
        assert!(older_main.exists());
        assert!(current_preview.exists());
    }

    #[test]
    fn an_empty_or_missing_data_dir_is_a_clean_noop_pass() {
        let temp = TempDir::new().unwrap();
        let stats = MediaGovernor::new(temp.path().join("does-not-exist")).run_pass();
        assert_eq!(stats, GovernorStats::default());
    }

    fn table_with_file(size: u64) -> (MediaHandleTable, String, String, TempDir) {
        let temp = TempDir::new().unwrap();
        let path = write_file(temp.path(), "attachment.dat", &vec![7_u8; size as usize]);
        let table = MediaHandleTable::new();
        let handle = table
            .open("acct", "conv", "msg", "attachment.dat", path, size)
            .unwrap();
        (table, handle, "acct".to_string(), temp)
    }

    #[test]
    fn read_chunk_streams_sequentially_and_reports_eof() {
        // Two chunks plus a remainder: the first read fills a full chunk,
        // the second carries the tail and flags EOF.
        let tail = 100_u64;
        let size = MEDIA_CHUNK_BYTES + tail;
        let (table, handle, _account, _temp) = table_with_file(size);

        let first = table.read_chunk(&handle, 0).unwrap();
        assert_eq!(first.data.len() as u64, MEDIA_CHUNK_BYTES);
        assert!(!first.eof);
        // Skipping forward, rewinding, or any non-sequential offset refuses.
        assert_eq!(
            table.read_chunk(&handle, 0).unwrap_err(),
            MediaHandleError::NotSequential {
                expected: MEDIA_CHUNK_BYTES
            }
        );
        let second = table.read_chunk(&handle, MEDIA_CHUNK_BYTES).unwrap();
        assert_eq!(second.data.len() as u64, tail);
        assert!(second.eof);
        // Past EOF the handle stays positional: the next expected offset is
        // the size, and asking for it yields the terminal empty chunk.
        let terminal = table.read_chunk(&handle, size).unwrap();
        assert!(terminal.data.is_empty());
        assert!(terminal.eof);
    }

    #[test]
    fn a_file_smaller_than_one_chunk_reads_in_a_single_eof_chunk() {
        let (table, handle, _account, _temp) = table_with_file(1024);
        let chunk = table.read_chunk(&handle, 0).unwrap();
        assert_eq!(chunk.data.len(), 1024);
        assert!(chunk.eof);
    }

    #[test]
    fn read_chunk_short_reads_flag_eof_when_the_file_shrank() {
        let (table, handle, _account, temp) = table_with_file(1024);
        // Simulate a concurrent governor deletion: same path, truncated.
        let path = temp.path().join("attachment.dat");
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(16)
            .unwrap();
        let chunk = table.read_chunk(&handle, 0).unwrap();
        assert_eq!(chunk.data.len(), 16);
        assert!(chunk.eof, "a truncated file must terminate the stream");
    }

    #[test]
    fn unknown_and_closed_handles_are_unreadable() {
        let (table, handle, _account, _temp) = table_with_file(16);
        assert_eq!(
            table.read_chunk("never-minted", 0).unwrap_err(),
            MediaHandleError::NotFound
        );
        assert!(table.close(&handle));
        // Closing twice stays idempotent, and a closed handle reads as
        // unknown — never as an error that reveals its past state.
        assert!(!table.close(&handle));
        assert_eq!(
            table.read_chunk(&handle, 16).unwrap_err(),
            MediaHandleError::NotFound
        );
    }

    #[test]
    fn clear_account_drops_only_that_accounts_handles() {
        let temp = TempDir::new().unwrap();
        let path = write_file(temp.path(), "a.dat", b"x");
        let table = MediaHandleTable::new();
        let mine = table
            .open("acct-a", "conv", "msg", "a.dat", path.clone(), 1)
            .unwrap();
        let other = table
            .open("acct-b", "conv", "msg", "a.dat", path, 1)
            .unwrap();

        assert_eq!(table.clear_account("acct-a"), 1);

        assert_eq!(
            table.read_chunk(&mine, 0).unwrap_err(),
            MediaHandleError::NotFound
        );
        // The other account's stream is untouched: single-origin by account.
        assert!(table.read_chunk(&other, 0).is_ok());
    }

    #[test]
    fn expired_handles_are_swept_lazily_on_the_next_table_use() {
        let temp = TempDir::new().unwrap();
        let path = write_file(temp.path(), "a.dat", b"x");
        let table = MediaHandleTable::new();
        let handle = table.open("acct", "conv", "msg", "a.dat", path, 1).unwrap();
        assert_eq!(table.live_count(), 1);

        table
            .entries
            .lock()
            .unwrap()
            .get_mut(&handle)
            .unwrap()
            .expires_at = Instant::now() - Duration::from_secs(1);

        // Any operation lazily reaps the expired entry; the caller sees the
        // same NotFound an unknown handle produces.
        assert_eq!(
            table.read_chunk(&handle, 0).unwrap_err(),
            MediaHandleError::NotFound
        );
        assert_eq!(table.live_count(), 0);
    }

    #[test]
    fn the_eighth_open_per_account_is_the_ceiling() {
        let temp = TempDir::new().unwrap();
        let path = write_file(temp.path(), "a.dat", b"x");
        let table = MediaHandleTable::new();
        for _ in 0..MAX_MEDIA_HANDLES_PER_ACCOUNT {
            table
                .open("acct", "conv", "msg", "a.dat", path.clone(), 1)
                .unwrap();
        }
        assert_eq!(
            table
                .open("acct", "conv", "msg", "a.dat", path.clone(), 1)
                .unwrap_err(),
            MediaHandleError::Exhausted
        );
        // The ceiling is per account, never global.
        table
            .open("acct-b", "conv", "msg", "a.dat", path, 1)
            .unwrap();
    }

    #[test]
    fn released_ceiling_slots_are_reusable_after_close_or_expiry() {
        let temp = TempDir::new().unwrap();
        let path = write_file(temp.path(), "a.dat", b"x");
        let table = MediaHandleTable::new();
        let mut handles = Vec::new();
        for _ in 0..MAX_MEDIA_HANDLES_PER_ACCOUNT {
            handles.push(
                table
                    .open("acct", "conv", "msg", "a.dat", path.clone(), 1)
                    .unwrap(),
            );
        }
        assert!(table.close(&handles[0]));
        table
            .open("acct", "conv", "msg", "a.dat", path, 1)
            .expect("a closed slot must be reusable");
    }
}
