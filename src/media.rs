// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded inbound media ingest (ADR 0002): the two connector-side machines
//! that make lifting `--ignore-attachments` survivable.
//!
//! [`MediaGovernor`] bounds the disk pressure signal-cli's eager downloads
//! create: a pass over one engine's `attachments/` directories deletes files
//! older than the TTL and, when the total exceeds the quota, the
//! oldest-`mtime` files until the directory is back inside it. Deletion is
//! ledger-free by design — signal-cli's `AttachmentStore` is a stateless file
//! store, so a deleted id simply makes the later fetch answer `UPSTREAM_ERROR`.
//!
//! [`MediaHandleTable`] bounds the delivery side: `open` mints a random
//! 128-bit handle bound to one resolved (account, conversation, message,
//! attachment) tuple, `readChunk` streams exactly one 256 KiB chunk at a
//! strictly sequential offset, and `closeHandle` releases early. Resident
//! memory is one chunk, never the file, and no filesystem path ever crosses
//! the host boundary.
//!
//! Trigger discipline for both (ADR 0002): one pass at process start plus one
//! per [`GOVERNOR_INBOUND_MESSAGE_INTERVAL`] inbound messages — a counter, no
//! resident timer thread. Filesystem work never holds the store lock; these
//! types take no store dependency at all.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant, SystemTime};

use crate::ids::random_id;

/// How long a downloaded attachment stays fetchable (ADR 0002): seven days,
/// mirroring Signal's own server-side retention expectations. Age is the
/// file's `mtime`, which signal-cli sets at download time.
pub const MEDIA_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Total attachment bytes one engine's data directory may retain (ADR 0002):
/// 2 GiB across all of the engine's accounts. Bounds *retention*, not the
/// instantaneous write — signal-cli downloads before the connector can veto,
/// and the blast radius of one oversized download is the server's own
/// 100 MiB per-attachment cap (ADR 0002).
pub const MEDIA_QUOTA_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Deletions one governor pass may perform (ADR 0002). A directory far over
/// quota or full of expired files walks back inside the budget over several
/// passes instead of stalling one.
pub const MAX_DELETIONS_PER_PASS: usize = 256;
/// Inbound messages that accumulate before the next governor pass (ADR 0002):
/// once per process start plus once per 200 inbound messages, whichever
/// first. A counter, never a resident timer thread.
pub const GOVERNOR_INBOUND_MESSAGE_INTERVAL: u64 = 200;
/// Raw bytes per `messages.attachments.readChunk` (ADR 0002). Base64 expands
/// one full chunk to `4 * ceil(262_144 / 3)` = 349_528 characters, far under
/// the 160 MiB host frame limit (`DEFAULT_HOST_FRAME_LIMIT`); resident memory
/// per request is one chunk, never the file.
pub const MEDIA_CHUNK_BYTES: u64 = 256 * 1024;
/// Handle lifetime (ADR 0002): 300 seconds. `closeHandle` releases early;
/// the TTL is the backstop so an abandoned renderer loop cannot pin memory
/// or an open-handle slot.
pub const MEDIA_HANDLE_TTL: Duration = Duration::from_secs(300);
/// Live handles one account may hold at once (ADR 0002). A leaking renderer
/// loop is refused at `open` instead of growing without bound.
pub const MAX_MEDIA_HANDLES_PER_ACCOUNT: usize = 8;

/// What one governor pass did. Counts and bytes only — never paths or ids
/// (AGENTS.md log discipline).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GovernorStats {
    pub deleted: usize,
    pub bytes_deleted: u64,
}

/// One engine's media retention machinery over its signal-cli data directory
/// (ADR 0002). State by construction: every pass recomputes reality from the
/// filesystem, so a crash mid-pass leaves nothing to clean up.
#[derive(Clone, Debug)]
pub struct MediaGovernor {
    data_dir: PathBuf,
}

impl MediaGovernor {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// The `attachments/` directories this engine may hold media in, probed
    /// per signal-cli's `PathConfig` layout (verified against the v0.14.8
    /// source): attachment files live under an `attachments/` directory next
    /// to the account settings. The single-account layout is
    /// `<data-dir>/attachments/`; with more than one account, signal-cli
    /// nests one directory per account under the data dir, so the probe
    /// falls back to scanning `<data-dir>` one level deep for subdirectories
    /// that contain their own `attachments/`. The eager `<data-dir>/attachments`
    /// probe always wins when it exists, matching the single-account install
    /// shape; the scan is sorted for deterministic pass order.
    pub fn attachments_dirs(&self) -> Vec<PathBuf> {
        let direct = self.data_dir.join("attachments");
        if direct.is_dir() {
            return vec![direct];
        }
        let mut found = Vec::new();
        let Ok(entries) = std::fs::read_dir(&self.data_dir) else {
            return found;
        };
        for entry in entries.flatten() {
            let candidate = entry.path().join("attachments");
            if entry.path().is_dir() && candidate.is_dir() {
                found.push(candidate);
            }
        }
        found.sort();
        found
    }

    /// Run one bounded pass (ADR 0002): orphaned/superseded `*.preview`
    /// files first, then the 7-day TTL, then the quota LRU — all sharing one
    /// [`MAX_DELETIONS_PER_PASS`] batch. Never called while holding the store
    /// lock; the caller runs it on its own task.
    pub fn run_pass(&self) -> GovernorStats {
        let mut stats = GovernorStats::default();
        let dirs = self.attachments_dirs();
        if dirs.is_empty() {
            return stats;
        }
        let mut files = Vec::new();
        for dir in &dirs {
            collect_regular_files(dir, &mut files);
        }
        let now = SystemTime::now();
        let mut budget = MAX_DELETIONS_PER_PASS;
        // Everything deleted so far this pass, so later phases neither
        // re-delete (NotFound would still report success) nor double-count.
        let mut deleted_paths: std::collections::HashSet<PathBuf> =
            std::collections::HashSet::new();

        // Preview cleanup first, so stale previews never count toward the
        // quota accounting below. A preview is stale when its main file is
        // gone (true orphan) or was re-downloaded after it (superseded).
        for (path, size, mtime) in &files {
            if budget == 0 {
                return stats;
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
                budget -= 1;
            }
        }

        // 7-day TTL.
        let mut remaining: Vec<&(PathBuf, u64, SystemTime)> = Vec::with_capacity(files.len());
        for entry in &files {
            let (path, size, mtime) = entry;
            if budget == 0 {
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
                    budget -= 1;
                }
                continue;
            }
            remaining.push(entry);
        }

        // Quota LRU: oldest mtime first until the survivor total is inside
        // the budget (ADR 0002 gate: ≤ quota after enough passes).
        let mut total: u64 = remaining.iter().map(|(_, size, _)| *size).sum();
        if total > MEDIA_QUOTA_BYTES {
            remaining.sort_by_key(|(_, _, mtime)| *mtime);
            for (path, size, _) in remaining {
                if budget == 0 || total <= MEDIA_QUOTA_BYTES {
                    break;
                }
                if delete_file(path) {
                    stats.deleted += 1;
                    stats.bytes_deleted += size;
                    total = total.saturating_sub(*size);
                    budget -= 1;
                }
            }
        }
        stats
    }
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
    /// `readChunk` offsets must be strictly sequential (ADR 0002): this
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
/// only the random handle crosses the wire (ADR 0002). The tuple fields are
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

/// Process-wide table of live chunk streams (ADR 0002). One instance serves
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
    /// exactly the bytes already delivered on this handle (ADR 0002: no
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

    /// Explicit early release (ADR 0002). Idempotent by design: a caller that
    /// closes after the TTL already reaped the handle still observes a
    /// released stream instead of a race-dependent error. Returns whether a
    /// live handle was released.
    pub fn close(&self, media_handle: &str) -> bool {
        let mut entries = self.entries.lock().expect("media handle table mutex");
        entries.remove(media_handle).is_some()
    }

    /// Drop every handle bound to one account. Runs when the account's local
    /// data is deleted (ADR 0002 gate 3: a handle cannot outlive its account
    /// session); the TTL backstops every other kind of abandonment. Returns
    /// how many handles were released.
    pub fn clear_account(&self, account_id: &str) -> usize {
        let mut entries = self.entries.lock().expect("media handle table mutex");
        let before = entries.len();
        entries.retain(|_, entry| entry.account_id != account_id);
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

    /// A data dir laid out like signal-cli's multi-account shape:
    /// `<data>/<account>/attachments/`.
    fn account_attachments_dir() -> (TempDir, PathBuf) {
        let temp = TempDir::new().unwrap();
        let attachments = temp.path().join("account-one").join("attachments");
        fs::create_dir_all(&attachments).unwrap();
        (temp, attachments)
    }

    #[test]
    fn attachments_probe_prefers_the_single_account_layout() {
        let temp = TempDir::new().unwrap();
        let direct = temp.path().join("attachments");
        fs::create_dir_all(&direct).unwrap();
        let governor = MediaGovernor::new(temp.path().to_path_buf());
        assert_eq!(governor.attachments_dirs(), vec![direct]);
    }

    #[test]
    fn attachments_probe_scans_one_level_for_account_subdirectories() {
        let (temp, attachments) = account_attachments_dir();
        // A second account directory joins the scan (sorted, deterministic);
        // a sibling without its own `attachments/` is ignored.
        let second = temp.path().join("account-two").join("attachments");
        fs::create_dir_all(&second).unwrap();
        fs::create_dir_all(temp.path().join("plain")).unwrap();
        let governor = MediaGovernor::new(temp.path().to_path_buf());
        assert_eq!(governor.attachments_dirs(), vec![attachments, second]);
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
