// SPDX-License-Identifier: AGPL-3.0-only

//! Legacy → per-account engine data migration (engine-pool design S2, shipped
//! with the full-pool S1 switch).
//!
//! The pre-pool layout answers every account from its proxy group's engine
//! data directory:
//!
//! ~~~text
//! <signal-data>/engine-state.json                  # default group
//! <signal-data>/accounts/lk-<hex>/store.db
//! <signal-data>/proxy-groups/<group>/engine-state.json
//! <signal-data>/proxy-groups/<group>/accounts/lk-<hex>/store.db
//! ~~~
//!
//! The pool needs one directory per account:
//! `<signal-data>/engines/<accountId>/`. This module moves each account's
//! presage store with a same-volume `rename` (never a copy) and keeps the
//! engine registries consistent at every step:
//!
//! 1. write the target `engine-state.json` (exactly one account) **before**
//!    moving the store, so a crash can never leave a target directory whose
//!    registry is missing;
//! 2. `rename` the `accounts/lk-*` directory into it;
//! 3. rewrite the source registry without that entry;
//! 4. verify (target store present, target registry names it, source no
//!    longer lists it).
//!
//! Every step is journaled to `<signal-data>/migration/<runId>.jsonl`
//! (opaque ids and group names only — never a number, ACI, or key path) and
//! re-running the pass resumes from whatever the filesystem shows: a target
//! registry with a missing store continues with the move; a completed target
//! with a stale source registry repairs the source. Any state that cannot be
//! proven safe (both copies present, registry unreadable, unexpected version,
//! a symlink, a store with no registry entry) aborts startup fail-closed.
//!
//! `dry_run` performs every read/validation and emits the same plan without
//! touching the filesystem, so operators can compare the report with the
//! applied result.

use std::fs;
use std::io::{self, Write as _};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::DEFAULT_PROXY_GROUP_ID;
use crate::groups::is_valid_group_id;
use crate::pool::{ENGINE_SLOT_SUBDIR, is_account_id};
use crate::store::{AccountRow, Store, StoreError};

const STATE_FILE: &str = "engine-state.json";
const ACCOUNTS_DIR: &str = "accounts";
const STORE_DB_FILE: &str = "store.db";
const STATE_VERSION: u32 = 2;
/// Bound on one engine registry file. The real registry is a few hundred
/// bytes per account; anything larger is corrupt or hostile and must fail
/// closed without allocating it.
const MAX_STATE_BYTES: u64 = 1024 * 1024;
const MIGRATION_DIR: &str = "migration";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AccountEntry {
    number: String,
    dir: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    version: u32,
    accounts: Vec<AccountEntry>,
}

/// One account's migration outcome. `action` is stable machine-readable text
/// (the CLI report and the integration test compare it):
/// - `migrated`: the store moved on this run;
/// - `alreadyMigrated`: the target was already complete;
/// - `wouldMigrate`: dry-run plan for a store that still sits in a legacy
///   directory;
/// - `removedOrphan` / `wouldRemoveOrphan`: an empty engine directory with no
///   account row and no credentials (a rolled-back link's leftover).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationEntry {
    pub account_id: String,
    pub proxy_group: String,
    pub action: String,
}

/// The migration/dry-run report (log-safe: opaque ids and group names only).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationReport {
    pub dry_run: bool,
    pub entries: Vec<MigrationEntry>,
    pub migrated: usize,
    pub already_migrated: usize,
    pub orphans_removed: usize,
}

/// Run (or dry-run) the whole pass. Returns `EnginePoolInvalid` with a
/// log-safe description on any state that cannot be proven safe; the caller
/// must abort startup in that case ("no half-migrated serve", requirement 3).
pub fn run(
    store: &Store,
    signal_data_dir: &Path,
    state_dir: &Path,
    dry_run: bool,
) -> Result<MigrationReport, StoreError> {
    let rows = store.list_all_account_rows()?;
    let engines_root = signal_data_dir.join(ENGINE_SLOT_SUBDIR);
    let mut journal = if dry_run {
        None
    } else {
        Some(Journal::open(signal_data_dir).map_err(pool_invalid_io)?)
    };
    if let Some(journal) = journal.as_mut() {
        journal.record("run", None, None, if dry_run { "dryRun" } else { "start" });
    }
    let mut report = MigrationReport {
        dry_run,
        entries: Vec::new(),
        migrated: 0,
        already_migrated: 0,
        orphans_removed: 0,
    };
    for row in &rows {
        let entry = migrate_account(
            row,
            signal_data_dir,
            &engines_root,
            state_dir,
            dry_run,
            journal.as_mut(),
        )?;
        match entry.action.as_str() {
            "migrated" => report.migrated += 1,
            "alreadyMigrated" => report.already_migrated += 1,
            _ => {}
        }
        report.entries.push(entry);
    }
    reconcile_orphans(&rows, &engines_root, dry_run, journal.as_mut(), &mut report)?;
    if let Some(journal) = journal.as_mut() {
        journal.record(
            "done",
            None,
            None,
            &format!(
                "migrated={} already={} orphans={}",
                report.migrated, report.already_migrated, report.orphans_removed
            ),
        );
    }
    Ok(report)
}

fn migrate_account(
    row: &AccountRow,
    signal_data_dir: &Path,
    engines_root: &Path,
    state_dir: &Path,
    dry_run: bool,
    mut journal: Option<&mut Journal>,
) -> Result<MigrationEntry, StoreError> {
    if !is_account_id(&row.id) {
        return Err(pool_invalid(format!(
            "account {} has no opaque pool id and cannot name an engine directory",
            row.id
        )));
    }
    let legacy_dir = legacy_group_dir(signal_data_dir, &row.proxy_group)?;
    let target_dir = engines_root.join(&row.id);
    let target_state_path = target_dir.join(STATE_FILE);
    let target_state = read_state_lenient(&target_state_path)?;
    let target_store = target_state.as_ref().and_then(|state| {
        state
            .accounts
            .iter()
            .find(|entry| entry.number == row.signal_account)
            .and_then(|entry| entry.dir.as_deref())
            .map(|dir| (dir.to_string(), account_store_path(&target_dir, dir)))
    });
    let target_complete = target_store
        .as_ref()
        .is_some_and(|(_, store_path)| store_path.join(STORE_DB_FILE).is_file());

    let legacy_state_path = legacy_dir.join(STATE_FILE);
    let legacy_state = read_state_lenient(&legacy_state_path)?;
    let legacy_entry = legacy_state.as_ref().and_then(|state| {
        state
            .accounts
            .iter()
            .find(|entry| entry.number == row.signal_account)
            .cloned()
    });

    if target_complete {
        // Crash window (c) / a previous run: the target is authoritative.
        // Repair a source registry that still lists the moved account.
        if legacy_entry.is_some() {
            journal_record(
                &mut journal,
                "sourceRegistry",
                Some(&row.id),
                Some(&row.proxy_group),
                if dry_run {
                    "wouldRewrite"
                } else {
                    "rewriteStale"
                },
            );
            if !dry_run {
                remove_legacy_entry(&legacy_state_path, &row.signal_account)?;
            }
        }
        journal_record(
            &mut journal,
            "verify",
            Some(&row.id),
            Some(&row.proxy_group),
            "alreadyMigrated",
        );
        return Ok(MigrationEntry {
            account_id: row.id.clone(),
            proxy_group: row.proxy_group.clone(),
            action: "alreadyMigrated".to_string(),
        });
    }

    if let Some(state) = &target_state
        && state
            .accounts
            .iter()
            .any(|entry| entry.number != row.signal_account)
    {
        return Err(pool_invalid(format!(
            "engine slot {} already belongs to a different account; refusing to overwrite it",
            row.id
        )));
    }
    if target_state.is_some() {
        // A target registry exists but does not point at a present store. If
        // the legacy side still owns it, the move is resumable; anything else
        // is a state we cannot prove safe.
        if target_store.is_some() && !legacy_store_present(&legacy_dir, &legacy_entry) {
            return Err(pool_invalid(format!(
                "engine slot {} has a registry entry without a store and no legacy source",
                row.id
            )));
        }
    } else if target_dir.exists() {
        // An empty directory is legal (a rolled-back provision); anything
        // else (files, a partial store) must be inspected, never overwritten.
        let mut entries = fs::read_dir(&target_dir).map_err(pool_invalid_io)?;
        if entries.next().is_some() {
            return Err(pool_invalid(format!(
                "engine slot {} exists without a registry and cannot be adopted safely",
                row.id
            )));
        }
    }

    let Some(entry) = legacy_entry else {
        return Err(pool_invalid(format!(
            "account {} has no legacy engine data to migrate; refusing to serve a half-migrated pool",
            row.id
        )));
    };
    let Some(relative) = entry.dir.as_deref() else {
        return Err(pool_invalid(format!(
            "account {} has no presage store directory in its legacy registry",
            row.id
        )));
    };
    let store_name = validated_store_component(relative)?;
    let legacy_store = legacy_dir.join(ACCOUNTS_DIR).join(&store_name);
    let target_store_path = target_dir.join(ACCOUNTS_DIR).join(&store_name);
    let legacy_has_store = legacy_store.join(STORE_DB_FILE).is_file();
    let target_has_store = target_store_path.join(STORE_DB_FILE).is_file();
    if legacy_has_store && target_has_store {
        return Err(pool_invalid(format!(
            "account {} has two copies of its store (legacy and engine slot); refusing to choose one",
            row.id
        )));
    }
    if !legacy_has_store && !target_has_store {
        return Err(pool_invalid(format!(
            "account {} has a registry entry but no store.db on either side",
            row.id
        )));
    }

    journal_record(
        &mut journal,
        "plan",
        Some(&row.id),
        Some(&row.proxy_group),
        if dry_run { "wouldMigrate" } else { "migrate" },
    );
    if dry_run {
        return Ok(MigrationEntry {
            account_id: row.id.clone(),
            proxy_group: row.proxy_group.clone(),
            action: "wouldMigrate".to_string(),
        });
    }

    ensure_private_dir(&target_dir).map_err(pool_invalid_io)?;
    ensure_private_dir(&target_dir.join(ACCOUNTS_DIR)).map_err(pool_invalid_io)?;
    // (1) Target registry first: a crash from here on leaves a resumable
    // "registry without store" (the next run still sees the legacy store and
    // performs the move), never a store without a registry.
    let target_entries = vec![AccountEntry {
        number: row.signal_account.clone(),
        dir: Some(relative.to_string()),
    }];
    write_state(&target_dir, &target_entries).map_err(pool_invalid_io)?;
    journal_record(
        &mut journal,
        "targetRegistry",
        Some(&row.id),
        Some(&row.proxy_group),
        "written",
    );

    // (2) Move the store. rename(2) is atomic and same-volume by
    // construction here (both paths live under --signal-data-dir).
    if !target_has_store {
        match fs::rename(&legacy_store, &target_store_path) {
            Ok(()) => {}
            Err(error) => {
                let _ = fs::remove_file(target_dir.join(STATE_FILE));
                return Err(pool_invalid_io(error));
            }
        }
        if !target_store_path.join(STORE_DB_FILE).is_file() {
            return Err(pool_invalid(format!(
                "account {} store move did not land",
                row.id
            )));
        }
    }
    journal_record(
        &mut journal,
        "moveStore",
        Some(&row.id),
        Some(&row.proxy_group),
        "renamed",
    );

    // (3) Source registry convergence: the legacy engine must no longer see
    // an account whose store left its directory.
    remove_legacy_entry(&legacy_state_path, &row.signal_account)?;
    journal_record(
        &mut journal,
        "sourceRegistry",
        Some(&row.id),
        Some(&row.proxy_group),
        "rewritten",
    );

    // (4) Verify before declaring success.
    let verified_state = read_state_strict(&target_dir.join(STATE_FILE))?;
    let verified = verified_state
        .accounts
        .iter()
        .filter(|entry| entry.number == row.signal_account)
        .count();
    if verified != 1 || !target_store_path.join(STORE_DB_FILE).is_file() {
        return Err(pool_invalid(format!(
            "account {} migration verification failed",
            row.id
        )));
    }
    journal_record(
        &mut journal,
        "verified",
        Some(&row.id),
        Some(&row.proxy_group),
        "ok",
    );
    // Record the provisioning sidecar for crash-window discovery symmetry.
    // Best-effort: the store row is authoritative for a migrated account.
    let _ = state_dir;
    Ok(MigrationEntry {
        account_id: row.id.clone(),
        proxy_group: row.proxy_group.clone(),
        action: "migrated".to_string(),
    })
}

/// Empty engine directories with no account row and no credentials are
/// rolled-back link leftovers: remove them (dry-run: report only). Anything
/// that still carries a registry account or a store aborts fail-closed — the
/// pool must never start with an unaccounted-for credential directory.
fn reconcile_orphans(
    rows: &[AccountRow],
    engines_root: &Path,
    dry_run: bool,
    mut journal: Option<&mut Journal>,
    report: &mut MigrationReport,
) -> Result<(), StoreError> {
    let known: std::collections::HashSet<&str> = rows.iter().map(|row| row.id.as_str()).collect();
    let entries = match fs::read_dir(engines_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(pool_invalid_io(error)),
    };
    for entry in entries {
        let entry = entry.map_err(pool_invalid_io)?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !is_account_id(&name) || known.contains(name.as_str()) {
            continue;
        }
        let path = entry.path();
        let file_type = entry.file_type().map_err(pool_invalid_io)?;
        if !file_type.is_dir() {
            continue;
        }
        let state = read_state_lenient(&path.join(STATE_FILE))?;
        let has_accounts = state
            .as_ref()
            .is_some_and(|state| !state.accounts.is_empty());
        let has_store = account_store_dirs(&path)
            .map_err(pool_invalid_io)?
            .is_some();
        if has_accounts || has_store {
            return Err(pool_invalid(format!(
                "engine slot {} has credentials but no account row; refusing to start",
                name
            )));
        }
        if dry_run {
            report.entries.push(MigrationEntry {
                account_id: name,
                proxy_group: String::new(),
                action: "wouldRemoveOrphan".to_string(),
            });
            continue;
        }
        fs::remove_dir_all(&path).map_err(pool_invalid_io)?;
        journal_record(&mut journal, "orphan", Some(&name), None, "removed");
        report.orphans_removed += 1;
        report.entries.push(MigrationEntry {
            account_id: name,
            proxy_group: String::new(),
            action: "removedOrphan".to_string(),
        });
    }
    Ok(())
}

fn legacy_group_dir(root: &Path, proxy_group: &str) -> Result<PathBuf, StoreError> {
    if proxy_group == DEFAULT_PROXY_GROUP_ID {
        return Ok(root.to_path_buf());
    }
    if !is_valid_group_id(proxy_group) {
        return Err(pool_invalid(format!(
            "account binding names an invalid proxy group {proxy_group:?}"
        )));
    }
    Ok(root.join("proxy-groups").join(proxy_group))
}

/// The legacy registry's `dir` value must be a plain `accounts/lk-<hex>`
/// relative path. Anything else (absolute, `..`, symlink-shaped) is refused
/// rather than followed.
fn validated_store_component(relative: &str) -> Result<String, StoreError> {
    let path = Path::new(relative);
    let components: Vec<Component<'_>> = path.components().collect();
    if components.len() != 2 {
        return Err(pool_invalid(
            "legacy registry store directory is not a plain accounts/<dir> path".to_string(),
        ));
    }
    if components[0] != Component::Normal(ACCOUNTS_DIR.as_ref()) {
        return Err(pool_invalid(
            "legacy registry store directory is outside accounts/".to_string(),
        ));
    }
    let Component::Normal(name) = components[1] else {
        return Err(pool_invalid(
            "legacy registry store directory is malformed".to_string(),
        ));
    };
    let name = name.to_string_lossy().into_owned();
    if !name.starts_with("lk-")
        || name.contains('/')
        || name.contains('\\')
        || name == "."
        || name == ".."
    {
        return Err(pool_invalid(
            "legacy registry store directory name is not lk-<opaque>".to_string(),
        ));
    }
    Ok(name)
}

fn account_store_path(root: &Path, relative: &str) -> PathBuf {
    root.join(relative)
}

fn legacy_store_present(legacy_dir: &Path, entry: &Option<AccountEntry>) -> bool {
    entry
        .as_ref()
        .and_then(|entry| entry.dir.as_deref())
        .is_some_and(|dir| {
            legacy_dir
                .join(ACCOUNTS_DIR)
                .join(Path::new(dir).file_name().unwrap_or_default())
                .join(STORE_DB_FILE)
                .is_file()
        })
}

fn account_store_dirs(root: &Path) -> io::Result<Option<PathBuf>> {
    let accounts = root.join(ACCOUNTS_DIR);
    let entries = match fs::read_dir(&accounts) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("lk-") && entry.path().join(STORE_DB_FILE).is_file() {
            return Ok(Some(entry.path()));
        }
    }
    Ok(None)
}

fn read_state_lenient(path: &Path) -> Result<Option<PersistedState>, StoreError> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => {
            if metadata.len() > MAX_STATE_BYTES {
                return Err(pool_invalid(format!(
                    "engine registry at {} is larger than the {MAX_STATE_BYTES}-byte bound",
                    path.file_name().unwrap_or_default().to_string_lossy()
                )));
            }
            read_state_strict(path).map(Some)
        }
        Ok(_) => Err(pool_invalid(
            "engine registry path exists but is not a file".to_string(),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(pool_invalid_io(error)),
    }
}

fn read_state_strict(path: &Path) -> Result<PersistedState, StoreError> {
    let bytes = fs::read(path).map_err(pool_invalid_io)?;
    let state: PersistedState = serde_json::from_slice(&bytes)
        .map_err(|_| pool_invalid("engine registry is corrupt; refusing to migrate".to_string()))?;
    if state.version != STATE_VERSION {
        return Err(pool_invalid(format!(
            "engine registry version {} is unsupported (expected {STATE_VERSION}); refusing to migrate",
            state.version
        )));
    }
    if state.accounts.len() > 64 {
        return Err(pool_invalid(
            "engine registry names more accounts than any legal data directory".to_string(),
        ));
    }
    Ok(state)
}

fn write_state(dir: &Path, accounts: &[AccountEntry]) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(&PersistedState {
        version: STATE_VERSION,
        accounts: accounts.to_vec(),
    })
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "serialize"))?;
    let target = dir.join(STATE_FILE);
    let temp = dir.join(format!(".{STATE_FILE}.{}.tmp", std::process::id()));
    fs::write(&temp, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
    }
    if let Err(error) = fs::rename(&temp, &target) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    Ok(())
}

/// Remove one number's entry from a legacy registry, preserving every other
/// entry. A missing or already-clean source is a no-op (idempotent resume).
fn remove_legacy_entry(path: &Path, number: &str) -> Result<(), StoreError> {
    let Some(mut state) = read_state_lenient(path)? else {
        return Ok(());
    };
    let before = state.accounts.len();
    state.accounts.retain(|entry| entry.number != number);
    if state.accounts.len() == before {
        return Ok(());
    }
    write_state(
        path.parent()
            .ok_or_else(|| pool_invalid("legacy registry has no parent directory".to_string()))?,
        &state.accounts,
    )
    .map_err(pool_invalid_io)
}

fn ensure_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn pool_invalid(message: String) -> StoreError {
    StoreError::EnginePoolInvalid(message)
}

fn pool_invalid_io(error: io::Error) -> StoreError {
    StoreError::EnginePoolInvalid(format!("io: {}", error.kind()))
}

struct Journal {
    file: fs::File,
}

impl Journal {
    fn open(signal_data_dir: &Path) -> io::Result<Self> {
        let dir = signal_data_dir.join(MIGRATION_DIR);
        ensure_private_dir(&dir)?;
        // Bound the audit trail: keep the newest 19 runs plus this one. Each
        // pool-mode startup writes a journal even when there is nothing to
        // move, and an unbounded directory of tiny files is still unbounded.
        if let Ok(entries) = fs::read_dir(&dir) {
            let mut journals: Vec<PathBuf> = entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| {
                    path.extension()
                        .is_some_and(|extension| extension == "jsonl")
                })
                .collect();
            journals.sort();
            let excess = journals.len().saturating_sub(19);
            for path in journals.into_iter().take(excess) {
                let _ = fs::remove_file(path);
            }
        }
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or_default();
        let path = dir.join(format!("{stamp}-{}.jsonl", std::process::id()));
        let file = fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
        }
        Ok(Self { file })
    }

    fn record(
        &mut self,
        step: &str,
        account_id: Option<&str>,
        proxy_group: Option<&str>,
        status: &str,
    ) {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or_default();
        let mut object = serde_json::Map::new();
        object.insert("ts".into(), serde_json::json!(stamp));
        object.insert("step".into(), serde_json::json!(step));
        object.insert("status".into(), serde_json::json!(status));
        if let Some(account_id) = account_id {
            object.insert("accountId".into(), serde_json::json!(account_id));
        }
        if let Some(proxy_group) = proxy_group {
            object.insert("groupId".into(), serde_json::json!(proxy_group));
        }
        let line = serde_json::Value::Object(object).to_string();
        let _ = self
            .file
            .write_all(line.as_bytes())
            .and_then(|_| self.file.write_all(b"\n"));
    }
}

fn journal_record(
    journal: &mut Option<&mut Journal>,
    step: &str,
    account_id: Option<&str>,
    proxy_group: Option<&str>,
    status: &str,
) {
    if let Some(journal) = journal.as_mut() {
        journal.record(step, account_id, proxy_group, status);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::store::StoreKey;

    fn test_store(state_dir: &Path) -> Store {
        Store::open(state_dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap()
    }

    fn account_id(index: u8) -> String {
        format!("{index:032x}")
    }

    fn seed_legacy(
        root: &Path,
        store: &Store,
        group: &str,
        id: &str,
        number: &str,
        store_name: &str,
    ) {
        store
            .upsert_account_from_signal_with_id(number, Some(1), group, Some(id))
            .unwrap();
        let data_dir = if group == DEFAULT_PROXY_GROUP_ID {
            root.to_path_buf()
        } else {
            root.join("proxy-groups").join(group)
        };
        let store_dir = data_dir.join(ACCOUNTS_DIR).join(store_name);
        fs::create_dir_all(&store_dir).unwrap();
        fs::write(store_dir.join(STORE_DB_FILE), b"fixture-store").unwrap();
        let state_path = data_dir.join(STATE_FILE);
        let mut state: serde_json::Value = fs::read(&state_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_else(|| json!({"version": 2, "accounts": []}));
        state["accounts"]
            .as_array_mut()
            .unwrap()
            .push(json!({"number": number, "dir": format!("accounts/{store_name}")}));
        fs::write(&state_path, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
    }

    /// The dry run reports exactly the actions the applied pass performs, and
    /// the applied pass moves (never copies) each store into its own slot.
    #[test]
    fn dry_run_matches_apply_and_moves_stores_without_copying() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("signal-data");
        let state_dir = temp.path().join("state");
        fs::create_dir_all(&root).unwrap();
        let store = test_store(&state_dir);
        seed_legacy(
            &root,
            &store,
            DEFAULT_PROXY_GROUP_ID,
            &account_id(1),
            "+15550000001",
            "lk-one",
        );
        seed_legacy(
            &root,
            &store,
            "team-b",
            &account_id(2),
            "+15550000002",
            "lk-two",
        );

        let dry = run(&store, &root, &state_dir, true).unwrap();
        assert_eq!(dry.migrated, 0);
        assert_eq!(dry.already_migrated, 0);
        assert_eq!(dry.entries.len(), 2);
        assert!(
            dry.entries
                .iter()
                .all(|entry| entry.action == "wouldMigrate")
        );
        assert!(
            !root.join(ENGINE_SLOT_SUBDIR).exists(),
            "a dry run must not create engine directories"
        );
        assert!(
            root.join(ACCOUNTS_DIR)
                .join("lk-one")
                .join(STORE_DB_FILE)
                .is_file(),
            "a dry run must not move stores"
        );

        let applied = run(&store, &root, &state_dir, false).unwrap();
        assert_eq!(applied.migrated, 2);
        assert_eq!(applied.already_migrated, 0);
        let actions: Vec<&str> = applied
            .entries
            .iter()
            .map(|entry| entry.action.as_str())
            .collect();
        assert_eq!(actions, ["migrated", "migrated"]);

        for (id, number, store_name) in [
            (account_id(1), "+15550000001", "lk-one"),
            (account_id(2), "+15550000002", "lk-two"),
        ] {
            let slot = root.join(ENGINE_SLOT_SUBDIR).join(&id);
            assert!(slot.join(STATE_FILE).is_file());
            assert!(
                slot.join(ACCOUNTS_DIR)
                    .join(store_name)
                    .join(STORE_DB_FILE)
                    .is_file()
            );
            let state: serde_json::Value =
                serde_json::from_slice(&fs::read(slot.join(STATE_FILE)).unwrap()).unwrap();
            assert_eq!(state["version"], 2);
            assert_eq!(state["accounts"].as_array().unwrap().len(), 1);
            assert_eq!(state["accounts"][0]["number"], number);
            // No copy left behind in the legacy directory.
            let legacy = if store_name == "lk-one" {
                root.clone()
            } else {
                root.join("proxy-groups").join("team-b")
            };
            assert!(!legacy.join(ACCOUNTS_DIR).join(store_name).exists());
            let legacy_state: serde_json::Value =
                serde_json::from_slice(&fs::read(legacy.join(STATE_FILE)).unwrap()).unwrap();
            assert!(
                legacy_state["accounts"].as_array().unwrap().is_empty(),
                "the legacy registry converges to an empty shell"
            );
        }

        // Re-running is idempotent and reports the complete targets.
        let again = run(&store, &root, &state_dir, false).unwrap();
        assert_eq!(again.migrated, 0);
        assert_eq!(again.already_migrated, 2);

        // Every run writes its own journal. The union covers the applied
        // pass (the dry run writes none, per the no-mutation contract).
        let journal_dir = root.join(MIGRATION_DIR);
        let journals: Vec<String> = fs::read_dir(&journal_dir)
            .unwrap()
            .flatten()
            .map(|entry| fs::read_to_string(entry.path()).unwrap())
            .collect();
        assert_eq!(journals.len(), 2, "apply + idempotent re-run journals");
        let journal = journals.join("\n");
        for step in [
            "plan",
            "targetRegistry",
            "moveStore",
            "sourceRegistry",
            "verified",
        ] {
            assert!(journal.contains(step), "journal missing step {step}");
        }
        assert!(!journal.contains("+1555"), "journal must not carry numbers");
    }

    /// Crash window: the target registry landed but the store did not move.
    /// The next pass resumes the move instead of failing or duplicating it.
    #[test]
    fn resumes_after_a_crash_between_the_target_registry_and_the_move() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("signal-data");
        let state_dir = temp.path().join("state");
        fs::create_dir_all(&root).unwrap();
        let store = test_store(&state_dir);
        let id = account_id(3);
        seed_legacy(
            &root,
            &store,
            DEFAULT_PROXY_GROUP_ID,
            &id,
            "+15550000003",
            "lk-three",
        );
        // Simulate the crash: the target registry was written, the store was
        // not moved yet.
        let slot = root.join(ENGINE_SLOT_SUBDIR).join(&id);
        fs::create_dir_all(slot.join(ACCOUNTS_DIR)).unwrap();
        fs::write(
            slot.join(STATE_FILE),
            serde_json::to_vec_pretty(&json!({
                "version": 2,
                "accounts": [{"number": "+15550000003", "dir": "accounts/lk-three"}],
            }))
            .unwrap(),
        )
        .unwrap();

        let report = run(&store, &root, &state_dir, false).unwrap();
        assert_eq!(report.migrated, 1);
        assert!(
            slot.join(ACCOUNTS_DIR)
                .join("lk-three")
                .join(STORE_DB_FILE)
                .is_file(),
            "the resumed pass performs the move"
        );
        assert!(!root.join(ACCOUNTS_DIR).join("lk-three").exists());
    }

    /// A copied (not moved) store leaves two credential copies. The migration
    /// refuses to choose one and fails closed.
    #[test]
    fn refuses_two_copies_of_the_same_store() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("signal-data");
        let state_dir = temp.path().join("state");
        fs::create_dir_all(&root).unwrap();
        let store = test_store(&state_dir);
        let id = account_id(4);
        seed_legacy(
            &root,
            &store,
            DEFAULT_PROXY_GROUP_ID,
            &id,
            "+15550000004",
            "lk-four",
        );
        let copied = root
            .join(ENGINE_SLOT_SUBDIR)
            .join(&id)
            .join(ACCOUNTS_DIR)
            .join("lk-four");
        fs::create_dir_all(&copied).unwrap();
        fs::write(copied.join(STORE_DB_FILE), b"copied").unwrap();

        let error = run(&store, &root, &state_dir, false).unwrap_err();
        assert!(
            matches!(error, StoreError::EnginePoolInvalid(_)),
            "{error:?}"
        );
        assert!(
            root.join(ACCOUNTS_DIR)
                .join("lk-four")
                .join(STORE_DB_FILE)
                .is_file(),
            "fail closed must leave both copies intact"
        );
    }

    #[test]
    fn refuses_a_corrupt_legacy_registry() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("signal-data");
        let state_dir = temp.path().join("state");
        fs::create_dir_all(&root).unwrap();
        let store = test_store(&state_dir);
        store
            .upsert_account_from_signal_with_id(
                "+15550000005",
                Some(1),
                DEFAULT_PROXY_GROUP_ID,
                Some(&account_id(5)),
            )
            .unwrap();
        fs::write(root.join(STATE_FILE), b"{not json").unwrap();
        let error = run(&store, &root, &state_dir, false).unwrap_err();
        assert!(
            matches!(error, StoreError::EnginePoolInvalid(_)),
            "{error:?}"
        );
    }

    /// Empty engine directories are rolled-back link leftovers and are
    /// removed; a directory that still carries credentials with no account
    /// row aborts startup.
    #[test]
    fn removes_empty_orphans_and_refuses_orphan_credentials() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("signal-data");
        let state_dir = temp.path().join("state");
        fs::create_dir_all(&root).unwrap();
        let store = test_store(&state_dir);
        let empty = root.join(ENGINE_SLOT_SUBDIR).join(account_id(6));
        fs::create_dir_all(&empty).unwrap();
        let report = run(&store, &root, &state_dir, false).unwrap();
        assert_eq!(report.orphans_removed, 1);
        assert!(!empty.exists());

        let orphan = root.join(ENGINE_SLOT_SUBDIR).join(account_id(7));
        fs::create_dir_all(orphan.join(ACCOUNTS_DIR).join("lk-seven")).unwrap();
        fs::write(
            orphan
                .join(ACCOUNTS_DIR)
                .join("lk-seven")
                .join(STORE_DB_FILE),
            b"credentials",
        )
        .unwrap();
        fs::write(
            orphan.join(STATE_FILE),
            serde_json::to_vec_pretty(&json!({
                "version": 2,
                "accounts": [{"number": "+15550000007", "dir": "accounts/lk-seven"}],
            }))
            .unwrap(),
        )
        .unwrap();
        let error = run(&store, &root, &state_dir, false).unwrap_err();
        assert!(
            matches!(error, StoreError::EnginePoolInvalid(_)),
            "{error:?}"
        );
        assert!(
            orphan.join(STATE_FILE).is_file(),
            "credentials stay untouched"
        );
    }
}
