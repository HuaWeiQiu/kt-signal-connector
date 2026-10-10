// SPDX-License-Identifier: AGPL-3.0-only

//! The read-only `runtime.doctor` diagnostic face (contract revision 1.96,
//! implementation-plan §4.79; the Signal runtime-environment plan's
//! diagnostics layer 3). One call answers the whole chain in one payload:
//!
//! - full-chain versions: the connector itself plus, per proxy group, the
//!   engine's own `version` answer and the kt-engine
//!   `engine.protocolCapabilities` self-certification face (presage rev /
//!   libsignal version baseline) passed through verbatim — an engine that is
//!   not running answers `unreachable` with the state's classification, and
//!   an engine that does not carry a method answers honestly with the engine
//!   error class instead of a guessed value;
//! - data-directory status per group: path, existence, writability, and the
//!   filesystem's free space;
//! - occupancy-lock status per group: `.kt-signal-connector.lock` presence,
//!   the recorded holder pid, kernel-held state, and the holder's liveness;
//! - the recent error families: a bounded ring of the connector's most
//!   recent classified errors (host dispatch failures and protocol
//!   warnings), aggregated into counts;
//! - network reachability, reused from the existing connectivity surfaces:
//!   the per-group engine status plus the process-wide watchdog/receive-drop
//!   counters. The doctor never probes the network itself.
//!
//! Read-only discipline (pinned by tests): the doctor never starts or stops
//! an engine, never cleans or deletes a lock, never writes anything
//! anywhere. The only lock interaction is a non-blocking exclusive `flock`
//! probe that is unlocked immediately — kernel state only, no file byte or
//! directory entry changes. Data-directory paths are reported to the
//! authenticated host on purpose (the launcher passed them at spawn, so the
//! peer already knows them); they never enter logs (AGENTS.md discipline).

use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{Value, json};

use crate::datalock::LOCK_FILE_NAME;
use crate::engine::{CallClass, EngineError, EngineHandle, EngineState, EngineStatus};
use crate::registry::ProxyGroupRuntime;

/// Bounded engine-call budget for the two doctor probes (`version`,
/// `engine.protocolCapabilities`): a diagnostic must not sit on a wedged
/// engine anywhere near the ordinary 15 s request timeout.
const DOCTOR_ENGINE_CALL_TIMEOUT: Duration = Duration::from_secs(3);

/// The bounded recent-error window (`recentErrors.window`).
const ERROR_RING_CAPACITY: usize = 64;

/// The lock-file content budget: the connector records exactly one decimal
/// pid; anything longer is not a pid this connector wrote.
const LOCK_PID_READ_BYTES: usize = 32;

// ---------------------------------------------------------------- report --

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DoctorReport {
    pub connector: ConnectorFace,
    pub groups: Vec<GroupReport>,
    pub network: NetworkFace,
    pub recent_errors: ErrorRingReport,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorFace {
    pub version: &'static str,
    pub api_version: &'static str,
    pub pid: u32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkFace {
    /// Existing metric (`metrics::record_watchdog_restart`): every engine
    /// restart the receive-liveness watchdog has executed this process.
    pub watchdog_restarts_total: u64,
    /// Existing metric (`metrics::record_receive_drop`): receives dropped
    /// because the persistence queue stayed saturated.
    pub receive_dropped_total: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupReport {
    pub group_id: String,
    pub engine: EngineFace,
    pub data_dir: DataDirFace,
    pub lock: LockFace,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineFace {
    pub state: EngineState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
    pub resource_pressure: bool,
    pub reachable: bool,
    /// Present exactly when `reachable` is false: the honest reason the
    /// engine face cannot answer, classified from the observed state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unreachable_reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<EngineMethodFace>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_capabilities: Option<EngineMethodFace>,
}

/// One engine method probe's outcome: the engine's own answer passed through
/// verbatim, or the honest failure class — never a value the connector
/// invented.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineMethodFace {
    pub present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub face: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
}

impl EngineMethodFace {
    fn answered(face: Value) -> Self {
        Self {
            present: true,
            face: Some(face),
            reason: None,
        }
    }

    fn failed(reason: &'static str) -> Self {
        Self {
            present: false,
            face: None,
            reason: Some(reason),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDirFace {
    /// The launcher-configured path, reported to the authenticated host on
    /// purpose (the launcher already knows it); never logged.
    pub path: String,
    pub exists: bool,
    /// Access-based writability of the directory itself, checked without
    /// writing (POSIX access(2); Windows falls back to the read-only
    /// attribute — best effort there, pending a Windows test host).
    pub writable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub free_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
}

/// The three lock forms the contract pins, carried in `classification`:
/// `none` (no lock file), `held` (a kernel lock is held — the holder is
/// alive by construction, the kernel releases it on any exit), `stale`
/// (file present, no holder, and the recorded pid is dead — a remnant of a
/// crashed-and-since-replaced holder), `released` (file present, no holder,
/// pid alive or unknown — the designed post-release remnant; the lock file
/// is deliberately never deleted, so this is the normal state after a clean
/// connector exit).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LockFace {
    pub present: bool,
    pub held: bool,
    /// The pid the connector wrote into the lock file at acquire time;
    /// absent for lock files written by pre-face connectors or corrupted
    /// content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// `Some(true)` whenever `held` (kernel guarantee), else the recorded
    /// pid's liveness when a pid is known; `None` when no pid is known.
    pub holder_alive: Option<bool>,
    pub classification: &'static str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorRingReport {
    /// The ring's fixed capacity: the newest `window` classified errors are
    /// the aggregation basis.
    pub window: usize,
    /// Every error ever recorded this process — when it exceeds `window`,
    /// the families below cover only the newest slice.
    pub recorded_total: u64,
    pub families: Vec<ErrorFamilyCount>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorFamilyCount {
    /// Where the classification came from: `host` (a host dispatch answered
    /// a structured error; `family` is the wire error code) or `protocol`
    /// (an engine protocol warning; `family` is the warning kind).
    pub source: &'static str,
    pub family: &'static str,
    pub retryable: bool,
    pub count: u64,
    pub last_at_unix_ms: u64,
}

// -------------------------------------------------------------- binding --

/// One group's read-only supervision snapshot, handed over by the supervisor
/// (`RuntimeSupervisor::doctor_face`): the launcher-config identity plus the
/// supervised engine status and a live engine handle when one exists.
pub(crate) struct DoctorGroupFace {
    pub data_dir: std::path::PathBuf,
    pub status: EngineStatus,
    pub engine: Option<EngineHandle>,
}

/// Assemble the whole doctor payload for the runtime: every launch-plan group
/// in launcher order plus the connector-wide faces. Read-only end to end.
pub async fn collect(runtime: &ProxyGroupRuntime) -> DoctorReport {
    let mut groups = Vec::with_capacity(8);
    for (group_id, supervisor) in runtime.doctor_supervisors() {
        let face = supervisor.doctor_face().await;
        let data_dir = probe_data_dir(&face.data_dir);
        let lock = probe_lock(&face.data_dir);
        let engine = engine_face(&face.status, face.engine.as_ref()).await;
        groups.push(GroupReport {
            group_id: group_id.to_string(),
            engine,
            data_dir,
            lock,
        });
    }
    DoctorReport {
        connector: ConnectorFace {
            version: env!("CARGO_PKG_VERSION"),
            api_version: crate::API_VERSION,
            pid: std::process::id(),
        },
        groups,
        network: NetworkFace {
            watchdog_restarts_total: crate::metrics::watchdog_restarts_total(),
            receive_dropped_total: crate::metrics::receive_dropped_total(),
        },
        recent_errors: error_ring_snapshot(),
    }
}

/// Per-group engine face: the supervised status verbatim plus, for a live
/// engine, the two read-only method probes (`version`,
/// `engine.protocolCapabilities`) under the bounded diagnostic budget. A
/// non-running engine marks `unreachable` with the state classification and
/// answers both probes `present: false` with that same reason.
async fn engine_face(status: &EngineStatus, engine: Option<&EngineHandle>) -> EngineFace {
    let reachable = status.state == EngineState::Running && engine.is_some();
    // A Running status with no live handle is a mid-transition race, not a
    // start failure; it gets its own honest classification.
    let degraded_reason = if status.state == EngineState::Running {
        "the engine is transitioning: the live handle vanished mid-probe"
    } else {
        unreachable_reason(status.state)
    };
    let (version, protocol_capabilities) = if let Some(engine) = engine.filter(|_| reachable) {
        (
            Some(
                probe_engine_method(
                    engine,
                    "version",
                    "the engine answered an error for the version method \
                     (an engine that does not carry it surfaces the upstream class)",
                )
                .await,
            ),
            Some(
                probe_engine_method(
                    engine,
                    "engine.protocolCapabilities",
                    "the engine answered an error for the self-certification method \
                     (a stock signal-cli engine does not carry the kt-engine face)",
                )
                .await,
            ),
        )
    } else {
        (
            Some(EngineMethodFace::failed(degraded_reason)),
            Some(EngineMethodFace::failed(degraded_reason)),
        )
    };
    EngineFace {
        state: status.state,
        pid: status.pid,
        rss_bytes: status.rss_bytes,
        resource_pressure: status.resource_pressure,
        reachable,
        unreachable_reason: (!reachable).then_some(degraded_reason),
        version,
        protocol_capabilities,
    }
}

/// One read-only engine method probe under the diagnostic budget.
async fn probe_engine_method(
    engine: &EngineHandle,
    method: &'static str,
    upstream_reason: &'static str,
) -> EngineMethodFace {
    match engine
        .call_with_timeout(
            method,
            json!({}),
            CallClass::ReadOnly,
            DOCTOR_ENGINE_CALL_TIMEOUT,
        )
        .await
    {
        Ok(face) => EngineMethodFace::answered(face),
        Err(
            EngineError::Timeout
            | EngineError::NotRunning
            | EngineError::Exited
            | EngineError::Backpressure,
        ) => EngineMethodFace::failed("the engine stopped answering mid-probe"),
        Err(_) => EngineMethodFace::failed(upstream_reason),
    }
}

/// The honest start/state failure classification for a non-running engine:
/// static, content-free state names only.
fn unreachable_reason(state: EngineState) -> &'static str {
    match state {
        EngineState::Running => "reachable",
        EngineState::Stopped => {
            "engine is stopped: not started since connector boot, or stopped by runtime.stop"
        }
        EngineState::Exited => {
            "engine exited: the process ended on its own; start it with runtime.start"
        }
        EngineState::Faulted => {
            "engine faulted: the process crashed or failed to start; the watchdog may restart it"
        }
    }
}

// ------------------------------------------------------------ data dir ----

/// Read-only data-directory status: existence, writability (access-based,
/// nothing written), and the filesystem's free space. Every failure degrades
/// to an absent optional field rather than a guessed value.
pub(crate) fn probe_data_dir(path: &Path) -> DataDirFace {
    let exists = path.is_dir();
    let writable = exists && path_is_writable(path);
    let (free_bytes, total_bytes) = if exists {
        filesystem_space(path)
    } else {
        (None, None)
    };
    DataDirFace {
        path: path.display().to_string(),
        exists,
        writable,
        free_bytes,
        total_bytes,
    }
}

/// POSIX `access(W_OK)`: the kernel answers for the real credentials without
/// a single byte touching the directory. Windows has no access(2) equivalent
/// in this crate's dependency set; the read-only attribute is the honest
/// best-effort signal there (pending a Windows test host).
fn path_is_writable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        rustix::fs::access(path, rustix::fs::Access::WRITE_OK).is_ok()
    }
    #[cfg(windows)]
    {
        std::fs::metadata(path)
            .map(|metadata| !metadata.permissions().readonly())
            .unwrap_or(false)
    }
}

/// Free/total bytes of the filesystem holding `path`; `None` when the
/// platform query fails (an absent field, never a zero guess).
fn filesystem_space(path: &Path) -> (Option<u64>, Option<u64>) {
    #[cfg(unix)]
    {
        match rustix::fs::statvfs(path) {
            Ok(stats) => {
                let free = stats.f_bavail.saturating_mul(stats.f_frsize);
                let total = stats.f_blocks.saturating_mul(stats.f_frsize);
                (Some(free), Some(total))
            }
            Err(_) => (None, None),
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let mut free: u64 = 0;
        let mut total: u64 = 0;
        let mut _unused: u64 = 0;
        let ok = unsafe { GetDiskFreeSpaceExW(wide.as_ptr(), &mut free, &mut total, &mut _unused) };
        if ok != 0 {
            (Some(free), Some(total))
        } else {
            (None, None)
        }
    }
}

// ----------------------------------------------------------------- lock ----

/// Read-only occupancy-lock status for one data directory. The kernel lock —
/// not the file's existence — is the occupancy truth: the probed lock is
/// released immediately, and no file byte or directory entry changes.
pub(crate) fn probe_lock(data_dir: &Path) -> LockFace {
    let lock_path = data_dir.join(LOCK_FILE_NAME);
    let Some(content) = read_lock_file(&lock_path) else {
        return LockFace {
            present: false,
            held: false,
            pid: None,
            holder_alive: None,
            classification: "none",
        };
    };
    let pid = recorded_pid(&content);
    match probe_held(&lock_path) {
        Some(true) => LockFace {
            present: true,
            held: true,
            pid,
            // Kernel-owned lock: a holder that died for any reason — SIGKILL
            // included — releases it, so `held` proves a live holder.
            holder_alive: Some(true),
            classification: "held",
        },
        held @ Some(false) | held @ None => {
            let holder_alive = pid.map(pid_alive);
            // Dead recorded pid + no kernel holder = the stale remnant of a
            // crashed holder; anything else is the designed post-release
            // remnant (the file is deliberately never deleted).
            let stale = holder_alive == Some(false);
            LockFace {
                present: true,
                held: held.unwrap_or(false),
                pid,
                holder_alive,
                classification: if stale { "stale" } else { "released" },
            }
        }
    }
}

/// The lock file's first bytes, bounded — a corrupted or foreign file never
/// pulls unbounded data into the diagnostic.
fn read_lock_file(lock_path: &Path) -> Option<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(lock_path).ok()?;
    let mut buffer = [0u8; LOCK_PID_READ_BYTES];
    let mut read = 0;
    loop {
        let result = file.read(&mut buffer[read..]);
        match result {
            Ok(0) => break,
            Ok(count) => {
                read += count;
                if read == buffer.len() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    String::from_utf8(buffer[..read].to_vec()).ok()
}

/// The decimal pid this connector writes at acquire time; anything else
/// parses to `None`.
fn recorded_pid(content: &str) -> Option<u32> {
    let trimmed = content.trim();
    if trimmed.is_empty() || trimmed.len() > LOCK_PID_READ_BYTES {
        return None;
    }
    trimmed.parse::<u32>().ok()
}

/// Non-blocking kernel-lock probe: `Some(true)` = a holder exists,
/// `Some(false)` = no holder (probed and released), `None` = the probe could
/// not run. The probe acquires nothing durably and changes no file byte.
fn probe_held(lock_path: &Path) -> Option<bool> {
    let file = std::fs::File::open(lock_path).ok()?;
    #[cfg(unix)]
    {
        match rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {
                let _ = rustix::fs::flock(&file, rustix::fs::FlockOperation::Unlock);
                Some(false)
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Some(true),
            Err(_) => None,
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx, UnlockFileEx,
        };
        use windows_sys::Win32::System::IO::OVERLAPPED;
        let mut overlapped = OVERLAPPED::default();
        let locked = unsafe {
            LockFileEx(
                file.as_raw_handle(),
                LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };
        if locked != 0 {
            let mut overlapped = OVERLAPPED::default();
            unsafe {
                UnlockFileEx(file.as_raw_handle(), 0, u32::MAX, u32::MAX, &mut overlapped);
            }
            Some(false)
        } else {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error()
                == Some(windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32)
            {
                Some(true)
            } else {
                None
            }
        }
    }
}

/// Liveness of a recorded pid: signal 0 on Unix (delivers nothing, answers
/// existence), a zero-wait process handle on Windows. Never sends a real
/// signal, never terminates anything.
fn pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let Ok(pid) = libc::pid_t::try_from(pid) else {
            return false;
        };
        unsafe { libc::kill(pid, 0) == 0 }
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{CloseHandle, WAIT_TIMEOUT};
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject,
        };
        unsafe {
            let handle = OpenProcess(PROCESS_SYNCHRONIZE, 0, pid);
            if handle.is_null() {
                return false;
            }
            let result = WaitForSingleObject(handle, 0);
            CloseHandle(handle);
            result == WAIT_TIMEOUT
        }
    }
}

// ---------------------------------------------------------- error ring ----

/// One bounded ring entry: a classified error with its recording time. The
/// family is always a fixed literal (a wire error code or a protocol-warning
/// kind) — never message text, never request content.
#[derive(Clone, Debug)]
struct ErrorRecord {
    at_unix_ms: u64,
    source: &'static str,
    family: &'static str,
    retryable: bool,
}

static ERROR_RING: StdMutex<VecDeque<ErrorRecord>> = StdMutex::new(VecDeque::new());
static ERROR_RING_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Unix milliseconds for a ring entry; a pre-epoch clock degrades to zero.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

/// Record one host dispatch failure (the wire error code is the family).
/// Bounded: the oldest entry leaves the ring when the window is full.
pub fn record_host_error(code: &'static str, retryable: bool) {
    record_error("host", code, retryable);
}

/// Record one engine protocol warning (the warning kind is the family).
pub fn record_protocol_warning(kind: &'static str) {
    record_error("protocol", kind, false);
}

fn record_error(source: &'static str, family: &'static str, retryable: bool) {
    ERROR_RING_TOTAL.fetch_add(1, Ordering::Relaxed);
    if let Ok(mut ring) = ERROR_RING.lock() {
        if ring.len() == ERROR_RING_CAPACITY {
            ring.pop_front();
        }
        ring.push_back(ErrorRecord {
            at_unix_ms: now_unix_ms(),
            source,
            family,
            retryable,
        });
    }
}

/// Aggregate the ring into per-family counts (count descending, then source
/// and family ascending for a deterministic answer).
pub fn error_ring_snapshot() -> ErrorRingReport {
    let mut aggregated: BTreeMap<(&'static str, &'static str), ErrorFamilyCount> = BTreeMap::new();
    if let Ok(ring) = ERROR_RING.lock() {
        for record in ring.iter() {
            let entry = aggregated
                .entry((record.source, record.family))
                .or_insert_with(|| ErrorFamilyCount {
                    source: record.source,
                    family: record.family,
                    retryable: record.retryable,
                    count: 0,
                    last_at_unix_ms: record.at_unix_ms,
                });
            entry.count += 1;
            entry.last_at_unix_ms = entry.last_at_unix_ms.max(record.at_unix_ms);
            entry.retryable = record.retryable;
        }
    }
    let mut families: Vec<ErrorFamilyCount> = aggregated.into_values().collect();
    families.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then(left.source.cmp(right.source))
            .then(left.family.cmp(right.family))
    });
    ErrorRingReport {
        window: ERROR_RING_CAPACITY,
        recorded_total: ERROR_RING_TOTAL.load(Ordering::Relaxed),
        families,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------ engine face --

    fn stopped_status(state: EngineState) -> EngineStatus {
        EngineStatus {
            state,
            pid: None,
            rss_bytes: None,
            resource_pressure: false,
        }
    }

    /// The doctor output structure: every contract key present with the
    /// contract type, degraded engine faces carrying the unreachable reason
    /// on both probes, and no fabricated version data anywhere.
    #[test]
    fn doctor_report_structure_matches_the_contract() {
        let report = DoctorReport {
            connector: ConnectorFace {
                version: env!("CARGO_PKG_VERSION"),
                api_version: crate::API_VERSION,
                pid: std::process::id(),
            },
            groups: vec![GroupReport {
                group_id: "default".into(),
                engine: EngineFace {
                    state: EngineState::Faulted,
                    pid: None,
                    rss_bytes: None,
                    resource_pressure: false,
                    reachable: false,
                    unreachable_reason: Some(unreachable_reason(EngineState::Faulted)),
                    version: Some(EngineMethodFace::failed(unreachable_reason(
                        EngineState::Faulted,
                    ))),
                    protocol_capabilities: Some(EngineMethodFace::failed(unreachable_reason(
                        EngineState::Faulted,
                    ))),
                },
                data_dir: DataDirFace {
                    path: "/tmp/never-logged".into(),
                    exists: true,
                    writable: true,
                    free_bytes: Some(4096),
                    total_bytes: Some(8192),
                },
                lock: LockFace {
                    present: true,
                    held: true,
                    pid: Some(7),
                    holder_alive: Some(true),
                    classification: "held",
                },
            }],
            network: NetworkFace {
                watchdog_restarts_total: 0,
                receive_dropped_total: 0,
            },
            recent_errors: ErrorRingReport {
                window: ERROR_RING_CAPACITY,
                recorded_total: 2,
                families: vec![ErrorFamilyCount {
                    source: "host",
                    family: "UPSTREAM_TIMEOUT",
                    retryable: true,
                    count: 2,
                    last_at_unix_ms: 1,
                }],
            },
        };
        let value = serde_json::to_value(&report).expect("serialize");
        let connector = value.get("connector").expect("connector face");
        assert!(connector.get("version").is_some_and(Value::is_string));
        assert_eq!(
            connector.get("apiVersion").and_then(Value::as_str),
            Some(crate::API_VERSION)
        );
        assert!(connector.get("pid").is_some_and(Value::is_u64));

        let group = &value["groups"][0];
        assert_eq!(group["groupId"], json!("default"));
        let engine = &group["engine"];
        assert_eq!(engine["state"], json!("faulted"));
        assert_eq!(engine["reachable"], json!(false));
        assert_eq!(
            engine["pid"],
            Value::Null,
            "absent optional keys stay absent"
        );
        assert!(
            engine["unreachableReason"]
                .as_str()
                .is_some_and(|reason| reason.contains("faulted")),
            "the state classification names the fault"
        );
        for probe in ["version", "protocolCapabilities"] {
            assert_eq!(engine[probe]["present"], json!(false));
            assert!(
                engine[probe]["reason"].is_string(),
                "{probe} degrades with the honest reason"
            );
            assert!(
                engine[probe].get("face").is_none(),
                "{probe} invents nothing"
            );
        }

        let data_dir = &group["dataDir"];
        assert!(data_dir["path"].is_string());
        assert_eq!(data_dir["exists"], json!(true));
        assert_eq!(data_dir["writable"], json!(true));
        assert_eq!(data_dir["freeBytes"], json!(4096));
        assert_eq!(data_dir["totalBytes"], json!(8192));

        let lock = &group["lock"];
        assert_eq!(lock["present"], json!(true));
        assert_eq!(lock["held"], json!(true));
        assert_eq!(lock["pid"], json!(7));
        assert_eq!(lock["holderAlive"], json!(true));
        assert_eq!(lock["classification"], json!("held"));

        let recent = &value["recentErrors"];
        assert_eq!(recent["window"], json!(ERROR_RING_CAPACITY));
        assert!(recent["recordedTotal"].is_u64());
        let family = &recent["families"][0];
        assert_eq!(family["source"], json!("host"));
        assert_eq!(family["family"], json!("UPSTREAM_TIMEOUT"));
        assert_eq!(family["retryable"], json!(true));
        assert_eq!(family["count"], json!(2));
        assert!(family["lastAtUnixMs"].is_u64());

        let network = &value["network"];
        assert!(network["watchdogRestartsTotal"].is_u64());
        assert!(network["receiveDroppedTotal"].is_u64());
    }

    /// Engine-unreachable degradation: a non-running engine marks
    /// `reachable: false`, classifies the state into the reason, and answers
    /// both probes `present: false` with that reason — never a guessed
    /// version or baseline, and never an engine spawn attempt.
    #[tokio::test]
    async fn engine_unreachable_degrades_honestly() {
        for state in [
            EngineState::Stopped,
            EngineState::Exited,
            EngineState::Faulted,
        ] {
            let face = engine_face(&stopped_status(state), None).await;
            assert!(!face.reachable, "{state:?} is not reachable");
            assert_eq!(face.unreachable_reason, Some(unreachable_reason(state)));
            let version = face.version.expect("version face always present");
            let capabilities = face
                .protocol_capabilities
                .expect("capabilities face always present");
            assert!(!version.present);
            assert_eq!(version.reason, Some(unreachable_reason(state)));
            assert!(!capabilities.present);
            assert_eq!(capabilities.reason, Some(unreachable_reason(state)));
        }
    }

    /// A reachable engine's method faces: answers pass through verbatim and
    /// engine failures degrade to the honest failure classes.
    #[tokio::test]
    async fn engine_method_faces_answer_and_degrade_verbatim() {
        let status = EngineStatus {
            state: EngineState::Running,
            pid: Some(9),
            rss_bytes: None,
            resource_pressure: false,
        };
        // No engine handle despite Running status: the honest unreachable.
        let face = engine_face(&status, None).await;
        assert!(!face.reachable);
        assert_eq!(
            face.unreachable_reason,
            Some("the engine is transitioning: the live handle vanished mid-probe")
        );
        assert_eq!(
            face.version.and_then(|version| version.reason),
            face.unreachable_reason
        );

        // Answered probes ride verbatim, including unknown keys.
        let answered =
            EngineMethodFace::answered(json!({"baseline": {"presageRev": "a"}, "extra": 1}));
        assert!(answered.present);
        assert_eq!(answered.face.expect("face")["extra"], json!(1));
    }

    // ------------------------------------------------------------- lock --

    #[cfg(unix)]
    fn hold_flock(path: &Path) -> std::fs::File {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .expect("open lock file");
        rustix::fs::flock(&file, rustix::fs::FlockOperation::LockExclusive).expect("flock");
        file
    }

    /// Lock form 1 — no lock file at all.
    #[test]
    fn lock_face_reports_absence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let face = probe_lock(dir.path());
        assert!(!face.present);
        assert!(!face.held);
        assert_eq!(face.pid, None);
        assert_eq!(face.holder_alive, None);
        assert_eq!(face.classification, "none");
    }

    /// Lock form 2 — a kernel-held lock: `held` proves a live holder by
    /// construction, the recorded pid surfaces, and the probe leaves the
    /// holder's lock in place.
    #[cfg(unix)]
    #[test]
    fn lock_face_reports_a_live_held_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let lock_path = dir.path().join(LOCK_FILE_NAME);
        std::fs::write(&lock_path, format!("{}", std::process::id())).expect("write pid");
        let _guard = hold_flock(&lock_path);
        let face = probe_lock(dir.path());
        assert!(face.present);
        assert!(face.held, "the probe must see the held kernel lock");
        assert_eq!(face.pid, Some(std::process::id()));
        assert_eq!(face.holder_alive, Some(true));
        assert_eq!(face.classification, "held");
    }

    /// Lock form 3 — a stale remnant: file present, no kernel holder, the
    /// recorded pid dead. Exactly the crashed-then-replaced holder shape.
    #[cfg(unix)]
    #[test]
    fn lock_face_reports_a_stale_dead_pid_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let lock_path = dir.path().join(LOCK_FILE_NAME);
        // A pid guaranteed dead: an exited child.
        let mut child = std::process::Command::new("true")
            .spawn()
            .expect("spawn true");
        let dead_pid = child.id();
        child.wait().expect("reap");
        std::fs::write(&lock_path, format!("{dead_pid}")).expect("write pid");
        let face = probe_lock(dir.path());
        assert!(face.present);
        assert!(!face.held);
        assert_eq!(face.pid, Some(dead_pid));
        assert_eq!(face.holder_alive, Some(false));
        assert_eq!(face.classification, "stale");
    }

    /// The designed post-release remnant: file present, no holder, pid alive
    /// (or unknown) — classified `released`, never `stale`.
    #[cfg(unix)]
    #[test]
    fn lock_face_reports_a_released_lock_without_a_dead_pid() {
        let dir = tempfile::tempdir().expect("tempdir");
        let lock_path = dir.path().join(LOCK_FILE_NAME);
        std::fs::write(&lock_path, format!("{}", std::process::id())).expect("write pid");
        let face = probe_lock(dir.path());
        assert!(face.present);
        assert!(!face.held);
        assert_eq!(face.pid, Some(std::process::id()));
        assert_eq!(face.holder_alive, Some(true));
        assert_eq!(face.classification, "released");

        // Pre-face connectors left empty lock files: pid unknown.
        std::fs::write(&lock_path, b"").expect("empty");
        let face = probe_lock(dir.path());
        assert_eq!(face.pid, None);
        assert_eq!(face.holder_alive, None);
        assert_eq!(face.classification, "released");
    }

    /// Corrupted lock content never parses into a pid and never panics.
    #[test]
    fn recorded_pid_rejects_non_pid_content() {
        assert_eq!(recorded_pid(""), None);
        assert_eq!(recorded_pid("not-a-pid"), None);
        assert_eq!(recorded_pid("-1"), None);
        assert_eq!(recorded_pid(&"9".repeat(LOCK_PID_READ_BYTES + 1)), None);
        assert_eq!(recorded_pid("4194304"), Some(4194304));
    }

    /// The read-only discipline, pinned: probing the lock and the data
    /// directory changes no file byte, no mtime, and no directory entry.
    #[cfg(unix)]
    #[test]
    fn probes_leave_the_data_directory_untouched() {
        let dir = tempfile::tempdir().expect("tempdir");
        let lock_path = dir.path().join(LOCK_FILE_NAME);
        std::fs::write(&lock_path, format!("{}", std::process::id())).expect("write pid");
        let _guard = hold_flock(&lock_path);

        let snapshot = |dir: &Path| -> Vec<(String, u64, std::time::SystemTime)> {
            let mut entries: Vec<_> = std::fs::read_dir(dir)
                .expect("read dir")
                .map(|entry| entry.expect("entry"))
                .map(|entry| {
                    let metadata = entry.metadata().expect("metadata");
                    (
                        entry.file_name().to_string_lossy().into_owned(),
                        metadata.len(),
                        metadata.modified().expect("mtime"),
                    )
                })
                .collect();
            entries.sort();
            entries
        };
        let before = snapshot(dir.path());

        let _ = probe_lock(dir.path());
        let _ = probe_data_dir(dir.path());
        let _ = filesystem_space(dir.path());
        let _ = path_is_writable(dir.path());

        assert_eq!(
            snapshot(dir.path()),
            before,
            "no byte, size, or entry changed"
        );
        assert_eq!(
            std::fs::read(&lock_path).expect("lock content"),
            format!("{}", std::process::id()).into_bytes(),
            "lock file content untouched"
        );
        // The held lock is still held after the probe.
        assert!(probe_lock(dir.path()).held);
    }

    /// Data-dir status: existence, writability, and real free-space numbers.
    #[test]
    fn data_dir_face_reports_status_without_writing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let face = probe_data_dir(dir.path());
        assert!(face.exists);
        assert!(face.writable);
        assert!(face.free_bytes.is_some_and(|bytes| bytes > 0));
        assert!(face.total_bytes.is_some_and(|bytes| bytes > 0));
        assert!(face.path.contains(dir.path().to_string_lossy().as_ref()));

        let missing = probe_data_dir(&dir.path().join("absent-subdir"));
        assert!(!missing.exists);
        assert!(!missing.writable);
        assert_eq!(
            missing.free_bytes, None,
            "no guessed numbers for a missing dir"
        );
    }

    // -------------------------------------------------------- error ring --

    /// The ring stays bounded, aggregates per family, and carries the
    /// recorded total so a consumer can see truncation.
    #[test]
    fn error_ring_is_bounded_and_aggregates_families() {
        for _ in 0..(ERROR_RING_CAPACITY + 8) {
            record_host_error("UPSTREAM_TIMEOUT", true);
        }
        record_protocol_warning("unknownOrDuplicateResponseId");
        let snapshot = error_ring_snapshot();
        assert_eq!(snapshot.window, ERROR_RING_CAPACITY);
        assert!(snapshot.recorded_total >= ERROR_RING_CAPACITY as u64 + 9);
        let timeout = snapshot
            .families
            .iter()
            .find(|family| family.family == "UPSTREAM_TIMEOUT")
            .expect("timeout family present");
        // The ring is bounded: capacity entries total, so the last (different)
        // family squeezed the oldest timeout out — 63 timeouts + 1 warning.
        assert_eq!(
            timeout.count,
            (ERROR_RING_CAPACITY - 1) as u64,
            "oldest entries leave the ring when the window is full"
        );
        assert!(timeout.retryable);
        assert_eq!(timeout.source, "host");
        let warning = snapshot
            .families
            .iter()
            .find(|family| family.family == "unknownOrDuplicateResponseId")
            .expect("protocol family present");
        assert_eq!(warning.source, "protocol");
        assert!(!warning.retryable);
        // Deterministic ordering: count descending.
        let counts: Vec<u64> = snapshot
            .families
            .iter()
            .map(|family| family.count)
            .collect();
        let mut sorted = counts.clone();
        sorted.sort_unstable_by(|left, right| right.cmp(left));
        assert_eq!(counts, sorted);
    }
}
