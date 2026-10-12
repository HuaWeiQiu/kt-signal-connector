// SPDX-License-Identifier: AGPL-3.0-only

//! S2a performance-baseline instrumentation layer (measure-only; the project
//! discipline is `kt-desktop` `docs/plan/signal-runtime-followup-projects.md`
//! §附四②: this batch establishes the measurement surface, it changes no
//! behavior — call order, return values, and control flow are untouched).
//!
//! Two distinct key spaces live here, deliberately never mixed:
//!
//! - **Local vocabulary** ([`keys`], [`KNOWN_KEYS`]): the connector's own
//!   bounded in-process measurements. Today that is the
//!   `receive_persistence_loop` per-envelope ingest transaction (contract
//!   revision 1.133, §4.116) — the registered largest-bottleneck candidate
//!   (30-envelope burst = 30 transactions). Counters cover the ok/retry
//!   outcome split and the host events each transaction produced (the
//!   transaction's realized batch size; the mean is events / ok).
//! - **Engine snapshot allowlist** ([`ENGINE_TIMING_KEYS`] /
//!   [`ENGINE_COUNTER_KEYS`]): the closed 31-key word list the engine's
//!   read-only `engine.perfMetrics` answer may carry (engine slice
//!   `38238b3`). [`filter_engine_snapshot`] projects a live answer down to
//!   those keys and counts everything else — the doctor's `perfMetrics`
//!   face never forwards an unbounded key space from a future engine.
//!
//! Shape follows the engine-side convention (`kt-signal-engine`
//! `src/service/perf_metrics.rs`): bounded aggregation, dropped counting
//! past the word list, deterministic BTreeMap snapshot key order, honest
//! absence (an unused key never appears as an invented zero). Durations are
//! microseconds (`totalUs/maxUs/lastUs`). No message content, phone
//! numbers, contacts, secrets, or key paths ever enters this layer.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use serde_json::{Map, Value, json};

/// Local measurement word list (closed set; snake_case, snapshots emit the
/// keys verbatim). New measurement points must register a constant first;
/// unknown keys count into [`Registry::unknown_key_dropped`] and never land.
pub mod keys {
    /// One `receive_persistence_loop` ingest transaction attempt (the
    /// blocking SQLite batch under the service lock), wall time per attempt
    /// including the failed ones.
    pub const RECEIVE_INGEST: &str = "receive_ingest";
    /// Ingest transactions that landed (store available, no retry).
    pub const RECEIVE_INGEST_OK: &str = "receive_ingest_ok";
    /// Ingest transaction attempts that failed against an unavailable
    /// store (each one sleeps and re-attempts; the retry loop itself is
    /// untouched — this only counts; attempts = ok + err).
    pub const RECEIVE_INGEST_ERR: &str = "receive_ingest_err";
    /// Host events the landed transactions produced, summed — the realized
    /// batch size per transaction; the mean is this value over `ok`.
    pub const RECEIVE_INGEST_EVENTS: &str = "receive_ingest_events";
    /// Engine snapshot keys the doctor's `engine.perfMetrics` projection
    /// dropped (outside the pinned engine allowlist), summed across probes.
    pub const ENGINE_SNAPSHOT_DROPPED: &str = "engine_snapshot_dropped";
    /// Account-domain transient notifications (contract 1.134, §4.117:
    /// `receive.emptied` / `receive.clockskew` /
    /// `receive.conversationState`) forwarded to the host lane after their
    /// signal account resolved.
    pub const ACCOUNT_TRANSIENT_FORWARDED: &str = "account_transient_forwarded";
    /// Account-domain transient notifications dropped because their signal
    /// account matched no bound connector account (the accountUnbound
    /// attribution discipline: unattributed, warn-only, never claimed as
    /// forwarded).
    pub const ACCOUNT_TRANSIENT_UNATTRIBUTED: &str = "account_transient_unattributed";
}

/// Local word list (timings + counters share one table; snapshots only emit
/// registered keys).
const KNOWN_KEYS: &[&str] = &[
    keys::RECEIVE_INGEST,
    keys::RECEIVE_INGEST_OK,
    keys::RECEIVE_INGEST_ERR,
    keys::RECEIVE_INGEST_EVENTS,
    keys::ENGINE_SNAPSHOT_DROPPED,
    keys::ACCOUNT_TRANSIENT_FORWARDED,
    keys::ACCOUNT_TRANSIENT_UNATTRIBUTED,
];

/// The engine `engine.perfMetrics` timing-series keys the connector
/// understands (engine slice `38238b3`, 12 timings). A live answer's
/// `timings` entry passes only when its key is in this list; everything
/// else is dropped and counted.
pub const ENGINE_TIMING_KEYS: &[&str] = &[
    "receive_envelope",
    "receive_project",
    "flush_barrier",
    "send_lock_wait",
    "send_text_total",
    "send_group_total",
    "rpc_dispatch",
    "prekey_quick_check",
    "prekey_maintenance_tick",
    "prekey_cleanup_tick",
    "reconnect_backoff",
    "initial_sync_requests",
];

/// The engine `engine.perfMetrics` counter keys the connector understands
/// (the other 19 keys of the engine's closed 31-key vocabulary).
pub const ENGINE_COUNTER_KEYS: &[&str] = &[
    "receive_kind_content",
    "receive_kind_identity_change",
    "receive_kind_decryption_error",
    "receive_kind_duplicate",
    "receive_kind_contacts",
    "receive_kind_queue_empty",
    "flush_barrier_ok",
    "flush_barrier_dead",
    "send_text_ok",
    "send_text_err",
    "send_group_mrm",
    "send_group_failover",
    "send_group_repair_round",
    "prekey_quick_check_time_trigger",
    "prekey_quick_check_volume_trigger",
    "stream_outcome_recoverable",
    "stream_outcome_connected",
    "stream_outcome_unbound",
    "stream_outcome_stdio_dead",
];

#[derive(Default)]
struct Series {
    count: u64,
    total_us: u64,
    max_us: u64,
    last_us: u64,
}

#[derive(Default)]
struct Registry {
    timings: BTreeMap<&'static str, Series>,
    counters: BTreeMap<&'static str, u64>,
    unknown_key_dropped: u64,
    started: Option<Instant>,
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

fn known(key: &str) -> bool {
    KNOWN_KEYS.contains(&key)
}

/// Record one duration (microseconds). A key outside the word list counts
/// the drop and stores nothing (bounded surface).
pub fn record_us(key: &'static str, us: u64) {
    let mut registry = registry().lock().expect("perf registry poisoned");
    if registry.started.is_none() {
        registry.started = Some(Instant::now());
    }
    if !known(key) {
        registry.unknown_key_dropped += 1;
        return;
    }
    let series = registry.timings.entry(key).or_default();
    series.count += 1;
    series.total_us = series.total_us.saturating_add(us);
    series.max_us = series.max_us.max(us);
    series.last_us = us;
}

/// Add to a counter. A key outside the word list counts the drop and stores
/// nothing.
pub fn add(key: &'static str, n: u64) {
    let mut registry = registry().lock().expect("perf registry poisoned");
    if registry.started.is_none() {
        registry.started = Some(Instant::now());
    }
    if !known(key) {
        registry.unknown_key_dropped += 1;
        return;
    }
    *registry.counters.entry(key).or_insert(0) += n;
}

/// The connector-side snapshot (`connector.perfMetrics` shape, consumed by
/// tests and future diagnostic faces): `{uptimeMs, timings, counters,
/// unknownKeyDropped}` with deterministic BTreeMap key order. A key that
/// was never recorded is honestly absent — never an invented zero.
pub fn snapshot_json() -> Value {
    let registry = registry().lock().expect("perf registry poisoned");
    let uptime_ms = registry
        .started
        .map(|started| started.elapsed().as_millis() as u64)
        .unwrap_or(0);
    let timings = Value::Object(Map::from_iter(registry.timings.iter().map(
        |(key, series)| {
            (
                (*key).to_string(),
                json!({
                    "count": series.count,
                    "totalUs": series.total_us,
                    "maxUs": series.max_us,
                    "lastUs": series.last_us,
                }),
            )
        },
    )));
    let counters = Value::Object(Map::from_iter(
        registry
            .counters
            .iter()
            .map(|(key, value)| ((*key).to_string(), json!(value))),
    ));
    json!({
        "uptimeMs": uptime_ms,
        "timings": timings,
        "counters": counters,
        "unknownKeyDropped": registry.unknown_key_dropped,
    })
}

/// Project one live `engine.perfMetrics` answer onto the connector's pinned
/// engine allowlist. Returns the filtered face plus the number of keys the
/// connector dropped (unknown top-level keys, unknown `timings`/`counters`
/// entries, and malformed entries). The projection is deliberately **not**
/// the verbatim passthrough the doctor's `version` /
/// `engine.protocolCapabilities` faces use: a future engine may grow its
/// vocabulary, and the connector never forwards an unbounded key space.
///
/// Honest-absence rules: a scalar the engine did not send or sent malformed
/// stays absent; `uptimeMs` / `unknownKeyDropped` pass through only as
/// `u64`; a non-object `timings`/`counters` is dropped (and counted)
/// wholesale; a whitelisted entry whose value is not an object is dropped
/// (and counted). Whitelisted entries that are present and well-formed keep
/// the engine's own series object verbatim.
pub fn filter_engine_snapshot(answer: &Value) -> (Value, u64) {
    let mut dropped = 0_u64;
    let mut out = Map::new();
    let Some(entries) = answer.as_object() else {
        return (json!({"connectorDroppedKeys": 1}), 1);
    };
    for (key, value) in entries {
        match key.as_str() {
            "uptimeMs" => {
                if let Some(uptime) = value.as_u64() {
                    out.insert("uptimeMs".into(), json!(uptime));
                } else {
                    dropped += 1;
                }
            }
            "unknownKeyDropped" => {
                if let Some(count) = value.as_u64() {
                    out.insert("unknownKeyDropped".into(), json!(count));
                } else {
                    dropped += 1;
                }
            }
            "timings" => {
                if let Some(series) = filter_series(value, ENGINE_TIMING_KEYS, &mut dropped) {
                    out.insert("timings".into(), Value::Object(series));
                }
            }
            "counters" => {
                if let Some(counters) = filter_counters(value, ENGINE_COUNTER_KEYS, &mut dropped) {
                    out.insert("counters".into(), Value::Object(counters));
                }
            }
            _ => dropped += 1,
        }
    }
    // Always present: the consumer can tell a clean projection from a
    // lossy one without diffing key sets. A meaningful zero, not absence.
    out.insert("connectorDroppedKeys".into(), json!(dropped));
    (Value::Object(out), dropped)
}

/// Keep the allowlisted, object-valued entries of one engine series section.
/// A non-object section drops wholesale (counted once).
fn filter_series(section: &Value, allow: &[&str], dropped: &mut u64) -> Option<Map<String, Value>> {
    let entries = section.as_object()?;
    let mut out = Map::new();
    for (key, value) in entries {
        if allow.contains(&key.as_str()) && value.is_object() {
            out.insert(key.clone(), value.clone());
        } else {
            *dropped += 1;
        }
    }
    Some(out)
}

/// Keep the allowlisted, numeric entries of one engine counter section.
fn filter_counters(
    section: &Value,
    allow: &[&str],
    dropped: &mut u64,
) -> Option<Map<String, Value>> {
    let entries = section.as_object()?;
    let mut out = Map::new();
    for (key, value) in entries {
        if allow.contains(&key.as_str()) && value.is_u64() {
            out.insert(key.clone(), value.clone());
        } else {
            *dropped += 1;
        }
    }
    Some(out)
}

#[cfg(test)]
pub(crate) fn reset_for_tests() {
    let mut registry = registry().lock().expect("perf registry poisoned");
    *registry = Registry {
        started: Some(Instant::now()),
        ..Registry::default()
    };
}

#[cfg(test)]
mod tests {
    use super::{
        ENGINE_COUNTER_KEYS, ENGINE_TIMING_KEYS, add, filter_engine_snapshot, keys, record_us,
        reset_for_tests, snapshot_json,
    };

    // The registry is process-wide and shared with parallel tests; every
    // test resets it and asserts only keys it just wrote.
    #[test]
    fn record_then_snapshot_has_series_and_counter_absent_until_used() {
        reset_for_tests();
        record_us(keys::RECEIVE_INGEST, 1_500);
        add(keys::RECEIVE_INGEST_OK, 2);
        let snapshot = snapshot_json();
        let timings = snapshot["timings"].as_object().expect("timings object");
        let series = &timings[keys::RECEIVE_INGEST];
        assert_eq!(series["count"], 1);
        assert_eq!(series["totalUs"], 1_500);
        assert_eq!(series["maxUs"], 1_500);
        assert_eq!(series["lastUs"], 1_500);
        let counters = snapshot["counters"].as_object().expect("counters object");
        assert_eq!(counters[keys::RECEIVE_INGEST_OK], 2);
        // Unused word-list keys stay honestly absent.
        assert!(!timings.contains_key("nonexistent"));
        assert!(!counters.contains_key(keys::RECEIVE_INGEST_ERR));
        assert_eq!(snapshot["unknownKeyDropped"], 0);
    }

    #[test]
    fn series_aggregates_count_total_max_last() {
        reset_for_tests();
        record_us(keys::RECEIVE_INGEST, 100);
        record_us(keys::RECEIVE_INGEST, 900);
        record_us(keys::RECEIVE_INGEST, 300);
        let snapshot = snapshot_json();
        let series = &snapshot["timings"][keys::RECEIVE_INGEST];
        assert_eq!(series["count"], 3);
        assert_eq!(series["totalUs"], 1_300);
        assert_eq!(series["maxUs"], 900);
        assert_eq!(series["lastUs"], 300);
    }

    #[test]
    fn unknown_key_is_dropped_and_counted_not_stored() {
        reset_for_tests();
        record_us("rogue_metric", 1);
        add("rogue_metric", 1);
        let snapshot = snapshot_json();
        assert_eq!(snapshot["unknownKeyDropped"], 2);
        assert!(
            snapshot["timings"]
                .as_object()
                .expect("timings object")
                .is_empty()
        );
    }

    #[test]
    fn local_word_list_is_closed_and_disjoint_from_the_engine_allowlist() {
        // The two key spaces never mix: a local key is never an engine key.
        for key in super::KNOWN_KEYS {
            assert!(
                !ENGINE_TIMING_KEYS.contains(key) && !ENGINE_COUNTER_KEYS.contains(key),
                "local key {key} collides with the engine allowlist"
            );
        }
        // The engine allowlist stays the engine's closed 31-key vocabulary.
        assert_eq!(ENGINE_TIMING_KEYS.len(), 12);
        assert_eq!(ENGINE_COUNTER_KEYS.len(), 19);
        let mut all: Vec<_> = ENGINE_TIMING_KEYS
            .iter()
            .chain(ENGINE_COUNTER_KEYS)
            .collect();
        all.sort();
        let total = all.len();
        all.dedup();
        assert_eq!(total, all.len(), "engine allowlist keys are unique");
        assert_eq!(total, 31);
    }

    #[test]
    fn snapshot_keys_are_deterministic() {
        reset_for_tests();
        add(keys::RECEIVE_INGEST_ERR, 1);
        add(keys::RECEIVE_INGEST_EVENTS, 7);
        let snapshot = snapshot_json();
        let counters = snapshot["counters"].as_object().expect("counters object");
        let collected: Vec<&String> = counters.keys().collect();
        let mut sorted = collected.clone();
        sorted.sort();
        assert_eq!(collected, sorted, "BTreeMap key order is deterministic");
    }

    #[test]
    fn engine_filter_passes_whitelisted_keys_and_drops_the_rest() {
        let answer = serde_json::json!({
            "uptimeMs": 42_u64,
            "timings": {
                "receive_envelope": {"count": 3, "totalUs": 9, "maxUs": 4, "lastUs": 1},
                "rogue_timing": {"count": 1},
            },
            "counters": {
                "flush_barrier_ok": 2_u64,
                "receive_kind_content": 5_u64,
                "rogue_counter": 9_u64,
            },
            "futureTopLevel": {"anything": true},
            "unknownKeyDropped": 1_u64,
        });
        let (face, dropped) = filter_engine_snapshot(&answer);
        assert_eq!(face["uptimeMs"], 42);
        assert_eq!(face["unknownKeyDropped"], 1);
        assert_eq!(face["connectorDroppedKeys"], dropped);
        assert_eq!(
            dropped, 3,
            "rogue timing + rogue counter + future top-level key"
        );
        let timings = face["timings"].as_object().expect("timings object");
        assert_eq!(timings.len(), 1);
        assert_eq!(
            timings["receive_envelope"],
            serde_json::json!({"count": 3, "totalUs": 9, "maxUs": 4, "lastUs": 1})
        );
        let counters = face["counters"].as_object().expect("counters object");
        assert_eq!(counters.len(), 2);
        assert_eq!(counters["flush_barrier_ok"], 2);
        assert_eq!(counters["receive_kind_content"], 5);
    }

    #[test]
    fn engine_filter_is_honest_about_absent_and_malformed_scalars() {
        // No uptimeMs, malformed scalars, sections missing entirely: the
        // projection carries only what survived, and the drop count tells
        // the consumer it was lossy.
        let answer = serde_json::json!({
            "uptimeMs": "not-a-number",
            "unknownKeyDropped": -1,
        });
        let (face, dropped) = filter_engine_snapshot(&answer);
        assert!(face.get("uptimeMs").is_none());
        assert!(face.get("unknownKeyDropped").is_none());
        assert!(face.get("timings").is_none());
        assert!(face.get("counters").is_none());
        assert_eq!(face["connectorDroppedKeys"], dropped);
        assert_eq!(dropped, 2);
    }

    #[test]
    fn engine_filter_fails_closed_on_a_non_object_answer() {
        let (face, dropped) = filter_engine_snapshot(&serde_json::json!("junk"));
        assert_eq!(dropped, 1);
        assert_eq!(face["connectorDroppedKeys"], 1);
        assert_eq!(face.as_object().expect("object").len(), 1);
    }

    #[test]
    fn engine_filter_clean_answer_reports_a_meaningful_zero() {
        let mut answer = serde_json::json!({
            "uptimeMs": 7_u64,
            "timings": {},
            "counters": {},
        });
        // Every allowlisted key the engine may grow into is still refused
        // today unless the engine actually sent it — empty sections stay
        // empty, drop count 0.
        let (face, dropped) = filter_engine_snapshot(&answer);
        assert_eq!(dropped, 0);
        assert_eq!(face["connectorDroppedKeys"], 0);
        assert_eq!(face["timings"].as_object().unwrap().len(), 0);
        answer["counters"] = serde_json::json!({"flush_barrier_ok": "not-numeric"});
        let (face, dropped) = filter_engine_snapshot(&answer);
        assert_eq!(dropped, 1);
        assert_eq!(face["counters"].as_object().unwrap().len(), 0);
    }
}
