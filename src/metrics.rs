// SPDX-License-Identifier: AGPL-3.0-only

//! Redacted in-process metrics, promised by docs/implementation-plan.md §8.
//!
//! Atomic counters and gauges only; a periodic task logs one structured line
//! per series through `tracing`. There is no external metrics service and no
//! listener. Every label is a fixed classification: method categories, result
//! classes, counts, sizes, and durations. No message content, phone numbers,
//! contacts, secrets, or key paths ever appear here.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::methods;
use crate::protocol::ApiError;

/// Coarse host-method categories (plan §8 "method categories"). The raw method
/// string comes from the peer and is never used as a label.
pub const METHOD_CLASSES: [&str; 7] = [
    "control", "accounts", "link", "read", "send", "contacts", "unknown",
];

/// Bounded result classification (plan §8 "result classes"): `ok`, `rejected`
/// (deterministic non-retryable refusal), `failed` (retryable failure), and
/// `unknown` (a mutating call whose outcome cannot be determined).
pub const RESULT_CLASSES: [&str; 4] = ["ok", "rejected", "failed", "unknown"];

/// Fixed metrics label for one method (plan §8 "method categories"),
/// derived from the single method table (`methods::METHODS`). The raw method
/// string comes from the peer and is never used as a label.
pub fn method_class(method: &str) -> &'static str {
    methods::metrics_class(method)
}

pub fn result_class(error: Option<&ApiError>) -> &'static str {
    match error {
        None => "ok",
        Some(error) if error.code.ends_with("OUTCOME_UNKNOWN") => "unknown",
        Some(error) if error.retryable => "failed",
        Some(_) => "rejected",
    }
}

struct ClassMetrics {
    ok: AtomicU64,
    rejected: AtomicU64,
    failed: AtomicU64,
    unknown: AtomicU64,
    duration_ms_total: AtomicU64,
    duration_ms_max: AtomicU64,
}

impl ClassMetrics {
    const fn new() -> Self {
        Self {
            ok: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            unknown: AtomicU64::new(0),
            duration_ms_total: AtomicU64::new(0),
            duration_ms_max: AtomicU64::new(0),
        }
    }

    fn counter(&self, result_class: &str) -> &AtomicU64 {
        match result_class {
            "ok" => &self.ok,
            "rejected" => &self.rejected,
            "failed" => &self.failed,
            _ => &self.unknown,
        }
    }
}

static REQUESTS: [ClassMetrics; METHOD_CLASSES.len()] =
    [const { ClassMetrics::new() }; METHOD_CLASSES.len()];

static RECEIVE_DROPPED: AtomicU64 = AtomicU64::new(0);
static WATCHDOG_RESTARTS: AtomicU64 = AtomicU64::new(0);
static HOST_PENDING_REQUESTS: AtomicU64 = AtomicU64::new(0);
static RECEIVE_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);

fn class_metrics(method_class: &str) -> &'static ClassMetrics {
    let index = METHOD_CLASSES
        .iter()
        .position(|class| *class == method_class)
        .unwrap_or(METHOD_CLASSES.len() - 1);
    &REQUESTS[index]
}

/// One completed host dispatch. `method_class`/`result_class` come from the
/// classifiers above; durations feed a total (for the mean) and a max.
pub fn record_request(method_class: &str, result_class: &str, duration: Duration) {
    let metrics = class_metrics(method_class);
    metrics
        .counter(result_class)
        .fetch_add(1, Ordering::Relaxed);
    let duration_ms = duration.as_millis().min(u64::MAX as u128) as u64;
    metrics
        .duration_ms_total
        .fetch_add(duration_ms, Ordering::Relaxed);
    metrics
        .duration_ms_max
        .fetch_max(duration_ms, Ordering::Relaxed);
}

/// A receive was dropped because the receive queue stayed full for the whole
/// enqueue timeout (engine-side degraded-storage path).
pub fn record_receive_drop() {
    RECEIVE_DROPPED.fetch_add(1, Ordering::Relaxed);
}

/// The watchdog executed an engine restart (initial or pending retry).
pub fn record_watchdog_restart() {
    WATCHDOG_RESTARTS.fetch_add(1, Ordering::Relaxed);
}

/// Current number of admitted-but-unfinished host requests.
pub fn set_host_pending(depth: usize) {
    HOST_PENDING_REQUESTS.store(depth as u64, Ordering::Relaxed);
}

/// A receive entered the persistence queue.
pub fn receive_queue_enqueued() {
    RECEIVE_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed);
}

/// A receive left the persistence queue towards the store.
pub fn receive_queue_drained() {
    RECEIVE_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
}

/// Log the current counters/gauges as structured, redacted lines. Counters are
/// process-lifetime cumulative; gauges are instantaneous.
pub fn log_snapshot() {
    tracing::info!(
        target: "metrics",
        receive_dropped_total = RECEIVE_DROPPED.load(Ordering::Relaxed),
        watchdog_restarts_total = WATCHDOG_RESTARTS.load(Ordering::Relaxed),
        host_pending_requests = HOST_PENDING_REQUESTS.load(Ordering::Relaxed),
        receive_queue_depth = RECEIVE_QUEUE_DEPTH.load(Ordering::Relaxed),
        "connector metrics snapshot"
    );
    for (index, class) in METHOD_CLASSES.iter().enumerate() {
        let metrics = &REQUESTS[index];
        let ok = metrics.ok.load(Ordering::Relaxed);
        let rejected = metrics.rejected.load(Ordering::Relaxed);
        let failed = metrics.failed.load(Ordering::Relaxed);
        let unknown = metrics.unknown.load(Ordering::Relaxed);
        let requests = ok + rejected + failed + unknown;
        if requests == 0 {
            continue;
        }
        tracing::info!(
            target: "metrics",
            method_class = *class,
            requests,
            rejected,
            failed,
            unknown,
            duration_ms_total = metrics.duration_ms_total.load(Ordering::Relaxed),
            duration_ms_max = metrics.duration_ms_max.load(Ordering::Relaxed),
            "host request metrics"
        );
    }
}

/// Per-engine RSS gauges for the snapshot (optimization-plan §6.4 M3.4): the
/// latest sample of each proxy group's engine, keyed by the launcher-defined
/// group id — an opaque id, the same classification the runtime status and
/// resource-pressure events already carry, never content. A group without a
/// sample (engine not running yet) logs nothing rather than a misleading 0.
pub fn log_engine_rss(samples: &[(String, Option<u64>)]) {
    for (group_id, rss_bytes) in samples {
        if let Some(rss_bytes) = rss_bytes {
            tracing::info!(
                target: "metrics",
                group_id = %group_id,
                rss_bytes,
                "engine rss sample"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_host_method_maps_to_a_known_class() {
        for (method, expected) in [
            ("runtime.status", "control"),
            ("runtime.start", "control"),
            ("runtime.stop", "control"),
            ("accounts.list", "accounts"),
            ("accounts.deleteLocalData", "accounts"),
            ("link.start", "link"),
            ("link.finish", "link"),
            ("link.cancel", "link"),
            ("conversations.list", "read"),
            ("messages.list", "read"),
            ("messages.getText", "read"),
            ("messages.sendText", "send"),
            ("messages.remoteDelete", "send"),
            ("messages.sendReaction", "send"),
            ("contacts.setLocalAlias", "send"),
            ("presence.setTypingMessage", "send"),
            ("contacts.sync", "contacts"),
            ("contacts.list", "contacts"),
            ("groups.get", "contacts"),
        ] {
            assert_eq!(method_class(method), expected, "method {method}");
        }
        assert_eq!(method_class("anything.else"), "unknown");
    }

    #[test]
    fn result_classes_split_rejection_failure_and_unknown() {
        assert_eq!(result_class(None), "ok");
        assert_eq!(
            result_class(Some(&ApiError::new("INVALID_REQUEST", "bad", false))),
            "rejected"
        );
        assert_eq!(
            result_class(Some(&ApiError::new("UPSTREAM_TIMEOUT", "slow", true))),
            "failed"
        );
        assert_eq!(
            result_class(Some(&ApiError::new(
                "SEND_OUTCOME_UNKNOWN",
                "unknown",
                false
            ))),
            "unknown"
        );
    }

    #[test]
    fn counters_are_keyed_by_method_and_result_class() {
        let before = class_metrics("send").failed.load(Ordering::Relaxed);
        record_request("send", "failed", Duration::from_millis(7));
        let metrics = class_metrics("send");
        assert_eq!(metrics.failed.load(Ordering::Relaxed), before + 1);
        assert!(metrics.duration_ms_max.load(Ordering::Relaxed) >= 7);
    }

    /// In-memory capture for snapshot-output assertions.
    #[derive(Clone, Default)]
    struct Buffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Buffer {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
        type Writer = Buffer;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn captured_output(log: impl FnOnce()) -> String {
        let buffer = Buffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buffer.clone())
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, log);
        String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap()
    }

    /// The snapshot lines are the whole metrics contract: fixed series keys,
    /// classified labels, counts and durations only. Counters are process-wide
    /// and shared with parallel tests, so this asserts structure and the
    /// absence of any content-shaped data, not exact values.
    #[test]
    fn snapshot_output_is_redacted_and_structured() {
        let output = captured_output(|| {
            record_request("send", "ok", Duration::from_millis(3));
            record_receive_drop();
            record_watchdog_restart();
            set_host_pending(2);
            log_snapshot();
            log_engine_rss(&[
                ("default".to_string(), Some(196 * 1024 * 1024)),
                ("team-a".to_string(), None),
            ]);
        });
        assert!(output.contains("connector metrics snapshot"));
        // Gauges are process-wide and shared with parallel tests, so assert
        // the series keys, never exact values.
        assert!(output.contains("receive_dropped_total="));
        assert!(output.contains("watchdog_restarts_total="));
        assert!(output.contains("host_pending_requests="));
        assert!(output.contains("receive_queue_depth="));
        assert!(output.contains("host request metrics"));
        assert!(output.contains("method_class=\"send\""));
        // Per-engine RSS gauges (M3.4): one line per sampled group, keyed by
        // the opaque group id; a group without a sample stays silent instead
        // of logging a misleading zero.
        assert!(output.contains("engine rss sample"));
        assert!(output.contains("group_id=default"));
        assert!(output.contains("rss_bytes=205520896"));
        assert!(!output.contains("group_id=team-a"));
        // Nothing content-shaped ever appears: no numbers, addresses, bodies.
        assert!(!output.contains("+1555"));
        assert!(!output.contains("text"));
    }

    #[test]
    fn engine_rss_lines_skip_unsampled_groups() {
        let output = captured_output(|| {
            log_engine_rss(&[
                ("stopped-group".to_string(), None),
                ("g1".to_string(), Some(1024)),
            ]);
        });
        assert!(output.contains("group_id=g1"));
        assert!(output.contains("rss_bytes=1024"));
        assert!(!output.contains("stopped-group"));
    }
}
