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
static WATCHDOG_RESTART_DEFERRALS: AtomicU64 = AtomicU64::new(0);
static WATCHDOG_RESTART_FAILURES: AtomicU64 = AtomicU64::new(0);
static WATCHDOG_RESTART_SUPPRESSIONS: AtomicU64 = AtomicU64::new(0);
static WATCHDOG_PING_TARGETS_SKIPPED: AtomicU64 = AtomicU64::new(0);
static WATCHDOG_LIVENESS_TICKS_SKIPPED: AtomicU64 = AtomicU64::new(0);
static HOST_PENDING_REQUESTS: AtomicU64 = AtomicU64::new(0);
static HOST_ACCOUNT_LANE_WAITS: AtomicU64 = AtomicU64::new(0);
static HOST_GLOBAL_LANE_WAITS: AtomicU64 = AtomicU64::new(0);
static RECEIVE_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);
static DECRYPTION_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Read-watermark write seams by source (contract revision 1.216, §4.216):
/// `byMessage` counts the explicit `messageId` target form, `byLatest` the
/// pre-1.216 whole-conversation form. The split is what makes the desktop's
/// migration to message-granular reads visible in the snapshot.
static READ_WATERMARK_BY_MESSAGE: AtomicU64 = AtomicU64::new(0);
static READ_WATERMARK_BY_LATEST: AtomicU64 = AtomicU64::new(0);
/// Explicit read targets refused because the id did not name a message of
/// the addressed account+conversation (or failed the opaque-id shape gate).
/// A rejection never moves the watermark.
static READ_WATERMARK_TARGETS_REJECTED: AtomicU64 = AtomicU64::new(0);
/// Read-watermark advances that hit the store (either source). Kept apart
/// from target rejections: this is an infrastructure fault, not a caller
/// mistake.
static READ_WATERMARK_STORE_FAILURES: AtomicU64 = AtomicU64::new(0);

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

/// The engine reported a decryption-error receive envelope (contract 1.165,
/// §4.163): an inbound envelope failed to decrypt and carries no message row.
/// Consumption counting only — the arm never acts on it (no session reset,
/// no retry), the count is what makes the failure class visible.
pub fn record_decryption_error() {
    DECRYPTION_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Process-lifetime decryption-error envelope count, for the metrics
/// snapshot (§4.163) — read back.
pub fn decryption_errors_total() -> u64 {
    DECRYPTION_ERRORS_TOTAL.load(Ordering::Relaxed)
}

/// A read leg used the explicit `messageId` target form and the advance
/// succeeded (contract revision 1.216, §4.216).
pub fn record_read_watermark_by_message() {
    READ_WATERMARK_BY_MESSAGE.fetch_add(1, Ordering::Relaxed);
}

/// A read leg used the whole-conversation (absent `messageId`) form — the
/// pre-1.216 behavior. Counted per prepare, including a conversation with
/// no incoming row (the no-op still took this path).
pub fn record_read_watermark_by_latest() {
    READ_WATERMARK_BY_LATEST.fetch_add(1, Ordering::Relaxed);
}

/// An explicit read target was refused: it named no message of the
/// addressed account+conversation (or failed the id shape gate). The
/// watermark is untouched and the request answers INVALID_REQUEST.
pub fn record_read_watermark_target_rejected() {
    READ_WATERMARK_TARGETS_REJECTED.fetch_add(1, Ordering::Relaxed);
}

/// A read-watermark advance hit the store (either source). The receipt legs
/// may still have ridden on the best-effort path; this counter keeps the
/// durable-divider cost visible.
pub fn record_read_watermark_store_failure() {
    READ_WATERMARK_STORE_FAILURES.fetch_add(1, Ordering::Relaxed);
}

/// Process-lifetime read-watermark counters, for the metrics snapshot and
/// tests: `(by_message, by_latest, targets_rejected, store_failures)`.
pub fn read_watermark_counters() -> (u64, u64, u64, u64) {
    (
        READ_WATERMARK_BY_MESSAGE.load(Ordering::Relaxed),
        READ_WATERMARK_BY_LATEST.load(Ordering::Relaxed),
        READ_WATERMARK_TARGETS_REJECTED.load(Ordering::Relaxed),
        READ_WATERMARK_STORE_FAILURES.load(Ordering::Relaxed),
    )
}

/// The watchdog executed an engine restart (immediate, or after its
/// backoff window elapsed).
pub fn record_watchdog_restart() {
    WATCHDOG_RESTARTS.fetch_add(1, Ordering::Relaxed);
}

/// Process-lifetime watchdog restart count, for the `runtime.doctor`
/// network face (§4.79) — the existing metric, read back.
pub fn watchdog_restarts_total() -> u64 {
    WATCHDOG_RESTARTS.load(Ordering::Relaxed)
}

/// The watchdog deferred a due restart behind its backoff window. Every
/// deferral is "the frequency budget said wait", never "the fault was
/// dropped": a dead receive path still has an armed deadline.
pub fn record_watchdog_restart_deferred() {
    WATCHDOG_RESTART_DEFERRALS.fetch_add(1, Ordering::Relaxed);
}

/// Process-lifetime deferred-restart count, for the metrics snapshot.
pub fn watchdog_restart_deferrals_total() -> u64 {
    WATCHDOG_RESTART_DEFERRALS.load(Ordering::Relaxed)
}

/// A watchdog restart attempt failed to bring the engine up. The deferred
/// backstop stays armed and retries; the counter keeps the failure class
/// visible instead of it disappearing behind a returned `Err`.
pub fn record_watchdog_restart_failed() {
    WATCHDOG_RESTART_FAILURES.fetch_add(1, Ordering::Relaxed);
}

/// Process-lifetime failed-restart-attempt count, for the metrics snapshot.
pub fn watchdog_restart_failures_total() -> u64 {
    WATCHDOG_RESTART_FAILURES.load(Ordering::Relaxed)
}

/// A watchdog restart was suppressed because a link flow is in flight
/// (restarting would invalidate the deviceLinkUri). Safety gate, not a
/// fault: it clears when the flow ends.
pub fn record_watchdog_restart_suppressed() {
    WATCHDOG_RESTART_SUPPRESSIONS.fetch_add(1, Ordering::Relaxed);
}

/// Process-lifetime link-suppressed restart count, for the metrics snapshot.
pub fn watchdog_restart_suppressions_total() -> u64 {
    WATCHDOG_RESTART_SUPPRESSIONS.load(Ordering::Relaxed)
}

/// One account left the liveness-ping candidate set because the engine does
/// not serve it: a store row absent from the engine's own account list, or a
/// `getUserStatus` the probe attributed to the account (another candidate
/// answered right after it). The row is never deleted here — reconciliation
/// owns that — this counter is what makes the exclusion visible.
pub fn record_watchdog_ping_target_skipped() {
    WATCHDOG_PING_TARGETS_SKIPPED.fetch_add(1, Ordering::Relaxed);
}

/// Process-lifetime excluded-ping-target count, for the metrics snapshot.
pub fn watchdog_ping_targets_skipped_total() -> u64 {
    WATCHDOG_PING_TARGETS_SKIPPED.load(Ordering::Relaxed)
}

/// One watchdog tick had no engine-confirmed ping target and skipped
/// liveness instead of counting a failure. A skip is never a restart cause:
/// pinging an account the engine does not serve proves nothing about the
/// receive path, it only churns the engine.
pub fn record_watchdog_liveness_tick_skipped() {
    WATCHDOG_LIVENESS_TICKS_SKIPPED.fetch_add(1, Ordering::Relaxed);
}

/// Process-lifetime liveness-skip tick count, for the metrics snapshot.
pub fn watchdog_liveness_ticks_skipped_total() -> u64 {
    WATCHDOG_LIVENESS_TICKS_SKIPPED.load(Ordering::Relaxed)
}

/// Process-lifetime degraded-receive drop count, for the `runtime.doctor`
/// network face (§4.79) — the existing metric, read back.
pub fn receive_dropped_total() -> u64 {
    RECEIVE_DROPPED.load(Ordering::Relaxed)
}

/// 某次 dispatch 在它自己的 per-account lane 上排队了（ADR 0005）：同账号
/// 并发是正常信号，持续增长说明单账号 lane 容量与桌面节奏不匹配。
pub fn record_host_account_lane_wait() {
    HOST_ACCOUNT_LANE_WAITS.fetch_add(1, Ordering::Relaxed);
}

/// 某次 dispatch 在全局兜底 lane 上排队了（ADR 0005）：正常容量下应长期
/// 为 0，增长意味着账号数或并发已超过「每账号容量 × 典型账号数」。
pub fn record_host_global_lane_wait() {
    HOST_GLOBAL_LANE_WAITS.fetch_add(1, Ordering::Relaxed);
}

/// Current number of admitted-but-unfinished host requests.
pub fn set_host_pending(depth: usize) {
    HOST_PENDING_REQUESTS.store(depth as u64, Ordering::Relaxed);
}

/// Read back the admitted-but-unfinished host request gauge (`runtime.doctor`
/// forensics and tests): the §4.168 bounded-answer face and the connection
/// exit reset both keep this value a truthful picture of the dispatch set.
pub fn host_pending_requests() -> u64 {
    HOST_PENDING_REQUESTS.load(Ordering::Relaxed)
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
        watchdog_restart_deferrals_total = WATCHDOG_RESTART_DEFERRALS.load(Ordering::Relaxed),
        watchdog_restart_failures_total = WATCHDOG_RESTART_FAILURES.load(Ordering::Relaxed),
        watchdog_restart_suppressions_total =
            WATCHDOG_RESTART_SUPPRESSIONS.load(Ordering::Relaxed),
        watchdog_ping_targets_skipped_total =
            WATCHDOG_PING_TARGETS_SKIPPED.load(Ordering::Relaxed),
        watchdog_liveness_ticks_skipped_total =
            WATCHDOG_LIVENESS_TICKS_SKIPPED.load(Ordering::Relaxed),
        host_pending_requests = HOST_PENDING_REQUESTS.load(Ordering::Relaxed),
        host_account_lane_waits_total = HOST_ACCOUNT_LANE_WAITS.load(Ordering::Relaxed),
        host_global_lane_waits_total = HOST_GLOBAL_LANE_WAITS.load(Ordering::Relaxed),
        receive_queue_depth = RECEIVE_QUEUE_DEPTH.load(Ordering::Relaxed),
        decryption_errors_total = DECRYPTION_ERRORS_TOTAL.load(Ordering::Relaxed),
        read_watermark_by_message_total = READ_WATERMARK_BY_MESSAGE.load(Ordering::Relaxed),
        read_watermark_by_latest_total = READ_WATERMARK_BY_LATEST.load(Ordering::Relaxed),
        read_watermark_targets_rejected_total =
            READ_WATERMARK_TARGETS_REJECTED.load(Ordering::Relaxed),
        read_watermark_store_failures_total =
            READ_WATERMARK_STORE_FAILURES.load(Ordering::Relaxed),
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
            record_decryption_error();
            record_watchdog_restart();
            record_watchdog_restart_deferred();
            record_watchdog_restart_failed();
            record_watchdog_restart_suppressed();
            record_watchdog_ping_target_skipped();
            record_watchdog_liveness_tick_skipped();
            set_host_pending(2);
            log_snapshot();
            // Leave the shared gauge clean for parallel tests (the §4.168
            // exit-reset semantics keep the honest value at zero).
            set_host_pending(0);
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
        // The watchdog's new visibility series: deferrals (backoff held a
        // restart back), failed attempts, and link-flow safety suppressions.
        assert!(output.contains("watchdog_restart_deferrals_total="));
        assert!(output.contains("watchdog_restart_failures_total="));
        assert!(output.contains("watchdog_restart_suppressions_total="));
        // The stale-row incident's visibility series: excluded ping targets
        // (an account the engine does not serve) and skipped liveness ticks
        // (no verified target — never a restart cause).
        assert!(output.contains("watchdog_ping_targets_skipped_total="));
        assert!(output.contains("watchdog_liveness_ticks_skipped_total="));
        assert!(output.contains("host_pending_requests="));
        assert!(output.contains("receive_queue_depth="));
        // §4.163: the decryption-error consumption counter rides the same
        // snapshot series (count-only, never content).
        assert!(output.contains("decryption_errors_total="));
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
