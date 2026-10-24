// SPDX-License-Identifier: AGPL-3.0-only

//! Shared harness-timeout forensics: when a deadline'd harness read fires, the
//! panic must carry the evidence the investigation asked for (fixture request
//! log tails, connector liveness, and where the host provides the tools, the
//! connector's open descriptors and native stacks) — converting the old
//! indefinite hang into an evidence-dumping failure.

use std::fs;
use std::path::PathBuf;

/// The process dump routines only exist where the host provides them.
#[cfg(any(target_os = "macos", target_os = "linux"))]
const PROCESS_DUMP_AVAILABLE: bool = true;
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
const PROCESS_DUMP_AVAILABLE: bool = false;

/// Forensic scene for a harness-side hang: the fixture's own request log
/// tails (what the engine actually received, in order), whether the connector
/// process is still alive, and — where the host provides the tools — the
/// connector's open descriptors and native stacks. Best effort everywhere:
/// a dump failure never masks the timeout itself.
pub fn dump_connector_crash_scene() -> String {
    let mut report = String::from("== harness hang crash scene ==\n");
    let mut fixture_logs: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = fs::read_dir("/tmp") {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("kt-fixture-debug-") {
                fixture_logs.push(entry.path());
            }
        }
    }
    fixture_logs.sort();
    for log in fixture_logs {
        report.push_str(&format!("--- {} (tail) ---\n", log.display()));
        match fs::read_to_string(&log) {
            Ok(contents) => {
                let lines: Vec<&str> = contents.lines().collect();
                let tail_start = lines.len().saturating_sub(20);
                for line in &lines[tail_start..] {
                    report.push_str(line);
                    report.push('\n');
                }
                if lines.len() > 20 {
                    report.push_str(&format!("({} older lines omitted)\n", lines.len() - 20));
                }
            }
            Err(err) => report.push_str(&format!("unreadable: {err}\n")),
        }
    }
    let mut connector_pids: Vec<String> = Vec::new();
    let scene = std::process::Command::new("/bin/ps")
        .arg("-axo")
        .args(["pid=", "comm="])
        .output();
    if let Ok(output) = scene {
        let listing = String::from_utf8_lossy(&output.stdout);
        for line in listing.lines() {
            if line.contains("kt-signal-connector") {
                connector_pids.push(line.trim().to_string());
            }
        }
    }
    if connector_pids.is_empty() {
        report.push_str("connector process: NOT RUNNING (exited or never spawned)\n");
    } else {
        report.push_str(&format!(
            "connector process alive: {} (runner kills do not reap these; \
             the gate timeout was the only thing ending them before)\n",
            connector_pids.join(", ")
        ));
        if PROCESS_DUMP_AVAILABLE {
            for listed in &connector_pids {
                if let Some(pid) = listed.split_whitespace().next() {
                    let lsof = std::process::Command::new("/usr/sbin/lsof")
                        .args(["-nP", "-p", pid])
                        .output();
                    match lsof {
                        Ok(output) if !output.stdout.is_empty() => {
                            report.push_str(&format!("--- lsof -p {pid} ---\n"));
                            let text = String::from_utf8_lossy(&output.stdout);
                            let lines: Vec<&str> = text.lines().collect();
                            for line in lines.iter().take(40) {
                                report.push_str(line);
                                report.push('\n');
                            }
                            if lines.len() > 40 {
                                report.push_str(&format!(
                                    "({} more lines omitted)\n",
                                    lines.len() - 40
                                ));
                            }
                        }
                        _ => report.push_str("lsof unavailable or failed\n"),
                    }
                    let sample = std::process::Command::new("/usr/bin/sample")
                        .arg(pid)
                        .args(["1", "-file", "/dev/stdout"])
                        .output();
                    match sample {
                        Ok(output) => {
                            report.push_str(&format!("--- sample {pid} (1s) ---\n"));
                            let text = String::from_utf8_lossy(&output.stdout);
                            let lines: Vec<&str> = text.lines().collect();
                            for line in lines.iter().take(80) {
                                report.push_str(line);
                                report.push('\n');
                            }
                            if lines.len() > 80 {
                                report.push_str(&format!(
                                    "({} more lines omitted)\n",
                                    lines.len() - 80
                                ));
                            }
                        }
                        _ => report.push_str("sample unavailable or failed\n"),
                    }
                }
            }
        }
    }
    report
}
