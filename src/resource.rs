// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::io;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("resource measurement io failed")]
    Io(#[from] io::Error),
    #[error("resource measurement failed")]
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessSample {
    pub label: String,
    pub pid: u32,
    pub rss_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceReport {
    pub scenario: String,
    pub platform: String,
    pub duration_ms: u64,
    pub samples: Vec<ProcessSample>,
    pub notes: Vec<String>,
}

pub fn sample_rss(pid: u32) -> Result<u64, ResourceError> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()?;
        if !output.status.success() {
            return Err(ResourceError::Failed);
        }
        let text = String::from_utf8_lossy(&output.stdout);
        let kb: u64 = text.trim().parse().map_err(|_| ResourceError::Failed)?;
        Ok(kb.saturating_mul(1024))
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = pid;
        Err(ResourceError::Failed)
    }
}

pub fn write_report(path: &Path, report: &ResourceReport) -> Result<(), ResourceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(report).unwrap())?;
    Ok(())
}

/// Short local soak used as a packaging smoke measurement. This is not a 24-hour gate.
pub fn measure_child_idle(
    label: &str,
    child: std::process::Child,
    settle: Duration,
) -> Result<(ResourceReport, std::process::Child), ResourceError> {
    let started = Instant::now();
    thread::sleep(settle);
    let pid = child.id();
    let rss = sample_rss(pid)?;
    let report = ResourceReport {
        scenario: "idle".into(),
        platform: crate::manifest::current_platform().into(),
        duration_ms: started.elapsed().as_millis() as u64,
        samples: vec![ProcessSample {
            label: label.into(),
            pid,
            rss_bytes: rss,
        }],
        notes: vec![
            "short settle sample only".into(),
            "not a 24-hour stability gate".into(),
        ],
    };
    Ok((report, child))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_current_process_rss() {
        let rss = sample_rss(std::process::id()).unwrap();
        assert!(rss > 0);
    }
}
