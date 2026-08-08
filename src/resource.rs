// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::io;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DEFAULT_RSS_PRESSURE_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_RSS_RECOVERY_BYTES: u64 = 420 * 1024 * 1024;
pub const DEFAULT_RSS_PRESSURE_SAMPLES: u32 = 3;
pub const DEFAULT_RSS_RECOVERY_SAMPLES: u32 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ResourcePressureState {
    Elevated,
    Recovered,
}

pub struct ResourcePressureTracker {
    elevated: bool,
    high_samples: u32,
    recovery_samples: u32,
    pressure_bytes: u64,
    recovery_bytes: u64,
    pressure_samples: u32,
    required_recovery_samples: u32,
}

impl ResourcePressureTracker {
    pub fn new(
        pressure_bytes: u64,
        recovery_bytes: u64,
        pressure_samples: u32,
        recovery_samples: u32,
    ) -> Result<Self, ResourceError> {
        if pressure_bytes == 0
            || recovery_bytes >= pressure_bytes
            || pressure_samples == 0
            || recovery_samples == 0
        {
            return Err(ResourceError::Failed);
        }
        Ok(Self {
            elevated: false,
            high_samples: 0,
            recovery_samples: 0,
            pressure_bytes,
            recovery_bytes,
            pressure_samples,
            required_recovery_samples: recovery_samples,
        })
    }

    pub fn observe(&mut self, rss_bytes: u64) -> Option<ResourcePressureState> {
        if self.elevated {
            if rss_bytes <= self.recovery_bytes {
                self.recovery_samples = self.recovery_samples.saturating_add(1);
                if self.recovery_samples >= self.required_recovery_samples {
                    self.elevated = false;
                    self.recovery_samples = 0;
                    self.high_samples = 0;
                    return Some(ResourcePressureState::Recovered);
                }
            } else {
                self.recovery_samples = 0;
            }
            return None;
        }

        if rss_bytes >= self.pressure_bytes {
            self.high_samples = self.high_samples.saturating_add(1);
            if self.high_samples >= self.pressure_samples {
                self.elevated = true;
                self.high_samples = 0;
                self.recovery_samples = 0;
                return Some(ResourcePressureState::Elevated);
            }
        } else {
            self.high_samples = 0;
        }
        None
    }
}

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
    #[cfg(target_os = "windows")]
    {
        use std::mem::{size_of, zeroed};

        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::ProcessStatus::{
            GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
        };
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_INFORMATION, PROCESS_VM_READ,
        };

        // SAFETY: the handle is checked, the buffer has the exact Win32 type and
        // size, and every successful OpenProcess call is paired with CloseHandle.
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, 0, pid);
            if handle.is_null() {
                return Err(ResourceError::Failed);
            }
            let mut counters: PROCESS_MEMORY_COUNTERS = zeroed();
            counters.cb = size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
            let sampled = GetProcessMemoryInfo(
                handle,
                &mut counters,
                size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
            );
            let _ = CloseHandle(handle);
            if sampled == 0 {
                return Err(ResourceError::Failed);
            }
            Ok(counters.WorkingSetSize as u64)
        }
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

    #[test]
    fn pressure_requires_sustained_high_samples_and_hysteresis_to_recover() {
        let mut tracker = ResourcePressureTracker::new(100, 80, 3, 2).unwrap();
        assert_eq!(tracker.observe(100), None);
        assert_eq!(tracker.observe(99), None);
        assert_eq!(tracker.observe(101), None);
        assert_eq!(tracker.observe(110), None);
        assert_eq!(tracker.observe(120), Some(ResourcePressureState::Elevated));
        assert_eq!(tracker.observe(81), None);
        assert_eq!(tracker.observe(80), None);
        assert_eq!(tracker.observe(79), Some(ResourcePressureState::Recovered));
    }

    #[test]
    fn pressure_policy_rejects_invalid_thresholds() {
        assert!(ResourcePressureTracker::new(0, 0, 1, 1).is_err());
        assert!(ResourcePressureTracker::new(100, 100, 1, 1).is_err());
        assert!(ResourcePressureTracker::new(100, 80, 0, 1).is_err());
    }
}
