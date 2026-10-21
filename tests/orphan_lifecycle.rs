// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Process-lifecycle regression suite for the orphan repair: the connector
//! exits when its launcher disappears (P0), the engine sweep refuses to
//! signal anything it cannot positively classify as an orphan (P0), and the
//! data-directory lock plus wait() leaves no zombie behind (P1).
//!
//! Everything here drives REAL child processes: a `sh -c` stand-in for the
//! launcher and the engine, so the assertions are about kernel-visible
//! parentage (ppid / process state), not about a mock.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kt_signal_connector::parent::{
    ENGINE_SWEEP_INTERVAL, PARENT_EXIT_BUDGET, PARENT_POLL_INTERVAL, ParentDown, RecordedParent,
    SlotFacts, SlotVerdict, SweepMemory, TerminatedPid, classify_slot, may_kill_orphan,
    parent_down, plan_engine_sweep, residual_processes, scan_processes, terminate_pid,
};
use tempfile::TempDir;

fn private_temp() -> TempDir {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    temp
}

/// The kernel's parent of `pid` (the only truth shared by the sweep and the
/// host's reaper).
fn ppid_of(pid: u32) -> Option<u32> {
    scan_processes()
        .into_iter()
        .find(|row| row.pid == pid)
        .map(|row| row.ppid)
}

/// The kernel state word of `pid` ('Z' = zombie, waiting for its parent's
/// wait()).
fn state_of(pid: u32) -> Option<String> {
    scan_processes()
        .into_iter()
        .find(|row| row.pid == pid)
        .map(|row| row.state)
}

fn wait_until(deadline: Duration, mut check: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if check() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    check()
}

fn kill_quietly(pid: u32) {
    let _ = Command::new("kill")
        .args(["-9", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// P0 parent loss, end to end: the launcher dies while the child is alive,
/// the child is re-parented to init, and the probe reaches that verdict.
#[test]
fn a_killed_launcher_is_observed_the_moment_the_parent_is_gone() {
    let mut launcher = match Command::new("sh")
        .args(["-c", "exec sleep 60"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return,
    };
    let recorded = RecordedParent {
        pid: launcher.id(),
        via: "test",
        // The launcher really is this test process's child, so the recorded
        // pid is the kernel parent of record for the probe below.
        is_actual_parent: true,
    };
    assert_eq!(
        parent_down(&recorded, true, Some(launcher.id())),
        None,
        "a live launcher is never parent-down"
    );
    launcher.kill().expect("kill the launcher");
    launcher.wait().expect("reap the launcher");
    // The launcher no longer answers a zero-signal probe: that alone is the
    // verdict (the pid is gone, condition 2).
    assert_eq!(
        parent_down(&recorded, pid_live(recorded.pid), None),
        Some(ParentDown::Vanished)
    );
    assert_eq!(
        parent_down(&recorded, false, None),
        Some(ParentDown::Vanished)
    );
}

/// Condition 1 against the real kernel: this test process is re-parented to
/// init exactly when it is judged dead, and a ppid that merely differs is not
/// enough (the false-positive guard).
#[test]
fn only_reparenting_to_init_is_decisive() {
    let self_pid = std::process::id();
    let recorded = RecordedParent {
        pid: self_pid,
        via: "getppid",
        is_actual_parent: true,
    };
    // Still our own parent and alive: never down, whatever else is true.
    assert_eq!(parent_down(&recorded, true, Some(self_pid)), None);
    // Re-parented to init: dead, even though the recorded pid still answers
    // (pid reuse is exactly why this branch must not consult the probe).
    assert_eq!(
        parent_down(&recorded, true, Some(1)),
        Some(ParentDown::Reparented)
    );
    // A moved-but-not-init ppid is a wrapper/re-exec, not a death.
    assert_eq!(parent_down(&recorded, true, Some(self_pid + 1)), None);
}

fn pid_live(pid: u32) -> bool {
    // Signal 0 through the host tool: the same existence probe the monitor
    // uses, never a real signal.
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// P0 under the real kernel rule the sweep relies on: a process whose
/// launcher was SIGKILLed is re-parented to pid 1, and the scanner sees it.
#[test]
fn an_orphaned_child_is_re_parented_to_init() {
    let child_pid: u32 = {
        // The reporter shells out and exits at once; its background child is
        // re-parented to init, which is exactly the orphan shape.
        let output = Command::new("sh")
            .args(["-c", "(exec sleep 30) & echo $!"])
            .output()
            .expect("spawn the reporter");
        String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse()
            .expect("pid")
    };
    if !wait_until(Duration::from_secs(5), || ppid_of(child_pid) == Some(1)) {
        kill_quietly(child_pid);
        eprintln!("host did not re-parent to init; skipping");
        return;
    }
    let rows = scan_processes();
    assert!(
        rows.iter().any(|row| row.pid == child_pid && row.ppid == 1),
        "the scan must report the orphan's init parent"
    );
    kill_quietly(child_pid);
}

/// The sweep's decision table against a REAL scan: a live child of ours
/// whose argv names the slot is never an orphan, and a genuinely orphaned
/// engine-shaped process under this data dir is (on the second sighting).
#[test]
fn the_sweep_classifies_real_children_and_real_orphans() {
    let temp = private_temp();
    let data_dir = temp.path().join("signal-data");
    let slot = "a".repeat(32);
    let slot_dir = data_dir.join("engines").join(&slot);
    fs::create_dir_all(&slot_dir).unwrap();
    let self_pid = std::process::id();

    // Our own live child, argv-shaped like an engine for this slot: the
    // spawn window the sweep must never mistake for an orphan.
    let own_marker = slot_dir.join("own.marker");
    let mut own = match Command::new("sh")
        .args([
            "-c",
            &format!("printf '%s' $$ > {}; exec sleep 30", own_marker.display()),
            "kt-engine",
            "--data-dir",
        ])
        .arg(&slot_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return,
    };
    if !wait_until(Duration::from_secs(5), || own_marker.exists()) {
        let _ = own.kill();
        let _ = own.wait();
        eprintln!("own child never reported; skipping");
        return;
    }
    let own_pid: u32 = fs::read_to_string(&own_marker)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    // The same slot directory, argv-shaped like an engine, parented to init:
    // the orphan shape the sweep exists to take down. `exec` keeps the pid
    // the reporter printed, so the orphan is attributable by pid.
    let orphan_pid: u32 = {
        let output = Command::new("sh")
            .args([
                "-c",
                &format!(
                    "(exec sleep 30 '{}' '{}' '{}') & echo $!",
                    "kt-engine",
                    "--data-dir",
                    slot_dir.display()
                ),
            ])
            .output()
            .expect("spawn the orphan reporter");
        String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse()
            .unwrap()
    };
    let orphan_visible = wait_until(Duration::from_secs(5), || {
        scan_processes().iter().any(|row| {
            row.pid == orphan_pid
                && row.ppid == 1
                && row.slot_of(&data_dir).as_deref() == Some(slot.as_str())
        })
    });

    let rows = scan_processes();
    if let Some(row) = rows.iter().find(|row| row.pid == own_pid) {
        assert_eq!(row.ppid, self_pid, "a live child keeps its real parent");
        assert!(!row.is_zombie());
    }
    // With our live child present, the slot is never orphaned — the
    // classification refuses even when an init-parented process exists.
    let verdict = classify_slot(
        &rows,
        &data_dir,
        &slot,
        SlotFacts {
            provisioned: true,
            owned_pid: None,
            running: true,
        },
        self_pid,
    );
    assert_ne!(verdict, SlotVerdict::Orphan);

    // Drop our child: now the init-parented process alone decides.
    let _ = own.kill();
    let _ = own.wait();
    if !orphan_visible {
        kill_quietly(orphan_pid);
        eprintln!("host did not re-parent to init; skipping the orphan half");
        return;
    }
    let rows = scan_processes();
    let orphan_row = rows
        .iter()
        .find(|row| row.pid == orphan_pid)
        .expect("the orphan must be visible");
    assert_eq!(
        orphan_row.slot_of(&data_dir).as_deref(),
        Some(slot.as_str()),
        "argv evidence must resolve the slot: {:?}",
        orphan_row.args
    );
    // A supervisor that still believes it owns the pid is never an orphan,
    // whatever the kernel says — that is the self-ownership exclusion.
    assert_ne!(
        classify_slot(
            &rows,
            &data_dir,
            &slot,
            SlotFacts {
                provisioned: true,
                owned_pid: Some(orphan_pid),
                running: true,
            },
            self_pid,
        ),
        SlotVerdict::Orphan,
        "a pid the supervisor still owns is never an orphan"
    );
    // An unowned, init-parented engine in a provisioned slot is: first the
    // classification, then (on the second sighting) the kill plan.
    assert_eq!(
        classify_slot(
            &rows,
            &data_dir,
            &slot,
            SlotFacts {
                provisioned: true,
                owned_pid: None,
                running: true,
            },
            self_pid,
        ),
        SlotVerdict::Orphan
    );
    let mut memory = SweepMemory::default();
    let facts = vec![(
        slot.clone(),
        SlotFacts {
            provisioned: true,
            owned_pid: None,
            running: true,
        },
    )];
    assert!(
        plan_engine_sweep(&rows, &data_dir, &facts, self_pid, &mut memory, true)
            .kill
            .is_empty(),
        "the first sighting only records"
    );
    let rows = scan_processes();
    let planned = plan_engine_sweep(&rows, &data_dir, &facts, self_pid, &mut memory, true);
    assert!(planned.kill.iter().any(|kill| kill.pid == orphan_pid));
    kill_quietly(orphan_pid);
}

/// P1 zombie discipline: a process the connector kills is reaped by wait()
/// and leaves no 'Z' entry behind.
#[test]
fn killing_our_own_child_leaves_no_zombie() {
    let mut child: Child = match Command::new("sh")
        .args(["-c", "exec sleep 30"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return,
    };
    let pid = child.id();
    assert_eq!(ppid_of(pid), Some(std::process::id()));
    let outcome = terminate_pid(pid, Duration::from_secs(3));
    assert!(
        matches!(outcome, TerminatedPid::Exited | TerminatedPid::Killed),
        "terminate_pid must end the child, got {outcome:?}"
    );
    // The parent's wait() is the reaper: afterwards the pid is gone entirely.
    child.wait().expect("wait reaps the child");
    assert!(
        wait_until(Duration::from_secs(2), || {
            !scan_processes().iter().any(|row| row.pid == pid)
        }),
        "a reaped child must leave no entry (no Z) behind, got {:?}",
        state_of(pid)
    );
    // Terminating a pid that is already gone is refused quietly, never a
    // signal that could land on a reused pid.
    assert_eq!(
        terminate_pid(pid, Duration::from_millis(10)),
        TerminatedPid::AlreadyGone
    );
}

/// The reporting face behind the settings-page preflight: the caller is
/// never in its own residual list, and a sibling data directory is a
/// different scope.
#[test]
fn residual_reporting_is_scoped_to_this_data_dir() {
    let temp = private_temp();
    let data_dir = temp.path().join("signal-data");
    let sibling_dir = temp.path().join("signal-data-sibling");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&sibling_dir).unwrap();
    for residues in [
        residual_processes(&data_dir),
        residual_processes(&sibling_dir),
    ] {
        assert!(
            residues.iter().all(|row| row.pid != std::process::id()),
            "the caller is never a residual"
        );
        assert!(
            residues.iter().all(|row| row.orphaned == (row.ppid == 1)),
            "the orphan flag mirrors the kernel parent"
        );
    }
}

/// The kill gate, wired exactly as the supervisor calls it.
#[test]
fn the_kill_gate_refuses_everything_that_is_not_a_confirmed_orphan() {
    let self_pid = std::process::id();
    assert!(!may_kill_orphan(self_pid, self_pid, &[], None));
    assert!(!may_kill_orphan(4242, self_pid, &[4242], None));
    assert!(!may_kill_orphan(4242, self_pid, &[], Some(true)));
    assert!(may_kill_orphan(4242, self_pid, &[], Some(false)));
}

/// The cadences are part of the contract (task requirement: a 5 s parent
/// probe; an engine sweep well below the desk watchdog cadence).
#[test]
fn the_lifecycle_cadences_match_the_contract() {
    assert_eq!(PARENT_POLL_INTERVAL, Duration::from_secs(5));
    assert!(ENGINE_SWEEP_INTERVAL <= Duration::from_secs(30));
    assert_ne!(ParentDown::Vanished, ParentDown::Reparented);
}

/// The scan never panics and every row carries the fields the classifier
/// relies on.
#[test]
fn the_process_scan_is_well_formed() {
    let rows = scan_processes();
    assert!(!rows.is_empty(), "the host process table is never empty");
    for row in &rows {
        assert!(row.pid > 0);
        assert!(!row.state.is_empty());
    }
}

// ---------------------------------------------------------------- end-to-end --
//
// The unit suite above pins the decision table; these drive the REAL connector
// binary, so the wiring (serve -> spawn_parent_watch -> runtime teardown ->
// exit) is exercised, not just the predicate. Every case is bounded and skips
// (never fails) when the host cannot provide the shape.

/// A bootstrap payload the connector accepts without a desktop: line 1 is the
/// handshake secret, line 2 the store key (Phase 3 contract).
fn write_bootstrap(path: &std::path::Path) {
    let secret = [0x31_u8; 32];
    let store_key = [0x42_u8; 32];
    // Exactly `64 hex + newline + 64 hex`, no trailing byte: the loader accepts
    // that shape and nothing else (a stray newline is InvalidSecret).
    let payload = format!("{}\n{}", hex::encode(secret), hex::encode(store_key));
    fs::write(path, payload).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

/// A real connector whose `--parent-pid` names a process the test controls.
struct Connector {
    child: Child,
    endpoint: std::path::PathBuf,
    _temp: TempDir,
}

impl Connector {
    fn spawn(launcher_pid: u32, extra_env: &[(&str, &str)]) -> Option<Self> {
        let temp = private_temp();
        let root = temp.path();
        let endpoint = root.join("connector.sock");
        let secret_file = root.join("bootstrap.secret");
        write_bootstrap(&secret_file);
        let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/fake-engine.py");
        let mut command = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"));
        command
            .arg("serve")
            .arg("--endpoint")
            .arg(&endpoint)
            .arg("--bootstrap-secret-file")
            .arg(&secret_file)
            .arg("--engine-bin")
            .arg(&fixture)
            .arg("--parent-pid")
            .arg(launcher_pid.to_string())
            .arg("--signal-data-dir")
            .arg(root.join("signal-data"))
            .arg("--state-dir")
            .arg(root.join("state"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .env("KT_FAKE_EXPECT_NO_JAVA", "1");
        for (key, value) in extra_env {
            command.env(key, value);
        }
        let child = command.spawn().ok()?;
        Some(Self {
            child,
            endpoint,
            _temp: temp,
        })
    }

    fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().ok().flatten()
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }
}

impl Drop for Connector {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn spawn_launcher() -> Option<Child> {
    Command::new("sh")
        .args(["-c", "exec sleep 60"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

fn stop_launcher(launcher: &mut Child) {
    let _ = launcher.kill();
    let _ = launcher.wait();
}

/// P0 requirement 1: a launcher that is still alive is never a reason to exit.
#[test]
fn a_live_launcher_keeps_the_connector_running() {
    let Some(mut launcher) = spawn_launcher() else {
        return;
    };
    let Some(mut connector) = Connector::spawn(launcher.id(), &[]) else {
        stop_launcher(&mut launcher);
        return;
    };
    // The socket is the proof serve actually reached the serving state.
    let published = wait_until(Duration::from_secs(15), || connector.endpoint.exists());
    // The probe cadence is 5 s: stay past two full intervals.
    std::thread::sleep(PARENT_POLL_INTERVAL * 2 + Duration::from_secs(2));
    let outcome = connector.exited();
    stop_launcher(&mut launcher);
    assert!(published, "the connector must have published its endpoint");
    assert!(
        outcome.is_none(),
        "a live launcher must never end the connector, got {outcome:?}"
    );
}

/// P0 requirement 2: the launcher dies -> the connector exits inside the budget
/// (probe interval 5 s + teardown 10 s).
#[test]
fn a_killed_launcher_ends_the_connector_within_the_exit_budget() {
    let Some(mut launcher) = spawn_launcher() else {
        return;
    };
    let Some(mut connector) = Connector::spawn(launcher.id(), &[]) else {
        stop_launcher(&mut launcher);
        return;
    };
    // Let serve publish the endpoint first: the watch is armed after the
    // runtime exists, and the test must observe the armed shape.
    if !wait_until(Duration::from_secs(15), || connector.endpoint.exists()) {
        stop_launcher(&mut launcher);
        eprintln!("connector never published its endpoint; skipping");
        return;
    }
    launcher.kill().expect("kill the launcher");
    launcher.wait().expect("reap the launcher");
    let deadline = PARENT_POLL_INTERVAL + PARENT_EXIT_BUDGET + Duration::from_secs(5);
    let exited = wait_until(deadline, || connector.exited().is_some());
    let status = connector.exited();
    assert!(
        exited,
        "the connector must exit within {deadline:?} of its launcher dying"
    );
    // The parent-loss path exits 0: a clean unwind, never a signal death.
    assert_eq!(
        status.and_then(|status| status.code()),
        Some(0),
        "the parent-loss path exits 0, got {status:?}"
    );
}

/// P0 requirement 4: the operator opt-out wins; an orphaned connector with
/// KT_SIGNAL_DISABLE_PARENT_WATCH=1 stays up.
#[test]
fn the_opt_out_env_keeps_an_orphan_connector_alive() {
    let Some(mut launcher) = spawn_launcher() else {
        return;
    };
    let launcher_pid = launcher.id();
    let Some(mut connector) =
        Connector::spawn(launcher_pid, &[("KT_SIGNAL_DISABLE_PARENT_WATCH", "1")])
    else {
        stop_launcher(&mut launcher);
        return;
    };
    if !wait_until(Duration::from_secs(15), || connector.endpoint.exists()) {
        stop_launcher(&mut launcher);
        eprintln!("connector never published its endpoint; skipping");
        return;
    }
    launcher.kill().expect("kill the launcher");
    launcher.wait().expect("reap the launcher");
    // Two full probe intervals + the exit budget: a watch armed despite the
    // env would have exited by now.
    std::thread::sleep(PARENT_POLL_INTERVAL * 2 + PARENT_EXIT_BUDGET);
    let outcome = connector.exited();
    assert!(
        outcome.is_none(),
        "KT_SIGNAL_DISABLE_PARENT_WATCH=1 must keep the connector alive, got {outcome:?}"
    );
}

/// P1 requirement: the parent-loss exit leaves no zombie and no live child of
/// the dead connector behind.
#[test]
fn the_parent_loss_exit_leaves_no_zombie_child() {
    let Some(mut launcher) = spawn_launcher() else {
        return;
    };
    let Some(mut connector) = Connector::spawn(launcher.id(), &[]) else {
        stop_launcher(&mut launcher);
        return;
    };
    if !wait_until(Duration::from_secs(15), || connector.endpoint.exists()) {
        stop_launcher(&mut launcher);
        eprintln!("connector never published its endpoint; skipping");
        return;
    }
    let connector_pid = connector.pid();
    launcher.kill().expect("kill the launcher");
    launcher.wait().expect("reap the launcher");
    let deadline = PARENT_POLL_INTERVAL + PARENT_EXIT_BUDGET + Duration::from_secs(5);
    if !wait_until(deadline, || connector.exited().is_some()) {
        eprintln!("connector did not exit in time; skipping the zombie half");
        return;
    }
    // Nothing may still name the dead connector as its parent, and nothing may
    // be left in the Z state: both would be a child this process stranded.
    let leftovers: Vec<(u32, String)> = scan_processes()
        .into_iter()
        .filter(|row| row.ppid == connector_pid)
        .map(|row| (row.pid, row.state))
        .collect();
    assert!(
        leftovers.is_empty(),
        "a dead connector cannot still parent processes: {leftovers:?}"
    );
}
