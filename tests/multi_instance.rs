// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! M1 multi-instance conflict guards (ADR 0002), at both library and
//! binary level: same-endpoint rejection, same-data-dir rejection with
//! crash-safe takeover, and overlong-endpoint rejection.

use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use kt_signal_connector::datalock::{DataDirLock, lock_plan_data_dirs};
use kt_signal_connector::groups::build_group_plan;
use kt_signal_connector::ipc::LocalListener;
use tempfile::TempDir;

fn private_temp() -> TempDir {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    temp
}

/// The two-line bootstrap payload shape (line 1 handshake secret, line 2
/// store key), written with the privacy the loader requires.
fn write_payload_file(path: &Path) {
    fs::write(path, format!("{}\n{}", "7".repeat(64), "5".repeat(64))).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn serve_command(temp: &TempDir, endpoint: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"));
    command
        .arg("serve")
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--signal-cli")
        .arg("/nonexistent/signal-cli")
        .arg("--signal-data-dir")
        .arg(temp.path().join("signal-data"))
        .arg("--state-dir")
        .arg(temp.path().join("state"));
    command
}

/// Library-level semantics of the occupancy lock: second taker rejected
/// with a diagnostic that names the group (never the path), a different
/// group's subdirectory under the same locked root stays lockable, and
/// dropping the holder releases occupancy.
#[test]
fn data_dir_lock_rejects_a_second_taker_and_releases_on_drop() {
    let temp = private_temp();
    let data_dir = temp.path().join("signal-data");
    let first = DataDirLock::acquire("default", &data_dir).unwrap();

    let error = DataDirLock::acquire("default", &data_dir).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    let message = error.to_string();
    assert!(
        message.contains("default"),
        "must name the group: {message}"
    );
    assert!(
        message.contains("already in use"),
        "must say what happened: {message}"
    );

    // A sibling connector's group subdirectory under the same root is a
    // different lock file and must not conflict (ADR 0002 disjointness is
    // per data directory, not per root).
    let _sibling = DataDirLock::acquire("team-b", &data_dir.join("proxy-groups/team-b")).unwrap();

    drop(first);
    DataDirLock::acquire("default", &data_dir)
        .unwrap_or_else(|error| panic!("drop must release occupancy: {error}"));
}

/// Plan-level semantics: the lock covers the `default` root and every
/// `proxy-groups/<groupId>/` subdirectory; a conflict anywhere fails the
/// whole start and rolls back every lock already taken.
#[test]
fn plan_locks_cover_the_default_root_and_each_group_subdirectory() {
    let temp = private_temp();
    let root = temp.path().join("signal-data");
    // A sibling connector already occupies only team-b's subdirectory.
    let holder = DataDirLock::acquire("team-b", &root.join("proxy-groups/team-b")).unwrap();

    let plan = build_group_plan(
        &[
            "team-a=127.0.0.1:1080".to_string(),
            "team-b=127.0.0.1:1081".to_string(),
        ],
        None,
        None,
        &root,
    )
    .unwrap();
    let error = lock_plan_data_dirs(&plan).unwrap_err();
    assert!(
        error.contains("team-b") && error.contains("already in use"),
        "must name the conflicting group: {error}"
    );
    // All-or-nothing: team-a's lock from the failed attempt was released,
    // so it is lockable again from this very process (flock conflicts are
    // per open file description, so this is a real re-acquisition).
    let team_a = DataDirLock::acquire("team-a", &root.join("proxy-groups/team-a"))
        .unwrap_or_else(|error| panic!("failed plan must roll back: {error}"));

    drop(holder);
    drop(team_a);
    let _locks = lock_plan_data_dirs(&plan)
        .unwrap_or_else(|error| panic!("whole plan must lock once free: {error}"));
}

/// Child half of `a_killed_holder_releases_the_data_dir_lock`. Activated
/// only via `KT_SIGNAL_LOCK_DIR` (the crash test drives this very test
/// binary); a normal run of this test is a no-op.
#[test]
fn data_dir_lock_child_holds_until_killed() {
    let Some(dir) = std::env::var("KT_SIGNAL_LOCK_DIR").ok() else {
        return;
    };
    let data_dir = PathBuf::from(dir);
    let _lock = DataDirLock::acquire("child", &data_dir).expect("child acquires the lock");
    fs::write(data_dir.join("child-ready"), b"ready").unwrap();
    // Hold until the parent's SIGKILL; cap the wait so an orphaned child
    // still exits by itself.
    std::thread::sleep(Duration::from_secs(60));
}

/// Stale-lock safety, the strongest form: a holder killed with SIGKILL
/// runs no destructors, and the kernel still releases its flock — the next
/// start takes the data directory over with no stale-lock ceremony.
#[test]
fn a_killed_holder_releases_the_data_dir_lock() {
    let temp = private_temp();
    let data_dir = temp.path().join("signal-data");
    let ready = data_dir.join("child-ready");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "--nocapture",
            "data_dir_lock_child_holds_until_killed",
        ])
        .env("KT_SIGNAL_LOCK_DIR", &data_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() {
        assert!(
            Instant::now() < deadline,
            "child never reported holding the lock"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    child.kill().unwrap();
    child.wait().unwrap();

    DataDirLock::acquire("default", &data_dir)
        .unwrap_or_else(|error| panic!("a killed holder must not brick the data dir: {error}"));
}

/// Binary-level same-data-dir rejection: `serve` over an occupied root
/// exits 1 before the bootstrap payload is read — the unreadable secret
/// path doubles as the ordering tripwire (same pattern as the proxy-group
/// validation test in tests/cli.rs).
#[test]
fn serve_on_an_occupied_data_dir_fails_closed_before_reading_the_secret() {
    let temp = private_temp();
    let data_dir = temp.path().join("signal-data");
    // Instance 1's occupancy, held by the same kernel mechanism serve uses.
    let _holder = DataDirLock::acquire("default", &data_dir).unwrap();

    let output = serve_command(&temp, &temp.path().join("connector.sock"))
        .arg("--bootstrap-secret-file")
        .arg("/nonexistent/bootstrap.secret")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("error output should be UTF-8");
    assert!(
        stderr.contains("already in use") && stderr.contains("default"),
        "unexpected error: {stderr}"
    );
}

/// Binary-level proxy-group subdirectory coverage: occupancy of
/// `proxy-groups/<groupId>/` alone rejects a second `serve` planning that
/// group, even with the data-dir root itself free.
#[test]
fn serve_on_an_occupied_proxy_group_subdirectory_fails_closed() {
    let temp = private_temp();
    let data_dir = temp.path().join("signal-data");
    let _holder = DataDirLock::acquire("team-b", &data_dir.join("proxy-groups/team-b")).unwrap();

    let output = serve_command(&temp, &temp.path().join("connector.sock"))
        .arg("--bootstrap-secret-file")
        .arg("/nonexistent/bootstrap.secret")
        .arg("--proxy-group")
        .arg("team-b=127.0.0.1:1081")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).expect("error output should be UTF-8");
    assert!(
        stderr.contains("already in use") && stderr.contains("team-b"),
        "unexpected error: {stderr}"
    );
}

/// Binary-level same-endpoint rejection: a second `serve` whose endpoint
/// path is already a published socket exits 1 with the existing
/// first-bind-wins diagnostic.
#[tokio::test]
async fn a_second_serve_on_an_existing_endpoint_fails_closed() {
    let temp = private_temp();
    let endpoint = temp.path().join("connector.sock");
    // Instance 1's listener: the published 0600 socket file is exactly
    // what the second instance must see.
    let _listener = LocalListener::bind(&endpoint).unwrap();
    let secret_file = temp.path().join("bootstrap.secret");
    write_payload_file(&secret_file);

    let output = serve_command(&temp, &endpoint)
        .arg("--bootstrap-secret-file")
        .arg(&secret_file)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).expect("error output should be UTF-8");
    assert!(
        stderr.contains("endpoint already exists"),
        "unexpected error: {stderr}"
    );
}

/// Binary-level overlong-endpoint rejection: `serve` exits 1 at startup
/// with a readable error naming the limit instead of a bare EINVAL.
#[test]
fn serve_rejects_an_overlong_endpoint_with_a_readable_error() {
    let temp = private_temp();
    let secret_file = temp.path().join("bootstrap.secret");
    write_payload_file(&secret_file);

    // Nest private directories until the canonical path exceeds the
    // platform sun_path limit (103 macOS / 107 Linux) with headroom.
    let mut dir = fs::canonicalize(temp.path()).unwrap();
    while dir.as_os_str().as_encoded_bytes().len() <= 120 {
        dir = dir.join("d".repeat(40));
        fs::create_dir_all(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let endpoint = dir.join("connector.sock");

    let output = serve_command(&temp, &endpoint)
        .arg("--bootstrap-secret-file")
        .arg(&secret_file)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).expect("error output should be UTF-8");
    assert!(
        stderr.contains("unix-socket path limit"),
        "unexpected error: {stderr}"
    );
}
