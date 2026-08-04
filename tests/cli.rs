// SPDX-License-Identifier: AGPL-3.0-only

use std::process::Command;

#[test]
fn version_reports_package_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"))
        .arg("--version")
        .output()
        .expect("connector binary should run");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("version output should be UTF-8"),
        format!("kt-signal-connector {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn missing_command_fails_closed() {
    let output = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"))
        .output()
        .expect("connector binary should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).expect("error output should be UTF-8"),
        "kt-signal-connector: runtime implementation is not available yet\n"
    );
}
