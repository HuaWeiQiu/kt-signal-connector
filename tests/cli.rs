// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::process::{Command, Stdio};

use ed25519_dalek::SigningKey;
use tempfile::TempDir;

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
    let stderr = String::from_utf8(output.stderr).expect("error output should be UTF-8");
    assert!(stderr.contains("Usage: kt-signal-connector <COMMAND>"));
}

#[test]
fn package_help_lists_lkg_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"))
        .args(["package", "--help"])
        .output()
        .expect("connector binary should run");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("help should be UTF-8");
    assert!(stdout.contains("manifest"));
    assert!(stdout.contains("stage"));
    assert!(stdout.contains("activate"));
    assert!(stdout.contains("rollback"));
    assert!(stdout.contains("verify"));
    assert!(stdout.contains("sign"));
}

#[test]
fn production_package_commands_generate_sign_and_verify_a_complete_bundle() {
    let binary = env!("CARGO_BIN_EXE_kt-signal-connector");
    let temp = TempDir::new().unwrap();
    let bundle = temp.path().join("bundle");
    for directory in ["bin", "licenses"] {
        fs::create_dir_all(bundle.join(directory)).unwrap();
    }
    for (relative, body) in [
        ("bin/kt-signal-connector", b"connector".as_slice()),
        (
            "licenses/kt-signal-connector.AGPL-3.0-only.txt",
            b"AGPL".as_slice(),
        ),
        ("licenses/libsignal.txt", b"AGPL".as_slice()),
        ("sbom.cdx.json", b"{}".as_slice()),
        ("build-record.json", b"{}".as_slice()),
        ("source.tar.gz", b"corresponding-source".as_slice()),
    ] {
        fs::write(bundle.join(relative), body).unwrap();
    }
    let manifest = bundle.join("manifest.json");
    let generated = Command::new(binary)
        .args(["package", "manifest", "--bundle-dir"])
        .arg(&bundle)
        .args([
            "--bundle-id",
            "test-production",
            "--platform",
            "macos-arm64",
            "--source-archive-path",
            "source.tar.gz",
            "--license",
            "kt-signal-connector:AGPL-3.0-only:licenses/kt-signal-connector.AGPL-3.0-only.txt",
            "--license",
            "libsignal:AGPL-3.0-only:licenses/libsignal.txt",
            "--output",
        ])
        .arg(&manifest)
        .output()
        .unwrap();
    assert!(generated.status.success(), "{:?}", generated);

    let seed = [7_u8; 32];
    let signing_key = SigningKey::from_bytes(&seed);
    let private_key = temp.path().join("private.key");
    let public_key = temp.path().join("public.key");
    fs::write(&private_key, seed).unwrap();
    fs::write(&public_key, signing_key.verifying_key().to_bytes()).unwrap();
    let signed = Command::new(binary)
        .args(["package", "sign", "--manifest"])
        .arg(&manifest)
        .args(["--key-id", "test-production-1", "--private-key-file"])
        .arg(&private_key)
        .arg("--output")
        .arg(&manifest)
        .output()
        .unwrap();
    assert!(signed.status.success(), "{:?}", signed);

    let verified = Command::new(binary)
        .args(["package", "verify", "--bundle-dir"])
        .arg(&bundle)
        .arg("--manifest")
        .arg(&manifest)
        .args([
            "--require-signature",
            "--trusted-key-id",
            "test-production-1",
            "--trusted-public-key-file",
        ])
        .arg(&public_key)
        .output()
        .unwrap();
    assert!(verified.status.success(), "{:?}", verified);
}

#[test]
fn package_stage_and_activate_reject_unsigned_bundles_unless_allowed() {
    let binary = env!("CARGO_BIN_EXE_kt-signal-connector");
    let temp = TempDir::new().unwrap();
    let bundle = temp.path().join("bundle");
    fs::create_dir_all(bundle.join("bin")).unwrap();
    fs::write(bundle.join("bin/kt-signal-connector"), b"connector").unwrap();
    let manifest = bundle.join("manifest.json");
    let generated = Command::new(binary)
        .args(["package", "manifest", "--bundle-dir"])
        .arg(&bundle)
        .args(["--bundle-id", "test-unsigned", "--output"])
        .arg(&manifest)
        .output()
        .unwrap();
    assert!(generated.status.success(), "{:?}", generated);

    let runtime = temp.path().join("runtime");
    let stage = |extra: &[&str]| {
        let mut command = Command::new(binary);
        command
            .args(["package", "stage", "--runtime-root"])
            .arg(&runtime)
            .args(["--version-id", "v1", "--bundle-dir"])
            .arg(&bundle)
            .args(extra);
        command.output().unwrap()
    };
    // Fail-closed: an unsigned bundle is rejected when no flag is passed.
    assert!(!stage(&[]).status.success());
    let staged = stage(&["--allow-unsigned"]);
    assert!(staged.status.success(), "{:?}", staged);

    let activate = |extra: &[&str]| {
        let mut command = Command::new(binary);
        command
            .args(["package", "activate", "--runtime-root"])
            .arg(&runtime)
            .args(extra);
        command.output().unwrap()
    };
    assert!(!activate(&[]).status.success());
    let activated = activate(&["--allow-unsigned"]);
    assert!(activated.status.success(), "{:?}", activated);
}

#[test]
fn serve_requires_exactly_one_bootstrap_secret_source() {
    let binary = env!("CARGO_BIN_EXE_kt-signal-connector");
    let required_args = [
        "--endpoint",
        "unused-endpoint",
        "--signal-cli",
        "unused-signal-cli",
        "--signal-data-dir",
        "unused-signal-data",
        "--state-dir",
        "unused-state",
    ];

    let missing = Command::new(binary)
        .arg("serve")
        .args(required_args)
        .output()
        .expect("connector binary should reject a missing secret source");
    assert_eq!(missing.status.code(), Some(2));

    let conflicting = Command::new(binary)
        .arg("serve")
        .args(required_args)
        .args([
            "--bootstrap-secret-file",
            "unused-secret",
            "--bootstrap-secret-stdin",
        ])
        .output()
        .expect("connector binary should reject conflicting secret sources");
    assert_eq!(conflicting.status.code(), Some(2));
}

/// Transitional launcher compatibility (ADR 0003): the desktop launcher
/// still passes `--java-home` on every start, so the flag must stay
/// accepted until the desktop-side cleanup batch lands.
#[test]
fn serve_help_keeps_the_java_home_launcher_compat_flag() {
    let output = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"))
        .args(["serve", "--help"])
        .output()
        .expect("connector binary should run");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("help should be UTF-8");
    assert!(stdout.contains("--java-home"));
}

#[test]
fn serve_rejects_an_invalid_socks_proxy_before_starting() {
    let binary = env!("CARGO_BIN_EXE_kt-signal-connector");
    let required_args = [
        "serve",
        "--endpoint",
        "unused-endpoint",
        "--bootstrap-secret-stdin",
        "--signal-cli",
        "unused-signal-cli",
        "--signal-data-dir",
        "unused-signal-data",
        "--state-dir",
        "unused-state",
    ];

    // Environment variable form (the path Desktop uses).
    let from_env = Command::new(binary)
        .args(required_args)
        .env("KT_SIGNAL_SOCKS_PROXY", "not a proxy")
        .output()
        .expect("connector binary should reject an invalid proxy env value");
    assert_eq!(from_env.status.code(), Some(2));
    let stderr = String::from_utf8(from_env.stderr).expect("error output should be UTF-8");
    assert!(stderr.contains("host:port"), "unexpected error: {stderr}");

    // Explicit flag form, missing port.
    let from_flag = Command::new(binary)
        .args(required_args)
        .args(["--socks-proxy", "127.0.0.1"])
        .output()
        .expect("connector binary should reject an invalid proxy flag value");
    assert_eq!(from_flag.status.code(), Some(2));
    let stderr = String::from_utf8(from_flag.stderr).expect("error output should be UTF-8");
    assert!(stderr.contains("host:port"), "unexpected error: {stderr}");
}

/// Phase 4 startup validation: proxy-group specs are checked before the
/// bootstrap payload is read, so a malformed launch fails with exit code 1
/// (not clap's usage exit 2) and diagnostics never echo any endpoint (R10).
#[test]
fn proxy_group_specs_fail_closed_before_bootstrap_is_read() {
    let binary = env!("CARGO_BIN_EXE_kt-signal-connector");
    // The secret file is never read: group validation fails first (that is
    // the point under test), so an unreadable path doubles as a tripwire.
    let base = [
        "serve",
        "--endpoint",
        "unused-endpoint",
        "--bootstrap-secret-file",
        "/nonexistent/bootstrap.secret",
        "--signal-cli",
        "unused-signal-cli",
        "--signal-data-dir",
        "unused-signal-data",
        "--state-dir",
        "unused-state",
    ];

    // A malformed spec (no ID=HOST:PORT split) aborts the launch.
    let malformed = Command::new(binary)
        .args(base)
        .args(["--proxy-group", "team-a"])
        .output()
        .expect("connector binary should reject a malformed group spec");
    assert_eq!(malformed.status.code(), Some(1));
    assert!(malformed.stdout.is_empty());
    let stderr = String::from_utf8(malformed.stderr).expect("error output should be UTF-8");
    assert!(stderr.contains("proxy group"), "unexpected error: {stderr}");

    // The implicit default group is reserved and cannot be redefined.
    let reserved = Command::new(binary)
        .args(base)
        .args(["--proxy-group", "default=10.20.30.40:1080"])
        .output()
        .expect("connector binary should reject a default redefinition");
    assert_eq!(reserved.status.code(), Some(1));
    let stderr = String::from_utf8(reserved.stderr).expect("error output should be UTF-8");
    assert!(stderr.contains("reserved"), "unexpected error: {stderr}");
    assert!(
        !stderr.contains("10.20.30.40"),
        "diagnostics must not echo proxy endpoints"
    );

    // More than MAX_PROXY_GROUPS entries in total (implicit default plus
    // eight explicit ones) exceeds the hard ceiling, via the env source.
    let mut env_spec = String::new();
    for index in 0..8 {
        if index > 0 {
            env_spec.push(',');
        }
        env_spec.push_str(&format!("team-{index}=10.20.30.{index}:1080"));
    }
    let ceiling = Command::new(binary)
        .args(base)
        .env("KT_SIGNAL_PROXY_GROUPS", &env_spec)
        .output()
        .expect("connector binary should reject an over-ceiling plan");
    assert_eq!(ceiling.status.code(), Some(1));
    let stderr = String::from_utf8(ceiling.stderr).expect("error output should be UTF-8");
    assert!(
        stderr.contains("too many proxy groups"),
        "unexpected error: {stderr}"
    );
    assert!(
        !stderr.contains("10.20.30."),
        "diagnostics must not echo proxy endpoints"
    );
}

/// Contract 1.147 (§4.145): the R2 TCP leg's argv face — `--listen` is the
/// endpoint's strict alternative (the same argv discipline as the secret
/// sources), and `--endpoint` keeps its old requiredness whenever `--listen`
/// is absent, so a legacy launcher's argv parses byte-identically.
#[test]
fn serve_takes_listen_as_the_strict_alternative_to_endpoint() {
    let binary = env!("CARGO_BIN_EXE_kt-signal-connector");
    let tail = [
        "--bootstrap-secret-stdin",
        "--signal-cli",
        "unused-signal-cli",
        "--signal-data-dir",
        "unused-signal-data",
        "--state-dir",
        "unused-state",
    ];

    let both = Command::new(binary)
        .arg("serve")
        .args(["--endpoint", "unused-endpoint", "--listen", "127.0.0.1:0"])
        .args(tail)
        .output()
        .expect("connector binary should reject the endpoint pair");
    assert_eq!(both.status.code(), Some(2));
    assert!(both.stdout.is_empty());
    let stderr = String::from_utf8(both.stderr).expect("error output should be UTF-8");
    assert!(stderr.contains("--listen"), "unexpected error: {stderr}");

    let neither = Command::new(binary)
        .arg("serve")
        .args(tail)
        .output()
        .expect("connector binary should reject the missing endpoint");
    assert_eq!(neither.status.code(), Some(2));
    assert!(neither.stdout.is_empty());
    let stderr = String::from_utf8(neither.stderr).expect("error output should be UTF-8");
    assert!(stderr.contains("--endpoint"), "unexpected error: {stderr}");
}

/// The §4.145 exposure boundary at the argv gate: only an IPv4 loopback
/// host:port parses — wildcard, off-host, IPv6 loopback, and a port-less
/// value are all usage errors before anything binds.
#[test]
fn serve_rejects_non_loopback_listen_addresses_at_parse_time() {
    let binary = env!("CARGO_BIN_EXE_kt-signal-connector");
    let tail = [
        "--bootstrap-secret-stdin",
        "--signal-cli",
        "unused-signal-cli",
        "--signal-data-dir",
        "unused-signal-data",
        "--state-dir",
        "unused-state",
    ];
    let run = |address: &str| {
        Command::new(binary)
            .arg("serve")
            .args(["--listen", address])
            .args(tail)
            .output()
            .expect("connector binary should reject the listen address")
    };

    for address in ["10.1.2.3:8080", "0.0.0.0:0", "[::1]:0"] {
        let output = run(address);
        assert_eq!(output.status.code(), Some(2), "address: {address}");
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8(output.stderr).expect("error output should be UTF-8");
        assert!(
            stderr.contains("loopback"),
            "address {address} must be rejected naming the loopback boundary: {stderr}"
        );
    }

    let portless = run("127.0.0.1");
    assert_eq!(portless.status.code(), Some(2));
    assert!(portless.stdout.is_empty());
}

/// `--listen` alone is a complete endpoint face: the argv parses (this launch
/// then fails at the deliberately empty stdin payload with the classified
/// exit 1, never a usage exit 2) and a failed launch announces nothing — the
/// stdout announce only ever follows a successful bind.
#[test]
fn serve_accepts_the_listen_face_past_argv_and_fails_without_an_announce() {
    let binary = env!("CARGO_BIN_EXE_kt-signal-connector");
    let temp = TempDir::new().unwrap();
    let output = Command::new(binary)
        .arg("serve")
        .args(["--listen", "127.0.0.1:0"])
        .args([
            "--bootstrap-secret-stdin",
            "--signal-cli",
            "unused-signal-cli",
            "--signal-data-dir",
        ])
        .arg(temp.path().join("signal-data"))
        .args(["--state-dir"])
        .arg(temp.path().join("state"))
        .stdin(Stdio::null())
        .output()
        .expect("connector binary should run the listen face");
    assert_eq!(output.status.code(), Some(1));
    assert!(
        output.stdout.is_empty(),
        "a failed launch must not announce: {:?}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8(output.stderr).expect("error output should be UTF-8");
    assert!(
        stderr.contains("bootstrap secret"),
        "the failure should be the payload classification: {stderr}"
    );
}
