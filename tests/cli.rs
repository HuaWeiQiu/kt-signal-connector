// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::process::Command;

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
    for directory in ["bin", "jre", "licenses"] {
        fs::create_dir_all(bundle.join(directory)).unwrap();
    }
    for (relative, body) in [
        ("bin/kt-signal-connector", b"connector".as_slice()),
        ("bin/signal-cli", b"signal-cli".as_slice()),
        ("jre/release", b"JAVA_VERSION=\"25\"\n".as_slice()),
        (
            "licenses/kt-signal-connector.AGPL-3.0-only.txt",
            b"AGPL".as_slice(),
        ),
        ("licenses/signal-cli.txt", b"GPL".as_slice()),
        ("licenses/libsignal.txt", b"AGPL".as_slice()),
        ("licenses/jre.txt", b"JRE license".as_slice()),
        ("licenses/NOTICE.txt", b"NOTICE".as_slice()),
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
            "signal-cli:GPL-3.0-only:licenses/signal-cli.txt",
            "--license",
            "libsignal:AGPL-3.0-only:licenses/libsignal.txt",
            "--license",
            "jre:NOASSERTION:licenses/jre.txt",
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
