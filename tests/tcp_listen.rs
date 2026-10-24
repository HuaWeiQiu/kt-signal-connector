// SPDX-License-Identifier: AGPL-3.0-only

//! Contract 1.147 (§4.145): the R2 connector-first dual-stack batch — the
//! `--listen` loopback TCP leg, its stdout announce frame, and the stdin
//! bootstrap-token chain. The unix-socket leg is pinned by the existing
//! suites, which stay untouched; this file only adds the TCP-mode faces and
//! the serve-mode stdout-silence contract both legs now share.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::process::{Child, ChildStdout, Command};
use tokio::time::timeout;
use tokio_util::codec::{Framed, LinesCodec};

#[path = "common/harness_scene.rs"]
mod harness_scene;
use harness_scene::dump_connector_crash_scene;

const API_VERSION: &str = "1.0";
/// The desktop's batch-2 reader gets a bounded window to see the announce;
/// a cold binary start fits far inside it.
const ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(10);
/// The window proving serve-mode stdout produces no byte after the announce
/// (the R2 backpressure pin: the launcher can drain without ever blocking).
const SILENCE_PROBE: Duration = Duration::from_millis(400);
const HARNESS_AUTH_TIMEOUT: Duration = Duration::from_secs(30);
const HARNESS_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Phase 3 contract payload over the stdin pipe: line 1 the bootstrap
/// secret, line 2 the 64-hex store key, no trailing byte.
fn bootstrap_payload(secret: &[u8; 32]) -> String {
    format!("{}\n{}", hex::encode(secret), hex::encode([0xCD_u8; 32]))
}

fn fixture() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

fn base_serve_command(root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"));
    command
        .arg("serve")
        // §4.148: the old engine flag spelling, pinned end to end — the
        // hidden compatibility alias must keep launching the real binary
        // (the primary name is `--engine-bin`; tests/cli.rs locks both).
        .arg("--signal-cli")
        .arg(fixture())
        .arg("--java-home")
        .arg(root.join("jre"))
        .arg("--signal-data-dir")
        .arg(root.join("signal-data"))
        .arg("--state-dir")
        .arg(root.join("state"))
        .env("KT_FAKE_EXPECT_NO_JAVA", "1")
        .kill_on_drop(true);
    command
}

/// The R2 TCP spawn: `--listen 127.0.0.1:0` + the stdin token, stdout piped
/// (the announce channel), everything else as the launcher passes it.
async fn spawn_tcp_connector(root: &Path, secret: &[u8; 32]) -> (Child, BufReader<ChildStdout>) {
    let mut command = base_serve_command(root);
    command
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--bootstrap-secret-stdin")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(bootstrap_payload(secret).as_bytes())
        .await
        .unwrap();
    stdin.shutdown().await.unwrap();
    let stdout = BufReader::new(child.stdout.take().expect("stdout must be piped"));
    (child, stdout)
}

async fn read_announce_line(stdout: &mut BufReader<ChildStdout>) -> String {
    let mut line = Vec::new();
    match timeout(ANNOUNCE_TIMEOUT, stdout.read_until(b'\n', &mut line)).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => panic!("reading the announce line failed: {error}"),
        Err(_) => panic!(
            "the announce line never arrived within {ANNOUNCE_TIMEOUT:?}:\n{}",
            dump_connector_crash_scene()
        ),
    }
    String::from_utf8(line).expect("the announce line must be UTF-8")
}

/// The announce contract, pinned byte-for-byte: one NDJSON line, the field
/// order `apiVersion, event, transport, host, port`, a kernel-assigned
/// non-zero port, and the bound loopback host. Returns the port.
fn assert_announce_frame(line: &str) -> u16 {
    assert!(
        line.ends_with('\n'),
        "the announce is one newline-terminated line: {line:?}"
    );
    let parsed: Value = serde_json::from_str(line.trim_end_matches('\n'))
        .unwrap_or_else(|error| panic!("the announce line must be JSON: {error}: {line:?}"));
    assert_eq!(parsed["apiVersion"], API_VERSION, "announce: {line:?}");
    assert_eq!(parsed["event"], "listening", "announce: {line:?}");
    assert_eq!(parsed["transport"], "tcp", "announce: {line:?}");
    assert_eq!(parsed["host"], "127.0.0.1", "announce: {line:?}");
    let port = parsed["port"]
        .as_u64()
        .unwrap_or_else(|| panic!("the announce port must be a number: {line:?}"));
    assert!(
        (1..=u16::MAX as u64).contains(&port),
        "the announce port must be a resolved kernel assignment: {line:?}"
    );
    let expected = format!(
        "{{\"apiVersion\":\"{API_VERSION}\",\"event\":\"listening\",\"transport\":\"tcp\",\
         \"host\":\"127.0.0.1\",\"port\":{port}}}\n"
    );
    assert_eq!(line, expected, "the announce frame must be byte-exact");
    port as u16
}

/// The R2 backpressure pin: after the announce, serve-mode stdout produces
/// no byte at all while the process lives (and zero bytes even at EOF).
async fn assert_stdout_silent(stdout: &mut BufReader<ChildStdout>) {
    let mut probe = Vec::new();
    match timeout(SILENCE_PROBE, stdout.read_until(b'\n', &mut probe)).await {
        Err(_elapsed) => {}
        Ok(Ok(0)) => {}
        Ok(Ok(_)) => panic!(
            "serve-mode stdout must stay silent after the announce; got: {:?}",
            String::from_utf8_lossy(&probe)
        ),
        Ok(Err(error)) => panic!("the stdout silence probe failed: {error}"),
    }
}

async fn authenticate<S>(client: &mut Framed<S, LinesCodec>, secret: &[u8; 32]) -> (Value, Value)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let challenge: Value = match timeout(HARNESS_AUTH_TIMEOUT, async {
        serde_json::from_str::<Value>(&client.next().await.unwrap().unwrap()).unwrap()
    })
    .await
    {
        Ok(challenge) => challenge,
        Err(_) => panic!(
            "the challenge never arrived within {HARNESS_AUTH_TIMEOUT:?}:\n{}",
            dump_connector_crash_scene()
        ),
    };
    assert_eq!(challenge["event"], "runtime.challenge");
    let server_nonce = challenge["data"]["serverNonce"].as_str().unwrap();
    let client_nonce = hex::encode([9_u8; 32]);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
    mac.update(b"kt-signal-connector-v1\0");
    mac.update(server_nonce.as_bytes());
    mac.update(b"\0");
    mac.update(client_nonce.as_bytes());
    mac.update(b"\0");
    mac.update(API_VERSION.as_bytes());
    let proof = hex::encode(mac.finalize().into_bytes());
    client
        .send(
            json!({
                "apiVersion": API_VERSION,
                "requestId": "handshake-1",
                "method": "handshake",
                "params": { "clientNonce": client_nonce, "proof": proof }
            })
            .to_string(),
        )
        .await
        .unwrap();
    let response: Value = match timeout(HARNESS_AUTH_TIMEOUT, async {
        serde_json::from_str::<Value>(&client.next().await.unwrap().unwrap()).unwrap()
    })
    .await
    {
        Ok(response) => response,
        Err(_) => panic!(
            "the handshake response never arrived within {HARNESS_AUTH_TIMEOUT:?}:\n{}",
            dump_connector_crash_scene()
        ),
    };
    (challenge, response)
}

async fn request<S>(
    client: &mut Framed<S, LinesCodec>,
    request_id: &str,
    method: &str,
    params: Value,
) -> Value
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let round = timeout(HARNESS_REQUEST_TIMEOUT, async {
        client
            .send(
                json!({
                    "apiVersion": API_VERSION,
                    "requestId": request_id,
                    "method": method,
                    "params": params
                })
                .to_string(),
            )
            .await
            .unwrap();
        loop {
            let response: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if response.get("requestId").and_then(Value::as_str) == Some(request_id) {
                return response;
            }
        }
    })
    .await;
    match round {
        Ok(response) => response,
        Err(_) => panic!(
            "the harness request {request_id} ({method}) exceeded \
             {HARNESS_REQUEST_TIMEOUT:?}:\n{}",
            dump_connector_crash_scene()
        ),
    }
}

/// The connector exits cleanly once the authenticated session ends, exactly
/// like the UDS leg.
async fn assert_clean_exit(connector: &mut Child) {
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .expect("the connector should exit after the host disconnects")
        .unwrap();
    if status.success() {
        return;
    }
    let mut stderr = String::new();
    if let Some(mut pipe) = connector.stderr.take() {
        use tokio::io::AsyncReadExt;
        let _ = pipe.read_to_string(&mut stderr).await;
    }
    panic!("the connector exited with {status:?}; stderr: {stderr}");
}

/// The full batch-2 dress rehearsal over the real binary: stdin token in,
/// one byte-exact announce line out, the announced port already accepting
/// (bind strictly precedes the announce), the existing challenge + HMAC
/// handshake over loopback TCP, a served request, stdout silent ever after,
/// and a clean exit.
#[tokio::test]
async fn tcp_listen_announces_the_bound_port_then_serves_over_loopback() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let secret = [21_u8; 32];

    let (mut connector, mut stdout) = spawn_tcp_connector(temp.path(), &secret).await;
    let announce = read_announce_line(&mut stdout).await;
    let port = assert_announce_frame(&announce);

    // Bind precedes announce: the port the announce named is connectable the
    // moment the line is read — an announce that raced its own bind would
    // have the desktop connection-refused here instead.
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    let (challenge, response) = authenticate(&mut client, &secret).await;
    assert_eq!(challenge["data"]["apiVersion"], API_VERSION);
    assert_eq!(response["result"]["apiVersion"], API_VERSION);

    let status = request(&mut client, "status-1", "runtime.status", json!({})).await;
    assert!(status.get("result").is_some());

    drop(client);
    assert_stdout_silent(&mut stdout).await;
    assert_clean_exit(&mut connector).await;
}

/// The stdin token chain, negative arm: the same spawn, a proof computed
/// from the wrong secret — the existing AUTHENTICATION_FAILED face answers
/// over TCP, which proves the authenticating secret is the one that crossed
/// stdin and nothing else.
#[tokio::test]
async fn tcp_listen_rejects_a_proof_from_the_wrong_stdin_token() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();

    let (mut connector, mut stdout) = spawn_tcp_connector(temp.path(), &[22_u8; 32]).await;
    let announce = read_announce_line(&mut stdout).await;
    let port = assert_announce_frame(&announce);

    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    let (_, response) = authenticate(&mut client, &[23_u8; 32]).await;
    assert_eq!(response["error"]["code"], "AUTHENTICATION_FAILED");

    // The failed probe must not consume the process: an unauthenticated
    // session ends, the connector lives (the §4.145 single-slot posture
    // inherited from the UDS accept loop), and the parent owns the exit.
    drop(client);
    let still_running = timeout(Duration::from_millis(300), connector.wait())
        .await
        .is_err();
    assert!(
        still_running,
        "a failed handshake must not stop the connector"
    );
}

/// The §3.1/§6-R1 contract, UDS leg: serve-mode stdout was silent before R2
/// and must stay silent — the batch-2 parser only ever trusts a line that
/// matches the announce shape, and this leg must never emit a candidate.
#[tokio::test]
async fn uds_serve_stdout_stays_silent() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [24_u8; 32];
    fs::write(&secret_file, bootstrap_payload(&secret)).unwrap();
    fs::set_permissions(&secret_file, fs::Permissions::from_mode(0o600)).unwrap();

    let mut command = base_serve_command(temp.path());
    command
        .arg("--endpoint")
        .arg(&endpoint)
        .arg("--bootstrap-secret-file")
        .arg(&secret_file)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut connector = command.spawn().unwrap();
    timeout(Duration::from_secs(3), async {
        while !endpoint.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the connector socket should appear");

    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    let (_, response) = authenticate(&mut client, &secret).await;
    assert_eq!(response["result"]["apiVersion"], API_VERSION);

    let mut stdout = BufReader::new(connector.stdout.take().expect("stdout must be piped"));
    assert_stdout_silent(&mut stdout).await;

    drop(client);
    assert_clean_exit(&mut connector).await;
}
