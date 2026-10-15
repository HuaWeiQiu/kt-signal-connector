// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! `messages.search` conversation scope (contract revision 1.162, §4.160;
//! the B3 in-conversation search batch): the optional `conversationId`
//! param scopes the account-wide store search to one conversation — the
//! official search-in-conversation posture (the header search action
//! carries `conversation.id`, Signal-Desktop
//! `ConversationHeader.preload.tsx:262-264` behind the header search
//! button `ConversationHeader.dom.tsx:378-385`). The key absent keeps the
//! pre-1.162 account-wide face byte-identically; the scoped face is a
//! pure filter over the same newest-first ordering and the same
//! account-bound cursor format. The hit rows already carry the deep-link
//! identity pair the official result row consumes
//! (`showConversation({conversationId, messageId: id})`,
//! `MessageSearchResult.dom.tsx:93-95`) — this file pins that face on the
//! wire rather than adding any key to it.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};
use tokio_util::codec::{Framed, LinesCodec};

const API_VERSION: &str = "1.0";
const TEST_STORE_KEY: [u8; 32] = [0x5A; 32];
const PEER_A: &str = "+15555550101";
const PEER_B: &str = "+15555550102";

fn bootstrap_payload(secret: &[u8; 32]) -> String {
    format!("{}\n{}", hex::encode(secret), hex::encode(TEST_STORE_KEY))
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

fn write_secret_file(path: &Path, secret: &[u8; 32]) {
    fs::write(path, bootstrap_payload(secret)).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn spawn_connector(root: &Path, endpoint: &Path, secret_file: &Path) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kt-signal-connector"));
    command
        .arg("serve")
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--bootstrap-secret-file")
        .arg(secret_file)
        .arg("--engine-bin")
        .arg(fixture())
        .arg("--signal-data-dir")
        .arg(root.join("signal-data"))
        .arg("--state-dir")
        .arg(root.join("state"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
        .env("KT_FAKE_EXPECT_NO_JAVA", "1")
        .env("JAVA_TOOL_OPTIONS", "poison")
        .env("_JAVA_OPTIONS", "poison")
        .env("JDK_JAVA_OPTIONS", "poison");
    command.spawn().unwrap()
}

async fn assert_clean_exit(connector: &mut Child) {
    let status = timeout(Duration::from_secs(2), connector.wait())
        .await
        .expect("connector should exit after the host disconnects")
        .unwrap();
    if status.success() {
        return;
    }
    let mut stderr = String::new();
    if let Some(mut pipe) = connector.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr).await;
    }
    panic!("connector exited with {status:?}; stderr: {stderr}");
}

async fn wait_for_path(path: &Path) {
    timeout(Duration::from_secs(3), async {
        while !path.exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("connector socket should appear");
}

async fn authenticate(client: &mut Framed<UnixStream, LinesCodec>, secret: &[u8; 32]) -> Value {
    let challenge: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
    assert_eq!(challenge["event"], "runtime.challenge");
    let server_nonce = challenge["data"]["serverNonce"].as_str().unwrap();
    let client_nonce = hex::encode([7_u8; 32]);
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
    let response: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
    assert_eq!(response["result"]["apiVersion"], API_VERSION);
    response
}

async fn request(
    client: &mut Framed<UnixStream, LinesCodec>,
    request_id: &str,
    method: &str,
    params: Value,
) -> Value {
    send_request_frame(client, request_id, method, params).await;
    loop {
        let response: Value = serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
        if response.get("requestId").and_then(Value::as_str) == Some(request_id) {
            return response;
        }
    }
}

async fn send_request_frame(
    client: &mut Framed<UnixStream, LinesCodec>,
    request_id: &str,
    method: &str,
    params: Value,
) {
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
}

async fn link_account(client: &mut Framed<UnixStream, LinesCodec>) -> String {
    let link = request(
        client,
        "link-start",
        "link.start",
        json!({ "deviceName": "KT-SearchScope" }),
    )
    .await;
    let finished = request(
        client,
        "link-finish",
        "link.finish",
        json!({ "linkSessionId": link["result"]["linkSessionId"] }),
    )
    .await;
    finished["result"]["id"].as_str().unwrap().to_string()
}

/// Two direct peers, three needle rows: two in conversation A (early and
/// late), one in conversation B — so the scope contrast has rows on both
/// sides of the account-wide ordering.
async fn inject_needle_rows(temp: &TempDir) {
    let signal_data = temp.path().join("signal-data");
    fs::create_dir_all(&signal_data).unwrap();
    fs::write(
        signal_data.join(".fixture-emit-envelopes.json"),
        json!({ "envelopes": [
            {
                "source": PEER_A,
                "timestamp": 1727000010000u64,
                "dataMessage": { "message": "needle early in chat one" }
            },
            {
                "source": PEER_A,
                "timestamp": 1727000030000u64,
                "dataMessage": { "message": "needle late in chat one" }
            },
            {
                "source": PEER_B,
                "timestamp": 1727000020000u64,
                "dataMessage": { "message": "needle in chat two" }
            }
        ] })
        .to_string(),
    )
    .unwrap();
}

/// Poll the unscoped search until all three needle rows have landed (the
/// receive path is asynchronous; the poll doubles as the pre-1.162
/// account-wide baseline every scoped assertion contrasts against).
async fn wait_for_needle_rows(
    client: &mut Framed<UnixStream, LinesCodec>,
    account_id: &str,
) -> Vec<Value> {
    for attempt in 0..200 {
        sleep(Duration::from_millis(20)).await;
        let _ = send_request_frame(
            client,
            &format!("poll-{attempt}"),
            "runtime.status",
            json!({}),
        )
        .await;
        loop {
            let frame: Value =
                serde_json::from_str(&client.next().await.unwrap().unwrap()).unwrap();
            if frame.get("requestId").and_then(Value::as_str)
                == Some(format!("poll-{attempt}").as_str())
            {
                break;
            }
        }
        let page = request(
            client,
            &format!("search-{attempt}"),
            "messages.search",
            json!({ "accountId": account_id, "query": "needle", "limit": 50 }),
        )
        .await;
        let items = page["result"]["items"].as_array().unwrap().clone();
        if items.len() == 3 {
            return items;
        }
    }
    panic!("the three needle rows never landed");
}

fn hit_text(row: &Value) -> &str {
    row["text"].as_str().unwrap()
}

/// The scoped face rides the wire: the scope filters the same newest-first
/// ordering to one conversation, the hit rows carry the deep-link identity
/// face the official result row consumes (id + conversationId + sentAt,
/// `MessageSearchResult.dom.tsx:37-41` fed to
/// `showConversation({conversationId, messageId: id})` at :93-95 — the
/// desktop's `openSearchHit` triple), and an unknown conversation answers
/// CONVERSATION_NOT_FOUND instead of a silent empty page.
#[tokio::test]
async fn search_conversation_scope_filters_and_carries_the_deep_link_face() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [67_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    request(&mut client, "start", "runtime.start", json!({})).await;
    let account_id = link_account(&mut client).await;

    inject_needle_rows(&temp).await;
    let unscoped_hits = wait_for_needle_rows(&mut client, &account_id).await;
    let conversation_a = unscoped_hits
        .iter()
        .find(|row| hit_text(row).contains("chat one"))
        .expect("a chat-one hit")["conversationId"]
        .as_str()
        .unwrap()
        .to_string();
    let conversation_b = unscoped_hits
        .iter()
        .find(|row| hit_text(row).contains("chat two"))
        .expect("a chat-two hit")["conversationId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(conversation_a, conversation_b, "two distinct conversations");

    // The scoped search: only conversation A's rows, newest first — the
    // scope is a filter, the ordering and row shape stay the 1.30 face.
    let scoped = request(
        &mut client,
        "search-scoped",
        "messages.search",
        json!({
            "accountId": account_id,
            "query": "needle",
            "conversationId": conversation_a,
            "limit": 50
        }),
    )
    .await;
    let hits = scoped["result"]["items"].as_array().unwrap();
    assert_eq!(hits.len(), 2, "only chat one's rows: {scoped}");
    assert_eq!(hit_text(&hits[0]), "needle late in chat one");
    assert_eq!(hit_text(&hits[1]), "needle early in chat one");
    for row in hits {
        assert_eq!(
            row["conversationId"],
            json!(conversation_a),
            "every hit belongs to the scoped conversation"
        );
    }

    // The deep-link face: each hit row already carries the minimal
    // sufficient identity the official result row consumes — the message
    // id, its conversation, and the sentAt sort stamp. No new key, no
    // reshape; the desktop's jump chain addresses the row by exactly these.
    for row in hits {
        assert!(
            row["id"].as_str().is_some_and(|id| !id.is_empty()),
            "the hit carries its message id: {row}"
        );
        assert!(
            row["sentAt"].is_u64(),
            "the hit carries its sentAt sort stamp: {row}"
        );
    }

    // The scope also holds on the other conversation: conversation B's own
    // scoped answer is exactly its one row.
    let scoped_b = request(
        &mut client,
        "search-scoped-b",
        "messages.search",
        json!({
            "accountId": account_id,
            "query": "needle",
            "conversationId": conversation_b,
            "limit": 50
        }),
    )
    .await;
    let hits_b = scoped_b["result"]["items"].as_array().unwrap();
    assert_eq!(hits_b.len(), 1, "only chat two's row: {scoped_b}");
    assert_eq!(hit_text(&hits_b[0]), "needle in chat two");

    // An unknown conversation is an addressing error, not an empty page —
    // the `messages.list` ladder (CONVERSATION_NOT_FOUND, non-retryable).
    let unknown = request(
        &mut client,
        "search-unknown",
        "messages.search",
        json!({
            "accountId": account_id,
            "query": "needle",
            "conversationId": "conv-does-not-exist",
            "limit": 50
        }),
    )
    .await;
    assert_eq!(
        unknown["error"]["code"], "CONVERSATION_NOT_FOUND",
        "the unknown scope answers the list ladder's error: {unknown}"
    );
    assert_eq!(unknown["error"]["retryable"], false);

    drop(client);
    sleep(Duration::from_millis(200)).await;
    assert_clean_exit(&mut connector).await;
}

/// The key absent keeps the pre-1.162 wire faces byte-identically, and the
/// cursor stays the account-bound sort-key format on both faces: a scoped
/// page's cursor keeps resolving inside its scope, and the same sort key
/// replayed unscoped (or the other way) stays a valid resume point over the
/// same global ordering — never an error. The addressing gate runs ahead of
/// the query normalization: an empty query inside a known conversation is
/// still the honest empty page, an empty query in an unknown conversation
/// is still the addressing error.
#[tokio::test]
async fn search_scope_absent_key_stays_pre162_and_cursor_pages_across_scopes() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let endpoint = temp.path().join("connector.sock");
    let secret_file = temp.path().join("bootstrap.secret");
    let secret = [67_u8; 32];
    write_secret_file(&secret_file, &secret);

    let mut connector = spawn_connector(temp.path(), &endpoint, &secret_file);
    wait_for_path(&endpoint).await;
    let stream = UnixStream::connect(&endpoint).await.unwrap();
    let mut client = Framed::new(stream, LinesCodec::new());
    authenticate(&mut client, &secret).await;
    request(&mut client, "start", "runtime.start", json!({})).await;
    let account_id = link_account(&mut client).await;

    inject_needle_rows(&temp).await;
    let unscoped_hits = wait_for_needle_rows(&mut client, &account_id).await;
    let conversation_a = unscoped_hits
        .iter()
        .find(|row| hit_text(row).contains("chat one"))
        .expect("a chat-one hit")["conversationId"]
        .as_str()
        .unwrap()
        .to_string();

    // The unscoped page face: exactly the pre-1.162 top-level keys, and the
    // hits span both conversations (the account-wide contract unchanged).
    let unscoped_page = request(
        &mut client,
        "search-unscoped",
        "messages.search",
        json!({ "accountId": account_id, "query": "needle", "limit": 50 }),
    )
    .await;
    let page_keys: Vec<String> = unscoped_page["result"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(
        page_keys,
        vec!["items".to_string()],
        "the unscoped page face stays items-only without a trailing cursor: {page_keys:?}"
    );
    assert_eq!(
        unscoped_page["result"]["items"].as_array().unwrap().len(),
        3
    );

    // A scoped page of one mints the same-format cursor; the second scoped
    // page carries the scope's remainder.
    let first_page = request(
        &mut client,
        "search-page-1",
        "messages.search",
        json!({
            "accountId": account_id,
            "query": "needle",
            "conversationId": conversation_a,
            "limit": 1
        }),
    )
    .await;
    let first_items = first_page["result"]["items"].as_array().unwrap();
    assert_eq!(first_items.len(), 1);
    assert_eq!(hit_text(&first_items[0]), "needle late in chat one");
    let cursor = first_page["result"]["nextCursor"]
        .as_str()
        .unwrap()
        .to_string();

    let second_page = request(
        &mut client,
        "search-page-2",
        "messages.search",
        json!({
            "accountId": account_id,
            "query": "needle",
            "conversationId": conversation_a,
            "cursor": cursor,
            "limit": 50
        }),
    )
    .await;
    let second_items = second_page["result"]["items"].as_array().unwrap();
    assert_eq!(
        second_items.iter().map(hit_text).collect::<Vec<_>>(),
        ["needle early in chat one"],
        "the scoped cursor pages the scope's remainder: {second_page}"
    );

    // The same cursor replayed unscoped is a valid resume point over the
    // same global ordering — the row the scoped page excluded (chat two's,
    // ordered between the two chat-one rows) reappears here.
    let replayed = request(
        &mut client,
        "search-replay",
        "messages.search",
        json!({
            "accountId": account_id,
            "query": "needle",
            "cursor": cursor,
            "limit": 50
        }),
    )
    .await;
    let replayed_items = replayed["result"]["items"].as_array().unwrap();
    assert_eq!(
        replayed_items.iter().map(hit_text).collect::<Vec<_>>(),
        ["needle in chat two", "needle early in chat one"],
        "the cursor is a sort-key resume, not a scope token: {replayed}"
    );

    // The addressing gate precedes the query normalization: an empty query
    // in a known conversation is the honest empty page, an empty query in
    // an unknown conversation is the addressing error.
    let empty_scoped = request(
        &mut client,
        "search-empty-scoped",
        "messages.search",
        json!({
            "accountId": account_id,
            "query": "   ",
            "conversationId": conversation_a,
            "limit": 50
        }),
    )
    .await;
    assert!(
        empty_scoped["result"]["items"]
            .as_array()
            .unwrap()
            .is_empty(),
        "an empty query keeps the empty-page answer inside a known scope: {empty_scoped}"
    );
    let empty_unknown = request(
        &mut client,
        "search-empty-unknown",
        "messages.search",
        json!({
            "accountId": account_id,
            "query": "",
            "conversationId": "conv-does-not-exist",
            "limit": 50
        }),
    )
    .await;
    assert_eq!(
        empty_unknown["error"]["code"], "CONVERSATION_NOT_FOUND",
        "the unknown scope answers before the query normalization: {empty_unknown}"
    );
    assert_eq!(empty_unknown["error"]["retryable"], false);

    drop(client);
    sleep(Duration::from_millis(200)).await;
    assert_clean_exit(&mut connector).await;
}
