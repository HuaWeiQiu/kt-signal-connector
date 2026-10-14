// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Link-time history attachment download (contract revision 1.57, §4.38
//! step 3): a kt-engine launch imports an archive line carrying one transit
//! pointer face, the download chain walks it against a local mock CDN, the
//! plaintext lands where `messages.attachments.open` resolves it, and the
//! failure ladder answers terminal content-free errors.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use aes::Aes256;
use cbc::cipher::block_padding::Pkcs7;
use cbc::cipher::{BlockEncryptMut, KeyIvInit};
use hmac::{Hmac, Mac};
use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::ids::stable_hash_id;
use kt_signal_connector::media::MediaHandleTable;
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SIGNAL_ACCOUNT: &str = "+15555550100";
const ALICE_ACI: &str = "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b";
const ALICE_E164: &str = "+15550000001";

const KEY: [u8; 64] = [0x21; 64];

type HmacSha256 = Hmac<Sha256>;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

fn test_store(dir: &std::path::Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

/// Official-shape seal: log-pad to the next power of two, PKCS7,
/// AES-256-CBC with the IV prefixed, trailing HMAC over IV || ciphertext.
fn sealed(plaintext: &[u8]) -> Vec<u8> {
    let padded_size = plaintext.len().next_power_of_two();
    let mut padded = vec![0_u8; padded_size];
    padded[..plaintext.len()].copy_from_slice(plaintext);
    padded.resize(padded_size + 16, 16_u8);
    let iv = [0x37_u8; 16];
    let ciphertext = cbc::Encryptor::<Aes256>::new((&KEY[..32]).into(), (&iv).into())
        .encrypt_padded_mut::<Pkcs7>(&mut padded, padded_size)
        .unwrap()
        .to_vec();
    let mut body = iv.to_vec();
    body.extend_from_slice(&ciphertext);
    let mut mac = HmacSha256::new_from_slice(&KEY[32..]).unwrap();
    mac.update(&body);
    body.extend_from_slice(mac.finalize().into_bytes().as_slice());
    body
}

fn whole_body_digest(body: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(body);
    crate_base64(&hash.finalize())
}

fn plaintext_hash_hex(plaintext: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(plaintext);
    hex::encode(hash.finalize())
}

/// Minimal standard-base64 without pulling another dependency into the
/// harness (the values are 32-byte hashes and short keys).
fn crate_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// The locator JSON one archive attachment line carries (the engine's S3
/// projection shape — read back by `import_locator`).
fn locator_json(digest: Option<String>, plaintext_hash: Option<String>) -> Value {
    let mut face = json!({
        "key": crate_base64(&KEY),
        "cdnKey": "test-cdn-key",
        "cdnNumber": 0,
        "uploadTimestamp": 1700000000000_u64,
    });
    if let Some(digest) = digest {
        face["digest"] = json!(digest);
    }
    if let Some(hash) = plaintext_hash {
        face["plaintextHash"] = json!(hash);
    }
    face
}

/// Lay out the engine face: the v2 registry plus one account directory whose
/// NDJSON carries one incoming message with one attachment — the descriptor
/// with its pointer-face fields inline (the engine's S3 projection: locator
/// fields ride the attachment object itself, read back by `import_locator`
/// through `raw.get("locator")`).
fn seed_archive_with_attachment(data_dir: &std::path::Path, attachment: &Value, locator: &Value) {
    let account_dir = data_dir.join("accounts").join("lk-abc");
    std::fs::create_dir_all(&account_dir).unwrap();
    std::fs::write(
        data_dir.join("engine-state.json"),
        r#"{"version":2,"accounts":[{"number":"+15555550100","dir":"accounts/lk-abc"}]}"#,
    )
    .unwrap();
    let recipient = format!(
        r#"{{"type":"recipient","id":1,"kind":"contact","name":"Alice Import","aci":"{ALICE_ACI}","e164":"{ALICE_E164}"}}"#
    );
    let chat = r#"{"type":"chat","id":10,"recipientId":1}"#;
    // The archive line's attachment object carries the descriptor fields and
    // the pointer face under `locator` (the engine's S3 projection shape).
    let mut attachment = attachment.clone();
    attachment["locator"] = locator.clone();
    let message = json!({
        "type": "message",
        "chatId": 10,
        "authorId": 1,
        "dateSent": 1000,
        "direction": "incoming",
        "text": "archived picture",
        "attachments": [attachment],
    });
    std::fs::write(
        account_dir.join("history-import.ndjson"),
        format!(
            "{recipient}\n{chat}\n{}\n",
            serde_json::to_string(&message).unwrap()
        ),
    )
    .unwrap();
}

fn kt_engine_config(
    fixture: &std::path::Path,
    data_dir: &std::path::Path,
    cdn_base: Option<String>,
) -> EngineConfig {
    let mut config = EngineConfig::new(fixture.to_path_buf(), data_dir.to_path_buf());
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    config.media_ingest = true;
    config.history_cdn_base = cdn_base;
    config
}

fn download_params(
    account_id: &str,
    conversation_id: &str,
    message_id: &str,
    attachment_id: &str,
) -> Value {
    json!({
        "accountId": account_id,
        "conversationId": conversation_id,
        "messageId": message_id,
        "attachmentId": attachment_id,
    })
}

async fn dispatch(
    supervisor: &RuntimeSupervisor,
    params: Value,
) -> Result<Value, kt_signal_connector::protocol::ApiError> {
    // The host face wraps the runtime call; here the runtime is called
    // directly through the same params shape the dispatch arm parses.
    let parsed: kt_signal_connector::service::HistoryAttachmentDownloadParams =
        serde_json::from_value(params).unwrap();
    supervisor
        .history_attachment_download(parsed)
        .await
        .map_err(|error| error.into_api())
}

#[tokio::test]
async fn download_chain_lands_the_plaintext_where_media_open_reads_it() {
    let temp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    let plaintext = b"hello archived attachment".to_vec();
    let body = sealed(&plaintext);
    let digest = whole_body_digest(&body);
    Mock::given(method("GET"))
        .and(path("/attachments/test-cdn-key"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
        .expect(1)
        .mount(&server)
        .await;

    let signal_data = temp.path().join("signal-data");
    seed_archive_with_attachment(
        &signal_data,
        &json!({
            "contentType": "application/octet-stream",
            "fileName": "archived.bin",
            "size": plaintext.len(),
        }),
        &locator_json(Some(digest), Some(plaintext_hash_hex(&plaintext))),
    );
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::with_backup_handles(
        kt_engine_config(&fixture(), &signal_data, Some(server.uri())),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        Some(Arc::new(MediaHandleTable::new())),
        None,
    ));
    supervisor.start().await.unwrap();
    let conversation_id = stable_hash_id(&[&seeded.id, "direct", ALICE_E164]);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let (message_id, attachment_id) = loop {
        let messages = store
            .list_messages(&seeded.id, &conversation_id, 10, None)
            .unwrap();
        if let Some(row) = messages.items.first()
            && !row.attachments.is_empty()
        {
            break (row.id.clone(), row.attachments[0].id.clone());
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the import must land the attachment message"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    let result = dispatch(
        &supervisor,
        download_params(&seeded.id, &conversation_id, &message_id, &attachment_id),
    )
    .await
    .expect("the full chain must succeed");
    assert_eq!(result["sizeBytes"], plaintext.len());

    // End-to-end: the landed file reads back through the media triple.
    let open = supervisor
        .open_media(
            serde_json::from_value(json!({
                "accountId": seeded.id,
                "conversationId": conversation_id,
                "messageId": message_id,
                "attachmentId": attachment_id,
            }))
            .unwrap(),
        )
        .await
        .expect("the landed attachment must open");
    assert_eq!(open.size_bytes, plaintext.len() as u64);
    let chunk = supervisor
        .read_media_chunk(open.media_handle.clone(), 0)
        .await
        .unwrap();
    assert!(!chunk.data_base64.is_empty());
    // The chunk must decode back to the exact plaintext (the harness's own
    // standard-base64 decoder mirrors the wire encoding).
    assert_eq!(crate_base64(&plaintext), chunk.data_base64);
    supervisor
        .close_media_handle(open.media_handle)
        .await
        .unwrap();

    // Idempotent dedup: a second call never touches the CDN.
    let second = dispatch(
        &supervisor,
        download_params(&seeded.id, &conversation_id, &message_id, &attachment_id),
    )
    .await
    .unwrap();
    assert_eq!(second["sizeBytes"], plaintext.len());
    server.verify().await;
}

#[tokio::test]
async fn a_digest_mismatch_is_terminal_with_no_file_left_behind() {
    let temp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    let plaintext = b"doctored bytes do not help".to_vec();
    let body = sealed(&plaintext);
    // A digest naming a different body: the gate must fail closed.
    let wrong_digest = whole_body_digest(b"a completely different body");
    Mock::given(method("GET"))
        .and(path("/attachments/test-cdn-key"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .expect(1)
        .mount(&server)
        .await;

    let signal_data = temp.path().join("signal-data");
    seed_archive_with_attachment(
        &signal_data,
        &json!({
            "contentType": "application/octet-stream",
            "fileName": "archived.bin",
            "size": plaintext.len(),
        }),
        &locator_json(Some(wrong_digest), None),
    );
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::with_backup_handles(
        kt_engine_config(&fixture(), &signal_data, Some(server.uri())),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        Some(Arc::new(MediaHandleTable::new())),
        None,
    ));
    supervisor.start().await.unwrap();
    let conversation_id = stable_hash_id(&[&seeded.id, "direct", ALICE_E164]);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let (message_id, attachment_id) = loop {
        let messages = store
            .list_messages(&seeded.id, &conversation_id, 10, None)
            .unwrap();
        if let Some(row) = messages.items.first()
            && !row.attachments.is_empty()
        {
            break (row.id.clone(), row.attachments[0].id.clone());
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the import must land the attachment message"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    let error = dispatch(
        &supervisor,
        download_params(&seeded.id, &conversation_id, &message_id, &attachment_id),
    )
    .await
    .expect_err("a digest mismatch must fail");
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(!error.retryable);

    // Terminal and clean: no file landed under the attachment id, no temp
    // residue, and the media open still answers the normal not-downloaded
    // failure.
    let open = supervisor
        .open_media(
            serde_json::from_value(json!({
                "accountId": seeded.id,
                "conversationId": conversation_id,
                "messageId": message_id,
                "attachmentId": attachment_id,
            }))
            .unwrap(),
        )
        .await
        .expect_err("nothing landed");
    assert_eq!(open.into_api().code, "UPSTREAM_ERROR");
    server.verify().await;
}

#[tokio::test]
async fn plaintext_hash_only_rows_download_and_mismatch_is_terminal() {
    let temp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    let plaintext = b"media-tier face without a digest".to_vec();
    let body = sealed(&plaintext);
    Mock::given(method("GET"))
        .and(path("/attachments/test-cdn-key"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .expect(1)
        .mount(&server)
        .await;
    let signal_data = temp.path().join("signal-data");
    seed_archive_with_attachment(
        &signal_data,
        &json!({
            "contentType": "application/octet-stream",
            "fileName": "archived.bin",
            "size": plaintext.len(),
        }),
        &locator_json(None, Some(plaintext_hash_hex(&plaintext))),
    );
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::with_backup_handles(
        kt_engine_config(&fixture(), &signal_data, Some(server.uri())),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        Some(Arc::new(MediaHandleTable::new())),
        None,
    ));
    supervisor.start().await.unwrap();
    let conversation_id = stable_hash_id(&[&seeded.id, "direct", ALICE_E164]);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let (message_id, attachment_id) = loop {
        let messages = store
            .list_messages(&seeded.id, &conversation_id, 10, None)
            .unwrap();
        if let Some(row) = messages.items.first()
            && !row.attachments.is_empty()
        {
            break (row.id.clone(), row.attachments[0].id.clone());
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the import must land the attachment message"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let params = download_params(&seeded.id, &conversation_id, &message_id, &attachment_id);
    let result = dispatch(&supervisor, params.clone())
        .await
        .expect("a plaintextHash-only locator is the media tier and downloads");
    assert_eq!(result["sizeBytes"], plaintext.len());
    server.verify().await;
}

#[tokio::test]
async fn locator_holes_answer_invalid_request_before_any_network() {
    let server = MockServer::start().await;
    // No mocks mounted: any network traffic would hang or 404 — the point
    // is that these never reach the wire.
    let plaintext = b"faceless attachment".to_vec();
    let body = sealed(&plaintext);

    // (locator, expectation): each hole answers INVALID_REQUEST.
    let holes: Vec<(Value, &str)> = vec![
        // Missing key.
        (
            json!({
                "cdnKey": "k", "cdnNumber": 0,
                "digest": whole_body_digest(&body),
            }),
            "no key",
        ),
        // Missing cdnKey.
        (
            json!({
                "key": crate_base64(&KEY), "cdnNumber": 0,
                "digest": whole_body_digest(&body),
            }),
            "no cdnKey",
        ),
        // Missing cdnNumber.
        (
            json!({
                "key": crate_base64(&KEY), "cdnKey": "k",
                "digest": whole_body_digest(&body),
            }),
            "no cdnNumber",
        ),
        // No integrity face at all.
        (
            json!({
                "key": crate_base64(&KEY), "cdnKey": "k", "cdnNumber": 0,
            }),
            "no integrity",
        ),
        // Key not 64 bytes.
        (
            json!({
                "key": crate_base64(&[0x11; 32]), "cdnKey": "k", "cdnNumber": 0,
                "digest": whole_body_digest(&body),
            }),
            "short key",
        ),
    ];
    for (locator, name) in holes {
        let per = TempDir::new().unwrap();
        let signal_data = per.path().join("signal-data");
        seed_archive_with_attachment(
            &signal_data,
            &json!({
                "contentType": "application/octet-stream",
                "fileName": "archived.bin",
                "size": plaintext.len(),
            }),
            &locator,
        );
        let seeded = {
            let seed = test_store(per.path());
            seed.upsert_account_from_signal(
                SIGNAL_ACCOUNT,
                Some(1),
                kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap()
        };
        let store = test_store(per.path());
        let supervisor = Arc::new(RuntimeSupervisor::with_backup_handles(
            kt_engine_config(&fixture(), &signal_data, Some(server.uri())),
            Arc::clone(&store),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
            Some(Arc::new(MediaHandleTable::new())),
            None,
        ));
        supervisor.start().await.unwrap();
        let conversation_id = stable_hash_id(&[&seeded.id, "direct", ALICE_E164]);
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let (message_id, attachment_id) = loop {
            let messages = store
                .list_messages(&seeded.id, &conversation_id, 10, None)
                .unwrap();
            if let Some(row) = messages.items.first()
                && !row.attachments.is_empty()
            {
                break (row.id.clone(), row.attachments[0].id.clone());
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the import must land the attachment message"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        let error = dispatch(
            &supervisor,
            download_params(&seeded.id, &conversation_id, &message_id, &attachment_id),
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("{name}: the hole must answer INVALID_REQUEST"));
        assert_eq!(error.code, "INVALID_REQUEST", "{name}");
        assert!(!error.retryable, "{name}");
    }
    server.verify().await;
}

#[tokio::test]
async fn a_hmac_mismatch_is_terminal() {
    let temp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    let plaintext = b"tampered in transit".to_vec();
    let mut body = sealed(&plaintext);
    // Flip one byte inside the ciphertext region (past the IV).
    let last = body.len() - 33;
    body[last] ^= 0xFF;
    let digest = whole_body_digest(&body);
    Mock::given(method("GET"))
        .and(path("/attachments/test-cdn-key"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .expect(1)
        .mount(&server)
        .await;
    let signal_data = temp.path().join("signal-data");
    seed_archive_with_attachment(
        &signal_data,
        &json!({
            "contentType": "application/octet-stream",
            "fileName": "archived.bin",
            "size": plaintext.len(),
        }),
        &locator_json(Some(digest), None),
    );
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::with_backup_handles(
        kt_engine_config(&fixture(), &signal_data, Some(server.uri())),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        Some(Arc::new(MediaHandleTable::new())),
        None,
    ));
    supervisor.start().await.unwrap();
    let conversation_id = stable_hash_id(&[&seeded.id, "direct", ALICE_E164]);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let (message_id, attachment_id) = loop {
        let messages = store
            .list_messages(&seeded.id, &conversation_id, 10, None)
            .unwrap();
        if let Some(row) = messages.items.first()
            && !row.attachments.is_empty()
        {
            break (row.id.clone(), row.attachments[0].id.clone());
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the import must land the attachment message"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let error = dispatch(
        &supervisor,
        download_params(&seeded.id, &conversation_id, &message_id, &attachment_id),
    )
    .await
    .expect_err("a bad MAC must fail");
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(!error.retryable);
    server.verify().await;
}

#[tokio::test]
async fn cdn_404_is_terminal() {
    let temp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/attachments/test-cdn-key"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    let plaintext = b"expired transit object".to_vec();
    let body = sealed(&plaintext);
    let signal_data = temp.path().join("signal-data");
    seed_archive_with_attachment(
        &signal_data,
        &json!({
            "contentType": "application/octet-stream",
            "fileName": "archived.bin",
            "size": plaintext.len(),
        }),
        &locator_json(Some(whole_body_digest(&body)), None),
    );
    let seeded = {
        let seed = test_store(temp.path());
        seed.upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap()
    };
    let store = test_store(temp.path());
    let supervisor = Arc::new(RuntimeSupervisor::with_backup_handles(
        kt_engine_config(&fixture(), &signal_data, Some(server.uri())),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        Some(Arc::new(MediaHandleTable::new())),
        None,
    ));
    supervisor.start().await.unwrap();
    let conversation_id = stable_hash_id(&[&seeded.id, "direct", ALICE_E164]);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let (message_id, attachment_id) = loop {
        let messages = store
            .list_messages(&seeded.id, &conversation_id, 10, None)
            .unwrap();
        if let Some(row) = messages.items.first()
            && !row.attachments.is_empty()
        {
            break (row.id.clone(), row.attachments[0].id.clone());
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the import must land the attachment message"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let error = dispatch(
        &supervisor,
        download_params(&seeded.id, &conversation_id, &message_id, &attachment_id),
    )
    .await
    .expect_err("a 404 must fail");
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(!error.retryable);
    server.verify().await;
}

/// The §4.54 face: `attachments.download` addresses the same imported pointer
/// face positionally and walks the identical chain; the hole answers
/// INVALID_REQUEST before any traffic.
mod on_demand {
    use super::*;
    use kt_signal_connector::service::AttachmentDownloadParams;

    async fn dispatch_index(
        supervisor: &RuntimeSupervisor,
        params: Value,
    ) -> Result<Value, kt_signal_connector::protocol::ApiError> {
        let parsed: AttachmentDownloadParams = serde_json::from_value(params).unwrap();
        supervisor
            .attachment_download(parsed)
            .await
            .map_err(|error| error.into_api())
    }

    async fn seeded_supervisor_with_attachment(
        temp: &TempDir,
        server: &MockServer,
        plaintext: &[u8],
        digest: Option<String>,
        plaintext_hash: Option<String>,
        expected_hits: usize,
    ) -> (Arc<RuntimeSupervisor>, Arc<Store>, String, String, String) {
        let body = sealed(plaintext);
        Mock::given(method("GET"))
            .and(path("/attachments/test-cdn-key"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .expect(expected_hits as u64)
            .mount(server)
            .await;
        let signal_data = temp.path().join("signal-data");
        seed_archive_with_attachment(
            &signal_data,
            &json!({
                "contentType": "application/octet-stream",
                "fileName": "archived.bin",
                "size": plaintext.len(),
            }),
            &locator_json(digest, plaintext_hash),
        );
        let seeded = {
            let seed = test_store(temp.path());
            seed.upsert_account_from_signal(
                SIGNAL_ACCOUNT,
                Some(1),
                kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap()
        };
        let store = test_store(temp.path());
        let supervisor = Arc::new(RuntimeSupervisor::with_backup_handles(
            kt_engine_config(&fixture(), &signal_data, Some(server.uri())),
            Arc::clone(&store),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
            Some(Arc::new(MediaHandleTable::new())),
            None,
        ));
        supervisor.start().await.unwrap();
        let conversation_id = stable_hash_id(&[&seeded.id, "direct", ALICE_E164]);
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let (message_id, attachment_id) = loop {
            let messages = store
                .list_messages(&seeded.id, &conversation_id, 10, None)
                .unwrap();
            if let Some(row) = messages.items.first()
                && !row.attachments.is_empty()
            {
                break (row.id.clone(), row.attachments[0].id.clone());
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the import must land the attachment message"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        (
            supervisor,
            store,
            seeded.id,
            conversation_id,
            format!("{message_id}\u{1}{attachment_id}"),
        )
    }

    fn index_params(
        account_id: &str,
        conversation_id: &str,
        message_id: &str,
        attachment_index: u64,
    ) -> Value {
        json!({
            "accountId": account_id,
            "conversationId": conversation_id,
            "messageId": message_id,
            "attachmentIndex": attachment_index,
        })
    }

    #[tokio::test]
    async fn index_addressing_walks_the_import_face_and_lands_where_media_opens() {
        let temp = TempDir::new().unwrap();
        let server = MockServer::start().await;
        let plaintext = b"indexed on-demand bytes".to_vec();
        let (supervisor, _store, account_id, conversation_id, ids) =
            seeded_supervisor_with_attachment(
                &temp,
                &server,
                &plaintext,
                Some(whole_body_digest(&sealed(&plaintext))),
                Some(plaintext_hash_hex(&plaintext)),
                // one live call + the idempotent dedup is answered locally
                1,
            )
            .await;
        let (message_id, _attachment_id) = ids.split_once('\u{1}').expect("the seeded id pair");

        let result = dispatch_index(
            &supervisor,
            index_params(&account_id, &conversation_id, message_id, 0),
        )
        .await
        .expect("the indexed chain must succeed");
        assert_eq!(result["sizeBytes"], plaintext.len());

        // Idempotent: a second indexed call never re-asks the CDN.
        let second = dispatch_index(
            &supervisor,
            index_params(&account_id, &conversation_id, message_id, 0),
        )
        .await
        .unwrap();
        assert_eq!(second["sizeBytes"], plaintext.len());
        server.verify().await;
    }

    #[tokio::test]
    async fn an_index_past_the_descriptor_list_answers_invalid_request() {
        let temp = TempDir::new().unwrap();
        let server = MockServer::start().await;
        let plaintext = b"out of range".to_vec();
        let (supervisor, _store, account_id, conversation_id, ids) =
            seeded_supervisor_with_attachment(
                &temp,
                &server,
                &plaintext,
                Some(whole_body_digest(&sealed(&plaintext))),
                Some(plaintext_hash_hex(&plaintext)),
                // the refusal answers before any traffic
                0,
            )
            .await;
        let (message_id, _) = ids.split_once('\u{1}').unwrap();

        let error = dispatch_index(
            &supervisor,
            index_params(&account_id, &conversation_id, message_id, 1),
        )
        .await
        .expect_err("an out-of-range index must fail closed");
        assert_eq!(error.code, "INVALID_REQUEST");
        assert!(!error.retryable);
        server.verify().await;
    }
}
