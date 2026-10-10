// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Story-anchored attachment byte faces (contract revision 1.86, §4.71):
//! the caller-supplied pointer face the story event carried walks the same
//! transit-CDN chain the row-anchored faces run — the shape gate answers
//! every malformed pointer before any network traffic, a good pointer lands
//! the plaintext where `stories.openAttachment` mints the chunk handle, and
//! an integrity failure is terminal with nothing left behind. No engine
//! process: the chain is pure connector (store + CDN + local crypto), so
//! the supervisor runs unstarted.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use aes::Aes256;
use cbc::cipher::block_padding::Pkcs7;
use cbc::cipher::{BlockEncryptMut, KeyIvInit};
use hmac::{Hmac, Mac};
use kt_signal_connector::engine::SignalCliConfig;
use kt_signal_connector::media::MediaHandleTable;
use kt_signal_connector::store::{Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SIGNAL_ACCOUNT: &str = "+15555550100";
const STORY_ATTACHMENT_ID: &str = "9f8b7c6d-5e4f-4a3b-2c1d-0e9f8a7b6c5d";

const KEY: [u8; 64] = [0x31; 64];

type HmacSha256 = Hmac<Sha256>;

/// Official-shape seal: log-pad to the next power of two, PKCS7,
/// AES-256-CBC with the IV prefixed, trailing HMAC over IV || ciphertext.
fn sealed(plaintext: &[u8]) -> Vec<u8> {
    let padded_size = plaintext.len().next_power_of_two();
    let mut padded = vec![0_u8; padded_size];
    padded[..plaintext.len()].copy_from_slice(plaintext);
    padded.resize(padded_size + 16, 16_u8);
    let iv = [0x5E_u8; 16];
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
/// harness (the values are 32-byte hashes and the fixed 64-byte key set).
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

/// The pointer params exactly the story event's attachment projection
/// round-trips into: id, size, and the locator face.
fn story_params(account_id: &str, id: &str, size: u64, locator: Value) -> Value {
    json!({
        "accountId": account_id,
        "attachment": { "id": id, "size": size, "locator": locator },
    })
}

fn good_locator(digest: Option<String>, plaintext_hash: Option<String>) -> Value {
    let mut face = json!({
        "key": crate_base64(&KEY),
        "cdnKey": "story-transit-object",
        "cdnNumber": 0,
    });
    if let Some(digest) = digest {
        face["digest"] = json!(digest);
    }
    if let Some(hash) = plaintext_hash {
        face["plaintextHash"] = json!(hash);
    }
    face
}

/// The story event's own projection shape (engine `project_attachment_pointer`):
/// rendering fields ride beside the locator — and the download params reject
/// them (deny_unknown_fields), so the round-trip through this shape is the
/// desktop contract this file pins.
fn story_event_attachment(size: u64, locator: Value) -> Value {
    json!({
        "id": STORY_ATTACHMENT_ID,
        "contentType": "image/jpeg",
        "filename": "story.jpg",
        "size": size,
        "width": 1080,
        "height": 1920,
        "isVoiceNote": false,
        "locator": locator,
    })
}

fn supervisor(temp: &TempDir, cdn_base: Option<String>) -> Arc<RuntimeSupervisor> {
    let mut config = SignalCliConfig::new(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-signal-cli.py"),
        temp.path().join("signal-data"),
    );
    config.request_timeout = Duration::from_secs(1);
    config.shutdown_grace = Duration::from_millis(100);
    config.media_ingest = true;
    config.history_cdn_base = cdn_base;
    let seeded = {
        let seed = Store::open(temp.path(), Some(StoreKey::from_bytes([0x5A; 32]))).unwrap();
        Arc::new(seed)
            .upsert_account_from_signal(
                SIGNAL_ACCOUNT,
                Some(1),
                kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
            )
            .unwrap()
    };
    assert!(!seeded.id.is_empty(), "the account row must exist");
    let store = Arc::new(Store::open(temp.path(), Some(StoreKey::from_bytes([0x5A; 32]))).unwrap());
    Arc::new(RuntimeSupervisor::with_backup_handles(
        config,
        store,
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        Some(Arc::new(MediaHandleTable::new())),
        None,
    ))
}

fn account_id(temp: &TempDir) -> String {
    let store = Store::open(temp.path(), Some(StoreKey::from_bytes([0x5A; 32]))).unwrap();
    store
        .account_by_signal(SIGNAL_ACCOUNT)
        .unwrap()
        .expect("the seeded account")
        .id
}

/// Minimal standard-base64 decoder for the chunk assertions (the harness
/// mirrors the wire encoding; no extra dependency).
fn decode_base64(input: &str) -> Vec<u8> {
    const INVALID: u8 = 0xFF;
    fn value(byte: u8) -> u8 {
        match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => INVALID,
        }
    }
    let bytes = input.as_bytes();
    assert!(bytes.len().is_multiple_of(4), "canonical base64 quads");
    let padding = bytes.iter().rev().take_while(|&&b| b == b'=').count();
    // Zero-fill the padding positions, decode every complete quad, then
    // trim the filler bytes the padding stood for.
    let mut chars = bytes.to_vec();
    let pad_start = chars.len() - padding;
    for byte in &mut chars[pad_start..] {
        *byte = b'A';
    }
    let mut out = Vec::with_capacity(chars.len() / 4 * 3);
    for quad in chars.as_chunks::<4>().0 {
        let v: [u8; 4] = quad
            .iter()
            .map(|&b| value(b))
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        assert!(v.iter().all(|&b| b != INVALID), "canonical base64 alphabet");
        out.push((v[0] << 2) | (v[1] >> 4));
        out.push((v[1] << 4) | (v[2] >> 2));
        out.push((v[2] << 6) | v[3]);
    }
    out.truncate(out.len() - padding);
    out
}

async fn dispatch_download(
    supervisor: &RuntimeSupervisor,
    params: Value,
) -> Result<Value, kt_signal_connector::protocol::ApiError> {
    let parsed: kt_signal_connector::service::StoryAttachmentDownloadParams =
        serde_json::from_value(params)
            .expect("the dispatch params must parse (the host arm answers the rest)");
    supervisor
        .story_attachment_download(parsed)
        .await
        .map_err(|error| error.into_api())
}

#[tokio::test]
async fn download_chain_lands_the_plaintext_where_the_story_open_reads_it() {
    let temp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    let plaintext = b"story media bytes for the viewer".to_vec();
    let body = sealed(&plaintext);
    Mock::given(method("GET"))
        .and(path("/attachments/story-transit-object"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
        .expect(1)
        .mount(&server)
        .await;

    let supervisor = supervisor(&temp, Some(server.uri()));
    let account = account_id(&temp);
    let params = story_params(
        &account,
        STORY_ATTACHMENT_ID,
        plaintext.len() as u64,
        good_locator(
            Some(whole_body_digest(&body)),
            Some(plaintext_hash_hex(&plaintext)),
        ),
    );

    let result = dispatch_download(&supervisor, params.clone())
        .await
        .expect("the full chain must succeed");
    assert_eq!(result["sizeBytes"], plaintext.len());

    // The bytes read back through the story open face: the handle mint, the
    // sequential chunk loop, EOF.
    let open_params: kt_signal_connector::service::StoryOpenAttachmentParams =
        serde_json::from_value(json!({
            "accountId": account,
            "attachmentId": STORY_ATTACHMENT_ID,
        }))
        .unwrap();
    let view = supervisor
        .story_open_attachment(open_params)
        .await
        .expect("the landed story attachment must open");
    assert_eq!(view.size_bytes, plaintext.len() as u64);
    let mut offset = 0_u64;
    let mut assembled = Vec::new();
    loop {
        let chunk = supervisor
            .read_media_chunk(view.media_handle.clone(), offset)
            .await
            .unwrap();
        offset += chunk.size_bytes;
        assembled.extend_from_slice(&decode_base64(&chunk.data_base64));
        if chunk.eof {
            break;
        }
    }
    assert_eq!(assembled, plaintext);
    supervisor
        .close_media_handle(view.media_handle)
        .await
        .unwrap();

    // Idempotent dedup: a second call for the same story attachment never
    // touches the CDN.
    let second = dispatch_download(&supervisor, params).await.unwrap();
    assert_eq!(second["sizeBytes"], plaintext.len());
    server.verify().await;
}

#[tokio::test]
async fn pointer_shape_holes_answer_invalid_request_before_any_network() {
    let server = MockServer::start().await;
    // One mock, zero expected hits: the only way a hole reaches it is a
    // wrong answer code, and verify() fails the test on any traffic.
    Mock::given(method("GET"))
        .and(path("/attachments/story-transit-object"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"irrelevant".to_vec()))
        .expect(0)
        .mount(&server)
        .await;
    let temp = TempDir::new().unwrap();
    let supervisor = supervisor(&temp, Some(server.uri()));
    let account = account_id(&temp);
    let body = sealed(b"whatever the pointer claims");
    let digest = whole_body_digest(&body);

    // Params-gate holes (the host arm answers INVALID_REQUEST at parse):
    // a required pointer field absent, or a rendering field riding the
    // pointer verbatim (deny_unknown_fields — the desktop passes exactly
    // id/size/locator, never the whole attachment projection).
    for (params, name) in [
        (
            json!({"accountId": account, "attachment": {"id": STORY_ATTACHMENT_ID, "size": 32}}),
            "missing locator",
        ),
        (
            story_params(
                &account,
                STORY_ATTACHMENT_ID,
                32,
                json!({"cdnKey": "k", "cdnNumber": 0, "digest": digest}),
            ),
            "missing key",
        ),
        (
            story_params(
                &account,
                STORY_ATTACHMENT_ID,
                32,
                json!({"key": crate_base64(&KEY), "cdnNumber": 0, "digest": digest}),
            ),
            "missing cdnKey",
        ),
        (
            story_params(
                &account,
                STORY_ATTACHMENT_ID,
                32,
                json!({"key": crate_base64(&KEY), "cdnKey": "k", "digest": digest}),
            ),
            "missing cdnNumber",
        ),
        (
            json!({
                "accountId": account,
                "attachment": story_event_attachment(32, good_locator(Some(digest.clone()), None)),
            }),
            "the whole rendering projection is a params error",
        ),
    ] {
        let parsed = serde_json::from_value::<
            kt_signal_connector::service::StoryAttachmentDownloadParams,
        >(params);
        assert!(parsed.is_err(), "params gate must reject: {name}");
    }

    // Post-parse holes: the ladder answers INVALID_REQUEST ahead of the
    // crypto-shape decode, which itself answers ahead of any network
    // traffic — every variant in this group is pre-CDN by construction.
    let holes: Vec<(Value, &str)> = vec![
        // Size zero / past the 100 MiB ceiling.
        (
            story_params(
                &account,
                STORY_ATTACHMENT_ID,
                0,
                good_locator(Some(digest.clone()), None),
            ),
            "size zero",
        ),
        (
            story_params(
                &account,
                STORY_ATTACHMENT_ID,
                100 * 1024 * 1024 + 1,
                good_locator(Some(digest.clone()), None),
            ),
            "size past the ceiling",
        ),
        // Key not a 64-byte set.
        (
            story_params(
                &account,
                STORY_ATTACHMENT_ID,
                32,
                good_locator(Some(digest.clone()), None)
                    .as_object_mut()
                    .map(|face| {
                        face.insert("key".into(), json!(crate_base64(&[0x31; 32])));
                        Value::Object(face.clone())
                    })
                    .unwrap(),
            ),
            "short key",
        ),
        // No integrity face at all (the official hasIntegrityCheck gate).
        (
            story_params(
                &account,
                STORY_ATTACHMENT_ID,
                32,
                json!({"key": crate_base64(&KEY), "cdnKey": "story-transit-object", "cdnNumber": 0}),
            ),
            "no integrity face",
        ),
        // Digest not valid base64.
        (
            story_params(
                &account,
                STORY_ATTACHMENT_ID,
                32,
                good_locator(Some("not base64 !!".into()), None),
            ),
            "digest not base64",
        ),
        // Digest not a SHA-256 value.
        (
            story_params(
                &account,
                STORY_ATTACHMENT_ID,
                32,
                good_locator(Some(crate_base64(b"nine bytes")), None),
            ),
            "digest not sha-256",
        ),
        // PlaintextHash not hex.
        (
            story_params(
                &account,
                STORY_ATTACHMENT_ID,
                32,
                good_locator(None, Some("zzzz".into())),
            ),
            "plaintextHash not hex",
        ),
    ];
    for (params, name) in holes {
        let error = dispatch_download(&supervisor, params)
            .await
            .expect_err("every hole must fail closed");
        assert_eq!(error.code, "INVALID_REQUEST", "hole: {name}");
        assert!(!error.retryable, "hole: {name}");
    }
    server.verify().await;
}

#[tokio::test]
async fn a_digest_mismatch_is_terminal_and_lands_nothing() {
    let temp = TempDir::new().unwrap();
    let server = MockServer::start().await;
    let plaintext = b"doctored story bytes do not help".to_vec();
    let body = sealed(&plaintext);
    let wrong_digest = whole_body_digest(b"a completely different body");
    Mock::given(method("GET"))
        .and(path("/attachments/story-transit-object"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
        .expect(1)
        .mount(&server)
        .await;

    let supervisor = supervisor(&temp, Some(server.uri()));
    let account = account_id(&temp);
    let error = dispatch_download(
        &supervisor,
        story_params(
            &account,
            STORY_ATTACHMENT_ID,
            plaintext.len() as u64,
            good_locator(Some(wrong_digest), None),
        ),
    )
    .await
    .expect_err("a digest mismatch must fail");
    assert_eq!(error.code, "UPSTREAM_ERROR");
    assert!(!error.retryable);

    // Terminal and clean: nothing landed under the story id, no temp
    // residue, and the story open face answers the not-available error.
    let attachments = temp.path().join("signal-data/attachments");
    let landed: Vec<_> = std::fs::read_dir(&attachments)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(landed.is_empty(), "no file may land: {landed:?}");
    let open_params: kt_signal_connector::service::StoryOpenAttachmentParams =
        serde_json::from_value(json!({
            "accountId": account,
            "attachmentId": STORY_ATTACHMENT_ID,
        }))
        .unwrap();
    let error = supervisor
        .story_open_attachment(open_params)
        .await
        .expect_err("nothing landed")
        .into_api();
    assert_eq!(error.code, "UPSTREAM_ERROR");
    server.verify().await;
}
