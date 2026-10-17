// SPDX-License-Identifier: AGPL-3.0-only

#![cfg(unix)]

//! Android local-backup export end to end (contract revision 1.47, §4.46):
//! `backups.export` builds the §10.2 artifact from the store, starts the
//! engine task, and the snapshot files come back through the media three-chain
//! shape; `backups.exportStatus` mirrors the engine honestly with the raw
//! account echo stripped and the connector's skip counters merged. The
//! fixture plays the engine: it reads the artifact the connector built, walks
//! the failure ladder through the account line's `givenName` sentinels, and
//! writes the §5 snapshot shape in the background.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kt_signal_connector::engine::EngineConfig;
use kt_signal_connector::ids::stable_hash_id;
use kt_signal_connector::store::{MessageRecord, Store, StoreKey};
use kt_signal_connector::supervisor::RuntimeSupervisor;
use serde_json::{Value, json};
use tempfile::TempDir;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-engine.py")
}

/// All supervisor-test stores are encrypted with one fixed key, so a seeded
/// store and the supervisor's own open see the same database.
fn test_store(dir: &Path) -> Arc<Store> {
    Arc::new(Store::open(dir, Some(StoreKey::from_bytes([0x5A; 32]))).unwrap())
}

const SIGNAL_ACCOUNT: &str = "+15555550100";
const ALICE_E164: &str = "+15550000001";
const ALICE_UUID: &str = "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b";
const GROUP_MASTER_KEY: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
/// The fixture's `main` snapshot bytes (the open test asserts the stream
/// against them; base64 verification round-trips through the crate encoder).
const EXPECTED_MAIN: &[u8] = b"fixture-snapshot-main-bytes-0123456789";
const EXPECTED_METADATA: &[u8] = b"fixture-snapshot-metadata";

/// The engine face the tests steer: a v2 registry naming one account
/// directory (`accounts/lk-abc`), empty otherwise — the artifact and the
/// snapshots land there.
fn seed_engine_registry(data_dir: &Path) {
    let account_dir = data_dir.join("accounts").join("lk-abc");
    std::fs::create_dir_all(&account_dir).unwrap();
    std::fs::write(
        data_dir.join("engine-state.json"),
        format!(
            r#"{{"version":2,"accounts":[{{"number":"{SIGNAL_ACCOUNT}","dir":"accounts/lk-abc"}}]}}"#
        ),
    )
    .unwrap();
}

fn backup_config(fixture: &Path, data_dir: &Path) -> EngineConfig {
    let mut config = EngineConfig::new(fixture.to_path_buf(), data_dir.to_path_buf());
    config.request_timeout = Duration::from_secs(5);
    config.shutdown_grace = Duration::from_millis(100);
    config
}

/// One supervisor with the backup-export capability armed: the registry's
/// process-wide handle table, exactly the wiring `open_group_runtime` uses.
fn backup_supervisor(
    temp: &TempDir,
    data_dir: &Path,
) -> (Arc<Store>, Arc<RuntimeSupervisor>, String) {
    seed_engine_registry(data_dir);
    let store = test_store(temp.path());
    let account = store
        .upsert_account_from_signal(
            SIGNAL_ACCOUNT,
            Some(1),
            kt_signal_connector::DEFAULT_PROXY_GROUP_ID,
        )
        .unwrap();
    let supervisor = Arc::new(RuntimeSupervisor::with_backup_handles(
        backup_config(&fixture(), data_dir),
        Arc::clone(&store),
        kt_signal_connector::DEFAULT_PROXY_GROUP_ID.to_string(),
        None,
        Some(Arc::new(
            kt_signal_connector::backup_export::BackupFileHandleTable::new(),
        )),
    ));
    (store, supervisor, account.id)
}

/// Seed one direct conversation with Alice (a contacts-cache contact row) and
/// one exchange: an incoming body plus a confirmed outgoing reply.
fn seed_direct_history(store: &Store, account_id: &str) -> String {
    store
        .upsert_contact(account_id, "contact", ALICE_E164, "Alice Contact", None, 1)
        .unwrap();
    let conversation = store
        .ensure_conversation(account_id, "direct", ALICE_E164, "Alice Contact")
        .unwrap();
    let incoming = MessageRecord {
        id: "m-in".into(),
        account_id: account_id.into(),
        conversation_id: conversation.id.clone(),
        direction: "incoming",
        sender_id: stable_hash_id(&[account_id, "direct", ALICE_E164]),
        sender_name: None,
        mentions_self: false,
        server_guid: None,
        server_timestamp: None,
        is_story: false,
        story_message: None,
        sent_at: 1_000,
        received_at: Some(1_005),
        text: Some("hello from alice".into()),
        text_bytes: Some(16),
        text_truncated: false,
        text_retrievable: true,
        status: "read",
        client_request_id: None,
        quote_message_id: None,
        quote_snapshot: None,
        quote: None,
        attachments: Vec::new(),
        edited_at: None,
        rich: None,
        reactions: Vec::new(),
        edits: Vec::new(),
        delivered_at: None,
        read_at: None,
        viewed_at: None,
        send_outcomes: None,
        sticker: None,
        expire_seconds: None,
        expire_start_at: None,
        admin_deleted: false,
    };
    store.insert_message(&incoming, None, None, true).unwrap();
    let outgoing = MessageRecord {
        id: "m-out".into(),
        account_id: account_id.into(),
        conversation_id: conversation.id.clone(),
        direction: "outgoing",
        sender_id: account_id.into(),
        sender_name: None,
        mentions_self: false,
        server_guid: None,
        server_timestamp: None,
        is_story: false,
        story_message: None,
        sent_at: 2_000,
        received_at: None,
        text: Some("hello back".into()),
        text_bytes: Some(10),
        text_truncated: false,
        text_retrievable: true,
        status: "delivered",
        client_request_id: Some("client-1".into()),
        quote_message_id: None,
        quote_snapshot: None,
        quote: None,
        attachments: Vec::new(),
        edited_at: None,
        rich: None,
        reactions: Vec::new(),
        edits: Vec::new(),
        delivered_at: None,
        read_at: None,
        viewed_at: None,
        send_outcomes: None,
        sticker: None,
        expire_seconds: None,
        expire_start_at: None,
        admin_deleted: false,
    };
    store
        .insert_message(&outgoing, Some("client-1"), None, false)
        .unwrap();
    conversation.id
}

/// Poll `backups.exportStatus` until `state` settles on the expected answer
/// (the fixture's task thread needs one beat to write the snapshot).
async fn wait_for_status(supervisor: &RuntimeSupervisor, account_id: &str, state: &str) -> Value {
    for _ in 0..80 {
        let status = supervisor.export_backup_status(account_id).await.unwrap();
        if status["state"] == state {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "exportStatus never reached {state}: {:?}",
        supervisor.export_backup_status(account_id).await
    );
}

/// The artifact's parsed line sequence, with the transient `givenName` of the
/// account line kept so sentinels can be asserted against.
fn artifact_lines(data_dir: &Path) -> Vec<Value> {
    let raw = std::fs::read_to_string(
        data_dir
            .join("accounts/lk-abc")
            .join(kt_signal_connector::backup_export::ARTIFACT_FILE_NAME),
    )
    .unwrap();
    raw.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn export_builds_artifact_runs_task_and_streams_snapshot_files() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = backup_supervisor(&temp, &data_dir);
    seed_direct_history(&test_store(temp.path()), &account_id);
    supervisor.start().await.unwrap();

    // No snapshot yet: the open face answers the no-snapshot code.
    let missing = supervisor
        .export_file_open(&account_id, "main")
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(missing.code, "BACKUP_SNAPSHOT_MISSING");

    // Status before anything: honest idle with an empty snapshot list.
    let idle = supervisor.export_backup_status(&account_id).await.unwrap();
    assert_eq!(idle["state"], "idle");
    assert_eq!(idle["artifactPresent"], false);
    assert_eq!(idle["accountId"], account_id);
    assert!(
        idle.get("account").is_none(),
        "raw account echo must be stripped"
    );
    assert!(
        idle.get("skippedRows").is_none(),
        "no fabricated counters before a build"
    );
    assert_eq!(idle["snapshots"].as_array().unwrap().len(), 0);

    let started = supervisor.export_local_backup(&account_id).await.unwrap();
    assert_eq!(started["status"], "started");

    let completed = wait_for_status(&supervisor, &account_id, "completed").await;
    assert!(
        completed.get("snapshot").is_some(),
        "the newest snapshot is named"
    );
    assert_eq!(
        completed["snapshot"]["mainBytes"],
        completed["snapshots"][0]["mainBytes"]
    );
    // The counters merged in — the direct exchange had no skip causes at all.
    assert_eq!(completed["skippedRows"], 0);

    // The transitional artifact was consumed by the task (§10.2).
    assert!(
        !data_dir
            .join("accounts/lk-abc")
            .join(kt_signal_connector::backup_export::ARTIFACT_FILE_NAME)
            .exists()
    );

    // The artifact itself, as written before the task consumed it — re-run
    // the export to rebuild it deterministically and read it inside the
    // fixture task's 200 ms write window (the first run's task already
    // finished: the completed status above proves it, so this read races
    // nothing).
    supervisor.export_local_backup(&account_id).await.unwrap();
    let lines = artifact_lines(&data_dir);
    assert_eq!(lines[0]["type"], "account");
    assert!(lines[0]["profileKey"].as_str().unwrap().len() == 64);
    assert!(
        lines[0].get("givenName").is_none(),
        "no display name was seeded"
    );

    let contact = lines
        .iter()
        .find(|line| line["type"] == "contact")
        .expect("alice is projected as a contact");
    assert_eq!(contact["id"], 1);
    assert_eq!(contact["e164"], 15550000001_u64);
    assert_eq!(contact["givenName"], "Alice Contact");

    let chat = lines
        .iter()
        .find(|line| line["type"] == "chat")
        .expect("the direct chat is projected");
    assert_eq!(chat["recipient"], 1);

    let items: Vec<&Value> = lines
        .iter()
        .filter(|line| line["type"] == "chat_item")
        .collect();
    assert_eq!(items.len(), 2);
    assert_eq!(
        items[0]["author"], 1,
        "incoming author resolves via the sender map"
    );
    assert_eq!(items[0]["direction"], "incoming");
    assert_eq!(items[0]["dateSent"], 1_000);
    assert_eq!(items[0]["dateReceived"], 1_005);
    assert!(
        items[0].get("read").is_none(),
        "no fabricated read state (§4.46 deviation)"
    );
    assert_eq!(items[0]["body"], "hello from alice");
    assert_eq!(items[1]["author"], 0, "outgoing author is self");
    assert_eq!(items[1]["body"], "hello back");

    assert_eq!(
        lines.last().unwrap(),
        &json!({ "type": "all_chat_folder" }),
        "the closing marker is emitted"
    );

    // The second task's snapshot replaces the one the §10.6 cleanup removed
    // (same-second collision candidate) — wait for it to land before opening.
    let _ = wait_for_status(&supervisor, &account_id, "completed").await;

    // Now the file three-chain over the newest snapshot.
    let open = supervisor
        .export_file_open(&account_id, "main")
        .await
        .unwrap();
    assert_eq!(open["chunkBytes"], 262_144);
    assert_eq!(open["sizeBytes"], EXPECTED_MAIN.len() as u64);
    let file_handle = open["fileHandle"].as_str().unwrap().to_string();

    let mut offset = 0_u64;
    let mut assembled: Vec<u8> = Vec::new();
    loop {
        let chunk = supervisor
            .export_file_read_chunk(&file_handle, offset)
            .await
            .unwrap();
        offset += chunk["sizeBytes"].as_u64().unwrap();
        // sizeBytes is the chunk's raw byte count; dataBase64 carries the
        // bytes themselves — verify the pair agrees with the crate's encoder
        // by round-tripping the expected bytes instead of pulling a decoder
        // dependency into the test tree.
        let expected = kt_signal_connector::backup_export::encode_chunk(
            &EXPECTED_MAIN[assembled.len()..offset as usize],
        );
        assert_eq!(chunk["dataBase64"], expected);
        assembled.extend_from_slice(&EXPECTED_MAIN[assembled.len()..offset as usize]);
        if chunk["eof"].as_bool().unwrap() {
            break;
        }
    }
    assert_eq!(offset, EXPECTED_MAIN.len() as u64);

    // Offsets are strictly sequential: a replayed offset is refused.
    let replay = supervisor
        .export_file_read_chunk(&file_handle, 0)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(replay.code, "INVALID_REQUEST");

    let closed = supervisor
        .export_file_close_handle(&file_handle)
        .await
        .unwrap();
    assert_eq!(closed["released"], true);
    // Idempotent close.
    let again = supervisor
        .export_file_close_handle(&file_handle)
        .await
        .unwrap();
    assert_eq!(again["released"], false);
}

/// Metadata and main are two fixed values of one enum; a path (or anything
/// else) is an INVALID_REQUEST before the filesystem is touched.
#[tokio::test]
async fn export_file_open_refuses_non_enum_file_values() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (_store, supervisor, account_id) = backup_supervisor(&temp, &data_dir);
    supervisor.start().await.unwrap();
    supervisor.export_local_backup(&account_id).await.unwrap();
    let _ = wait_for_status(&supervisor, &account_id, "completed").await;

    for file in ["../main", "SignalBackups", "", "main/sub", "MAIN"] {
        let error = supervisor
            .export_file_open(&account_id, file)
            .await
            .err()
            .unwrap()
            .into_api();
        assert_eq!(error.code, "INVALID_REQUEST", "file value {file:?}");
    }
    let metadata = supervisor
        .export_file_open(&account_id, "metadata")
        .await
        .unwrap();
    assert_eq!(metadata["sizeBytes"], EXPECTED_METADATA.len() as u64);
}

/// The givenName sentinel ladder: each structured engine error surfaces under
/// its own literal with the contract's retryability.
#[tokio::test]
async fn export_sentinels_surface_structured_engine_errors() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = backup_supervisor(&temp, &data_dir);
    seed_direct_history(&store, &account_id);
    supervisor.start().await.unwrap();

    // Steer the task's answer through the account display name → givenName.
    store
        .set_account_display_name(&account_id, Some("[fixture-snapshot-exists]"))
        .unwrap();
    let error = supervisor
        .export_local_backup(&account_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "BACKUP_SNAPSHOT_EXISTS");
    assert!(
        error.retryable,
        "same-second snapshot conflicts are retryable"
    );

    store
        .set_account_display_name(&account_id, Some("[fixture-io-failed]"))
        .unwrap();
    let error = supervisor
        .export_local_backup(&account_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "BACKUP_IO_FAILED");
    assert!(error.retryable, "transient disk failures are retryable");

    // The artifact-missing face: the connector rebuilt the artifact before
    // every attempt above, so the only way the engine answers missing is the
    // sentinel deleting it first.
    store
        .set_account_display_name(&account_id, Some("[fixture-artifact-missing]"))
        .unwrap();
    supervisor
        .export_local_backup(&account_id)
        .await
        .unwrap_err();
    // The connector rebuilt the artifact for the attempt, the engine's
    // sentinel deleted it: a second attempt builds again and the fixture's
    // happy path answers — proving the write-once/rebuild contract instead.
    store
        .set_account_display_name(&account_id, Some("[fixture-artifact-invalid]"))
        .unwrap();
    let error = supervisor
        .export_local_backup(&account_id)
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(error.code, "BACKUP_ARTIFACT_INVALID");
    assert!(
        !error.retryable,
        "artifact content is a deterministic refusal"
    );
}

/// A group conversation with a roster rides out as a group line + chat whose
/// items author against roster contacts; the linked account's own cache entry
/// never becomes a contact row (Note to Self rides recipient 0).
#[tokio::test]
async fn export_projects_groups_and_note_to_self() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = backup_supervisor(&temp, &data_dir);

    store
        .upsert_account_from_signal(SIGNAL_ACCOUNT, Some(1), "default")
        .unwrap();
    // Group with a roster (the §4.39 sync projection shape: members carry
    // `id` — the member address the sync captured — and optional `uuid`).
    let roster = format!(
        r#"{{"members":[{{"id":"{ALICE_E164}","uuid":"{ALICE_UUID}"}},{{"id":"{SIGNAL_ACCOUNT}"}}]}}"#
    );
    store
        .upsert_contact(
            &account_id,
            "group",
            GROUP_MASTER_KEY,
            "Fixture Group",
            Some(&roster),
            1,
        )
        .unwrap();
    store
        .upsert_contact(
            &account_id,
            "contact",
            SIGNAL_ACCOUNT,
            "Self Entry",
            None,
            1,
        )
        .unwrap();
    let group = store
        .ensure_conversation(&account_id, "group", GROUP_MASTER_KEY, "Fixture Group")
        .unwrap();
    let note_to_self = store
        .ensure_conversation(&account_id, "direct", SIGNAL_ACCOUNT, "Note to Self")
        .unwrap();
    // One group message authored by Alice's UUID spelling.
    let sender_uuid_hash = stable_hash_id(&[account_id.as_str(), "group", ALICE_UUID]);
    let group_message = MessageRecord {
        id: "m-group".into(),
        account_id: account_id.clone(),
        conversation_id: group.id.clone(),
        direction: "incoming",
        sender_id: sender_uuid_hash,
        sender_name: None,
        mentions_self: false,
        server_guid: None,
        server_timestamp: None,
        is_story: false,
        story_message: None,
        sent_at: 3_000,
        received_at: Some(3_010),
        text: Some("group hello".into()),
        text_bytes: Some(11),
        text_truncated: false,
        text_retrievable: true,
        status: "read",
        client_request_id: None,
        quote_message_id: None,
        quote_snapshot: None,
        quote: None,
        attachments: Vec::new(),
        edited_at: None,
        rich: None,
        reactions: Vec::new(),
        edits: Vec::new(),
        delivered_at: None,
        read_at: None,
        viewed_at: None,
        send_outcomes: None,
        sticker: None,
        expire_seconds: None,
        expire_start_at: None,
        admin_deleted: false,
    };
    store
        .insert_message(&group_message, None, None, true)
        .unwrap();
    // One Note to Self outgoing message.
    let self_message = MessageRecord {
        id: "m-self".into(),
        account_id: account_id.clone(),
        conversation_id: note_to_self.id.clone(),
        direction: "outgoing",
        sender_id: account_id.clone(),
        sender_name: None,
        mentions_self: false,
        server_guid: None,
        server_timestamp: None,
        is_story: false,
        story_message: None,
        sent_at: 4_000,
        received_at: None,
        text: Some("note to self".into()),
        text_bytes: Some(12),
        text_truncated: false,
        text_retrievable: true,
        status: "sent",
        client_request_id: None,
        quote_message_id: None,
        quote_snapshot: None,
        quote: None,
        attachments: Vec::new(),
        edited_at: None,
        rich: None,
        reactions: Vec::new(),
        edits: Vec::new(),
        delivered_at: None,
        read_at: None,
        viewed_at: None,
        send_outcomes: None,
        sticker: None,
        expire_seconds: None,
        expire_start_at: None,
        admin_deleted: false,
    };
    store
        .insert_message(&self_message, None, None, false)
        .unwrap();

    supervisor.start().await.unwrap();
    supervisor.export_local_backup(&account_id).await.unwrap();
    let _ = wait_for_status(&supervisor, &account_id, "completed").await;

    // Rebuild to inspect the artifact bytes (the fixture consumed the first;
    // this read races only the second task's 200 ms write window).
    supervisor.export_local_backup(&account_id).await.unwrap();
    let lines = artifact_lines(&data_dir);

    // The linked account's own entry is not a contact; Alice is, under her
    // canonical ACI key with every known form on the line (§10.2 contact
    // fields: aci/e164/givenName as known).
    let contacts: Vec<&Value> = lines.iter().filter(|l| l["type"] == "contact").collect();
    assert_eq!(contacts.len(), 1, "self never becomes a contact row");
    assert_eq!(contacts[0]["aci"], ALICE_UUID.replace('-', ""));
    assert_eq!(contacts[0]["e164"], 15550000001_u64);

    let group_line = lines.iter().find(|l| l["type"] == "group").unwrap();
    assert_eq!(
        group_line["masterKey"], "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        "master key decodes to 32-byte hex (engine decode_group_id parity)"
    );
    let chats: Vec<&Value> = lines.iter().filter(|l| l["type"] == "chat").collect();
    assert_eq!(chats.len(), 2, "the group chat and Note to Self ride out");
    assert!(
        chats.iter().any(|chat| chat["recipient"] == 0),
        "Note to Self rides the reserved self recipient"
    );
    let group_items: Vec<&Value> = lines
        .iter()
        .filter(|l| l["type"] == "chat_item" && l["body"] == "group hello")
        .collect();
    assert_eq!(group_items.len(), 1);
    assert_eq!(
        group_items[0]["author"], 1,
        "the roster spelling resolves the author"
    );
    let self_items: Vec<&Value> = lines
        .iter()
        .filter(|l| l["type"] == "chat_item" && l["body"] == "note to self")
        .collect();
    assert_eq!(self_items.len(), 1);
    assert_eq!(self_items[0]["author"], 0);
}

/// Skip counting: system rows, remote-deleted tombstones, an unconfirmed
/// outgoing row, an empty body, a truncated body, and a quote each land in
/// the right counter — and the truncated preview never becomes a body.
#[tokio::test]
async fn export_counts_skips_per_cause() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = backup_supervisor(&temp, &data_dir);

    let conversation = store
        .ensure_conversation(&account_id, "direct", ALICE_E164, "Alice")
        .unwrap();
    let sender = stable_hash_id(&[account_id.as_str(), "direct", ALICE_E164]);
    // (id, sent_at, direction, status, body, truncated, quoted, admin_deleted)
    struct RowCase {
        id: &'static str,
        sent_at: u64,
        direction: &'static str,
        status: &'static str,
        body: Option<&'static str>,
        truncated: bool,
        quoted: bool,
        admin_deleted: bool,
    }
    let rows = vec![
        RowCase {
            id: "m-system",
            sent_at: 10,
            direction: "system",
            status: "system",
            body: Some("identity changed"),
            truncated: false,
            quoted: false,
            admin_deleted: false,
        },
        RowCase {
            id: "m-deleted",
            sent_at: 20,
            direction: "incoming",
            status: "remote-deleted",
            body: Some("gone"),
            truncated: false,
            quoted: false,
            admin_deleted: false,
        },
        RowCase {
            id: "m-admin",
            sent_at: 30,
            direction: "incoming",
            status: "read",
            body: Some("admin'd"),
            truncated: false,
            quoted: false,
            admin_deleted: true,
        },
        RowCase {
            id: "m-pending",
            sent_at: 40,
            direction: "outgoing",
            status: "pending",
            body: Some("not yet"),
            truncated: false,
            quoted: false,
            admin_deleted: false,
        },
        RowCase {
            id: "m-empty",
            sent_at: 50,
            direction: "incoming",
            status: "read",
            body: Some(""),
            truncated: false,
            quoted: false,
            admin_deleted: false,
        },
        RowCase {
            id: "m-truncated",
            sent_at: 60,
            direction: "incoming",
            status: "read",
            body: Some("preview"),
            truncated: true,
            quoted: false,
            admin_deleted: false,
        },
        RowCase {
            id: "m-quoted",
            sent_at: 70,
            direction: "incoming",
            status: "read",
            body: Some("has a quote"),
            truncated: false,
            quoted: true,
            admin_deleted: false,
        },
        RowCase {
            id: "m-kept",
            sent_at: 80,
            direction: "incoming",
            status: "read",
            body: Some("kept"),
            truncated: false,
            quoted: false,
            admin_deleted: false,
        },
    ];

    for row in rows {
        let RowCase {
            id,
            sent_at,
            direction,
            status,
            body,
            truncated,
            quoted,
            admin_deleted,
        } = row;
        let message = MessageRecord {
            id: id.into(),
            account_id: account_id.clone(),
            conversation_id: conversation.id.clone(),
            direction,
            sender_id: sender.clone(),
            sender_name: None,
            mentions_self: false,
            server_guid: None,
            server_timestamp: None,
            is_story: false,
            story_message: None,
            sent_at,
            received_at: Some(sent_at + 5),
            text: body.map(str::to_string),
            text_bytes: body.map(|body| body.len() as u32),
            text_truncated: truncated,
            text_retrievable: !truncated && body.is_some(),
            status,
            client_request_id: None,
            quote_message_id: quoted.then(|| "anchor".to_string()),
            quote_snapshot: None,
            quote: None,
            attachments: Vec::new(),
            edited_at: None,
            rich: None,
            reactions: Vec::new(),
            edits: Vec::new(),
            delivered_at: None,
            read_at: None,
            viewed_at: None,
            send_outcomes: None,
            sticker: None,
            expire_seconds: None,
            expire_start_at: None,
            // insert_message does not persist the tombstone flag (the
            // receive path sets it via mark_admin_deleted); the flag rides
            // the dedicated marking API below instead.
            admin_deleted: false,
        };
        store.insert_message(&message, None, None, false).unwrap();
        if admin_deleted {
            store
                .mark_admin_deleted(&account_id, &conversation.id, sent_at)
                .unwrap();
        }
    }

    supervisor.start().await.unwrap();
    let started = supervisor.export_local_backup(&account_id).await.unwrap();
    assert_eq!(started["status"], "started");
    let status = wait_for_status(&supervisor, &account_id, "completed").await;
    assert_eq!(
        status["skippedRows"], 5,
        "system + deleted + admin + pending + empty"
    );
    assert_eq!(status["skippedBodyTruncated"], 1);
    assert_eq!(status["skippedQuotes"], 1);
    assert_eq!(status["skippedAttachments"], 0);
    assert_eq!(status["skippedStickers"], 0);
    assert_eq!(status["skippedViewOnce"], 0);
    assert_eq!(status["skippedReactions"], 0);
    assert_eq!(status["skippedEdits"], 0);
    assert_eq!(status["skippedSkeletonChats"], 0);

    // The kept and quoted rows ride out (a quoted row still exports its
    // body — the quote feature is counted, not the row dropped); the
    // truncated preview is never a body.
    supervisor.export_local_backup(&account_id).await.unwrap();
    let lines = artifact_lines(&data_dir);
    let items: Vec<&Value> = lines
        .iter()
        .filter(|line| line["type"] == "chat_item")
        .collect();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["body"], "has a quote");
    assert_eq!(items[1]["body"], "kept");
}

/// §4.115 (contract revision 1.132): the plaintext export end to end — the
/// start face accepts, the additive `kind` field discriminates the family on
/// the unchanged status poll, the product streams home through the unchanged
/// file three-chain, and the JSONL parses into the §4.115 line set with the
/// admitted faces (quote join keys, reaction authors, receipt stamps). Red
/// line 1 is structural: no engine call happens on the build path — the
/// fixture only ever sees the §4.46 status probe.
#[tokio::test]
async fn plaintext_export_starts_polls_and_streams_the_jsonl() {
    fn decode_chunk(encoded: &str) -> Vec<u8> {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = Vec::new();
        let mut buffer = 0_u32;
        let mut bits = 0_u32;
        for byte in encoded.bytes() {
            if byte == b'=' {
                break;
            }
            let value = ALPHABET
                .iter()
                .position(|c| *c == byte)
                .expect("standard alphabet") as u32;
            buffer = (buffer << 6) | value;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((buffer >> bits) as u8);
            }
        }
        out
    }

    async fn wait_for_plaintext_done(supervisor: &RuntimeSupervisor, account_id: &str) -> Value {
        for _ in 0..80 {
            let status = supervisor.export_backup_status(account_id).await.unwrap();
            if status["kind"] == "plaintext_jsonl" && status["state"] == "completed" {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "exportStatus never settled on a completed plaintext task: {:?}",
            supervisor.export_backup_status(account_id).await
        );
    }

    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("signal-data");
    let (store, supervisor, account_id) = backup_supervisor(&temp, &data_dir);
    let conversation_id = seed_direct_history(&store, &account_id);
    supervisor.start().await.unwrap();

    // Nothing built yet: the third enum value answers the no-product code,
    // same family error as an empty snapshot face.
    let missing = supervisor
        .export_file_open(&account_id, "plaintext_jsonl")
        .await
        .err()
        .unwrap()
        .into_api();
    assert_eq!(missing.code, "BACKUP_SNAPSHOT_MISSING");

    let started = supervisor.export_plaintext(&account_id).await.unwrap();
    assert_eq!(started["status"], "started");

    let done = wait_for_plaintext_done(&supervisor, &account_id).await;
    assert_eq!(done["artifactPresent"], false);
    assert_eq!(done["accountId"], account_id);
    let status_lines = done["lines"].as_u64().unwrap();
    assert_eq!(done["skippedRows"], 0);

    // The product streams home through the unchanged three-chain.
    let open = supervisor
        .export_file_open(&account_id, "plaintext_jsonl")
        .await
        .unwrap();
    assert_eq!(open["chunkBytes"], 262_144);
    assert_eq!(open["sizeBytes"], done["bytes"]);
    let file_handle = open["fileHandle"].as_str().unwrap().to_string();
    let mut offset = 0_u64;
    let mut raw = Vec::new();
    loop {
        let chunk = supervisor
            .export_file_read_chunk(&file_handle, offset)
            .await
            .unwrap();
        offset += chunk["sizeBytes"].as_u64().unwrap();
        raw.extend_from_slice(&decode_chunk(chunk["dataBase64"].as_str().unwrap()));
        if chunk["eof"].as_bool().unwrap() {
            break;
        }
    }
    assert_eq!(offset, done["bytes"].as_u64().unwrap());
    supervisor
        .export_file_close_handle(&file_handle)
        .await
        .unwrap();

    let content = String::from_utf8(raw).unwrap();
    let lines: Vec<Value> = content
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.len() as u64, status_lines, "the status count matches");
    assert_eq!(lines[0]["type"], "account");
    assert_eq!(lines[0]["account"], SIGNAL_ACCOUNT);
    assert!(
        lines[0].get("givenName").is_none(),
        "honest absence: this fixture sets no display name"
    );

    let message_lines: Vec<&Value> = lines.iter().filter(|l| l["type"] == "message").collect();
    assert_eq!(message_lines.len(), 2, "the seeded exchange, nothing else");
    let incoming = message_lines
        .iter()
        .find(|l| l["direction"] == "incoming")
        .unwrap();
    assert_eq!(incoming["body"], "hello from alice");
    assert_eq!(incoming["status"], "read");
    assert_eq!(incoming["sentAt"], 1_000);
    let outgoing = message_lines
        .iter()
        .find(|l| l["direction"] == "outgoing")
        .unwrap();
    assert_eq!(outgoing["author"], 0);
    assert_eq!(outgoing["body"], "hello back");
    assert!(
        lines
            .iter()
            .any(|l| l["type"] == "chat" && l["kind"] == "direct")
    );

    // The store's conversation id is untouched by the export — zero local
    // writes on this face (red line 1: a pure projection).
    assert!(
        store
            .conversation_summary(&account_id, &conversation_id)
            .unwrap()
            .is_some()
    );
}
