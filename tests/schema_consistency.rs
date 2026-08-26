// SPDX-License-Identifier: AGPL-3.0-only

//! Phase 0 safety net (docs/optimization-plan.md, 2026-08-25): machine-checked
//! consistency between `schemas/connector-api-v1.schema.json` and the code, so
//! contract drift fails the test suite instead of shipping silently.
//!
//! Code-side sets are extracted mechanically where a stable source exists:
//!
//! - methods: `PHASE2_CAPABILITIES` (src/lib.rs:23) plus `"handshake"`, which is
//!   handled before capability dispatch (src/host.rs:307). Every schema method
//!   must also appear as a dispatch literal in src/host.rs.
//! - events: every `HostEvent::new("<name>", ...)` literal in src/ (the wire
//!   serializer is the emission boundary).
//! - error codes: every `ApiError::new("<CODE>", ...)` literal in src/.
//!
//! Known drift is encoded in the explicit allowlists below, each with a reason
//! and the phase that resolves it. Any drift outside an allowlist fails.

#![cfg(unix)]

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use serde_json::Value;

use kt_signal_connector::PHASE2_CAPABILITIES;

/// Events the code emits but the schema event enum does not declare. Currently
/// none: `runtime.resourcePressure` (emitted by the RSS sampler bridge,
/// src/host.rs) was registered in the schema by optimization-plan Phase 1,
/// recording existing behavior rather than changing the protocol.
const EVENT_CODE_ONLY_ALLOWLIST: &[&str] = &[];

/// Events the schema declares but the code never emits. Currently none:
/// `message.statusChanged` gained a producer in optimization-plan Phase 2
/// (send completion paths in src/service.rs) and is emitted like every other
/// event, so it is covered by the plain set diff.
const EVENT_SCHEMA_ONLY_ALLOWLIST: &[&str] = &[];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn schema() -> Value {
    let path = repo_root().join("schemas/connector-api-v1.schema.json");
    let raw = fs::read_to_string(&path).expect("schema file must be readable");
    serde_json::from_str(&raw).expect("schema file must be valid JSON")
}

fn schema_enum(pointer: &str) -> BTreeSet<String> {
    schema()
        .pointer(pointer)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("schema must declare an enum at {pointer}"))
        .iter()
        .map(|value| {
            value
                .as_str()
                .expect("schema enum entries must be strings")
                .to_string()
        })
        .collect()
}

fn schema_methods() -> BTreeSet<String> {
    schema_enum("/$defs/request/properties/method/enum")
}

fn schema_events() -> BTreeSet<String> {
    schema_enum("/$defs/event/properties/event/enum")
}

fn schema_error_codes() -> BTreeSet<String> {
    schema_enum("/$defs/error/properties/code/enum")
}

/// All Rust sources under src/, as (relative path, content) pairs. Test-only
/// `#[cfg(test)]` modules are part of the crate's code and are scanned too:
/// a literal that only a unit test uses is still a defined code/event name.
fn sources() -> Vec<(String, String)> {
    let src = repo_root().join("src");
    let mut entries: Vec<(String, String)> = fs::read_dir(&src)
        .expect("src/ must be readable")
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .map(|path| {
            (
                path.file_name().unwrap().to_string_lossy().into_owned(),
                fs::read_to_string(&path).unwrap(),
            )
        })
        .collect();
    entries.sort();
    assert!(!entries.is_empty(), "src/ must contain Rust sources");
    entries
}

/// Extract the string literal in the first argument position of every
/// `Constructor::new("literal", ...)` call site, tolerating line breaks
/// between the opening parenthesis and the literal.
fn first_string_literals(sources: &[(String, String)], constructor: &str) -> BTreeSet<String> {
    let marker = format!("{constructor}::new(");
    let mut found = BTreeSet::new();
    for (_name, content) in sources {
        let mut rest = content.as_str();
        while let Some(at) = rest.find(&marker) {
            rest = &rest[at + marker.len()..];
            let trimmed = rest.trim_start();
            let Some(literal) = trimmed.strip_prefix('"') else {
                continue;
            };
            let Some(end) = literal.find('"') else {
                continue;
            };
            found.insert(literal[..end].to_string());
            rest = &literal[end..];
        }
    }
    found
}

fn code_events() -> BTreeSet<String> {
    first_string_literals(&sources(), "HostEvent")
}

fn code_error_codes() -> BTreeSet<String> {
    first_string_literals(&sources(), "ApiError")
        .into_iter()
        .filter(|literal| {
            !literal.is_empty()
                && literal
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        })
        .collect()
}

#[test]
fn schema_methods_match_code_methods() {
    let schema_methods = schema_methods();

    // Code-side method list: the advertised capability list (src/lib.rs:23-38)
    // plus "handshake", which is answered before capability dispatch
    // (src/host.rs:307).
    let mut code_methods: BTreeSet<String> = PHASE2_CAPABILITIES
        .iter()
        .map(|method| (*method).to_string())
        .collect();
    code_methods.insert("handshake".to_string());

    let schema_only: Vec<_> = schema_methods.difference(&code_methods).collect();
    let code_only: Vec<_> = code_methods.difference(&schema_methods).collect();
    assert!(
        schema_only.is_empty() && code_only.is_empty(),
        "method drift: schema-only {schema_only:?}, code-only {code_only:?}"
    );

    // Every schema method must be dispatched by the host: it appears as a
    // string literal in src/host.rs.
    let host = sources()
        .into_iter()
        .find(|(name, _)| name == "host.rs")
        .expect("src/host.rs must exist")
        .1;
    for method in &schema_methods {
        assert!(
            host.contains(&format!("\"{method}\"")),
            "schema method {method} has no dispatch literal in src/host.rs"
        );
    }
}

#[test]
fn schema_error_codes_match_code_error_codes() {
    let schema_codes = schema_error_codes();
    let code_codes = code_error_codes();
    assert!(
        !code_codes.is_empty(),
        "error-code scan found no ApiError::new literals; the scanner is broken"
    );

    let schema_only: Vec<_> = schema_codes.difference(&code_codes).collect();
    let code_only: Vec<_> = code_codes.difference(&schema_codes).collect();
    assert!(
        schema_only.is_empty() && code_only.is_empty(),
        "error-code drift: schema-only {schema_only:?}, code-only {code_only:?}"
    );
}

#[test]
fn schema_events_match_emitted_events_within_allowlist() {
    let schema_events = schema_events();
    let emitted = code_events();
    assert!(
        !emitted.is_empty(),
        "event scan found no HostEvent::new literals; the scanner is broken"
    );

    let allowlisted_code_only: BTreeSet<&str> = EVENT_CODE_ONLY_ALLOWLIST.iter().copied().collect();
    let allowlisted_schema_only: BTreeSet<&str> =
        EVENT_SCHEMA_ONLY_ALLOWLIST.iter().copied().collect();

    let code_only: BTreeSet<&str> = emitted
        .iter()
        .map(String::as_str)
        .filter(|event| !schema_events.contains(*event))
        .collect();
    let schema_only: BTreeSet<&str> = schema_events
        .iter()
        .map(String::as_str)
        .filter(|event| !emitted.contains(*event))
        .collect();

    assert_eq!(
        code_only, allowlisted_code_only,
        "code emits events outside the schema and the allowlist (or an allowlisted \
         drift was fixed — remove its entry): {code_only:?}"
    );
    assert_eq!(
        schema_only, allowlisted_schema_only,
        "schema declares events the code never emits, outside the allowlist (or an \
         allowlisted drift was fixed — remove its entry): {schema_only:?}"
    );
}
