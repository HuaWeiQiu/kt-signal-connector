// SPDX-License-Identifier: AGPL-3.0-only

//! Launcher-defined proxy groups (ADR 0001, optimization-plan Phase 4).
//!
//! Groups are process input from the trusted launcher, never IPC input (R1):
//! repeatable `serve --proxy-group <id>=<host:port>` or the
//! `KT_SIGNAL_PROXY_GROUPS` comma-separated equivalent. The reserved `default`
//! group always exists and is the compatibility anchor (R2): with no group
//! configuration the plan is exactly one direct-connection `default` group,
//! byte-identical to the pre-Phase-4 process model. The legacy global proxy
//! configures the `default` group only. The hard ceiling is 8 groups (R6).
//!
//! Error messages carry group ids but never proxy endpoints (R10).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::DEFAULT_PROXY_GROUP_ID;
use crate::engine::SocksProxy;

/// Hard ceiling on the number of groups including `default` (ADR 0001 R6).
pub const MAX_PROXY_GROUPS: usize = 8;

/// One launcher-planned group: opaque id, optional SOCKS proxy, and the
/// per-group signal-cli data directory (R4). The proxy endpoint stays inside
/// the connector; it never crosses IPC or logs.
#[derive(Clone, Debug)]
pub struct ProxyGroupPlanEntry {
    pub id: String,
    pub proxy: Option<SocksProxy>,
    pub data_dir: PathBuf,
}

/// Ordered launch plan for all groups. Order is launcher configuration order:
/// `default` first, then flag entries in order, then env entries in order.
#[derive(Clone, Debug)]
pub struct ProxyGroupPlan {
    pub groups: Vec<ProxyGroupPlanEntry>,
}

/// Group id grammar: `^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$` — lowercase
/// alphanumerics with inner hyphens, at most 32 bytes, filesystem-safe so it
/// can name a data directory (R1).
pub fn is_valid_group_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.is_empty() || bytes.len() > 32 {
        return false;
    }
    let alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if !alphanumeric(bytes[0]) || !alphanumeric(bytes[bytes.len() - 1]) {
        return false;
    }
    // A single-character id has no interior; anything longer keeps hyphens
    // out of the boundary positions.
    bytes.len() < 2
        || bytes[1..bytes.len() - 1]
            .iter()
            .all(|&byte| alphanumeric(byte) || byte == b'-')
}

/// Parse one `id=host:port` spec. The error text names the spec's id but never
/// echoes the proxy endpoint value.
fn parse_group_spec(spec: &str) -> Result<(String, SocksProxy), String> {
    let Some((id, proxy_value)) = spec.split_once('=') else {
        return Err(format!(
            "proxy group {spec:?} must use the form <id>=<host:port>"
        ));
    };
    if !is_valid_group_id(id) {
        return Err(format!("proxy group id {id:?} is invalid"));
    }
    let proxy = SocksProxy::parse(proxy_value).map_err(|_| {
        format!("proxy group {id:?} has an invalid proxy value (expected host:port)")
    })?;
    Ok((id.to_string(), proxy))
}

/// Build the full launch plan. `default` always exists first; every additional
/// entry gets its own data directory under `<signal-data-dir>/proxy-groups/`
/// (R4). Any duplicate id, a redefinition of `default`, more than
/// [`MAX_PROXY_GROUPS`] entries, or any malformed id or proxy aborts startup.
///
/// When both the flag and the environment variable are provided, flag entries
/// come first in launcher order, then environment entries; duplicates across
/// the two sources are rejected rather than merged.
pub fn build_group_plan(
    cli_specs: &[String],
    env_spec: Option<&str>,
    legacy_proxy: Option<SocksProxy>,
    signal_data_dir: &Path,
) -> Result<ProxyGroupPlan, String> {
    let mut specs: Vec<&str> = cli_specs.iter().map(String::as_str).collect();
    if let Some(env) = env_spec {
        specs.extend(
            env.split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty()),
        );
    }

    let mut extra = Vec::with_capacity(specs.len());
    let mut seen = HashSet::new();
    for spec in specs {
        let (id, proxy) = parse_group_spec(spec)?;
        if id == DEFAULT_PROXY_GROUP_ID {
            return Err("proxy group 'default' is reserved and cannot be redefined".into());
        }
        if !seen.insert(id.clone()) {
            return Err(format!("proxy group {id:?} is defined more than once"));
        }
        extra.push((id, proxy));
    }
    if extra.len() + 1 > MAX_PROXY_GROUPS {
        return Err(format!(
            "too many proxy groups: {} configured, the hard ceiling is {MAX_PROXY_GROUPS}",
            extra.len() + 1,
        ));
    }

    let mut groups = vec![ProxyGroupPlanEntry {
        id: DEFAULT_PROXY_GROUP_ID.to_string(),
        proxy: legacy_proxy,
        // R2/R4: the default group keeps today's --signal-data-dir path.
        data_dir: signal_data_dir.to_path_buf(),
    }];
    groups.extend(extra.into_iter().map(|(id, proxy)| ProxyGroupPlanEntry {
        data_dir: signal_data_dir.join("proxy-groups").join(&id),
        id,
        // Every explicit group spec carries its own endpoint by construction.
        proxy: Some(proxy),
    }));
    Ok(ProxyGroupPlan { groups })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proxy(host: &str, port: u16) -> Option<SocksProxy> {
        Some(SocksProxy {
            host: host.into(),
            port,
        })
    }

    #[test]
    fn group_id_grammar_matches_the_contract_regex() {
        for id in ["a", "0", "team-a", "a1-b2-c3", &"a".repeat(32)] {
            assert!(is_valid_group_id(id), "{id:?} must be valid");
        }
        for id in [
            "",
            "-a",
            "a-",
            "A",
            "Team",
            "a_b",
            "a.b",
            "a b",
            "ä",
            "-",
            &"a".repeat(33),
        ] {
            assert!(!is_valid_group_id(id), "{id:?} must be rejected");
        }
    }

    #[test]
    fn empty_configuration_is_a_single_direct_default_group() {
        let plan =
            build_group_plan(&[], None, None, Path::new("/data")).expect("empty config is valid");
        assert_eq!(plan.groups.len(), 1);
        assert_eq!(plan.groups[0].id, "default");
        assert_eq!(plan.groups[0].proxy, None);
        assert_eq!(plan.groups[0].data_dir, PathBuf::from("/data"));
    }

    #[test]
    fn default_is_reserved_and_cannot_be_redefined() {
        let error = build_group_plan(
            &["default=127.0.0.1:1080".to_string()],
            None,
            None,
            Path::new("/data"),
        )
        .unwrap_err();
        assert!(error.contains("'default' is reserved"), "{error}");
    }

    #[test]
    fn malformed_specs_are_rejected_without_echoing_endpoints() {
        for spec in [
            "no-equals-sign",
            "=127.0.0.1:1080",
            "Bad=127.0.0.1:1080",
            "a-=127.0.0.1:1080",
            "ok=:not-a-port",
            "ok=127.0.0.1",
        ] {
            let error =
                build_group_plan(&[spec.to_string()], None, None, Path::new("/data")).unwrap_err();
            assert!(!error.contains("127.0.0.1"), "endpoint leaked: {error}");
        }
    }

    #[test]
    fn plans_keep_launcher_order_and_per_group_data_dirs() {
        let plan = build_group_plan(
            &[
                "alpha=10.0.0.1:1080".to_string(),
                "beta=10.0.0.2:1080".to_string(),
            ],
            None,
            proxy("127.0.0.1", 1080),
            Path::new("/data"),
        )
        .expect("valid plan");
        let ids: Vec<_> = plan.groups.iter().map(|g| g.id.as_str()).collect();
        assert_eq!(ids, ["default", "alpha", "beta"]);
        assert_eq!(plan.groups[0].proxy, proxy("127.0.0.1", 1080));
        assert_eq!(plan.groups[1].proxy, proxy("10.0.0.1", 1080));
        assert_eq!(
            plan.groups[1].data_dir,
            PathBuf::from("/data/proxy-groups/alpha")
        );
        assert_eq!(
            plan.groups[2].data_dir,
            PathBuf::from("/data/proxy-groups/beta")
        );
    }

    #[test]
    fn environment_entries_append_after_flag_entries_and_duplicates_fail() {
        let plan = build_group_plan(
            &["flagged=127.0.0.1:1".to_string()],
            Some("from-env=127.0.0.1:2"),
            None,
            Path::new("/data"),
        )
        .expect("flag plus env is valid");
        let ids: Vec<_> = plan.groups.iter().map(|g| g.id.as_str()).collect();
        assert_eq!(ids, ["default", "flagged", "from-env"]);

        // Whitespace around comma-separated entries is tolerated; empties skip.
        let plan = build_group_plan(
            &[],
            Some(" a=127.0.0.1:3 , , b=127.0.0.1:4 "),
            None,
            Path::new("/data"),
        )
        .expect("env parsing tolerates whitespace");
        let ids: Vec<_> = plan.groups.iter().map(|g| g.id.as_str()).collect();
        assert_eq!(ids, ["default", "a", "b"]);

        let error = build_group_plan(
            &["dup=127.0.0.1:5".to_string()],
            Some("dup=127.0.0.1:6"),
            None,
            Path::new("/data"),
        )
        .unwrap_err();
        assert!(error.contains("more than once"), "{error}");
    }

    #[test]
    fn group_count_is_hard_capped_at_eight_including_default() {
        let specs: Vec<String> = (1..=7)
            .map(|index| format!("group-{index}=127.0.0.1:{index}"))
            .collect();
        let plan = build_group_plan(&specs, None, None, Path::new("/data"))
            .expect("8 total groups are allowed");
        assert_eq!(plan.groups.len(), MAX_PROXY_GROUPS);

        let too_many: Vec<String> = (1..=8)
            .map(|index| format!("group-{index}=127.0.0.1:{index}"))
            .collect();
        let error = build_group_plan(&too_many, None, None, Path::new("/data")).unwrap_err();
        assert!(error.contains("hard ceiling"), "{error}");
    }
}
