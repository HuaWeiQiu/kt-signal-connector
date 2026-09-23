// SPDX-License-Identifier: AGPL-3.0-only

use std::path::PathBuf;
use std::process::{Command as ProcessCommand, Stdio};
use std::time::Duration;

use clap::{Parser, Subcommand};
use kt_signal_connector::auth::{load_bootstrap_payload, load_bootstrap_payload_from_reader};
use kt_signal_connector::datalock;
use kt_signal_connector::engine::{SignalCliMode, SocksProxy};
use kt_signal_connector::groups;
use kt_signal_connector::host::serve;
use kt_signal_connector::ipc::LocalListener;
#[cfg(windows)]
use kt_signal_connector::ipc::harden_private_directory;
use kt_signal_connector::lkg::RuntimeLayout;
use kt_signal_connector::manifest::{
    LicenseRef, RuntimeManifest, artifact_for_file, build_local_unsigned_manifest,
    load_signing_key, load_verifying_key,
};
#[cfg(windows)]
use kt_signal_connector::parent::wait_for_parent_exit;
use kt_signal_connector::registry::open_group_runtime;
use kt_signal_connector::resource::{measure_child_idle, write_report};
use kt_signal_connector::store::store_key_from_env;

#[derive(Debug, Parser)]
#[command(name = "kt-signal-connector", version, about)]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Serve the private KT host protocol.
    Serve {
        #[arg(long)]
        endpoint: PathBuf,
        #[arg(
            long,
            conflicts_with = "bootstrap_secret_stdin",
            required_unless_present = "bootstrap_secret_stdin"
        )]
        bootstrap_secret_file: Option<PathBuf>,
        #[arg(
            long,
            default_value_t = false,
            conflicts_with = "bootstrap_secret_file"
        )]
        bootstrap_secret_stdin: bool,
        #[arg(long)]
        parent_pid: Option<u32>,
        #[arg(long)]
        signal_cli: PathBuf,
        /// The signal-cli executable is a GraalVM native binary, not the JVM
        /// launcher script (env: KT_SIGNAL_CLI_NATIVE=1, the form Desktop
        /// uses). Native mode skips JAVA_HOME validation and JAVA_OPTS
        /// injection and passes a configured SOCKS proxy as `-D` argv
        /// properties instead.
        #[arg(
            long,
            env = "KT_SIGNAL_CLI_NATIVE",
            num_args = 0..=1,
            default_value_t = false,
            default_missing_value = "true",
            value_parser = clap::builder::BoolishValueParser::new(),
        )]
        signal_cli_native: bool,
        #[arg(long)]
        java_home: Option<PathBuf>,
        /// SOCKS proxy for the signal-cli child as host:port (env:
        /// KT_SIGNAL_SOCKS_PROXY, preferred so it stays off the connector
        /// command line). JVM mode forwards it via JAVA_OPTS; native mode
        /// passes it to the child as `-D` argv properties.
        #[arg(long, env = "KT_SIGNAL_SOCKS_PROXY", value_name = "HOST:PORT")]
        socks_proxy: Option<SocksProxy>,
        /// Additional proxy group as ID=HOST:PORT; may be repeated (env:
        /// KT_SIGNAL_PROXY_GROUPS adds comma-separated entries). The implicit
        /// `default` group keeps this legacy SOCKS proxy and data directory.
        #[arg(long = "proxy-group", value_name = "ID=HOST:PORT")]
        proxy_group: Vec<String>,
        /// Media ingest opt-in (ADR 0002): the spawned signal-cli keeps
        /// inbound attachment downloads (no `--ignore-attachments`) under the
        /// connector's bounded media governor and chunked handle delivery.
        /// Without the flag the engine spawns byte-identical to the pre-PoC
        /// launcher and every media method answers CAPABILITY_UNAVAILABLE.
        #[arg(long, default_value_t = false)]
        media_ingest: bool,
        #[arg(long)]
        signal_data_dir: PathBuf,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Local packaging / LKG helpers (production signatures required by default).
    Package {
        #[command(subcommand)]
        command: PackageCommand,
    },
}

#[derive(Debug, Subcommand)]
enum PackageCommand {
    /// Build an unsigned local runtime manifest over an existing bundle directory.
    Manifest {
        #[arg(long)]
        bundle_dir: PathBuf,
        #[arg(long)]
        bundle_id: String,
        #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
        connector_version: String,
        #[arg(long, default_value = "0.14.7")]
        signal_cli_version: String,
        #[arg(long, default_value = "25")]
        jre_version: String,
        #[arg(long)]
        platform: Option<String>,
        #[arg(long, default_value = "bin/kt-signal-connector")]
        connector_path: String,
        #[arg(long, default_value = "bin/signal-cli")]
        signal_cli_path: String,
        #[arg(long, default_value = "jre/release")]
        jre_path: String,
        #[arg(long)]
        source_archive_path: Option<String>,
        #[arg(long = "license", value_name = "COMPONENT:SPDX:PATH")]
        licenses: Vec<String>,
        #[arg(long)]
        output: PathBuf,
    },
    /// Apply an Ed25519 production signature to a complete manifest.
    Sign {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        key_id: String,
        #[arg(long)]
        private_key_file: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Verify artifact hashes and, when required, an Ed25519 production signature.
    Verify {
        #[arg(long)]
        bundle_dir: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long, default_value_t = false)]
        require_signature: bool,
        #[arg(long)]
        trusted_key_id: Option<String>,
        #[arg(long)]
        trusted_public_key_file: Option<PathBuf>,
    },
    /// Copy a verified bundle into runtime versions/ and mark it staged.
    Stage {
        #[arg(long)]
        runtime_root: PathBuf,
        #[arg(long)]
        version_id: String,
        #[arg(long)]
        bundle_dir: PathBuf,
        #[arg(long, default_value_t = false)]
        allow_unsigned: bool,
        #[arg(long)]
        trusted_key_id: Option<String>,
        #[arg(long)]
        trusted_public_key_file: Option<PathBuf>,
    },
    /// Promote staged -> active and move previous active to LKG.
    Activate {
        #[arg(long)]
        runtime_root: PathBuf,
        #[arg(long, default_value_t = false)]
        allow_unsigned: bool,
        #[arg(long)]
        trusted_key_id: Option<String>,
        #[arg(long)]
        trusted_public_key_file: Option<PathBuf>,
    },
    /// Point active back at LKG.
    Rollback {
        #[arg(long)]
        runtime_root: PathBuf,
        #[arg(long, default_value_t = false)]
        allow_unsigned: bool,
        #[arg(long)]
        trusted_key_id: Option<String>,
        #[arg(long)]
        trusted_public_key_file: Option<PathBuf>,
    },
    /// Short idle RSS sample of an executable (smoke measurement, not a 24h gate).
    MeasureIdle {
        #[arg(long)]
        executable: PathBuf,
        #[arg(long = "arg")]
        args: Vec<String>,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 500)]
        settle_ms: u64,
    },
}

struct ServeOptions {
    endpoint: PathBuf,
    bootstrap_secret_file: Option<PathBuf>,
    bootstrap_secret_stdin: bool,
    parent_pid: Option<u32>,
    signal_cli: PathBuf,
    signal_cli_native: bool,
    java_home: Option<PathBuf>,
    socks_proxy: Option<SocksProxy>,
    proxy_group: Vec<String>,
    media_ingest: bool,
    signal_data_dir: PathBuf,
    state_dir: PathBuf,
}

/// Diagnostics go through `tracing` (AGENTS.md log discipline: no message
/// bodies, numbers, contacts, QR payloads, secrets, tokens, or key paths).
/// The final sink is stderr, same as the previous `eprintln!` output. Level
/// defaults to INFO; KT_SIGNAL_LOG overrides it (e.g. "debug").
fn init_tracing() {
    let level = std::env::var("KT_SIGNAL_LOG")
        .ok()
        .and_then(|value| value.parse::<tracing::Level>().ok())
        .unwrap_or(tracing::Level::INFO);
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(level)
        .try_init();
}

#[tokio::main]
async fn main() {
    init_tracing();
    let cli = Cli::parse();
    let result = match cli.command {
        CliCommand::Serve {
            endpoint,
            bootstrap_secret_file,
            bootstrap_secret_stdin,
            parent_pid,
            signal_cli,
            signal_cli_native,
            java_home,
            socks_proxy,
            proxy_group,
            media_ingest,
            signal_data_dir,
            state_dir,
        } => serve_command(ServeOptions {
            endpoint,
            bootstrap_secret_file,
            bootstrap_secret_stdin,
            parent_pid,
            signal_cli,
            signal_cli_native,
            java_home,
            socks_proxy,
            proxy_group,
            media_ingest,
            signal_data_dir,
            state_dir,
        })
        .await
        .map_err(|error| error.to_string()),
        CliCommand::Package { command } => package_command(command),
    };
    if let Err(error) = result {
        // The final exit diagnostic: `error` is the classified message of the
        // failing error type (Display is content-free across this crate).
        tracing::error!(%error, "connector exiting with an error");
        std::process::exit(1);
    }
}

async fn serve_command(options: ServeOptions) -> Result<(), Box<dyn std::error::Error>> {
    let ServeOptions {
        endpoint,
        bootstrap_secret_file,
        bootstrap_secret_stdin,
        parent_pid,
        signal_cli,
        signal_cli_native,
        java_home,
        socks_proxy,
        proxy_group,
        media_ingest,
        signal_data_dir,
        state_dir,
    } = options;
    if cfg!(windows) && !bootstrap_secret_stdin {
        return Err("Windows requires the inherited bootstrap secret pipe".into());
    }
    if cfg!(windows) && parent_pid.is_none() {
        return Err("Windows requires the parent process monitor".into());
    }
    // Proxy-group configuration is validated before anything else touches
    // the filesystem or the bootstrap payload: a malformed spec must fail the
    // launch without leaking any endpoint into diagnostics (ADR 0001 R10).
    // Flag entries come first in launcher order, then environment entries;
    // duplicates across the two sources are rejected rather than merged.
    let env_spec = std::env::var("KT_SIGNAL_PROXY_GROUPS").ok();
    let plan = groups::build_group_plan(
        &proxy_group,
        env_spec.as_deref(),
        socks_proxy,
        &signal_data_dir,
    )?;
    // ADR 0002 occupancy guard: exclusive cross-process locks over every
    // planned data directory (the `default` root and each proxy-group
    // subdirectory), acquired before the bootstrap payload is read and held
    // for process lifetime. A second connector over any shared data
    // directory fails fast here without touching secrets or publishing an
    // endpoint; a crashed holder's locks die with the process (kernel
    // release), so this can never brick the next start.
    let _data_dir_locks = datalock::lock_plan_data_dirs(&plan)?;
    let payload = if bootstrap_secret_stdin {
        load_bootstrap_payload_from_reader(std::io::stdin().lock())?
    } else {
        load_bootstrap_payload(
            bootstrap_secret_file
                .as_deref()
                .ok_or("bootstrap secret source is required")?,
        )?
    };
    // Phase 3 key contract: the desktop delivers the 32-byte store key as
    // bootstrap payload line 2; KT_SIGNAL_STORE_KEY is the dev/test override
    // and wins when set. The key never crosses the socket, never enters the
    // wire protocol, and is never logged.
    let store_key = match store_key_from_env()? {
        Some(key) => Some(key),
        None => payload.store_key,
    };
    #[cfg(windows)]
    {
        let root = state_dir
            .parent()
            .ok_or("Windows state directory must have a profile root")?;
        std::fs::create_dir_all(root)?;
        std::fs::create_dir_all(&signal_data_dir)?;
        std::fs::create_dir_all(&state_dir)?;
        harden_private_directory(root)?;
        harden_private_directory(&signal_data_dir)?;
        harden_private_directory(&state_dir)?;
    }
    let listener = LocalListener::bind(&endpoint)?;
    let signal_cli_mode = if signal_cli_native {
        SignalCliMode::Native
    } else {
        SignalCliMode::Jvm
    };
    let runtime = open_group_runtime(
        plan,
        signal_cli,
        &state_dir,
        java_home,
        store_key,
        signal_cli_mode,
        media_ingest,
    )?;
    #[cfg(windows)]
    {
        let parent_pid = parent_pid.expect("validated Windows parent PID");
        let parent_runtime = runtime.clone();
        tokio::spawn(async move {
            let _ = wait_for_parent_exit(parent_pid).await;
            let _ = parent_runtime.shutdown().await;
            std::process::exit(0);
        });
    }
    serve(listener, payload.secret, runtime).await?;
    Ok(())
}

fn package_command(command: PackageCommand) -> Result<(), String> {
    match command {
        PackageCommand::Manifest {
            bundle_dir,
            bundle_id,
            connector_version,
            signal_cli_version,
            jre_version,
            platform,
            connector_path,
            signal_cli_path,
            jre_path,
            source_archive_path,
            licenses,
            output,
        } => {
            let mut manifest = build_local_unsigned_manifest(
                &bundle_dir,
                bundle_id,
                connector_version,
                signal_cli_version,
                jre_version,
                &connector_path,
                &signal_cli_path,
                &jre_path,
            )
            .map_err(|error| error.to_string())?;
            if let Some(platform) = platform {
                if !matches!(
                    platform.as_str(),
                    "macos-arm64" | "macos-x64" | "windows-x64"
                ) {
                    return Err("unsupported production runtime platform".into());
                }
                manifest.platform = platform;
            }
            if let Some(source_archive_path) = source_archive_path {
                manifest.source_archive = Some(
                    artifact_for_file(&bundle_dir, &source_archive_path)
                        .map_err(|error| error.to_string())?,
                );
            }
            if !licenses.is_empty() {
                manifest.licenses = licenses
                    .iter()
                    .map(|value| parse_license_spec(&bundle_dir, value))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            manifest.save(&output).map_err(|error| error.to_string())?;
            println!(
                "wrote unsigned local manifest for {} at {}",
                manifest.platform,
                output.display()
            );
            Ok(())
        }
        PackageCommand::Verify {
            bundle_dir,
            manifest,
            require_signature,
            trusted_key_id,
            trusted_public_key_file,
        } => {
            let manifest = RuntimeManifest::load(&manifest).map_err(|error| error.to_string())?;
            if require_signature {
                let key_id = trusted_key_id
                    .as_deref()
                    .ok_or("--trusted-key-id is required with --require-signature")?;
                let key_path = trusted_public_key_file
                    .as_deref()
                    .ok_or("--trusted-public-key-file is required with --require-signature")?;
                let key = load_verifying_key(key_path).map_err(|error| error.to_string())?;
                manifest
                    .verify_production(&bundle_dir, key_id, &key)
                    .map_err(|error| error.to_string())?;
            } else {
                if trusted_key_id.is_some() || trusted_public_key_file.is_some() {
                    return Err("trusted key options require --require-signature".into());
                }
                manifest
                    .verify_artifacts(&bundle_dir)
                    .map_err(|error| error.to_string())?;
            }
            println!("manifest artifacts verified");
            Ok(())
        }
        PackageCommand::Sign {
            manifest,
            key_id,
            private_key_file,
            output,
        } => {
            let bundle_dir = manifest
                .parent()
                .ok_or("manifest must have a bundle directory")?
                .to_path_buf();
            let mut manifest =
                RuntimeManifest::load(&manifest).map_err(|error| error.to_string())?;
            manifest
                .verify_artifacts(&bundle_dir)
                .map_err(|error| error.to_string())?;
            let key = load_signing_key(&private_key_file).map_err(|error| error.to_string())?;
            manifest
                .sign_ed25519(key_id, &key)
                .map_err(|error| error.to_string())?;
            manifest.save(&output).map_err(|error| error.to_string())?;
            println!("signed runtime manifest");
            Ok(())
        }
        PackageCommand::Stage {
            runtime_root,
            version_id,
            bundle_dir,
            allow_unsigned,
            trusted_key_id,
            trusted_public_key_file,
        } => {
            let layout = RuntimeLayout::new(runtime_root);
            let trust =
                load_optional_trust(allow_unsigned, trusted_key_id, trusted_public_key_file)?;
            let path = if let Some((key_id, key)) = trust.as_ref() {
                layout.stage_production_bundle(&version_id, &bundle_dir, key_id, key)
            } else {
                layout.stage_bundle(&version_id, &bundle_dir)
            }
            .map_err(|error| error.to_string())?;
            println!("staged {} at {}", version_id, path.display());
            Ok(())
        }
        PackageCommand::Activate {
            runtime_root,
            allow_unsigned,
            trusted_key_id,
            trusted_public_key_file,
        } => {
            let layout = RuntimeLayout::new(runtime_root);
            let trust =
                load_optional_trust(allow_unsigned, trusted_key_id, trusted_public_key_file)?;
            let active = if let Some((key_id, key)) = trust.as_ref() {
                layout.activate_staged_production(key_id, key)
            } else {
                layout.activate_staged()
            }
            .map_err(|error| error.to_string())?;
            println!("activated {}", active.version_id);
            Ok(())
        }
        PackageCommand::Rollback {
            runtime_root,
            allow_unsigned,
            trusted_key_id,
            trusted_public_key_file,
        } => {
            let layout = RuntimeLayout::new(runtime_root);
            let trust =
                load_optional_trust(allow_unsigned, trusted_key_id, trusted_public_key_file)?;
            let active = if let Some((key_id, key)) = trust.as_ref() {
                layout.rollback_to_lkg_production(key_id, key)
            } else {
                layout.rollback_to_lkg()
            }
            .map_err(|error| error.to_string())?;
            println!("rolled back to {}", active.version_id);
            Ok(())
        }
        PackageCommand::MeasureIdle {
            executable,
            args,
            output,
            settle_ms,
        } => {
            let child = ProcessCommand::new(&executable)
                .args(&args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .map_err(|error| error.to_string())?;
            let (report, mut child) =
                measure_child_idle("target", child, Duration::from_millis(settle_ms))
                    .map_err(|error| error.to_string())?;
            let _ = child.kill();
            let _ = child.wait();
            write_report(&output, &report).map_err(|error| error.to_string())?;
            println!("wrote resource report to {}", output.display());
            Ok(())
        }
    }
}

fn parse_license_spec(bundle_dir: &std::path::Path, value: &str) -> Result<LicenseRef, String> {
    let mut parts = value.splitn(3, ':');
    let component = parts.next().unwrap_or_default();
    let spdx = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    if component.is_empty()
        || component.len() > 128
        || spdx.is_empty()
        || spdx.len() > 128
        || path.is_empty()
    {
        return Err("license must use COMPONENT:SPDX:PATH".into());
    }
    artifact_for_file(bundle_dir, path).map_err(|error| error.to_string())?;
    Ok(LicenseRef {
        component: component.into(),
        spdx: spdx.into(),
        path: path.into(),
    })
}

fn load_optional_trust(
    allow_unsigned: bool,
    trusted_key_id: Option<String>,
    trusted_public_key_file: Option<PathBuf>,
) -> Result<Option<(String, ed25519_dalek::VerifyingKey)>, String> {
    if allow_unsigned {
        if trusted_key_id.is_some() || trusted_public_key_file.is_some() {
            return Err("trusted key options are not accepted with --allow-unsigned".into());
        }
        return Ok(None);
    }
    let key_id = trusted_key_id.ok_or("--trusted-key-id is required unless --allow-unsigned")?;
    let key_path = trusted_public_key_file
        .ok_or("--trusted-public-key-file is required unless --allow-unsigned")?;
    let key = load_verifying_key(&key_path).map_err(|error| error.to_string())?;
    Ok(Some((key_id, key)))
}
