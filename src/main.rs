// SPDX-License-Identifier: AGPL-3.0-only

use std::path::PathBuf;
use std::process::{Command as ProcessCommand, Stdio};
use std::time::Duration;

use clap::{Parser, Subcommand};
use kt_signal_connector::auth::load_bootstrap_secret;
use kt_signal_connector::host::serve;
use kt_signal_connector::ipc::LocalListener;
use kt_signal_connector::lkg::RuntimeLayout;
use kt_signal_connector::manifest::{
    RuntimeManifest, build_local_unsigned_manifest, current_platform,
};
use kt_signal_connector::resource::{measure_child_idle, write_report};
use kt_signal_connector::supervisor::open_supervisor;

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
        #[arg(long)]
        bootstrap_secret_file: PathBuf,
        #[arg(long)]
        signal_cli: PathBuf,
        #[arg(long)]
        signal_data_dir: PathBuf,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Local packaging / LKG helpers (unsigned development manifests by default).
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
        #[arg(long, default_value = "bin/kt-signal-connector")]
        connector_path: String,
        #[arg(long, default_value = "bin/signal-cli")]
        signal_cli_path: String,
        #[arg(long, default_value = "jre/release")]
        jre_path: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Verify artifact hashes in a bundle. Production signatures remain optional until keys exist.
    Verify {
        #[arg(long)]
        bundle_dir: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long, default_value_t = false)]
        require_signature: bool,
    },
    /// Copy a verified bundle into runtime versions/ and mark it staged.
    Stage {
        #[arg(long)]
        runtime_root: PathBuf,
        #[arg(long)]
        version_id: String,
        #[arg(long)]
        bundle_dir: PathBuf,
    },
    /// Promote staged -> active and move previous active to LKG.
    Activate {
        #[arg(long)]
        runtime_root: PathBuf,
    },
    /// Point active back at LKG.
    Rollback {
        #[arg(long)]
        runtime_root: PathBuf,
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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        CliCommand::Serve {
            endpoint,
            bootstrap_secret_file,
            signal_cli,
            signal_data_dir,
            state_dir,
        } => serve_command(
            endpoint,
            bootstrap_secret_file,
            signal_cli,
            signal_data_dir,
            state_dir,
        )
        .await
        .map_err(|error| error.to_string()),
        CliCommand::Package { command } => package_command(command),
    };
    if let Err(error) = result {
        eprintln!("kt-signal-connector: {error}");
        std::process::exit(1);
    }
}

async fn serve_command(
    endpoint: PathBuf,
    bootstrap_secret_file: PathBuf,
    signal_cli: PathBuf,
    signal_data_dir: PathBuf,
    state_dir: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let secret = load_bootstrap_secret(&bootstrap_secret_file)?;
    let listener = LocalListener::bind(&endpoint)?;
    let supervisor = open_supervisor(signal_cli, signal_data_dir, state_dir)?;
    serve(listener, secret, supervisor).await?;
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
            connector_path,
            signal_cli_path,
            jre_path,
            output,
        } => {
            let manifest = build_local_unsigned_manifest(
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
            manifest.save(&output).map_err(|error| error.to_string())?;
            println!(
                "wrote unsigned local manifest for {} at {}",
                current_platform(),
                output.display()
            );
            Ok(())
        }
        PackageCommand::Verify {
            bundle_dir,
            manifest,
            require_signature,
        } => {
            let manifest = RuntimeManifest::load(&manifest).map_err(|error| error.to_string())?;
            manifest
                .verify_artifacts(&bundle_dir, require_signature)
                .map_err(|error| error.to_string())?;
            println!("manifest artifacts verified");
            Ok(())
        }
        PackageCommand::Stage {
            runtime_root,
            version_id,
            bundle_dir,
        } => {
            let layout = RuntimeLayout::new(runtime_root);
            let path = layout
                .stage_bundle(&version_id, &bundle_dir)
                .map_err(|error| error.to_string())?;
            println!("staged {} at {}", version_id, path.display());
            Ok(())
        }
        PackageCommand::Activate { runtime_root } => {
            let layout = RuntimeLayout::new(runtime_root);
            let active = layout
                .activate_staged()
                .map_err(|error| error.to_string())?;
            println!("activated {}", active.version_id);
            Ok(())
        }
        PackageCommand::Rollback { runtime_root } => {
            let layout = RuntimeLayout::new(runtime_root);
            let active = layout
                .rollback_to_lkg()
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
