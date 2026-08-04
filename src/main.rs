// SPDX-License-Identifier: AGPL-3.0-only

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use kt_signal_connector::auth::load_bootstrap_secret;
use kt_signal_connector::engine::SignalCliConfig;
use kt_signal_connector::host::serve;
use kt_signal_connector::ipc::LocalListener;
use kt_signal_connector::supervisor::RuntimeSupervisor;

#[derive(Debug, Parser)]
#[command(name = "kt-signal-connector", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
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
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Serve {
            endpoint,
            bootstrap_secret_file,
            signal_cli,
            signal_data_dir,
        } => {
            let secret = match load_bootstrap_secret(&bootstrap_secret_file) {
                Ok(secret) => secret,
                Err(error) => {
                    eprintln!("kt-signal-connector: {error}");
                    std::process::exit(2);
                }
            };
            let listener = match LocalListener::bind(&endpoint) {
                Ok(listener) => listener,
                Err(error) => {
                    eprintln!("kt-signal-connector: local IPC could not start: {error}");
                    std::process::exit(2);
                }
            };
            let supervisor = Arc::new(RuntimeSupervisor::new(SignalCliConfig::new(
                signal_cli,
                signal_data_dir,
            )));
            serve(listener, secret, supervisor).await
        }
    };
    if let Err(error) = result {
        eprintln!("kt-signal-connector: {error}");
        std::process::exit(1);
    }
}
