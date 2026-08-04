// SPDX-License-Identifier: AGPL-3.0-only

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() {
    let mut args = std::env::args_os();
    let _program = args.next();

    match (args.next(), args.next()) {
        (Some(flag), None) if flag == "--version" => {
            println!("kt-signal-connector {VERSION}");
        }
        _ => {
            eprintln!("kt-signal-connector: runtime implementation is not available yet");
            std::process::exit(2);
        }
    }
}
