//! `lakeleto` — single-binary CLI entrypoint.
//!
//! Headless-first (like its sibling modules): `lakeleto schema/head/profile/info <path>` is the
//! scriptable surface the future desktop UI is built on top of, both binding to the same
//! [`Engine`](lakeleto::engine::Engine) trait.

use clap::Parser;

use lakeleto::cli::{run, Cli};

fn main() {
    let cli = Cli::parse();
    match run(cli) {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("lakeleto: {e}");
            std::process::exit(1);
        }
    }
}
