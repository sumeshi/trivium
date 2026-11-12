//! Stable JSON-lines adapter around the desktop application's Rust engine.
#[path = "cli/mod.rs"]
mod cli;

fn main() {
    if let Err(error) = cli::run() {
        eprintln!("trivium-cli: {error:#}");
        std::process::exit(2);
    }
}
