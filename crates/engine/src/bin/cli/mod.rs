mod dispatch;
mod protocol;

use std::{
    ffi::OsString,
    io::{self, BufRead, Write},
    path::PathBuf,
};

use anyhow::{bail, Context, Result};
use trivium_engine::store::ProjectsStore;

const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
const USAGE: &str = "Usage: trivium-cli --workspace DIRECTORY\n\nRead one protocol-v1 JSON request per stdin line and write one JSON response\nper stdout line. Diagnostics go to stderr. Send {\"method\":\"hello\"} for methods.\nThe workspace directory must be explicit.";

enum Options {
    Workspace(PathBuf),
    Help,
    Version,
}

fn options(args: impl IntoIterator<Item = OsString>) -> Result<Options> {
    let mut args = args.into_iter();
    let mut workspace = None;
    while let Some(argument) = args.next() {
        if argument == "--help" || argument == "-h" {
            return Ok(Options::Help);
        }
        if argument == "--version" {
            return Ok(Options::Version);
        }
        if argument == "--workspace" && workspace.is_none() {
            let path = args.next().context("--workspace requires a directory")?;
            if path.is_empty() {
                bail!("--workspace requires a nonempty directory");
            }
            workspace = Some(PathBuf::from(path));
        } else {
            bail!(
                "unknown or duplicate argument: {}\n{USAGE}",
                argument.to_string_lossy()
            );
        }
    }
    workspace
        .map(Options::Workspace)
        .context("--workspace DIRECTORY is required")
}

pub fn run() -> Result<()> {
    let root = match options(std::env::args_os().skip(1))? {
        Options::Workspace(path) => path,
        Options::Help => {
            println!("{USAGE}");
            return Ok(());
        }
        Options::Version => {
            println!("trivium-cli {} (protocol 1)", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
    };
    let store = ProjectsStore::new(root).context("failed to open workspace")?;
    let stdin = io::stdin();
    let stdout = io::stdout();
    serve(&store, stdin.lock(), stdout.lock())
}

fn serve(store: &ProjectsStore, mut input: impl BufRead, mut output: impl Write) -> Result<()> {
    let mut line = Vec::new();
    while let Some(oversized) = read_line(&mut input, &mut line)? {
        let response = if oversized {
            protocol::error(
                None,
                "request_too_large",
                "Request exceeds the 8 MiB limit.",
            )
        } else {
            protocol::respond(store, &line)
        };
        serde_json::to_writer(&mut output, &response).context("failed to encode response")?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}

/// Discard an oversized line in bounded chunks, then resume with its successor.
/// Return None only for clean EOF; EOF after bytes still finishes that request.
fn read_line(input: &mut impl BufRead, line: &mut Vec<u8>) -> io::Result<Option<bool>> {
    line.clear();
    let mut oversized = false;
    let mut read_any = false;
    loop {
        let buffer = input.fill_buf()?;
        if buffer.is_empty() {
            return Ok(read_any.then_some(oversized));
        }
        read_any = true;
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let length = newline.map_or(buffer.len(), |index| index + 1);
        if !oversized {
            if line.len().saturating_add(length) > MAX_REQUEST_BYTES {
                oversized = true;
                line.clear();
            } else {
                line.extend_from_slice(&buffer[..length]);
            }
        }
        input.consume(length);
        if newline.is_some() {
            return Ok(Some(oversized));
        }
    }
}
