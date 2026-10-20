//! Attach-only stdio entry. Exits if `/health` is not `ok`. Never spawns fakeprint.

use fakeprint_mcp::{serve, Control, HttpControl};
use std::io::{BufReader, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    let base = match base_from_args() {
        Ok(b) => b,
        Err(e) => {
            eprintln!("fakeprint-mcp: {e}");
            return ExitCode::from(2);
        }
    };
    let mut client = match HttpControl::parse(&base) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("fakeprint-mcp: {e}");
            return ExitCode::from(2);
        }
    };
    if let Err(e) = probe(&mut client) {
        eprintln!("fakeprint-mcp: {e}");
        return ExitCode::from(1);
    }
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    if let Err(e) = serve(BufReader::new(stdin.lock()), &mut stdout, &mut client) {
        eprintln!("fakeprint-mcp: {e}");
        return ExitCode::from(1);
    }
    let _ = stdout.flush();
    ExitCode::SUCCESS
}

fn probe(ctl: &mut HttpControl) -> Result<(), String> {
    match ctl.exchange("GET", "/health", None) {
        Ok(r) if r.status == 200 && fakeprint_mcp::trim_ascii(&r.body) == b"ok" => Ok(()),
        Ok(r) => Err(format!(
            "fakeprint at {} answered HTTP {} on /health, not ok. This server does not spawn fakeprint.",
            ctl.base(),
            r.status
        )),
        Err(e) => Err(format!(
            "fakeprint is not reachable at {}: {e}. Start it first. This server does not spawn fakeprint and does not touch fakescan.",
            ctl.base()
        )),
    }
}

fn base_from_args() -> Result<String, String> {
    let mut base =
        std::env::var("FAKEPRINT_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:3921".to_string());
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            println!(
                "fakeprint-mcp — attach-only stdio MCP for a running fakeprint\n\
                 \n\
                 Usage: fakeprint-mcp [--base-url http://127.0.0.1:3921]\n\
                 \n\
                 Does not spawn fakeprint. Odd ports (3921, 3923, …). Use fakescan-mcp for fakescan."
            );
            std::process::exit(0);
        }
        if arg == "--version" {
            println!("{}", env!("CARGO_PKG_VERSION"));
            std::process::exit(0);
        }
        if let Some(v) = arg.strip_prefix("--base-url=") {
            if v.is_empty() {
                return Err("--base-url needs a value".into());
            }
            base = v.to_string();
            continue;
        }
        if arg == "--base-url" {
            base = args.next().ok_or("--base-url needs a value")?;
            continue;
        }
        return Err(format!("unknown argument {arg}"));
    }
    Ok(base)
}
