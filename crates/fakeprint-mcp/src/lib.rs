//! Optional MCP server for a running fakeprint.
//!
//! The printer stays a separate process. This crate only talks to its control API, and the
//! tests never open a socket to one. State-changing calls are `send_*` and return a plan until
//! `confirm=true`. File copies are `write_*` and refuse to replace an existing file unless
//! `overwrite=true`. Everything else only reads.
//!
//! See `docs/proposals/mcp.md`.

mod control;
mod stdio;
mod tools;

pub use control::{trim_ascii, Control, HttpControl, RawReply};
pub use stdio::serve;
pub use tools::{call_tool, capabilities, FEATURES, TOOLS};
