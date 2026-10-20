//! MCP stdio framing (Content-Length headers, JSON-RPC 2.0). One process, one printer.

use std::io::{BufRead, Write};

use serde_json::{json, Value};

use crate::control::Control;
use crate::tools::{call_tool, tool_specs, INSTRUCTIONS};

const MAX_FRAME: usize = 1 << 20;
const PROTOCOL: &str = "2024-11-05";
const KNOWN_PROTOCOLS: &[&str] = &["2024-11-05", "2025-03-26", "2025-06-18"];

/// Read frames from `input` and write replies to `output` until the client closes stdin.
pub fn serve<R: BufRead, W: Write>(
    mut input: R,
    output: &mut W,
    ctl: &mut dyn Control,
) -> std::io::Result<()> {
    loop {
        let Some(bytes) = read_frame(&mut input)? else {
            return Ok(());
        };
        let msg: Value = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(_) => {
                write_frame(
                    output,
                    &json!({
                        "jsonrpc": "2.0",
                        "id": null,
                        "error": {"code": -32700, "message": "parse error"}
                    }),
                )?;
                continue;
            }
        };
        if let Some(reply) = handle_message(&msg, ctl) {
            write_frame(output, &reply)?;
        }
    }
}

pub fn read_frame<R: BufRead>(input: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut content_length: Option<usize> = None;
    let mut saw_header = false;
    loop {
        let mut line = String::new();
        let n = input.read_line(&mut line)?;
        if n == 0 {
            if !saw_header {
                return Ok(None);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed the stream mid-frame",
            ));
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        saw_header = true;
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("Content-Length") {
                content_length = value.trim().parse().ok();
            }
        }
    }
    let Some(len) = content_length else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "MCP frame has no Content-Length",
        ));
    };
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("MCP frame is {len} bytes, over the {MAX_FRAME} byte cap"),
        ));
    }
    let mut buf = vec![0u8; len];
    input.read_exact(&mut buf)?;
    Ok(Some(buf))
}

fn write_frame<W: Write>(output: &mut W, msg: &Value) -> std::io::Result<()> {
    let body = serde_json::to_vec(msg).expect("JSON value serializes");
    write!(output, "Content-Length: {}\r\n\r\n", body.len())?;
    output.write_all(&body)?;
    output.flush()
}

fn handle_message(msg: &Value, ctl: &mut dyn Control) -> Option<Value> {
    let id = msg.get("id").cloned();
    let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
    if method.starts_with("notifications/") || (id.is_none() && method != "initialize") {
        return None;
    }
    let id = id.unwrap_or(Value::Null);
    if let Some(v) = msg.get("jsonrpc").and_then(Value::as_str) {
        if v != "2.0" {
            return Some(rpc_error(id, -32600, "jsonrpc must be \"2.0\""));
        }
    }
    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    let result = match method {
        "initialize" => initialize(&params),
        "ping" => json!({}),
        "tools/list" => json!({"tools": tool_specs()}),
        "tools/call" => tools_call(&params, ctl),
        _ => return Some(rpc_error(id, -32601, &format!("unknown method {method}"))),
    };
    Some(json!({"jsonrpc": "2.0", "id": id, "result": result}))
}

fn initialize(params: &Value) -> Value {
    let asked = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(PROTOCOL);
    let version = if KNOWN_PROTOCOLS.contains(&asked) {
        asked
    } else {
        PROTOCOL
    };
    json!({
        "protocolVersion": version,
        "capabilities": {"tools": {"listChanged": false}},
        "serverInfo": {"name": "fakeprint-mcp", "version": env!("CARGO_PKG_VERSION")},
        "instructions": INSTRUCTIONS,
    })
}

fn tools_call(params: &Value, ctl: &mut dyn Control) -> Value {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return tool_error("tools/call needs a name");
    };
    let args = params.get("arguments").cloned().unwrap_or(Value::Null);
    let text_len = args.to_string().len();
    if text_len > MAX_FRAME {
        return tool_error(&format!(
            "arguments are {text_len} bytes, over the {MAX_FRAME} byte cap"
        ));
    }
    let value = call_tool(name, &args, ctl);
    let is_error = matches!(value.get("error"), Some(Value::String(_)));
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
    json!({
        "content": [{"type": "text", "text": text}],
        "isError": is_error,
    })
}

fn tool_error(message: &str) -> Value {
    let text = serde_json::to_string_pretty(&json!({
        "error": message,
        "code": "invalid_argument",
    }))
    .expect("error json");
    json!({
        "content": [{"type": "text", "text": text}],
        "isError": true,
    })
}

fn rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message},
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{Control, RawReply};
    use std::io::Cursor;

    struct Boom;
    impl Control for Boom {
        fn base(&self) -> &str {
            "http://127.0.0.1:3921"
        }
        fn exchange(
            &mut self,
            method: &str,
            path: &str,
            _: Option<&[u8]>,
        ) -> Result<RawReply, String> {
            panic!("control plane was called: {method} {path}");
        }
    }

    fn frame(value: &Value) -> Vec<u8> {
        let body = serde_json::to_vec(value).unwrap();
        let mut out = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
        out.extend_from_slice(&body);
        out
    }

    fn roundtrip(messages: &[Value]) -> Vec<Value> {
        let mut raw = Vec::new();
        for m in messages {
            raw.extend(frame(m));
        }
        let mut out = Vec::new();
        let mut ctl = Boom;
        serve(Cursor::new(raw), &mut out, &mut ctl).unwrap();
        let mut replies = Vec::new();
        let mut cur = Cursor::new(out);
        while let Some(body) = read_frame(&mut cur).unwrap() {
            replies.push(serde_json::from_slice(&body).unwrap());
        }
        replies
    }

    #[test]
    fn initialize_lists_every_tool_and_a_dry_run_does_not_call_the_printer() {
        let replies = roundtrip(&[
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}}
            }),
            json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "send_reset", "arguments": {}}
            }),
        ]);
        assert_eq!(
            replies.len(),
            3,
            "the initialized notification has no reply"
        );
        let init = &replies[0]["result"];
        assert_eq!(init["serverInfo"]["name"], "fakeprint-mcp");
        assert!(init["instructions"]
            .as_str()
            .unwrap()
            .contains("confirm=true"));
        assert!(init["instructions"]
            .as_str()
            .unwrap()
            .contains("overwrite=true"));
        let names: Vec<&str> = replies[1]["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, crate::TOOLS);
        let text = replies[2]["result"]["content"][0]["text"].as_str().unwrap();
        let plan: Value = serde_json::from_str(text).unwrap();
        assert_eq!(plan["sent"], false);
        assert_eq!(replies[2]["result"]["isError"], false);
    }
}
