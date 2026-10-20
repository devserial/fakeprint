//! Tool implementations. Names are unprefixed, matching fakescan's MCP rather than hardly's `hardly_` prefix.
//! `send_*` is confirm-gated. `write_*` will not replace a file unless `overwrite=true`.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{json, Map, Value};

use crate::control::{Control, RawReply, MAX_HTTP_BODY};

/// Same sentence hardly uses, so a sibling server can match it.
const CONFIRM_HINT: &str = "Nothing was sent. Tell the person exactly what will be sent, then repeat the same call with confirm=true.";

const OVERWRITE_HINT: &str = "Pass overwrite=true to replace it, or choose another path.";

/// Largest capture `write_job` will copy. Matches the inbox document cap.
const MAX_DOCUMENT: u64 = 256 << 20;

pub const INSTRUCTIONS: &str = "\
Attach-only MCP for a running fakeprint (odd ports, default 3921). \
It does not spawn or kill fakeprint and it does not touch fakescan. \
Read tools only read the control API. \
send_* tools return a plan and send nothing until confirm=true. \
write_* tools refuse to overwrite a file unless overwrite=true.";

pub const NEXT: &str = "\
Read status, jobs and requests before changing anything. \
send_* returns a plan and sends nothing until confirm=true. \
write_* will not replace an existing file unless overwrite=true. \
This server only attaches to a running fakeprint; it does not start one.";

pub const FEATURES: &[&str] = &[
    "attach_only",
    "confirm_gate",
    "write_overwrite_guard",
    "control_api",
    "request_log",
    "job_capture",
    "offline_tests",
];

/// Advertised tool names, in the order `tools/list` returns them.
pub const TOOLS: &[&str] = &[
    "server_status",
    "health",
    "status",
    "devices",
    "ctl_info",
    "jobs",
    "job",
    "faults",
    "timing",
    "requests",
    "request",
    "requests_config",
    "send_reset",
    "send_faults",
    "send_profile",
    "send_timing",
    "send_levels",
    "send_jobs",
    "send_ctl",
    "send_requests_clear",
    "send_requests_config",
    "write_request",
    "write_job",
];

pub fn capabilities() -> Value {
    json!({
        "name": "fakeprint",
        "version": env!("CARGO_PKG_VERSION"),
        "features": FEATURES,
        "tools": TOOLS,
        "tool_count": TOOLS.len(),
        "next": NEXT,
    })
}

#[derive(Clone, Copy)]
struct Arg {
    name: &'static str,
    typ: &'static str,
    description: &'static str,
    required: bool,
}

struct Tool {
    name: &'static str,
    description: &'static str,
    args: Vec<Arg>,
}

const CONFIRM: Arg = Arg {
    name: "confirm",
    typ: "boolean",
    description: "When false (the default), return the plan and send nothing.",
    required: false,
};

fn catalog() -> Vec<Tool> {
    vec![
        Tool {
            name: "server_status",
            description: "Capabilities of this MCP server: name, version, features, tools, tool_count, next. Does not call the printer.",
            args: vec![],
        },
        Tool {
            name: "health",
            description: "GET /health. ok is true only when the body is the text ok.",
            args: vec![],
        },
        Tool {
            name: "status",
            description: "GET /v1/status. Profile, UUIDs, counters, inbox_dir, available profiles.",
            args: vec![],
        },
        Tool {
            name: "devices",
            description: "GET /v1/devices. The device set this process is serving.",
            args: vec![],
        },
        Tool {
            name: "ctl_info",
            description: "GET /v1/ctl. Name, port, uuid, pipe path and the ctl command names.",
            args: vec![],
        },
        Tool {
            name: "jobs",
            description: "GET /v1/jobs. Captured inbox files and the print-job table.",
            args: vec![],
        },
        Tool {
            name: "job",
            description: "GET /v1/jobs/{id}. One print job, including document paths.",
            args: vec![Arg {
                name: "id",
                typ: "integer",
                description: "Print job id.",
                required: true,
            }],
        },
        Tool {
            name: "faults",
            description: "GET /v1/faults. Pending one-shots, active conditions and injection rules.",
            args: vec![],
        },
        Tool {
            name: "timing",
            description: "GET /v1/timing. The runtime print timers.",
            args: vec![],
        },
        Tool {
            name: "requests",
            description: "GET /v1/requests. The per-device raw request log.",
            args: vec![
                Arg { name: "since", typ: "integer", description: "Only records with seq greater than this.", required: false },
                Arg { name: "limit", typ: "integer", description: "Page size, clamped to 1..=10000. Default is the printer's (100).", required: false },
                Arg { name: "action", typ: "string", description: "Filter by action name.", required: false },
                Arg { name: "job", typ: "integer", description: "Filter by print job id.", required: false },
                Arg { name: "direction", typ: "string", description: "Filter by direction.", required: false },
                Arg { name: "bodies", typ: "boolean", description: "When false, the printer omits stored bodies.", required: false },
            ],
        },
        Tool {
            name: "request",
            description: "GET /v1/requests/{seq}. One log record, with bodies.",
            args: vec![Arg {
                name: "seq",
                typ: "integer",
                description: "Log sequence number.",
                required: true,
            }],
        },
        Tool {
            name: "requests_config",
            description: "GET /v1/requests/config. Whether the log is on, and its caps.",
            args: vec![],
        },
        Tool {
            name: "send_reset",
            description: "POST /v1/reset. Clears in-memory jobs. Inbox files on disk stay. Sends nothing until confirm=true.",
            args: vec![CONFIRM],
        },
        Tool {
            name: "send_faults",
            description: "POST /v1/faults. Inject conditions and rules. Body is the FaultBody JSON. {\"reset\": true} clears. Sends nothing until confirm=true.",
            args: vec![
                CONFIRM,
                Arg { name: "body", typ: "object", description: "FaultBody. Passed through. Default {}.", required: false },
            ],
        },
        Tool {
            name: "send_profile",
            description: "POST /v1/profile. Hot-swap by id (a profile file stem) or by an inline profile object. Exactly one. Sends nothing until confirm=true.",
            args: vec![
                CONFIRM,
                Arg { name: "id", typ: "string", description: "Profile stem, such as generic-wsd. Not a path.", required: false },
                Arg { name: "profile", typ: "object", description: "Schema-2 profile document, instead of id.", required: false },
            ],
        },
        Tool {
            name: "send_timing",
            description: "POST /v1/timing. Partial timer patch (preset, processing_base_ms, …). Sends nothing until confirm=true.",
            args: vec![
                CONFIRM,
                Arg { name: "body", typ: "object", description: "Timing patch. Default {}.", required: false },
            ],
        },
        Tool {
            name: "send_levels",
            description: "POST /v1/levels. Consumable and input-bin levels, -1 or 0..=100. Sends nothing until confirm=true.",
            args: vec![
                CONFIRM,
                Arg { name: "consumables", typ: "object", description: "Name to level. Default {}.", required: false },
                Arg { name: "inputs", typ: "object", description: "Tray name to level. Default {}.", required: false },
            ],
        },
        Tool {
            name: "send_jobs",
            description: "POST /v1/jobs. Seed engine jobs. Body is one job object or {\"jobs\": [...]}. Sends nothing until confirm=true.",
            args: vec![
                CONFIRM,
                Arg { name: "body", typ: "object", description: "Seed body. Required.", required: true },
            ],
        },
        Tool {
            name: "send_ctl",
            description: "POST /v1/ctl. Dispatch one ctl command. Sends nothing until confirm=true.",
            args: vec![
                CONFIRM,
                Arg { name: "cmd", typ: "string", description: "Command name, such as status or profile.", required: true },
                Arg { name: "args", typ: "object", description: "Command arguments. Default {}.", required: false },
            ],
        },
        Tool {
            name: "send_requests_clear",
            description: "DELETE /v1/requests. Drops the request log. Sends nothing until confirm=true.",
            args: vec![CONFIRM],
        },
        Tool {
            name: "send_requests_config",
            description: "POST /v1/requests/config. Partial log config (enabled, redact, caps). Sends nothing until confirm=true.",
            args: vec![
                CONFIRM,
                Arg { name: "body", typ: "object", description: "Request-log patch. Default {}.", required: false },
            ],
        },
        Tool {
            name: "write_request",
            description: "GET /v1/requests/{seq}/raw and write those bytes to output_path. Refuses to replace an existing file unless overwrite=true.",
            args: vec![
                Arg { name: "seq", typ: "integer", description: "Log sequence number.", required: true },
                Arg { name: "output_path", typ: "string", description: "File to create. This tool never picks a path.", required: true },
                Arg { name: "overwrite", typ: "boolean", description: "Replace an existing file. Default false.", required: false },
                Arg { name: "part", typ: "string", description: "request (default) or response.", required: false },
                Arg { name: "what", typ: "string", description: "all (default, head and body) or body.", required: false },
            ],
        },
        Tool {
            name: "write_job",
            description: "Copy one captured document to output_path, only when its path stays inside the printer's inbox_dir. Refuses to replace an existing file unless overwrite=true.",
            args: vec![
                Arg { name: "id", typ: "integer", description: "Print job id.", required: true },
                Arg { name: "output_path", typ: "string", description: "File to create. This tool never picks a path.", required: true },
                Arg { name: "overwrite", typ: "boolean", description: "Replace an existing file. Default false.", required: false },
                Arg { name: "document", typ: "integer", description: "Document seq. Default is the first captured document.", required: false },
            ],
        },
    ]
}

pub(crate) fn tool_specs() -> Vec<Value> {
    catalog()
        .into_iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "inputSchema": input_schema(&t.args),
            })
        })
        .collect()
}

fn input_schema(args: &[Arg]) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for arg in args {
        properties.insert(
            arg.name.into(),
            json!({"type": arg.typ, "description": arg.description}),
        );
        if arg.required {
            required.push(Value::String(arg.name.into()));
        }
    }
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

/// Run one tool. `args` is the MCP `arguments` object (`null` counts as empty).
pub fn call_tool(name: &str, args: &Value, ctl: &mut dyn Control) -> Value {
    let empty = Map::new();
    let args = if args.is_null() {
        &empty
    } else if let Some(map) = args.as_object() {
        map
    } else {
        return err(
            "arguments must be an object",
            "invalid_argument",
            "Pass a JSON object.",
        );
    };
    match dispatch(name, args, ctl) {
        Ok(v) | Err(v) => v,
    }
}

fn dispatch(name: &str, args: &Map<String, Value>, ctl: &mut dyn Control) -> Result<Value, Value> {
    match name {
        "server_status" => {
            check_keys(args, &[])?;
            Ok(capabilities())
        }
        "health" => {
            check_keys(args, &[])?;
            health(ctl)
        }
        "status" => read(args, ctl, &[], "/v1/status"),
        "devices" => read(args, ctl, &[], "/v1/devices"),
        "ctl_info" => read(args, ctl, &[], "/v1/ctl"),
        "jobs" => read(args, ctl, &[], "/v1/jobs"),
        "job" => {
            check_keys(args, &["id"])?;
            let id = req_u32(args, "id")?;
            http_json(ctl, "GET", &format!("/v1/jobs/{id}"), None)
        }
        "faults" => read(args, ctl, &[], "/v1/faults"),
        "timing" => read(args, ctl, &[], "/v1/timing"),
        "requests" => requests(args, ctl),
        "request" => {
            check_keys(args, &["seq"])?;
            let seq = req_u64(args, "seq")?;
            http_json(ctl, "GET", &format!("/v1/requests/{seq}"), None)
        }
        "requests_config" => read(args, ctl, &[], "/v1/requests/config"),
        "send_reset" => {
            check_keys(args, &["confirm"])?;
            gate(flag(args, "confirm")?, "send_reset", "POST", "/v1/reset", json!({}), ctl)
        }
        "send_faults" => {
            check_keys(args, &["confirm", "body"])?;
            let body = obj_or_empty(args, "body")?;
            gate(flag(args, "confirm")?, "send_faults", "POST", "/v1/faults", body, ctl)
        }
        "send_profile" => send_profile(args, ctl),
        "send_timing" => {
            check_keys(args, &["confirm", "body"])?;
            let body = obj_or_empty(args, "body")?;
            gate(flag(args, "confirm")?, "send_timing", "POST", "/v1/timing", body, ctl)
        }
        "send_levels" => send_levels(args, ctl),
        "send_jobs" => {
            check_keys(args, &["confirm", "body"])?;
            let body = req_object(args, "body")?;
            gate(flag(args, "confirm")?, "send_jobs", "POST", "/v1/jobs", body, ctl)
        }
        "send_ctl" => send_ctl(args, ctl),
        "send_requests_clear" => {
            check_keys(args, &["confirm"])?;
            gate(
                flag(args, "confirm")?,
                "send_requests_clear",
                "DELETE",
                "/v1/requests",
                Value::Null,
                ctl,
            )
        }
        "send_requests_config" => {
            check_keys(args, &["confirm", "body"])?;
            let body = obj_or_empty(args, "body")?;
            gate(
                flag(args, "confirm")?,
                "send_requests_config",
                "POST",
                "/v1/requests/config",
                body,
                ctl,
            )
        }
        "write_request" => write_request(args, ctl),
        "write_job" => write_job(args, ctl),
        _ => Err(err(
            format!("unknown tool {name}"),
            "unknown_tool",
            "Call server_status to see the tool list. If a name is missing, restart this MCP server.",
        )),
    }
}

fn health(ctl: &mut dyn Control) -> Result<Value, Value> {
    let reply = ctl.exchange("GET", "/health", None).map_err(|e| {
        err(
            e,
            "control_unreachable",
            "Start fakeprint and check --base-url. This server does not spawn it.",
        )
    })?;
    let ok = reply.status == 200 && crate::control::trim_ascii(&reply.body) == b"ok";
    Ok(json!({"ok": ok, "base_url": ctl.base(), "status": reply.status}))
}

fn read(
    args: &Map<String, Value>,
    ctl: &mut dyn Control,
    keys: &[&str],
    path: &str,
) -> Result<Value, Value> {
    check_keys(args, keys)?;
    http_json(ctl, "GET", path, None)
}

fn requests(args: &Map<String, Value>, ctl: &mut dyn Control) -> Result<Value, Value> {
    check_keys(
        args,
        &["since", "limit", "action", "job", "direction", "bodies"],
    )?;
    let since = opt_u64(args, "since")?;
    let limit = match opt_u64(args, "limit")? {
        Some(0) => {
            return Err(err(
                "limit must be at least 1",
                "invalid_argument",
                "Omit limit to use the printer's default, or pass 1..=10000.",
            ))
        }
        Some(n) => Some(n.min(10_000)),
        None => None,
    };
    let action = opt_short(args, "action")?;
    let job = opt_u32(args, "job")?;
    let direction = opt_short(args, "direction")?;
    let bodies = flag_opt(args, "bodies")?;
    let mut query = Vec::new();
    if let Some(n) = since {
        query.push(format!("since={n}"));
    }
    if let Some(n) = limit {
        query.push(format!("limit={n}"));
    }
    if let Some(s) = action {
        query.push(format!("action={}", pct(&s)));
    }
    if let Some(n) = job {
        query.push(format!("job={n}"));
    }
    if let Some(s) = direction {
        query.push(format!("direction={}", pct(&s)));
    }
    if bodies == Some(false) {
        query.push("bodies=0".into());
    }
    let path = if query.is_empty() {
        "/v1/requests".to_string()
    } else {
        format!("/v1/requests?{}", query.join("&"))
    };
    http_json(ctl, "GET", &path, None)
}

fn send_profile(args: &Map<String, Value>, ctl: &mut dyn Control) -> Result<Value, Value> {
    check_keys(args, &["confirm", "id", "profile"])?;
    let id = opt_string(args, "id")?;
    let profile = opt_object(args, "profile")?;
    let body = match (id, profile) {
        (Some(id), None) => {
            if !profile_id_ok(&id) {
                return Err(err(
                    format!("profile id {id:?} is not a file stem"),
                    "invalid_argument",
                    "Pass a stem such as generic-wsd, or pass profile={...} for an inline document. Ids are not paths.",
                ));
            }
            json!({"id": id})
        }
        (None, Some(profile)) => json!({"profile": profile}),
        _ => {
            return Err(err(
                "send_profile needs exactly one of id or profile",
                "invalid_argument",
                "Pass id=<stem> or profile={...}, not both and not neither.",
            ))
        }
    };
    gate(
        flag(args, "confirm")?,
        "send_profile",
        "POST",
        "/v1/profile",
        body,
        ctl,
    )
}

fn profile_id_ok(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        && id != "."
        && id != ".."
        && !id.contains("..")
}

fn send_levels(args: &Map<String, Value>, ctl: &mut dyn Control) -> Result<Value, Value> {
    check_keys(args, &["confirm", "consumables", "inputs"])?;
    let consumables = obj_or_empty(args, "consumables")?;
    let inputs = obj_or_empty(args, "inputs")?;
    let body = json!({"consumables": consumables, "inputs": inputs});
    gate(
        flag(args, "confirm")?,
        "send_levels",
        "POST",
        "/v1/levels",
        body,
        ctl,
    )
}

fn send_ctl(args: &Map<String, Value>, ctl: &mut dyn Control) -> Result<Value, Value> {
    check_keys(args, &["confirm", "cmd", "args"])?;
    let cmd = req_string(args, "cmd")?;
    if !cmd_ok(&cmd) {
        return Err(err(
            format!("cmd {cmd:?} is not a ctl command name"),
            "invalid_argument",
            "Pass the command name, such as status. The printer rejects unknown commands.",
        ));
    }
    let extra = obj_or_empty(args, "args")?;
    let body = json!({"cmd": cmd, "args": extra});
    gate(
        flag(args, "confirm")?,
        "send_ctl",
        "POST",
        "/v1/ctl",
        body,
        ctl,
    )
}

fn cmd_ok(cmd: &str) -> bool {
    !cmd.is_empty()
        && cmd.len() <= 64
        && cmd
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn write_request(args: &Map<String, Value>, ctl: &mut dyn Control) -> Result<Value, Value> {
    check_keys(args, &["seq", "output_path", "overwrite", "part", "what"])?;
    let seq = req_u64(args, "seq")?;
    let output = req_string(args, "output_path")?;
    let overwrite = flag(args, "overwrite")?;
    let part = opt_choice(args, "part", &["request", "response"], "request")?;
    let what = opt_choice(args, "what", &["all", "body"], "all")?;
    let dest = prepare_output(&output, overwrite)?;
    let path = format!("/v1/requests/{seq}/raw?part={part}&what={what}");
    let reply = match ctl.exchange("GET", &path, None) {
        Ok(r) => r,
        Err(e) => {
            return Err(err(
                e,
                "control_unreachable",
                "Start fakeprint and check --base-url. This server does not spawn it.",
            ))
        }
    };
    if !(200..300).contains(&reply.status) {
        return Err(interpret(reply).expect_err("non-2xx is an error"));
    }
    if reply.body.len() > MAX_HTTP_BODY {
        return Err(err(
            format!(
                "raw record is {} bytes, over the {MAX_HTTP_BODY} byte cap",
                reply.body.len()
            ),
            "input_too_large",
            "Refusing to write it.",
        ));
    }
    let bytes = write_bytes(&dest, overwrite, &reply.body)?;
    Ok(json!({
        "wrote": true,
        "bytes": bytes,
        "output_path": dest.display().to_string(),
        "path": path,
    }))
}

fn write_job(args: &Map<String, Value>, ctl: &mut dyn Control) -> Result<Value, Value> {
    check_keys(args, &["id", "output_path", "overwrite", "document"])?;
    let id = req_u32(args, "id")?;
    let output = req_string(args, "output_path")?;
    let overwrite = flag(args, "overwrite")?;
    let document = opt_u64(args, "document")?;
    let dest = prepare_output(&output, overwrite)?;
    let status = http_json(ctl, "GET", "/v1/status", None)?;
    let inbox = status
        .get("inbox_dir")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            err(
                "status has no inbox_dir",
                "control_error",
                "write_job needs the printer's inbox directory. Use write_request for log bytes.",
            )
        })?
        .to_string();
    let job = http_json(ctl, "GET", &format!("/v1/jobs/{id}"), None)?;
    let raw = capture_path(&job, document)?;
    let src = resolve_capture(&inbox, &raw)?;
    let bytes = copy_file(&src, &dest, overwrite)?;
    Ok(json!({
        "wrote": true,
        "bytes": bytes,
        "output_path": dest.display().to_string(),
        "source": src.display().to_string(),
        "job_id": id,
    }))
}

fn capture_path(job: &Value, document: Option<u64>) -> Result<String, Value> {
    let docs = job
        .get("documents")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            err(
                "job has no documents",
                "not_found",
                "Seeded jobs without a capture have no file to copy.",
            )
        })?;
    let doc = if let Some(seq) = document {
        docs.iter()
            .find(|d| {
                d.get("seq").and_then(Value::as_u64) == Some(seq)
                    || d.get("id").and_then(Value::as_u64) == Some(seq)
            })
            .ok_or_else(|| {
                err(
                    format!("job has no document {seq}"),
                    "not_found",
                    "Pass document= the document seq from the job.",
                )
            })?
    } else {
        docs.iter()
            .find(|d| {
                d.get("path")
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty())
            })
            .ok_or_else(|| {
                err(
                    "job has no captured document",
                    "not_local",
                    "Nothing was written. Use write_request if the bytes are in the request log.",
                )
            })?
    };
    doc.get("path")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            err(
                "document has no local path",
                "not_local",
                "Nothing was written. Use write_request for bytes the control API returns.",
            )
        })
}

fn resolve_capture(inbox: &str, raw: &str) -> Result<PathBuf, Value> {
    let inbox = Path::new(inbox);
    let inbox_c =
        inbox.canonicalize().map_err(|e| {
            err(
            format!("inbox_dir is not a local directory ({}): {e}", inbox.display()),
            "not_local",
            "write_job only copies a file on this machine. Use write_request for a remote printer.",
        )
        })?;
    let raw_path = Path::new(raw);
    let joined = if raw_path.is_absolute() {
        raw_path.to_path_buf()
    } else {
        inbox_c.join(raw_path)
    };
    let doc = joined.canonicalize().map_err(|e| {
        err(
            format!("document path is not a local file ({raw}): {e}"),
            "not_local",
            "Nothing was written. Use write_request for bytes the control API returns.",
        )
    })?;
    if !doc.starts_with(&inbox_c) {
        return Err(err(
            format!("document path is outside inbox_dir: {}", doc.display()),
            "path_outside_inbox",
            "Refusing to read it. Use write_request for bytes the control API returns.",
        ));
    }
    if !doc.is_file() {
        return Err(err(
            format!("document path is not a file: {}", doc.display()),
            "not_local",
            "Use write_request for bytes the control API returns.",
        ));
    }
    let len = doc.metadata().map(|m| m.len()).unwrap_or(0);
    if len > MAX_DOCUMENT {
        return Err(err(
            format!("document is {len} bytes, over the {MAX_DOCUMENT} byte cap"),
            "input_too_large",
            "Refusing to copy it.",
        ));
    }
    Ok(doc)
}

fn prepare_output(path: &str, overwrite: bool) -> Result<PathBuf, Value> {
    if path.is_empty() || path.contains('\0') {
        return Err(err(
            "output_path is required",
            "missing_argument",
            "write_* tools never choose a path: pass output_path=<file> (use a scratch directory).",
        ));
    }
    let dest = PathBuf::from(path);
    if dest.is_dir() {
        return Err(err(
            format!("output_path is a directory: {}", dest.display()),
            "invalid_argument",
            "Give a file name.",
        ));
    }
    if let Some(parent) = dest.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            return Err(err(
                format!("output directory does not exist: {}", parent.display()),
                "file_not_found",
                "Create the directory, or pass a path inside one that exists.",
            ));
        }
    }
    if dest.exists() && !overwrite {
        return Err(err(
            format!("output_path already exists: {}", dest.display()),
            "output_exists",
            OVERWRITE_HINT,
        ));
    }
    Ok(dest)
}

fn write_bytes(dest: &Path, overwrite: bool, bytes: &[u8]) -> Result<u64, Value> {
    let mut file = open_dest(dest, overwrite)?;
    file.write_all(bytes).map_err(|e| {
        if !overwrite {
            let _ = std::fs::remove_file(dest);
        }
        err(
            format!("write {}: {e}", dest.display()),
            "io_error",
            "The file may be partial.",
        )
    })?;
    Ok(bytes.len() as u64)
}

fn copy_file(src: &Path, dest: &Path, overwrite: bool) -> Result<u64, Value> {
    let mut input = File::open(src).map_err(|e| {
        err(
            format!("open {}: {e}", src.display()),
            "not_local",
            "Use write_request if the file is gone.",
        )
    })?;
    let mut output = open_dest(dest, overwrite)?;
    let n = std::io::copy(&mut input, &mut output).map_err(|e| {
        if !overwrite {
            let _ = std::fs::remove_file(dest);
        }
        err(
            format!("write {}: {e}", dest.display()),
            "io_error",
            "The file may be partial.",
        )
    })?;
    Ok(n)
}

fn open_dest(dest: &Path, overwrite: bool) -> Result<File, Value> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true);
    if overwrite {
        opts.create(true).truncate(true);
    } else {
        opts.create_new(true);
    }
    opts.open(dest).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            err(
                format!("output_path already exists: {}", dest.display()),
                "output_exists",
                OVERWRITE_HINT,
            )
        } else {
            err(
                format!("open {}: {e}", dest.display()),
                "io_error",
                "Check the path and permissions.",
            )
        }
    })
}

fn gate(
    confirm: bool,
    tool: &str,
    method: &str,
    path: &str,
    body: Value,
    ctl: &mut dyn Control,
) -> Result<Value, Value> {
    if !confirm {
        return Ok(json!({
            "confirmed": false,
            "sent": false,
            "plan": {"tool": tool, "method": method, "path": path, "body": body},
            "hint": CONFIRM_HINT,
        }));
    }
    let payload = if method == "DELETE" { None } else { Some(body) };
    http_json(ctl, method, path, payload)
}

fn http_json(
    ctl: &mut dyn Control,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> Result<Value, Value> {
    let bytes = match body {
        Some(v) => {
            let raw = serde_json::to_vec(&v).map_err(|e| {
                err(
                    format!("could not encode the body: {e}"),
                    "invalid_argument",
                    "Pass JSON values only.",
                )
            })?;
            if raw.len() > MAX_HTTP_BODY {
                return Err(err(
                    format!(
                        "body is {} bytes, over the {MAX_HTTP_BODY} byte cap",
                        raw.len()
                    ),
                    "input_too_large",
                    "Split the change into a smaller body.",
                ));
            }
            Some(raw)
        }
        None => None,
    };
    let reply = ctl.exchange(method, path, bytes.as_deref()).map_err(|e| {
        err(
            e,
            "control_unreachable",
            "Start fakeprint and check --base-url. This server does not spawn it.",
        )
    })?;
    interpret(reply)
}

fn interpret(reply: RawReply) -> Result<Value, Value> {
    let parsed = body_value(&reply.body);
    if (200..300).contains(&reply.status) {
        if parsed.is_null() {
            return Ok(json!({"ok": true}));
        }
        return Ok(parsed);
    }
    Err(json!({
        "error": format!("control API answered HTTP {}", reply.status),
        "code": "control_error",
        "status": reply.status,
        "body": parsed,
        "hint": "The call was sent. Read body for the printer's reason.",
    }))
}

fn body_value(bytes: &[u8]) -> Value {
    let trim = crate::control::trim_ascii(bytes);
    if trim.is_empty() {
        return Value::Null;
    }
    if trim.first() == Some(&b'{') || trim.first() == Some(&b'[') {
        if let Ok(v) = serde_json::from_slice::<Value>(trim) {
            return v;
        }
    }
    Value::String(String::from_utf8_lossy(trim).into_owned())
}

fn err(message: impl Into<String>, code: &str, hint: &str) -> Value {
    json!({"error": message.into(), "code": code, "hint": hint})
}

fn check_keys(args: &Map<String, Value>, allowed: &[&str]) -> Result<(), Value> {
    let bad: Vec<&str> = args
        .keys()
        .filter(|k| !allowed.contains(&k.as_str()))
        .map(String::as_str)
        .collect();
    if bad.is_empty() {
        return Ok(());
    }
    Err(err(
        format!("unknown argument(s): {}", bad.join(", ")),
        "invalid_argument",
        &format!(
            "Valid arguments: {}.",
            if allowed.is_empty() {
                "(none)".to_string()
            } else {
                allowed.join(", ")
            }
        ),
    ))
}

fn flag(args: &Map<String, Value>, key: &str) -> Result<bool, Value> {
    Ok(flag_opt(args, key)?.unwrap_or(false))
}

fn flag_opt(args: &Map<String, Value>, key: &str) -> Result<Option<bool>, Value> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(v)) => Ok(Some(*v)),
        _ => Err(err(
            format!("{key} must be a boolean"),
            "invalid_argument",
            "Use true or false.",
        )),
    }
}

fn req_u64(args: &Map<String, Value>, key: &str) -> Result<u64, Value> {
    opt_u64(args, key)?.ok_or_else(|| {
        err(
            format!("{key} is required"),
            "missing_argument",
            &format!("Pass {key}=...; see the tool description."),
        )
    })
}

fn req_u32(args: &Map<String, Value>, key: &str) -> Result<u32, Value> {
    let n = req_u64(args, key)?;
    u32::try_from(n).map_err(|_| {
        err(
            format!("{key} does not fit a job id"),
            "invalid_argument",
            "Job ids are 32-bit.",
        )
    })
}

fn opt_u32(args: &Map<String, Value>, key: &str) -> Result<Option<u32>, Value> {
    match opt_u64(args, key)? {
        None => Ok(None),
        Some(n) => u32::try_from(n).map(Some).map_err(|_| {
            err(
                format!("{key} does not fit a job id"),
                "invalid_argument",
                "Job ids are 32-bit.",
            )
        }),
    }
}

fn opt_u64(args: &Map<String, Value>, key: &str) -> Result<Option<u64>, Value> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n.as_u64().map(Some).ok_or_else(|| {
            err(
                format!("{key} must be a non-negative integer"),
                "invalid_argument",
                "Pass a whole number, not a float or a negative.",
            )
        }),
        _ => Err(err(
            format!("{key} must be a non-negative integer"),
            "invalid_argument",
            "Pass a JSON number.",
        )),
    }
}

fn req_string(args: &Map<String, Value>, key: &str) -> Result<String, Value> {
    opt_string(args, key)?.ok_or_else(|| {
        err(
            format!("{key} is required"),
            "missing_argument",
            &format!("Pass {key}=...; see the tool description."),
        )
    })
}

fn opt_string(args: &Map<String, Value>, key: &str) -> Result<Option<String>, Value> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if !s.is_empty() => Ok(Some(s.clone())),
        Some(Value::String(_)) => Err(err(
            format!("{key} is empty"),
            "invalid_argument",
            &format!("Pass a non-empty {key}."),
        )),
        _ => Err(err(
            format!("{key} must be a string"),
            "invalid_argument",
            "Pass a JSON string.",
        )),
    }
}

fn opt_short(args: &Map<String, Value>, key: &str) -> Result<Option<String>, Value> {
    let Some(s) = opt_string(args, key)? else {
        return Ok(None);
    };
    if s.len() > 64 || s.chars().any(|c| !c.is_ascii() || c.is_ascii_control()) {
        return Err(err(
            format!("{key} must be a short ASCII string"),
            "invalid_argument",
            "Use the filter text the request log stores, at most 64 ASCII characters.",
        ));
    }
    Ok(Some(s))
}

fn opt_choice(
    args: &Map<String, Value>,
    key: &str,
    allowed: &[&str],
    default: &str,
) -> Result<String, Value> {
    match opt_string(args, key)? {
        None => Ok(default.to_string()),
        Some(s) if allowed.contains(&s.as_str()) => Ok(s),
        Some(s) => Err(err(
            format!("{key} {s:?} is not valid"),
            "invalid_argument",
            &format!("Valid {key} values: {}.", allowed.join(", ")),
        )),
    }
}

fn req_object(args: &Map<String, Value>, key: &str) -> Result<Value, Value> {
    opt_object(args, key)?.ok_or_else(|| {
        err(
            format!("{key} is required"),
            "missing_argument",
            &format!("Pass {key}={{...}}."),
        )
    })
}

fn obj_or_empty(args: &Map<String, Value>, key: &str) -> Result<Value, Value> {
    Ok(opt_object(args, key)?.unwrap_or_else(|| json!({})))
}

fn opt_object(args: &Map<String, Value>, key: &str) -> Result<Option<Value>, Value> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(map)) => Ok(Some(Value::Object(map.clone()))),
        _ => Err(err(
            format!("{key} must be an object"),
            "invalid_argument",
            "Pass a JSON object.",
        )),
    }
}

fn pct(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

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

    struct Script {
        calls: Vec<(String, String, Option<Vec<u8>>)>,
        replies: VecDeque<Result<RawReply, String>>,
    }
    impl Script {
        fn ok(body: Value) -> Self {
            let mut replies = VecDeque::new();
            replies.push_back(Ok(RawReply {
                status: 200,
                body: serde_json::to_vec(&body).unwrap(),
            }));
            Self {
                calls: Vec::new(),
                replies,
            }
        }
    }
    impl Control for Script {
        fn base(&self) -> &str {
            "http://127.0.0.1:3921"
        }
        fn exchange(
            &mut self,
            method: &str,
            path: &str,
            body: Option<&[u8]>,
        ) -> Result<RawReply, String> {
            self.calls
                .push((method.into(), path.into(), body.map(|b| b.to_vec())));
            self.replies
                .pop_front()
                .unwrap_or_else(|| panic!("no scripted reply for {method} {path}"))
        }
    }

    struct Tmp(PathBuf);
    impl Tmp {
        fn new() -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let p = std::env::temp_dir().join(format!("fakeprint-mcp-{nanos}-{n}"));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
    }
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn send_cases() -> Vec<(&'static str, Value, &'static str, &'static str)> {
        vec![
            ("send_reset", json!({}), "POST", "/v1/reset"),
            (
                "send_faults",
                json!({"body": {"paper_jam": true}}),
                "POST",
                "/v1/faults",
            ),
            (
                "send_profile",
                json!({"id": "generic-wsd"}),
                "POST",
                "/v1/profile",
            ),
            (
                "send_timing",
                json!({"body": {"preset": "instant"}}),
                "POST",
                "/v1/timing",
            ),
            (
                "send_levels",
                json!({"consumables": {"Black": 15}}),
                "POST",
                "/v1/levels",
            ),
            (
                "send_jobs",
                json!({"body": {"name": "seeded"}}),
                "POST",
                "/v1/jobs",
            ),
            ("send_ctl", json!({"cmd": "status"}), "POST", "/v1/ctl"),
            ("send_requests_clear", json!({}), "DELETE", "/v1/requests"),
            (
                "send_requests_config",
                json!({"body": {"redact": true}}),
                "POST",
                "/v1/requests/config",
            ),
        ]
    }

    #[test]
    fn advertised_tools_match_the_registry_and_carry_no_product_prefix() {
        let registered: Vec<&str> = catalog().iter().map(|t| t.name).collect();
        assert_eq!(registered, TOOLS);
        let caps = capabilities();
        assert_eq!(caps["tools"], json!(TOOLS));
        assert_eq!(caps["tool_count"], TOOLS.len());
        assert_eq!(caps["name"], "fakeprint");
        assert_eq!(caps["features"], json!(FEATURES));
        for name in TOOLS {
            assert!(!name.starts_with("hardly_"), "{name}");
            assert!(!name.starts_with("fakeprint_"), "{name}");
        }
        assert!(FEATURES.contains(&"confirm_gate"));
        assert!(FEATURES.contains(&"write_overwrite_guard"));
    }

    #[test]
    fn send_tools_return_a_plan_and_do_not_touch_the_printer_until_confirm() {
        for (name, args, method, path) in send_cases() {
            let mut boom = Boom;
            let dry = call_tool(name, &args, &mut boom);
            assert_eq!(dry["confirmed"], false, "{name}");
            assert_eq!(dry["sent"], false, "{name}");
            assert_eq!(dry["hint"], CONFIRM_HINT, "{name}");
            assert_eq!(dry["plan"]["tool"], name);
            assert_eq!(dry["plan"]["method"], method);
            assert_eq!(dry["plan"]["path"], path);

            let mut args = args;
            args["confirm"] = json!("true");
            let refused = call_tool(name, &args, &mut boom);
            assert_eq!(refused["code"], "invalid_argument", "{name} {refused}");

            args["confirm"] = json!(true);
            let mut script = Script::ok(json!({"applied": true}));
            let sent = call_tool(name, &args, &mut script);
            assert_eq!(sent, json!({"applied": true}), "{name}");
            assert_eq!(script.calls.len(), 1, "{name}");
            assert_eq!(script.calls[0].0, method);
            assert_eq!(script.calls[0].1, path);
            if method == "DELETE" {
                assert!(script.calls[0].2.is_none(), "{name}");
            } else {
                let raw = script.calls[0].2.as_ref().unwrap();
                let body: Value = serde_json::from_slice(raw).unwrap();
                assert_eq!(body, dry["plan"]["body"], "{name}");
            }
        }
    }

    #[test]
    fn a_profile_id_cannot_be_a_path() {
        let mut boom = Boom;
        let v = call_tool(
            "send_profile",
            &json!({"id": "../generic-wsd", "confirm": true}),
            &mut boom,
        );
        assert_eq!(v["code"], "invalid_argument");
    }

    #[test]
    fn read_tools_use_get_and_server_status_does_not() {
        let mut boom = Boom;
        let caps = call_tool("server_status", &json!({}), &mut boom);
        assert_eq!(caps["tool_count"], TOOLS.len());

        let reads = [
            ("health", json!({}), "GET", "/health"),
            ("status", json!({}), "GET", "/v1/status"),
            ("devices", json!({}), "GET", "/v1/devices"),
            ("ctl_info", json!({}), "GET", "/v1/ctl"),
            ("jobs", json!({}), "GET", "/v1/jobs"),
            ("job", json!({"id": 4}), "GET", "/v1/jobs/4"),
            ("faults", json!({}), "GET", "/v1/faults"),
            ("timing", json!({}), "GET", "/v1/timing"),
            ("request", json!({"seq": 9}), "GET", "/v1/requests/9"),
            ("requests_config", json!({}), "GET", "/v1/requests/config"),
        ];
        for (name, args, method, path) in reads {
            let body = if name == "health" {
                b"ok".to_vec()
            } else {
                serde_json::to_vec(&json!({"seen": name})).unwrap()
            };
            let mut script = Script {
                calls: Vec::new(),
                replies: VecDeque::from([Ok(RawReply { status: 200, body })]),
            };
            let v = call_tool(name, &args, &mut script);
            assert_eq!(script.calls.len(), 1, "{name}");
            assert_eq!(script.calls[0].0, method);
            assert_eq!(script.calls[0].1, path);
            assert!(script.calls[0].2.is_none(), "{name}");
            if name == "health" {
                assert_eq!(v["ok"], true);
            } else {
                assert_eq!(v["seen"], name);
            }
        }
    }

    #[test]
    fn request_filters_are_encoded_and_clamped() {
        let mut script = Script::ok(json!({"records": []}));
        let v = call_tool(
            "requests",
            &json!({"since": 3, "limit": 50000, "action": "a b", "bodies": false}),
            &mut script,
        );
        assert!(v.get("error").is_none(), "{v}");
        assert_eq!(
            script.calls[0].1,
            "/v1/requests?since=3&limit=10000&action=a%20b&bodies=0"
        );
    }

    #[test]
    fn write_request_refuses_an_existing_file_and_does_not_fetch_it() {
        let tmp = Tmp::new();
        let dest = tmp.0.join("out.bin");
        std::fs::write(&dest, b"keep").unwrap();
        let mut boom = Boom;
        let v = call_tool(
            "write_request",
            &json!({"seq": 1, "output_path": dest.display().to_string()}),
            &mut boom,
        );
        assert_eq!(v["code"], "output_exists");
        assert!(v["hint"].as_str().unwrap().contains("overwrite=true"));
        assert_eq!(std::fs::read(&dest).unwrap(), b"keep");
    }

    #[test]
    fn write_request_writes_the_raw_bytes_once() {
        let tmp = Tmp::new();
        let dest = tmp.0.join("raw.bin");
        let mut script = Script {
            calls: Vec::new(),
            replies: VecDeque::from([Ok(RawReply {
                status: 200,
                body: b"%PDF".to_vec(),
            })]),
        };
        let v = call_tool(
            "write_request",
            &json!({
                "seq": 7,
                "part": "response",
                "what": "body",
                "output_path": dest.display().to_string(),
            }),
            &mut script,
        );
        assert_eq!(v["wrote"], true, "{v}");
        assert_eq!(std::fs::read(&dest).unwrap(), b"%PDF");
        assert_eq!(
            script.calls[0].1,
            "/v1/requests/7/raw?part=response&what=body"
        );
        let again = call_tool(
            "write_request",
            &json!({"seq": 7, "output_path": dest.display().to_string(), "overwrite": false}),
            &mut script,
        );
        assert_eq!(again["code"], "output_exists");
        assert_eq!(script.calls.len(), 1, "a refused overwrite does not fetch");
    }

    fn job_reply(path: &Path) -> Value {
        json!({
            "id": 3,
            "documents": [{"seq": 1, "id": 1, "path": path.display().to_string(), "bytes": 4}]
        })
    }

    #[test]
    fn write_job_copies_a_file_inside_the_inbox_only() {
        let tmp = Tmp::new();
        let inbox = tmp.0.join("inbox");
        std::fs::create_dir(&inbox).unwrap();
        let src = inbox.join("1.bin");
        std::fs::write(&src, b"%PDF").unwrap();
        let outside = tmp.0.join("secret.bin");
        std::fs::write(&outside, b"nope").unwrap();
        let dest = tmp.0.join("copy.bin");

        let mut script = Script {
            calls: Vec::new(),
            replies: VecDeque::from([
                Ok(RawReply {
                    status: 200,
                    body: serde_json::to_vec(&json!({"inbox_dir": inbox.display().to_string()}))
                        .unwrap(),
                }),
                Ok(RawReply {
                    status: 200,
                    body: serde_json::to_vec(&job_reply(&src)).unwrap(),
                }),
            ]),
        };
        let v = call_tool(
            "write_job",
            &json!({"id": 3, "output_path": dest.display().to_string()}),
            &mut script,
        );
        assert_eq!(v["wrote"], true, "{v}");
        assert_eq!(std::fs::read(&dest).unwrap(), b"%PDF");

        let escaped = tmp.0.join("escaped.bin");
        let mut script = Script {
            calls: Vec::new(),
            replies: VecDeque::from([
                Ok(RawReply {
                    status: 200,
                    body: serde_json::to_vec(&json!({"inbox_dir": inbox.display().to_string()}))
                        .unwrap(),
                }),
                Ok(RawReply {
                    status: 200,
                    body: serde_json::to_vec(&job_reply(&outside)).unwrap(),
                }),
            ]),
        };
        let v = call_tool(
            "write_job",
            &json!({"id": 3, "output_path": escaped.display().to_string()}),
            &mut script,
        );
        assert_eq!(v["code"], "path_outside_inbox", "{v}");
        assert!(!escaped.exists());
    }

    #[cfg(unix)]
    #[test]
    fn write_job_refuses_a_symlink_that_leaves_the_inbox() {
        let tmp = Tmp::new();
        let inbox = tmp.0.join("inbox");
        std::fs::create_dir(&inbox).unwrap();
        let secret = tmp.0.join("secret.bin");
        std::fs::write(&secret, b"nope").unwrap();
        let link = inbox.join("link.bin");
        std::os::unix::fs::symlink(&secret, &link).unwrap();
        let dest = tmp.0.join("copy.bin");
        let mut script = Script {
            calls: Vec::new(),
            replies: VecDeque::from([
                Ok(RawReply {
                    status: 200,
                    body: serde_json::to_vec(&json!({"inbox_dir": inbox.display().to_string()}))
                        .unwrap(),
                }),
                Ok(RawReply {
                    status: 200,
                    body: serde_json::to_vec(&job_reply(&link)).unwrap(),
                }),
            ]),
        };
        let v = call_tool(
            "write_job",
            &json!({"id": 3, "output_path": dest.display().to_string()}),
            &mut script,
        );
        assert_eq!(v["code"], "path_outside_inbox", "{v}");
        assert!(!dest.exists());
    }
}
