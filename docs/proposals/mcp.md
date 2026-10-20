# MCP over the control API

Optional stdio server in `crates/fakeprint-mcp`. It attaches to a fakeprint that is already running and speaks that process's `/health` and `/v1/*` routes. It is not part of the `fakeprint` binary and it is not a default feature. Build and run it on its own:

```
cargo run -p fakeprint-mcp -- --base-url http://127.0.0.1:3921
```

`--base-url` defaults to `http://127.0.0.1:3921` (or `FAKEPRINT_BASE_URL`). If `GET /health` is not the text `ok`, the process exits. It never starts or stops fakeprint, and it never talks to fakescan or to `%LOCALAPPDATA%\fakescan`.

The Python entry `fakeprint-mcp` (`python/fakeprint/mcp_server.py`) is unchanged. It is still the small attach-only server whose tools call the control API immediately (`health`, `status`, `jobs`, `reset`, `ctl_info`, `ctl`, `set_profile`). The Rust server is the one that follows the gates below. The two binaries share a name; invoke the Rust one through `cargo run -p fakeprint-mcp` unless you have chosen which one is on `PATH`.

## Gates

The shape matches [hardly](https://github.com/b1tninja/hardly) and is the shape a fakescan MCP should use too. Tool names are unprefixed (`status`, not `fakeprint_status` or `hardly_status`), which is how fakescan's public MCP names `status`, `ctl`, `fault` and the rest.

- A tool that changes printer state or sends a request with a side effect is named `send_`. Called without `confirm=true` it returns a plan and does not open a socket. The hint is the same sentence hardly uses: "Nothing was sent. Tell the person exactly what will be sent, then repeat the same call with confirm=true."
- A tool that writes a file is named `write_`. It does not pick a path. If `output_path` already exists it returns `code: "output_exists"` and writes nothing, unless `overwrite=true`.
- Every other tool only reads.

`confirm` must be a JSON boolean. The string `"true"` is refused and still sends nothing.

A dry run looks like this:

```json
{
  "confirmed": false,
  "sent": false,
  "plan": {
    "tool": "send_faults",
    "method": "POST",
    "path": "/v1/faults",
    "body": { "paper_jam": true }
  },
  "hint": "Nothing was sent. Tell the person exactly what will be sent, then repeat the same call with confirm=true."
}
```

With `confirm=true` the tool returns the control API's JSON unchanged. `DELETE` plans use `"body": null` and send no body.

## Tools

Read: `server_status`, `health`, `status`, `devices`, `ctl_info`, `jobs`, `job`, `faults`, `timing`, `requests`, `request`, `requests_config`.

Send: `send_reset` (`POST /v1/reset`), `send_faults` (`POST /v1/faults`, body is the `FaultBody`; `{"reset": true}` clears), `send_profile` (`id` stem or inline `profile`, exactly one), `send_timing`, `send_levels`, `send_jobs` (seed), `send_ctl`, `send_requests_clear` (`DELETE /v1/requests`), `send_requests_config`.

Write: `write_request` saves `GET /v1/requests/{seq}/raw` (`part` is `request` or `response`, `what` is `all` or `body`). `write_job` copies one captured document only after the path canonicalizes to a file inside the `inbox_dir` from `GET /v1/status`. A symlink that leaves the inbox, a path outside it, or a path that is not a local file is refused and nothing is written. Those cases are `path_outside_inbox` or `not_local`; the hint points at `write_request`.

`server_status` does not call the printer. It returns the capability object, the same fields hardly advertises from `capabilities()`:

`name`, `version`, `features`, `tools`, `tool_count`, `next`.

`features` includes `confirm_gate` and `write_overwrite_guard`. `tools` is the list above, and the test that registers the tools asserts it equals that constant. A client that is missing a name should restart this process.

## What a fakescan twin would share

fakescan's public MCP (attach-only, unprefixed, fails if the scanner is down) is the naming source. It does not use `send_` yet. A twin built the same way would keep those verbs and add the gates:

| fakescan today | this server |
| --- | --- |
| `status`, `ctl_info`, `health` | same names, still read-only |
| `pages` | `jobs` / `job` (print's capture list) |
| `reset` | `send_reset` |
| `ctl` | `send_ctl` |
| `fault` | `send_faults` (plural: `POST /v1/faults` takes a batch) |
| `pattern`, `scenario` | no print equivalent |

The plan object, the hint sentence, the `output_exists` refusal, and `server_status` are the shared shape. Scan-only tools stay on the scanner.

## Limits and what this server will not do

- Base URL is `http://host:port` only. No path, no userinfo, no TLS. The control plane has no TLS listener.
- A response or a request body over 32 MiB is refused. `write_job` will not copy a file over 256 MiB.
- Profile ids are a single stem (`generic-wsd`), not a path. `../` is rejected here, before anything is sent.
- ctl command names are a short token. The printer still decides whether the command exists.
- Unknown arguments are an error, so a misspelled `confirm` cannot fall through into a send.
- This server does not add authentication to `/v1/*`. Who may call the control plane is fakeprint's own listener policy.

Tests build the tool results against an in-memory `Control`. They do not bind a fakeprint and they do not need a network beyond one loopback check of the HTTP parser. `cargo test -p fakeprint-mcp` is the offline run. CI runs that next to `cargo test -p fakeprint`.
