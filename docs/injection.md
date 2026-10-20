# Test-time request injection and runtime timing

Foreign clients talk straight to the HTTP server, so failures a test wants (a SOAP fault on the
second `SendDocument`, a 503, a dropped connection, a slow printer) are armed on the server.

`POST /v1/faults` accepts `"inject": [rule, ...]` next to the existing keys, and
`"inject_clear": true` to drop pending rules first. `GET /v1/faults` reports
`"inject": {"pending": [...], "hits": N, "skipped": N}`; `DELETE /v1/faults` (and `POST /v1/reset`)
clears the rules. A malformed rule, or an unknown `fail_next_document` value, answers HTTP 400
with the reason and changes nothing. A device keeps at most 256 pending rules; a POST that would
pass that cap is HTTP 400 and adds none of its rules. Rules never apply to `/health`, `/` or `/v1/*`.

```json
{ "op": "SendDocument",           // short name (case-insensitive) or full action URI; absent = any
  "endpoint": "print_service",    // device | print_service | discovery | ipp; absent = any WSD endpoint
  "skip": 1,                      // matching requests to let through first (default 0)
  "count": 2,                     // requests to affect; null = forever; absent = 1
  "delay_ms": 250,                // async sleep (max 600000)
  "delay_after": false,           // true: process normally, then sleep before replying
  "effect": { "type": "fault", "code": "receiver", "subcode": "wprt:ServerErrorTemporaryError",
              "reason": "try later", "detail": "text", "http_status": 500 } }
```

Effects (`effect` defaults to `{"type":"pass"}`; with a delay that is a slow printer):

* `{"type":"fault", "code"?, "subcode"?, "reason"?, "detail"?, "http_status"?}`: a well-formed SOAP
  fault with `RelatesTo` from the request. `subcode` is `wprt:Name`, `wsa:Name`, a bare catalogue
  name, or `{ns}local`; `code` (`sender`/`receiver`), `reason` and the HTTP status default from the
  catalogued fault; `detail` is text placed in `soap:Detail`.
* `{"type":"http","status":503,"body":"...","content_type":"text/html"}`: a non-SOAP answer.
* `{"type":"close_connection"}`: the connection is dropped without a status line.
* `{"type":"ipp_status", "status", "message"?}` (IPP endpoint only): a well-formed IPP reply with that
  status code; see [IPP](#ipp-endpoint).

Matching: for each request the **first matching rule decides**. While its `skip` is above zero the
request passes untouched (later rules are not consulted); then its effect applies and `count` drops;
an exhausted rule is removed. Rules are consumed in order, under one lock, so concurrent requests
each consume exactly one slot. Injection happens before dispatch: a faulted `SendDocument` never
reaches the job engine. IPP requests match only rules with `"endpoint": "ipp"`, and there `op`
names an IPP operation (below).

Library: `ServerHandle::{inject, clear_inject, pending_inject, inject_hits, injector, router}`,
`PrintCtx::{inject, clear_inject, pending_inject}`, `InjectRule::{fault, http, close_connection,
delay}` with `.for_op(..) .on(..) .skipping(n) .times(n) .forever() .delayed(ms) .delayed_after(ms)`.

## IPP endpoint

Rules with `"endpoint": "ipp"` apply to `POST /ipp/print` (and `/d/{id}/ipp/print`); rules without an
endpoint never touch IPP. `op` is the IPP operation, spelling-insensitive (`print_job`, `Print-Job`,
`PrintJob` are the same): `print_job`, `validate_job`, `create_job`, `send_document`, `cancel_job`,
`get_job_attributes`, `get_jobs`, `get_printer_attributes` (also `print_uri`, `send_uri`); absent
matches every request, the browser `GET` included. `skip`, `count`, `delay_ms`, `delay_after` are as above.

Effects: `ipp_status` answers a valid IPP response with the given status and `status-message`
(`"status"` is a registry name like `server-error-busy` or `client-error-not-authorized`, a decimal
number or `0x` hex; the table is `ipp::codec::STATUS_NAMES`); the request id and version are echoed
and the job engine never sees the request. `http` and `fault` (bare HTTP status), `close_connection`
and `pass`+delay behave as for WSD. A rule with `ipp_status` outside the IPP endpoint, or with an
unknown status, is refused with HTTP 400. Full reference: [ipp.md](ipp.md).

```json
{ "endpoint": "ipp", "op": "print_job", "skip": 1, "count": 2,
  "effect": { "type": "ipp_status", "status": "server-error-busy", "message": "try later" } }
```

## Job timing at run time

`PrintTiming::instant()` (no processing time) and `PrintTiming::realistic()` (2 s + 1 s/MiB, 0.5 s
terminating) set the start-up timing. On a running server: `GET /v1/timing`, and
`POST /v1/timing {"preset": "instant"|"realistic"|"default", "processing_base_ms": .., "processing_per_mib_ms": ..,
"document_timeout_ms": .., "terminating_ms": .., "document_busy_ms": .., "bytes_per_sheet": ..}`
(also `ctl timing`). Documents that arrive after the call use the new values. A job that is still
processing can be cancelled (`Canceled`); the documents it captured leave the captured list and the
inbox.
