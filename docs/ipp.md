# IPP: the strict IPP/2.0 test device

fakeprint's IPP face is a complete, deliberately strict IPP/2.0 (IPP Everywhere style) printer for
developing and conformance-testing IPP **client libraries**, in the same spirit as the WSD side: it
follows the specification (RFC 8010 encoding, RFC 8011 model, PWG 5100.x job-template vocabulary),
every state is scripted, and every fault is injectable. It is not a model of any one product.

Jobs are **ordinary engine jobs**. A job created over IPP is the same object a WS-Print job is: it
appears in `GET /v1/jobs` and `GET /d/{id}/v1/jobs` (with its documents, counters and a
ticket-equivalent), follows the same timing presets (`instant` / `realistic` / `default`), fault
plan, inbox and history, and can be listed and cancelled over either protocol.

Code: `crates/fakeprint/src/ipp/` (`codec.rs` wire format, `caps.rs` profile -> IPP values,
`attrs.rs` attributes and filtering, `service.rs` operations, `validate.rs` reply validation,
`job.rs`, `mod.rs` HTTP face). Tests: `src/ipp/*` unit tests and `tests/ipp_loopback.rs`.

## URL shapes

| Device | Printer URI (use as `printer-uri`) | HTTP target | Control plane |
|--------|------------------------------------|-------------|---------------|
| default | `ipp://HOST:PORT/ipp/print` | `POST http://HOST:PORT/ipp/print` | `http://HOST:PORT/v1/...` |
| device `x` | `ipp://HOST:PORT/d/x/ipp/print` | `POST http://HOST:PORT/d/x/ipp/print` | `http://HOST:PORT/d/x/v1/...` |

* Content-Type must be `application/ipp` (anything else with a body: HTTP 415). A body larger than
  the printer's `max_body` (64 MiB by default) is HTTP 413 and is not parsed. Other IPP errors are
  IPP status codes inside an HTTP 200; those HTTP errors only come from injection, a wrong content
  type, or that body cap.
* `job-uri` is `<printer-uri>/<job-id>`; a client may also POST a job operation to that path
  (`/ipp/print/<job-id>`), the body decides.
* `printer-uri` is checked on **path** only: scheme `ipp`, `ipps`, `http` or `https`, any host and
  port, exactly the device's path. A URI with another path is `client-error-not-found`, another
  scheme `client-error-uri-scheme-not-supported`. Each device reports its own `printer-uri-supported`
  and `printer-uuid` (the IPP UUID, never equal to the WSD UUID; jobs get `job-uuid`s in the fake
  `00000000-0000-4000-8000-` namespace).
* `GET /ipp/print` (no IPP body) answers a small JSON note for browsers.

## Using fakeprint as the fake printer for client libraries

Start a headless server (no WS-Discovery, no WSD endpoints, no JetDirect, no single-instance lock,
no named pipe, reply validation on, job timing `instant`):

```text
fakeprint --headless --profiles profiles --profile multi-format --inbox <dir>
fakeprint --headless --listen 127.0.0.1:6310 ...          # fixed port instead of an ephemeral one
fakeprint --headless --timing realistic ...                # keep visible processing time
fakeprint --headless --exit-on-stdin-close ...             # harness owns the process: close stdin to stop
```

* Binds `127.0.0.1` on an ephemeral port unless `--listen` says otherwise. Logs go to **stderr**;
  **stdout** gets exactly one machine-readable line once it accepts connections:

  ```text
  FAKEPRINT_READY {"control_url":"http://127.0.0.1:52194","devices":[{"control_url":"http://127.0.0.1:52194","id":"default","ipp_url":"ipp://127.0.0.1:52194/ipp/print"}],"inbox":"...","ipp_url":"ipp://127.0.0.1:52194/ipp/print","port":52194}
  ```

  Export `ipp_url` as `FAKEPRINT_IPP_URL` and `control_url` as `FAKEPRINT_CONTROL_URL` for a test run.
* Shuts down cleanly on Ctrl-C, SIGTERM (Unix), or end of file on stdin with `--exit-on-stdin-close`.
* Printers other than the default: `--device ID=PROFILE` (repeatable); the ready line lists each
  device's `ipp_url` and `control_url`.
* Embedded in a Rust test: `ServerConfig::embedded(inbox)` + `serve(cfg)`, `cfg.clock = Some(manual
  clock)` for a fake clock, `cfg.timing = PrintTiming::instant()`.
* No `ipptool` is needed; `curl --data-binary @request.ipp -H 'Content-Type: application/ipp'`
  works with any request file.

### Deterministic mode

`--headless` already uses `instant` timing: a document is "printed" on the next engine tick (the
engine also ticks on every request), cancels end at once. Switch at run time with
`POST /v1/timing {"preset":"instant"|"realistic"|"default"}`; single fields:
`ipp_multiple_operation_timeout_ms`, `ipp_history_retention_ms` (plus the WSD ones in
[injection.md](injection.md)). With a fake clock (`ManualClock`, embedded only) nothing happens
until the test advances it.

**Pre-seed jobs** so Get-Jobs / reconcile tests need no waiting: `POST /v1/jobs` (or
`/d/{id}/v1/jobs`) with one job object or `{"jobs":[...]}` (at most 1000), answer
`{"created":[ids]}`:

```json
{ "name": "seeded", "user": "alice",
  "state": "pending | processing | completed | canceled | aborted",   // default completed
  "abort_reason": "timeout | format | transfer | compression",        // aborted only
  "format": "application/pdf", "bytes": 1024,                         // one document, or
  "documents": [{"format": "image/jpeg", "bytes": 5000}],
  "copies": 1, "age_ms": 0 }                                          // age: created/finished this long ago
```

A seeded `processing` job stays processing for an hour of engine time (cancel it to end it); a
`pending` one never times out. A completed job older than the retention window (`age_ms` above
`ipp_history_retention_ms`) is already forgotten by IPP. `POST /v1/reset` drops all jobs.

Control endpoints a client test needs: `GET /v1/status` (`ipp_actions`, `ipp_validation`),
`GET|POST /v1/timing`, `GET /v1/jobs`, `GET /v1/jobs/{id}`, `POST /v1/jobs` (seed), `GET|POST|DELETE
/v1/faults` (device conditions and request injection), `POST /v1/levels` (consumable and tray
levels), `POST /v1/reset`, `POST /v1/profile {"id": ...}`.

### Fault injection from a client test

All rules are armed with `POST /v1/faults {"inject":[...]}` (see [injection.md](injection.md) for
the general rule shape). For IPP the rule needs `"endpoint": "ipp"`:

| `op` (spelling-insensitive: `print_job`, `Print-Job`, `PrintJob`) | operation |
|---|---|
| `print_job` `validate_job` `create_job` `send_document` | submission |
| `cancel_job` `get_job_attributes` `get_jobs` `get_printer_attributes` | queries |
| `print_uri` `send_uri` | unimplemented operations (answer `server-error-operation-not-supported` anyway) |
| absent | any operation (and the browser `GET`) |

| effect | result |
|---|---|
| `{"type":"ipp_status","status":"server-error-busy","message":"try later"}` | a well-formed IPP reply with that status (registry name such as `server-error-busy`, a decimal number or `0x` hex) and `status-message`; request id and version echoed; **the operation never reaches the job engine** |
| `{"type":"http","status":503,"body":"..."}` | a plain HTTP answer |
| `{"type":"fault","http_status":500}` | a bare HTTP error |
| `{"type":"close_connection"}` | the TCP connection is dropped without a status line |
| `{"type":"pass"}` with `delay_ms` / `delay_after` | a slow printer |

`skip` (requests let through first) and `count` (requests affected, `null` = forever) work as for
WSD. Examples: `{"endpoint":"ipp","op":"print_job","skip":1,"count":2,"effect":{"type":"ipp_status","status":"server-error-busy"}}`
(second and third Print-Job are busy); `{"endpoint":"ipp","op":"get_jobs","delay_ms":300}`.
A rule with `ipp_status` but no `"endpoint":"ipp"`, or an unknown status name, is refused with 400.
Engine-level states are also scriptable through `POST /v1/faults`: `not_accepting_jobs`
(`server-error-not-accepting-jobs`), `temporary_error` (`server-error-temporary-error`),
`internal_error` (`server-error-internal-error`), conditions (`paper_out`, `paper_jam`,
`cover_open`, `toner_low`, `toner_empty`, ... shown in `printer-state-reasons`).

## What is implemented

### Operations (RFC 8011 sections 4.2 and 4.3)

| Operation | Notes |
|---|---|
| Get-Printer-Attributes | `requested-attributes` names and the groups `all`, `job-template`, `printer-description`; unknown names come back in the unsupported group with `successful-ok-ignored-or-substituted-attributes` |
| Validate-Job | every check of Print-Job, no job created |
| Print-Job | one document in the request body (empty body: `client-error-bad-request`); reply `job-id`, `job-uri`, `job-uuid`, `job-state`, `job-state-reasons` |
| Create-Job + Send-Document | `last-document` is required; several documents when the profile has `multiple_document_jobs` (else `server-error-multiple-document-jobs-not-supported`); an empty Send-Document with `last-document=true` closes the job; target by `printer-uri`+`job-id` or `job-uri` |
| Get-Job-Attributes | `requested-attributes` (names, `all`, `job-template`, `job-description`) |
| Get-Jobs | `which-jobs` (`not-completed` default, `completed`), `my-jobs` (needs `requesting-user-name`), `limit`, `requested-attributes` (default `job-id`,`job-uri`); completed newest first, not-completed by id |
| Cancel-Job | `client-error-not-possible` for ended or already-cancelling jobs, `client-error-not-found` for unknown or forgotten ones |

Not implemented (answer `server-error-operation-not-supported`): Print-URI, Send-URI, Hold/Release/
Restart-Job, Pause/Resume/Purge, Set-*, Identify-Printer, Get-Printer-Supported-Values,
Cancel-My-Jobs, Close-Job, subscriptions and notifications, CUPS extensions, authentication
(`uri-authentication-supported` is `none`; use injection for `client-error-not-authorized`),
TLS (`uri-security-supported` is `none`), document objects (Get-Documents), job ownership
enforcement on Cancel-Job, sniffing of document bytes against `document-format` (bytes are stored
as sent), decompression of `compression` (the compressed bytes are stored; the document is
recorded with its compression), page ranges.

### Strictness

Parse level (`codec.rs`, RFC 8010): header of 8 bytes; every length must fit the message; fixed
sizes (integer/enum 4, boolean 1 and 0 or 1, dateTime 11, resolution 9 with units 3 or 4,
rangeOfInteger 8); text, name, uri are UTF-8, keyword/charset/mime/language are US-ASCII;
`textWithLanguage`/`nameWithLanguage` lengths must add up; out-of-band values carry no data;
collections must close and nest at most 8 deep; an attribute name may appear once per group; a
continuation value needs an attribute; reserved delimiter tags (`0x00`, `0x0b`-`0x0f`) and a
missing `end-of-attributes` are refused. Everything after `end-of-attributes` is document data.

Request level (`service.rs`, RFC 8011 sections 4.1.1 to 4.1.7): version 1.0, 1.1 or 2.0 (else
`server-error-version-not-supported`, answered as 2.0); `request-id` 1 to 2^31-1; the first group
is the operation attributes, once; each other group at most once; the first two operation
attributes are `attributes-charset` then `attributes-natural-language`; the charset is `utf-8` or
`us-ascii` (else `client-error-charset-not-supported`); known operation attributes have the right
syntax and one value (except `requested-attributes`); over-long names (255), text (1023), uris
(1023) are `client-error-request-value-too-long`; an operation attribute that belongs to another
operation is ignored and reported in the unsupported group (`successful-ok-ignored-or-substituted-attributes`).
Every reply echoes the version (or 2.0) and request id, starts with `attributes-charset`,
`attributes-natural-language`, adds `status-message` on errors, and orders groups operation,
unsupported, then job or printer groups.

`ipp-attribute-fidelity` (RFC 8011 section 4.1.7): job-template attributes whose name or value
is outside the printer's own `*-supported` lists are listed in the unsupported-attributes group
(an unknown name as the out-of-band `unsupported` value, a bad value verbatim, a `media-col` reduced
to its offending members). With fidelity **false** (default) the job is created without them and
the status is `successful-ok-ignored-or-substituted-attributes`; with fidelity **true** the operation
fails with `client-error-attributes-or-values-not-supported` and nothing is created. An
unsupported `document-format` (`client-error-document-format-not-supported`) or `compression`
(`client-error-compression-not-supported`) fails whatever the fidelity says.

### Job-template attributes (honoured or rejected by the profile's lists)

`copies` (`print.copies`), `sides`, `orientation-requested` (3 portrait, 4 landscape, 5/6 reverse),
`print-quality` (3 draft, 4 normal, 5 high/photo), `printer-resolution` (dpi, `print.resolutions`),
`media` (`print.media_sizes`), `media-col` (members `media-size`, `media-type`, `media-color`,
`media-source`), `print-color-mode` (`auto` when both colour and monochrome, `color`,
`monochrome`), `number-up` (`print.pages_per_sheet`), `job-priority` (`print.priority`),
`job-hold-until` (only `no-hold`), `finishings` (only `none`/3). `output-bin` is accepted when the
profile has output bins. `print-scaling` (PWG 5100.13: `auto`, `auto-fit`, `fill`, `fit`, `none`;
`print.scaling`, default all five; `default_ticket.scaling`, default `none`) is advertised with
`print-scaling-supported` / `print-scaling-default` only when `print.scaling` is not empty (profile
`minimal-ipp` has `[]`: no advertisement, and the attribute is out-of-band `unsupported`). The job
row's `ipp` object carries `scaling` (the keyword in effect: the job's, else the default),
`media` and `effective_scale`: the uniform scale the printer would apply to the first page of the
stored PDF (`/MediaBox` read from the bytes) to put it on the job's media (`none` 1, `fit` the
largest fitting scale, `auto-fit` that capped at 1, `fill` the smallest covering scale; `auto` is
the printer's choice and is reported as 1). The stored document itself is unchanged: a test
multiplies the scale it measures on the document by `effective_scale`. `null` when the document
is not a flat PDF. Accepted attributes are echoed by Get-Job-Attributes, kept on the job
(`/v1/jobs` row key `ipp`) and translated to a WS-Print ticket (`ticket`).

`document-format-supported` is exactly the profile's `documents.formats` (`generic-wsd` lists PDF,
octet-stream, PWG raster, PostScript and PCL; `multi-format` adds JPEG, PNG, TIFF, URF, text and
more). `document-format-default` is `application/octet-stream` when listed, else the first format.

### Printer attributes

Identity and URIs: `printer-uri-supported`, `uri-security-supported`, `uri-authentication-supported`,
`printer-name`, `printer-location`, `printer-info`, `printer-make-and-model`, `printer-device-id`,
`printer-uuid`. State: `printer-state` (idle 3, processing 4, stopped 5), `printer-state-reasons`,
`printer-state-message`, `printer-is-accepting-jobs`, `queued-job-count`, `printer-up-time`,
`printer-current-time`. Protocol: `ipp-versions-supported` (1.0, 1.1, 2.0), `ipp-features-supported`
(`ipp-everywhere`), `operations-supported`, `charset-*`, `natural-language-configured`,
`generated-natural-language-supported`, `document-format-*`, `pdl-override-supported`,
`compression-supported`, `multiple-document-jobs-supported`, `multiple-operation-time-out`,
`multiple-operation-timeout-action`, `job-creation-attributes-supported`, `job-ids-supported`,
`which-jobs-supported`, `job-k-octets-supported`, `page-ranges-supported`. Capabilities: `color-supported`,
`pages-per-minute[-color]`, `media-*` (`media-supported`, `media-ready`, `media-col-ready`,
`media-size-supported`, `media-col-supported`, `media-source-supported`, `media-type-supported`,
`media-color-supported`), the `*-default` / `*-supported` pairs of the job-template attributes,
`marker-names`/`-types`/`-colors`/`-levels`/`-low-levels`/`-high-levels` from the profile's consumables
(levels follow `POST /v1/levels`).

`printer-state-reasons` maps the live conditions: paper out `media-empty-error`, paper low
`media-low-warning`, jam `media-jam-error`, cover open `cover-open-error`, door `door-open-error`,
toner low `toner-low-warning` (`marker-supply-low-warning` without toner consumables), toner empty
`toner-empty-error`, output full `output-area-full-error`, other `other-*`; a severity suffix of
`-error` (critical), `-warning` or `-report` is appended. A critical condition makes the printer
`stopped` (jobs in progress go to `processing-stopped` with `printer-stopped`). Not accepting jobs
adds `offline-report` and `printer-is-accepting-jobs=false`.

### Job attributes and states

`job-id`, `job-uri`, `job-uuid`, `job-state`, `job-state-reasons`, `job-state-message`, `job-name`,
`job-originating-user-name` (`requesting-user-name`, else `anonymous`), `job-printer-uri`,
`job-printer-up-time`, `time-at-creation`, `time-at-processing`, `time-at-completed` (`no-value`
until they happen; seconds on the `printer-up-time` scale), `date-time-at-creation`,
`number-of-documents`, `job-impressions-completed`, `job-media-sheets-completed`, `job-k-octets`,
`job-k-octets-processed`, `number-of-intervening-jobs` (pending jobs), plus the accepted
job-template attributes.

| Engine state | `job-state` | `job-state-reasons` |
|---|---|---|
| created, no data yet | pending (3) | `job-incoming`, `job-data-insufficient` |
| printing | processing (5) | `job-printing` (+ `job-incoming` while more documents may come) |
| held by a critical condition | processing-stopped (6) | `printer-stopped` |
| cancel requested (while `terminating` time runs) | processing (5) | `processing-to-stop-point`, `job-canceled-by-user` |
| done | completed (9) | `job-completed-successfully` / `job-completed-with-warnings` |
| cancelled | canceled (7) | `job-canceled-by-user` / `job-canceled-at-device` |
| second operation never came | aborted (8) | `aborted-by-system`, `submission-interrupted` |
| document failure (lab `fail_next_document`) | aborted (8) | `aborted-by-system` + `document-format-error` / `compression-error` / `document-access-error` |

**Timing** is the engine's: processing time and sheet counters follow `PrintTiming` (presets
`instant`, `realistic`, `default`), `job-media-sheets-completed` grows with the progress,
`job-impressions-completed` is the sheets (twice for two-sided jobs).
`multiple-operation-time-out` (default 120 s, advertised in seconds, `ipp_multiple_operation_timeout_ms`)
is enforced after Create-Job and after every non-final document by the engine clock. RFC 8011
section 5.4.31 asks implementations to wait on the order of a minute or more; the value is not
restricted to 60 to 240 s so a test can use a short one. **History**: finished jobs stay visible to
IPP (Get-Jobs `completed`, Get-Job-Attributes) for `ipp_history_retention_ms` (default 300 s,
never below the 60 s RFC 8011 section 5.3.7 expects unless a test sets it lower); after that IPP
answers `client-error-not-found` while `/v1/jobs` and WSD still list the job (until the engine
history cap of 100 pushes it out).

### Status codes produced

`successful-ok`, `successful-ok-ignored-or-substituted-attributes`, `client-error-bad-request`,
`client-error-not-possible`, `client-error-not-found`, `client-error-request-entity-too-large`
(document over `limits.max_document`), `client-error-request-value-too-long`,
`client-error-document-format-not-supported`, `client-error-attributes-or-values-not-supported`,
`client-error-uri-scheme-not-supported`, `client-error-charset-not-supported`,
`client-error-compression-not-supported`, `server-error-internal-error`,
`server-error-operation-not-supported`, `server-error-version-not-supported`,
`server-error-temporary-error`, `server-error-not-accepting-jobs`, `server-error-busy` (too many
active jobs, document being processed), `server-error-multiple-document-jobs-not-supported`.
Producible only by injection: `client-error-forbidden`, `-not-authenticated`, `-not-authorized`,
`-timeout`, `-gone`, `-conflicting-attributes`, `server-error-service-unavailable`,
`-device-error`, `-job-canceled`, `-printer-is-deactivated`. The full table is `STATUS_NAMES` in
`codec.rs`.

## Reply validation

The switch is the WS-Print one: `FAKEPRINT_VALIDATE_REPLIES=1`, `--validate-replies`, headless
mode, or `ServerConfig::validate_replies = Some(true)` (embedded servers default to on). Every IPP
reply (including injected statuses) is parsed again and checked for what RFC 8011 requires:
version and request-id echo, the two leading operation attributes, `status-message` on errors, group
order, the unsupported group exactly when the status calls for it, the mandatory printer
attributes of a full Get-Printer-Attributes, the four job attributes after a job is created,
`job-id` in every Get-Jobs entry, value syntax of the well-known attributes, parallel arrays
(`uri-*-supported`, `marker-*`). Violations are never fatal: they are logged, counted in
`GET /v1/status` under `ipp_validation` (`enabled`, `checked`, `problems`, `recent`) and available
as `ServerHandle::ipp_validation_problems()`.

## Conformance checklist for client libraries

What a client should be exercised against, and the test in `tests/ipp_loopback.rs` (or the unit
tests of `src/ipp/service.rs`) that shows the server side of it:

* Discovery of capabilities: Get-Printer-Attributes with and without `requested-attributes`, group
  names, unknown names, parallel arrays, collections (`media-col-ready`), `printer-state-reasons`
  with severity suffixes: `get_printer_attributes_reports_the_profile_and_the_live_status`,
  `printer_state_reasons_follow_the_fault_flags`.
* Print-Job with job-template attributes, counters and the inbox: `print_job_lifecycle_and_the_shared_job_rows`;
  large (>2 MiB) documents: `a_document_over_two_mebibytes_prints`.
* Unsupported values, `ipp-attribute-fidelity` both ways, `media-col` offenders, format and
  compression refusals, Validate-Job: `job_template_unsupported_values_follow_the_fidelity_rule`.
* Create-Job + Send-Document, `last-document`, empty closing document, `job-uri` targeting:
  `create_job_and_send_document_with_last_document`.
* `multiple-operation-time-out` advertised and enforced, restart per document, configurable:
  `the_multiple_operation_timeout_is_advertised_and_enforced_on_the_fake_clock`.
* Cancel-Job, in-flight cancel (`processing-to-stop-point`), cancel after completion, late
  Send-Document: `cancel_job_races_and_error_codes`.
* Get-Jobs `which-jobs`, `my-jobs`, `limit`, `requested-attributes`, ordering:
  `get_jobs_which_jobs_my_jobs_limit_and_requested_attributes`; forgotten jobs:
  `finished_jobs_are_forgotten_after_the_retention_window_but_not_by_wsd`; seeding:
  `seeded_jobs_make_get_jobs_scenarios_instant`.
* Protocol errors: truncated attributes, request-id 0, charset/language order, version, charset,
  unknown operation, wrong `printer-uri`, bad syntax, over-long values, HTTP 415:
  `envelope_errors_are_ipp_errors_not_http_errors`.
* Server-side states a client must survive: `not-accepting-jobs`, temporary and internal errors:
  `engine_fault_flags_map_to_server_errors`; busy, service-unavailable, any status code, HTTP
  503, dropped connection, slow printer: `ipp_status_effect_skip_count_and_op_names`,
  `http_close_and_delay_effects_on_the_ipp_endpoint`.
* Several printers: `each_device_has_its_own_ipp_endpoint_uuid_and_jobs`; jobs shared with WSD:
  `jobs_created_over_wsd_are_listed_and_cancelable_over_ipp`.
* Counters and the validator: `status_reports_ipp_counters_and_validation`.

## Not verified

No real IPP client (CUPS `ipptool`, `lp`, the Windows IPP class driver) has been run against the
new face; the loopback tests use an independent byte-level client written for the tests. The server
is built from the RFC 8010 / 8011 and PWG 5100.x semantics as read, so a disagreement between a
real client and this device is worth a look at both sides.
