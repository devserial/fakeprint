# Lab instance control

Single-instance guard, device registry, ctl surface, named pipe, and optional MCP for **fakeprint**.

Companion for fakescan: [fakescan lab-instance](../../fakescan/docs/lab-instance.md) (separate binary, separate registry, separate ports). See [siblings.md](siblings.md) for co-existence rules.

---

## Why a single instance?

Windows WSDMON / PnP keys off the device UUID **and** the advertise URL
(`http://<ip>:<port>/WebServices/Device`). Restarting fakeprint with a
different port or UUID forces a remove + re-Add cycle in Printers & scanners.

Keep one fakeprint process alive on a fixed port and UUID. Reconfigure it
live via `/v1/*` or `fakeprint.ps1 ctl`. Only restart when you must change
the port or UUID.

---

## Port band

| Product | Default port | Lab ports |
|---------|-------------|-----------|
| **fakeprint** | **3921** | odd: 3921, 3923, 3925, … |
| fakescan | 3920 | even: 3920, 3922, 3924, … |

Scripts enforce the band: `Add-FakeprintDevice` rejects even ports. Never
steal fakescan's canonical `:3920`.

---

## Default identity

| Field | Value |
|-------|-------|
| Port | `3921` |
| Listen | `0.0.0.0:3921` |
| UUID | `00000000-0000-4000-8000-0000000000f3` |
| Instance name | `default` |
| Pipe | `\\.\pipe\fakeprint-default` |

---

## Device registry

`%LOCALAPPDATA%\fakeprint\devices.json` — this product's persistent rows.
Never shares with `%LOCALAPPDATA%\fakescan`.

```json
{
  "advertise_default": "",
  "port_policy": { "base": 3921, "step": 2 },
  "devices": [
    {
      "name": "default",
      "listen": "0.0.0.0:3921",
      "advertise": "",
      "device_uuid": "00000000-0000-4000-8000-0000000000f3",
      "profile": "generic-wsd",
      "exe": "",
      "autostart": false,
      "added": { "pnp": false }
    }
  ]
}
```

Auto-created on first `list` or `ensure` call.

### Registry commands (PowerShell)

```powershell
.\scripts\fakeprint.ps1 list                     # show all rows
.\scripts\fakeprint.ps1 add -Name bpl0           # add second instance (port 3923)
.\scripts\fakeprint.ps1 remove -Name bpl0        # remove row (doesn't stop process)
.\scripts\fakeprint.ps1 save                     # persist running config → registry
.\scripts\fakeprint.ps1 mark-added               # set added.pnp=true (after Add device)
```

Or directly via the module:

```powershell
Import-Module .\scripts\Fakeprint.psm1
Get-FakeprintDevices
Add-FakeprintDevice -Name bpl0
Remove-FakeprintDevice -Name bpl0
Save-FakeprintDevice -Name default
Set-FakeprintDeviceAdded -Name default -Pnp $true
```

---

## Ensure: start or attach

```powershell
.\scripts\fakeprint.ps1 ensure               # default instance
.\scripts\fakeprint.ps1 ensure -Name bpl0   # named instance
.\scripts\fakeprint.ps1 ensure -WsdAdd      # skip IPP; open Settings -> Printers & scanners
```

- If fakeprint is already running on the row's port → attach (prints status; no restart).
- If not running → start it from `exe` (or `cargo run` in a dev checkout).
- After confirming the process is up: if `added.pnp = false`, `ensure` only says so and prints the
  manual steps. **It never adds a printer** (multi-device rule 5; an earlier version auto-ran the
  IPP install when elevated). Pass `-WsdAdd` to also open Settings -> Printers & scanners.
  Use `install-ipp` below for an explicit, human-invoked IPP install.

**Never kills fakescan.** The scripts touch only fakeprint ports and
`%LOCALAPPDATA%\fakeprint`.

---

## Non-interactive IPP spooler queue install (admin)

`ensure` does not run this; it is an explicit, human-invoked command:

```powershell
# Elevated PowerShell only:
.\scripts\fakeprint.ps1 install-ipp                         # default instance
.\scripts\fakeprint.ps1 install-ipp -Name bpl0             # named instance
.\scripts\fakeprint.ps1 install-ipp -Force                  # re-create port + queue
.\scripts\fakeprint.ps1 install-ipp -PrinterName "My Lab Printer"
```

What it does (`Add-Printer -IppURL` path, new binary with `--ipp-uuid`):

```powershell
# Preferred: Internet Port via -IppURL (requires fakeprint >= 0.1 with --ipp-uuid).
# If the JD queue is present, install-ipp removes it first then calls -IppURL.
# Creates WSD Port Monitor Internet Port (not TCPMON).
Add-Printer -IppURL "http://192.168.1.70:3921/ipp/print" -Name "Fake WSD printer"

# Lab fall-back (old binary, or if -IppURL fails):
# TCPMON RAW host:9100 + fakeprint JetDirect listener -> inbox (peer=tcpmon).
# Never pass an http:// URL to Add-PrinterPort -- that leaves PrinterHostAddress
# as a URL and jobs stick at Printing/Retained.
Add-PrinterPort -Name "Fakeprint-JD-default" -PrinterHostAddress "192.168.1.70" -PortNumber 9100
Add-Printer -Name "Fake WSD printer" -DriverName "Microsoft IPP Class Driver" -PortName "Fakeprint-JD-default"

# Mark the registry row so ensure does not re-attempt
.\scripts\fakeprint.ps1 mark-added
```

Verify after install:

```powershell
Get-Printer | Where-Object PortName -like 'Fakeprint-IPP-*' | Format-List Name,DriverName,PortName
Get-PrinterPort -Name 'Fakeprint-IPP-default'
Invoke-RestMethod http://127.0.0.1:3921/v1/jobs   # jobs land here after printing
```

Remove the queue (e.g., before re-testing):

```powershell
.\scripts\fakeprint.ps1 remove-ipp
```

### Fall-back: WSD person-click path

If the IPP path fails (driver not in store, access denied) or you pass `-WsdAdd`:

```powershell
.\scripts\fakeprint.ps1 ensure -WsdAdd    # opens Settings -> Printers & scanners
.\scripts\fakeprint.ps1 add-device        # same, explicit
```

Then under **Add a printer or scanner** click **Add device** next to **Fake WSD printer**.
`add-device` also starts `Watch-FakeprintWsdAdd` — it prints each milestone
(Subscribe, GetPrinterElements, PRINTENUM, WSD queue) as it arrives. Run
`.\scripts\fakeprint.ps1 watch-wsd` in a second window if you want the watcher
without reopening Settings.

### IPP Print-Job capture

The fakeprint server stores IPP Print-Job documents to the inbox directory
(same path as WSD SendDocument). After printing via the IPP queue:

```powershell
Invoke-RestMethod http://127.0.0.1:3921/v1/jobs   # shows captured IPP jobs
Get-ChildItem D:\temp\fakeprint-inbox\            # raw .bin + .json sidecar
```

The job appears in `/v1/jobs` with `peer = "ipp"` to distinguish it from WSD prints.

---

## Single-instance lock

The process acquires `%TEMP%\fakeprint-<port>.lock` on startup. A second
start on the same port prints the attach message and exits:

```
fakeprint already running on :3921 (lab UUID 00000000-0000-4000-8000-0000000000f3).
Attach:  Invoke-RestMethod http://127.0.0.1:3921/v1/status
Reconfigure via /v1/* — do NOT kill/restart (breaks Windows PnP / WSDMON).
Start helper:  .\scripts\fakeprint.ps1 ensure
```

Stale locks (dead PID) are replaced automatically.

---

## Control API

### GET /v1/ctl — identity

```powershell
Invoke-RestMethod http://127.0.0.1:3921/v1/ctl
```

```json
{
  "name": "default",
  "port": 3921,
  "uuid": "00000000-0000-4000-8000-0000000000f3",
  "base_url": "http://127.0.0.1:3921",
  "pipe": "\\\\.\\pipe\\fakeprint-default",
  "cmds": ["status", "health", "jobs", "reset", "profile", "save"]
}
```

### POST /v1/ctl — dispatch

```powershell
# Shorthand via fakeprint.ps1:
.\scripts\fakeprint.ps1 ctl status
.\scripts\fakeprint.ps1 ctl profile -CtlArgs @{id='generic-wsd'}

# Direct IRM:
Invoke-RestMethod -Method Post http://127.0.0.1:3921/v1/ctl `
    -ContentType application/json `
    -Body '{"cmd":"status"}'
```

### POST /v1/profile — hot-swap profile

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:3921/v1/profile `
    -ContentType application/json -Body '{"id":"generic-wsd"}'
```

No restart needed. WSD identity (UUID, endpoint URL) stays the same.

### Available cmds

| cmd | Effect |
|-----|--------|
| `status` | Current status JSON |
| `health` | `{"ok":true}` |
| `jobs` | Captured job list |
| `reset` | Clear in-memory jobs |
| `profile` | Hot-swap profile (`args.id` required) |
| `save` | Guidance — use `fakeprint.ps1 save` |

---

## Named pipe (Windows)

`\\.\pipe\fakeprint-<name>` carries the same JSONL ctl protocol.

```powershell
# Send one command via pipe (PowerShell):
$pipe = New-Object System.IO.Pipes.NamedPipeClientStream('.', 'fakeprint-default', 'InOut')
$pipe.Connect(2000)
$writer = New-Object System.IO.StreamWriter($pipe); $writer.AutoFlush = $true
$reader = New-Object System.IO.StreamReader($pipe)
$writer.WriteLine('{"cmd":"status"}')
$reader.ReadLine()
$pipe.Dispose()
```

Non-Windows: pipe is not compiled in; use HTTP only.

---

## Python client

```python
from fakeprint.client import Client

with Client("http://127.0.0.1:3921") as c:
    print(c.ctl("status"))
    print(c.ctl("profile", id="generic-wsd"))
    print(c.ctl_info())
    print(c.set_profile("generic-wsd"))
```

CLI:

```bash
fakeprint-lab ctl status
fakeprint-lab ctl profile --profile-id generic-wsd
fakeprint-lab ctl-info
```

---

## MCP server (optional)

Attach-only stdio MCP for Cursor / claude-desktop. Requires `pip install -e './python[mcp]'`.

```jsonc
// Cursor settings → MCP servers
{
  "fakeprint": {
    "command": "fakeprint-mcp",
    "args": ["--base-url", "http://127.0.0.1:3921"]
  }
}
```

**Rules:**
- Fails fast if fakeprint is not running (never spawns the binary).
- Never touches fakescan ports or `%LOCALAPPDATA%\fakescan`.
- One Cursor server entry per running fakeprint instance.
- Use `fakescan-mcp` for fakescan — two separate server entries.

Tools: `health`, `status`, `jobs`, `reset`, `ctl_info`, `ctl`, `set_profile`.

Those Python tools call the control API immediately. A separate Rust binary, `crates/fakeprint-mcp`, speaks the same attach-only rule and adds the hardly gates: `send_*` returns a plan until `confirm=true`, and `write_*` will not replace a file unless `overwrite=true`. It is not on the default `fakeprint` build. See [proposals/mcp.md](proposals/mcp.md).

---

## Parallel instances within fakeprint

```powershell
# Start default on 3921
.\scripts\fakeprint.ps1 ensure

# Add and start a second instance on 3923
.\scripts\fakeprint.ps1 add -Name bpl0
.\scripts\fakeprint.ps1 ensure -Name bpl0
```

Each gets a separate UUID (edit the row after add), a separate pipe
(`\\.\pipe\fakeprint-bpl0`), and a separate WSDMON queue after the person
clicks Add device for each instance.

---

## Reboot / autostart

Set `autostart: true` in the registry row, then install a logon task:

```powershell
# Example one-liner (adapt exe path):
$action  = New-ScheduledTaskAction -Execute 'fakeprint.exe' `
    -Argument '--listen 0.0.0.0:3921 --device-uuid 00000000-0000-4000-8000-0000000000f3'
$trigger = New-ScheduledTaskTrigger -AtLogOn
Register-ScheduledTask -TaskName 'FakeprintLab' -Action $action -Trigger $trigger -RunLevel Highest
```

Task name `FakeprintLab` — separate from `FakescanLab`.

---

## Do not kill sibling

| Action | fakeprint scripts | fakescan scripts |
|--------|------------------|-----------------|
| Ports managed | 3921, 3923, … (odd) | 3920, 3922, … (even) |
| Registry | `%LOCALAPPDATA%\fakeprint\devices.json` | `%LOCALAPPDATA%\fakescan\devices.json` |
| Processes killed | None (only `ensure` starts) | None |
| Pipe prefix | `fakeprint-*` | `fakescan-*` |

A lab script that kills or restarts **either** product's process can break
WSDMON for that UUID. Prefer `ensure` over kill/restart.

---

## Install-attention UX

When interactive Add device is still required (`added.pnp = false` and no
silent install has succeeded), `Open-FakeprintAddDevice` fires a multi-channel
alert via the private `Invoke-LabAttention` helper.

### Default (no flag)

1. **Yellow console banner** — always printed; survives output redirection.
2. **`[Console]::Beep(1000, 300)`** — 1 kHz tone for 300 ms.
3. **BEL (`[char]0x7`)** — emitted alongside the beep.
4. **WinForms `NotifyIcon.ShowBalloonTip`** — system-tray balloon, held 3 s then
   disposed.

### Flags

| Flag | Effect |
|------|--------|
| *(default)* | Banner + beep + BEL + balloon |
| `-Loud` | All of the above **plus** `FlashWindowEx` on the first parent-process window handle found by walking the process tree. |
| `-Quiet` | **Banner only** — beep and balloon suppressed. |

### Where it fires

- **`ensure`** — only when a spooler queue is still missing. Order when `added.pnp` is false:
  1. Existing Fake / `Fakeprint-IPP-*` queue → `mark-added`
  2. Elevated session → `Install-FakeprintIppQueue` (no UI)
  3. Non-admin → `Start-FakeprintElevatedInstallIpp` (UAC one-shot); on success mark-added
  4. Else attention + Settings (`Open-FakeprintAddDevice`); `-WsdAdd` skips IPP and goes here
- **`install-ipp`** — same UAC one-shot when not already elevated
- **`elevated`** — interactive elevated shell with the module loaded
- **`add-device`** — direct command.

### Examples

```powershell
# Default attention (banner + beep + balloon)
.\scripts\fakeprint.ps1 ensure

# Beep-free for shared offices
.\scripts\fakeprint.ps1 add-device -Quiet

# Flash terminal window as well (good for background automation)
.\scripts\fakeprint.ps1 ensure -Loud

# Direct attention signal only
.\scripts\fakeprint.ps1 add-device -Loud
```

---

## See also

- [siblings.md](siblings.md) — fakeprint vs fakescan co-existence
- [lab-harness.md](lab-harness.md) — Python print-SOAP harness
- [architecture.md](architecture.md) — layout and ports
- [windows.md](windows.md) — Add printer (person clicks Add device)
