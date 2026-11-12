"""Bounded startup evidence suitable for a public CI annotation.

Never return log text, file paths, command lines, environment variables or
unrelated processes. The complete owned app log remains only in its artifact.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess

LOG_LIMIT = 8192
MAX_CHILDREN = 16
PHASES = ('before-builder', 'setup-start', 'state-ready')


def log_diagnostics(path: Path | None) -> dict:
    result = {'phase': 'none', 'panic': False, 'hresults': []}
    if path is None:
        return result
    try:
        with path.open('rb') as stream:
            stream.seek(0, 2)
            size = stream.tell()
            result['log_bytes'] = size
            result['tail_truncated'] = size > LOG_LIMIT
            stream.seek(max(0, size - LOG_LIMIT))
            text = stream.read(LOG_LIMIT).decode('utf-8', errors='replace')
    except OSError:
        result['log_status'] = 'unavailable'
        return result
    phases = re.findall(r'(?m)^\[trivium-native\] (' + '|'.join(PHASES) + r')\r?$', text)
    if phases:
        result['phase'] = phases[-1]
    result['panic'] = bool(re.search(r'\bpanicked\b', text))
    codes = re.findall(r'HRESULT\s*\(?\s*(0x[0-9a-fA-F]{8}|-?\d{1,11})\b', text, flags=re.I)
    for code in codes:
        value = f'0x{int(code, 16 if code.lower().startswith("0x") else 10) & 0xffffffff:08X}'
        if value not in result['hresults']:
            result['hresults'].append(value)
        if len(result['hresults']) == 3:
            break
    return result


def sanitize_process_snapshot(value, root_pid: int) -> dict:
    if not isinstance(value, dict) or not isinstance(value.get('items'), list):
        return {'probe': 'invalid-result'}
    rows = []
    for item in value['items'][:64]:
        if not isinstance(item, dict):
            continue
        pid, parent, name = item.get('pid'), item.get('parent'), item.get('name')
        if (type(pid) is int and type(parent) is int and 0 < pid <= 0xffffffff
                and 0 <= parent <= 0xffffffff and isinstance(name, str)):
            name = name if re.fullmatch(r'[A-Za-z0-9_. -]{1,64}', name) else 'unrecognized'
            rows.append({'pid': pid, 'parent': parent, 'name': name})
    owned = {root_pid}
    selected = [row for row in rows if row['pid'] == root_pid][:1]
    for _ in range(MAX_CHILDREN):
        changed = False
        for row in rows:
            if len(owned) >= MAX_CHILDREN + 1:
                break
            if row['parent'] in owned and row['pid'] not in owned:
                owned.add(row['pid'])
                selected.append(row)
                changed = True
        if not changed:
            break
    return {'has_window': value.get('has_window') is True, 'count': len(selected), 'items': selected}


def process_diagnostics(process, *, platform: str | None = None) -> dict:
    if (platform or os.name) != 'nt':
        return {'probe': 'not-windows'}
    root_pid = getattr(process, 'pid', None)
    if type(root_pid) is not int or not 0 < root_pid <= 0xffffffff:
        return {'probe': 'unavailable-pid'}
    # Select only these three CIM fields; never inspect CommandLine or env.
    script = r'''
$ErrorActionPreference = 'Stop'
$nativeRootPid = ROOT_PID
$all = @(Get-CimInstance Win32_Process | Select-Object ProcessId, ParentProcessId, Name)
$queue = [System.Collections.Generic.Queue[uint32]]::new()
$seen = [System.Collections.Generic.HashSet[uint32]]::new()
$records = [System.Collections.Generic.List[object]]::new()
$queue.Enqueue([uint32]$nativeRootPid)
[void]$seen.Add([uint32]$nativeRootPid)
while ($queue.Count -gt 0 -and $records.Count -lt 17) {
    $nativeCurrentPid = $queue.Dequeue()
    $entry = $all | Where-Object { $_.ProcessId -eq $nativeCurrentPid } | Select-Object -First 1
    if ($entry) {
        $records.Add(@{pid=[uint32]$entry.ProcessId;parent=[uint32]$entry.ParentProcessId;name=[string]$entry.Name})
    }
    foreach ($child in ($all | Where-Object { $_.ParentProcessId -eq $nativeCurrentPid } | Sort-Object ProcessId)) {
        if ($seen.Count -ge 17) { break }
        if ($seen.Add([uint32]$child.ProcessId)) { $queue.Enqueue([uint32]$child.ProcessId) }
    }
}
$app = Get-Process -Id $nativeRootPid -ErrorAction SilentlyContinue
@{has_window=[bool]($app -and $app.MainWindowHandle -ne 0);items=$records.ToArray()} | ConvertTo-Json -Compress -Depth 3
'''.replace('ROOT_PID', str(root_pid))
    try:
        completed = subprocess.run(['powershell.exe', '-NoLogo', '-NoProfile', '-NonInteractive', '-Command', script],
                                   capture_output=True, text=True, timeout=5, check=False,
                                   creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        if completed.returncode:
            return {'probe': 'nonzero-exit', 'exit_code': completed.returncode}
        return sanitize_process_snapshot(json.loads(completed.stdout), root_pid)
    except (OSError, subprocess.TimeoutExpired, ValueError) as error:
        return {'probe': type(error).__name__}


def startup_diagnostics(process, path: Path | None, *, platform: str | None = None) -> dict:
    return {'log': log_diagnostics(path), 'process': process_diagnostics(process, platform=platform)}
