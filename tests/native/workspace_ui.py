"""Real Tauri/WebKit regression for workspace, filters, analysis and themes.

Run under Xvfb with the same embedded native binary and drivers as
row_navigation.py. All projects and app settings use an isolated profile.
Native file-picker interaction is excluded: the actual export dialog is
checked, then its applied UI QuerySpec is exported through real native IPC.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any
from urllib.error import URLError
import uuid

from row_navigation import ELEMENT, WebView, free_port, open_project, wait_for
from feedback_checks import verify_feedback
from refinement_checks import verify_refinements
from column_pin_checks import verify_column_pinning
from detail_timeline_checks import verify_detail_timeline
from compact_detail_checks import verify_compact_detail
from scroll_render_checks import verify_scroll_rendering
from wide_table_checks import verify_wide_table
from runtime import ApplicationSession, IPC_COMMAND_JS, isolated_environment, native_driver_default, start_driver, stop_driver


FLAGS = [("Safe", "safe"), ("Suspicious", "suspicious"), ("Critical", "critical"), ("Unflagged", "none")]
TOOLS = {"Conditions": "conditions", "Column stats": "stats", "Groups": "groups", "Time analysis": "time", "Timeline": "timeline", "Presets": "presets", "Bulk annotation": "bulk", "IOC rules": "iocs"}
HEAVY = {"column_stats", "histogram", "time_analysis", "group_tree"}


class WorkspaceView(WebView):
    def node(self, selector: str, index: int = 0) -> dict[str, str]:
        return wait_for(lambda: self.script(
            "return [...document.querySelectorAll(arguments[0])][arguments[1]]??null;", selector, index,
        ), bool, f"{selector}[{index}]")

    def named(self, name: str, container: str = "body") -> None:
        element = wait_for(lambda: self.script(
            "return [...document.querySelectorAll(arguments[0]+' button,'+arguments[0]+' summary')]"
            ".find(e=>(e.getAttribute('aria-label')===arguments[1]||e.textContent.trim()===arguments[1])"
            "&&!e.disabled&&e.getClientRects().length)??null;", container, name,
        ), bool, f"enabled {name}")
        self.click(element)

    def tap(self, element: dict[str, str]) -> None:
        point = self.script("const e=arguments[0],r=e.getBoundingClientRect();const x=Math.round((Math.max(0,r.left)+Math.min(innerWidth,r.right))/2),y=Math.round((Math.max(0,r.top)+Math.min(innerHeight,r.bottom))/2),h=document.elementFromPoint(x,y);return {x,y,hit:e.contains(h),target:h?{tag:h.tagName,classes:String(h.className)}:null};", element)
        assert point["hit"], f"A real pointer must reach this control without an overlay: {point}"
        self.request("POST", self.route("actions"), {"actions": [{"type": "pointer", "id": "native-mouse", "parameters": {"pointerType": "mouse"}, "actions": [
            {"type": "pointerMove", "x": point["x"], "y": point["y"], "duration": 0, "origin": "viewport"},
            {"type": "pointerDown", "button": 0}, {"type": "pointerUp", "button": 0}
        ]}]})

    def choose(self, selector: str, value: str, index: int = 0) -> None:
        option = self.script("return [...document.querySelectorAll(arguments[0])][arguments[2]]?.querySelector('option[value='+CSS.escape(arguments[1])+']')??null;", selector, value, index)
        assert option, (selector, value, index)
        self.click(option)

    def write(self, selector: str, value: str, index: int = 0) -> None:
        element = self.node(selector, index)
        if value:
            self.request("POST", self.route(f"element/{element[ELEMENT]}/clear"), {})
            self.request("POST", self.route(f"element/{element[ELEMENT]}/value"), {"text": value})
        else:
            # WebKit's clear command changes the DOM value but does not emit
            # the input event used by Svelte bindings. Real keys do both.
            self.request("POST", self.route(f"element/{element[ELEMENT]}/value"), {"text": "\ue009a\ue000\ue003"})

    def instrument(self) -> None:
        assert self.script("""
          window.__workspaceErrors=[];window.__workspaceIpc=[];
          addEventListener('error',e=>window.__workspaceErrors.push(e.message));
          addEventListener('unhandledrejection',e=>window.__workspaceErrors.push(String(e.reason)));
          const original=window.fetch;
          const wrapped=(input,options,...rest)=>{
""" + IPC_COMMAND_JS + """
            if(!name||name.startsWith('plugin:')||name==='active_jobs')return original.call(window,input,options,...rest);
            let args={};
            try{args=JSON.parse(typeof options.body==='string'?options.body:new TextDecoder().decode(options.body));}catch{}
            const request=args?.payload??args?.request??{};
            const entry={name,request,requestId:args.requestId??null,
              metadataOnly:args.metadataOnly??null,started:performance.now()};
            window.__workspaceIpc.push(entry);
            if(window.__workspaceIpc.length>2000)window.__workspaceIpc.shift();
            const perform=()=>{entry.dispatched=true;return original.call(window,input,options,...rest);};
            const before=window.__workspaceBeforeCommit;
            let promise;
            if(before&&!before.ready&&before.name===name&&before.projectId===request.projectId
              &&(!('rowIndex' in before)||before.rowIndex===(request.row_index??request.rowIndex))){
              before.ready=true;before.request=structuredClone(request);entry.dispatched=false;
              promise=new Promise(resolve=>{let released=false;before.release=()=>{
                if(released)return;released=true;resolve(perform());
              };});
            }else promise=perform();
            return promise.then(async response=>{
              const value=await response.clone().json();
              entry.elapsed_ms=performance.now()-entry.started;
              if(response.headers.get('Tauri-Response')==='ok'){
                entry.result=name==='histogram'?value:{count:value?.count,updated:value?.updated,
                  rows:Array.isArray(value?.rows)?value.rows.length:value?.rows,
                  total_rows:value?.total_rows,total_filtered_rows:value?.total_filtered_rows};
              }else entry.error=String(value);
              const hold=window.__workspaceHold;
              if(hold&&!hold.ready&&entry.result&&hold.name===name&&hold.projectId===request.projectId
                &&(!('search' in hold)||hold.search===request.search)
                &&(!('offset' in hold)||hold.offset===request.offset)
                &&(!('total' in hold)||hold.total===entry.result.total_filtered_rows)){
                hold.request=structuredClone(request);hold.result=entry.result;hold.ready=true;
                return new Promise(resolve=>{hold.release=()=>resolve(response);});
              }
              return response;
            },error=>{entry.error=String(error);throw error;});
          };
          window.fetch=wrapped;return window.fetch===wrapped;
        """), "Real native IPC instrumentation must be active"

    def trace(self) -> list[dict[str, Any]]:
        return self.script("return window.__workspaceIpc;")

    def visible_column_count(self) -> int:
        return self.script("return Number(document.querySelector('[aria-label=\"Choose visible columns\"] .count-chip').textContent.split('/')[0]);")

    def diagnostics(self) -> dict[str, Any]:
        return self.script("return {js_errors:window.__workspaceErrors??[],ipc:window.__workspaceIpc??[]};")

    def state(self) -> dict[str, Any]:
        state = self.view()
        state.update(self.script("""
          return {active:document.querySelector('[role=tab][aria-selected=true]')?.id??'',
            headers:[...document.querySelectorAll('.data-header .header-label')].map(e=>e.textContent.trim()),
            selectedIds:[...document.querySelectorAll('.data-row input[aria-label^="Select row "]:checked')]
              .map(e=>Number(e.getAttribute('aria-label').replace('Select row ',''))),
            selectionTotal:Number(document.querySelectorAll('.table-meta-left strong')[1]?.textContent.replaceAll(',','')??0),
            scrollLeft:document.querySelector('.virtual-viewport')?.scrollLeft??0,
            views:document.querySelectorAll('.project-view').length,
            tabs:document.querySelectorAll('[role=tab]').length};
        """))
        if not state.get("ready"):
            state.update(self.script("return {total:Number(document.querySelector('.table-meta-left strong')?.textContent.replaceAll(',','')??-1),search:document.querySelector('input[aria-label=\"Search rows\"]')?.value??''};"))
        return state

    def horizontal(self, delta: int) -> dict[str, int]:
        before = self.state()["scrollLeft"]
        point = self.script("const r=document.querySelector('.table-header-scroll').getBoundingClientRect();return {x:Math.round(Math.min(r.left+600,innerWidth-20)),y:Math.round(r.top+r.height/2)};")
        self.request("POST", self.route("actions"), {"actions": [{"type": "wheel", "id": f"native-horizontal-{uuid.uuid4().hex}", "actions": [
            {"type": "scroll", "x": point["x"], "y": point["y"], "deltaX": delta, "deltaY": 0, "duration": 0}
        ]}]})
        return wait_for(lambda: self.script("return {body:document.querySelector('.virtual-viewport').scrollLeft,header:document.querySelector('.table-header-scroll').scrollLeft};"),
                        lambda value: value["body"] == max(0, before + delta) and value["header"] == value["body"], "actual horizontal wheel synchronizes header and body")

    def results(self, count: int) -> dict[str, Any]:
        def ready(state: dict[str, Any]) -> bool:
            if not state.get("ready"):
                return count == 0 and state.get("total") == 0 and self.script("return !!document.querySelector('.empty-rows');")
            if state["total"] != count or not state["rows"] or any(row["id"] is None for row in state["rows"]):
                return False
            visible = [row for row in state["rows"] if row["bottom"] > state["top"] + .5 and row["top"] < state["bottom"] - .5]
            return bool(visible) and visible[0]["top"] <= state["top"] + 1 and (
                count * 32 < state["viewportHeight"] or visible[-1]["bottom"] >= state["bottom"] - 1)
        return wait_for(self.state, ready, f"{count} fully loaded workspace results")

    def applied(self, **expected: Any) -> dict[str, Any]:
        def latest() -> dict[str, Any] | None:
            entry = next((item for item in reversed(self.trace()) if item["name"] == "query_project_rows"
                          and "searchColumns" in item["request"] and "flagFilter" in item["request"]), None)
            return entry if entry and "result" in entry and all(entry["request"].get(key) == value for key, value in expected.items()) else None
        return wait_for(latest, bool, f"applied native query {expected}")["request"]

    def query(self) -> dict[str, Any]:
        request = self.applied()
        return {key: request.get(key) for key in ["projectId", "search", "searchColumns", "condition", "flagFilter", "bookmarkedOnly", "sortKey", "sortDirection"]}

    def refreshed(self, after: int) -> None:
        wait_for(lambda: next((entry for entry in reversed(self.trace()[after:])
            if entry["name"] == "query_project_rows" and "searchColumns" in entry["request"]), None),
            lambda entry: bool(entry and "result" in entry), "annotation refresh finishes before independent readback")

    def close_drawer(self) -> None:
        if self.script("return !!document.querySelector('#analysis-drawer');"):
            self.named("Close analysis panel")
            wait_for(lambda: self.script("return !document.querySelector('#analysis-drawer');"), bool, "analysis drawer closes")

    def drawer(self, name: str) -> None:
        before = self.view().get("viewportHeight")
        tool = TOOLS[name]
        group = "Filters" if tool in {"conditions", "presets"} else "Annotations" if tool in {"bulk", "iocs"} else "Analysis"
        if not self.script("return !!document.querySelector(arguments[0]);", f"#tool-tab-{tool}"):
            self.named(f"Open {group}")
        self.click(self.element(f"#tool-tab-{tool}"))
        wait_for(lambda: self.script("return document.querySelector(arguments[0])?.getAttribute('aria-selected');", f"#tool-tab-{tool}"), lambda value: value == "true", f"{name} drawer")
        assert self.script("return document.querySelectorAll('#analysis-drawer').length;") == 1
        after = self.view().get("viewportHeight")
        if before is not None and after is not None:
            assert abs(after - before) <= 1, "Analysis must not reduce table height"

    def flags(self, tokens: list[str]) -> str:
        self.close_drawer()
        if not self.script("return !!document.querySelector('#flag-filter-menu');"):
            self.click(self.element('button[aria-controls="flag-filter-menu"]'))
        for label, token in FLAGS:
            checkbox = self.script("return [...document.querySelectorAll('#flag-filter-menu label')].find(e=>e.textContent.trim()===arguments[0])?.querySelector('input');", label)
            assert checkbox, label
            checked = self.script("return arguments[0].checked;", checkbox)
            if checked != (token in tokens):
                self.click(checkbox)
        actual = self.script("return [...document.querySelectorAll('#flag-filter-menu label')].filter(e=>e.querySelector('input').checked).map(e=>e.textContent.trim());")
        assert actual == [label for label, token in FLAGS if token in tokens]
        self.click(self.element('button[aria-controls="flag-filter-menu"]'))
        wire = "|".join(token for _, token in FLAGS if token in tokens) or "all"
        self.applied(flagFilter=wire)
        return wire

    def tab(self, project_id: str) -> None:
        self.click(self.element(f'#project-tab-{project_id}'))
        wait_for(lambda: self.state()["active"], lambda value: value == f"project-tab-{project_id}", "active project tab")


class NativeApp:
    def __init__(self, args: argparse.Namespace, root: Path, evidence: Path):
        self.args, self.root, self.evidence = args, root, evidence
        self.port, self.native_port = free_port(), free_port()
        while self.native_port == self.port:
            self.native_port = free_port()
        environment = isolated_environment(root)
        self.environment = environment
        self.application = None
        self.log = (evidence / "driver.log").open("w")
        self.driver = start_driver([args.driver, "--port", str(self.port), "--native-port", str(self.native_port),
                                    "--native-driver", args.native_driver], environment, self.log)
        self.web = WorkspaceView(self.port)
        self.sessions: list[dict[str, Any]] = []

    def start(self) -> WorkspaceView:
        self.web.origin = f"http://127.0.0.1:{self.native_port}"
        def ready() -> bool:
            if self.driver.poll() is not None:
                raise AssertionError("tauri-driver exited")
            try:
                return bool(self.web.request("GET", "/status"))
            except (OSError, URLError):
                return False
        wait_for(ready, bool, "native WebDriver ready", 20)
        self.web.origin = f"http://127.0.0.1:{self.port}"
        self.application = ApplicationSession(self.args.binary.resolve(), self.environment, self.evidence)
        try:
            session = self.web.request("POST", "/session", {"capabilities": {"alwaysMatch": self.application.capabilities}})
        except BaseException:
            self.application.close()
            self.application = None
            raise
        self.web.session = session["sessionId"]
        self.web.origin = f"http://127.0.0.1:{self.native_port}"
        self.web.request("POST", self.web.route("timeouts"), {"script": 600000, "pageLoad": 60000, "implicit": 0})
        self.web.request("POST", self.web.route("window/rect"), {"width": 1280, "height": 827})
        wait_for(lambda: self.web.script("return typeof window.__TAURI_INTERNALS__?.invoke==='function';"), bool, "Tauri IPC ready")
        wait_for(lambda: self.web.script("return !!document.querySelector('.app-header');"), bool, "native workspace UI ready")
        assert ":5173" not in self.web.script("return location.href;"), "Embedded native app required"
        self.web.instrument()
        if self.web.script("return !!document.querySelector('.workspace-drawer');"):
            self.web.named("Close workspace menu", ".workspace-drawer")
            wait_for(lambda: self.web.script("return !document.querySelector('.workspace-drawer');"), bool, "initial project menu closes")
        return self.web

    def end_session(self) -> None:
        try:
            if self.web.session:
                diagnostic = self.web.diagnostics()
                if self.application is not None:
                    diagnostic["startup"] = self.application.details
                self.sessions.append(diagnostic)
                self.web.request("DELETE", f"/session/{self.web.session}")
        finally:
            self.web.session = None
            if self.application is not None:
                self.application.close()
                self.application = None

    def close(self) -> None:
        try:
            self.end_session()
        finally:
            stop_driver(self.driver)
            self.log.close()


def fixture(web: WorkspaceView, root: Path, total: int = 512) -> dict[str, Any]:
    source = root / f"workspace-{uuid.uuid4().hex[:8]}.csv"
    columns = ["source_id", "score", "cohort", "timestamp"] + [f"col_{i:02d}" for i in range(5, 28)]
    timestamps = {i: f"{'2001-01-01' if i <= 2 else '2026-11-11'}T12:{i // 60:02d}:{i % 60:02d}.{123456000 + i:09d}Z" for i in range(1, total + 1)}
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows([f"r-{i:07d}", i, "drop" if i % 5 == 0 else "keep", timestamps[i]] + [f"value-{column}-{i % 7}" for column in columns[4:]] for i in range(1, total + 1))
    sha = hashlib.sha256(source.read_bytes()).hexdigest()
    project = web.command("create_project", {"path": str(source)})
    project_id = project["meta"]["id"]
    for label, token in FLAGS[:3]:
        ids = [i - 1 for i in range(1, total + 1) if i % 4 == {"safe": 0, "suspicious": 1, "critical": 2}[token]]
        payload = {"projectId": project_id, "target": "selection", "rowIndices": ids, "flag": token}
        preview = web.command("bulk_preview", payload)
        applied = web.command("bulk_update", {**payload, "previewToken": preview["previewToken"]})
        assert applied["updated"] == len(ids), (label, applied)
    web.command("update_flag", {"projectId": project_id, "row_index": 9, "memo": "Persist native audit", "flag": "critical"})
    detail = web.command("load_project", {"projectId": project_id}, request=True)
    ids = {column["label"]: column["id"] for column in detail["columns_meta"]}
    assert len(ids) == 27, ids
    return {"id": project_id, "name": project["meta"]["name"], "filename": source.name, "source": source,
            "columns": ids, "sha": sha, "total": total, "timestamps": timestamps}


def flag_filters(web: WorkspaceView, data: dict[str, Any], root: Path, report: dict[str, Any]) -> None:
    total, checked = data["total"], []
    for step in range(16):
        mask = step ^ (step >> 1)
        tokens = [token for index, (_, token) in enumerate(FLAGS) if mask & (1 << index)]
        wire = web.flags(tokens)
        expected = total if not tokens else total * len(tokens) // 4
        web.results(expected)
        query = web.query()
        assert query["flagFilter"] == wire and not query.get("search") and query.get("condition") is None, query
        web.named("Export rows", ".filters")
        wait_for(lambda: web.script("return document.querySelector('.export-dialog')?.textContent;"), lambda value: bool(value and f"Current results ({expected})" in value), "export uses applied flag count")
        web.named("Close", ".export-dialog")
        destination = root / f"flags-{mask}.csv"
        result = web.command("export_project", {**query, "destination": str(destination), "scope": "filtered", "includeAnnotations": True})
        rows = list(csv.DictReader(destination.open(encoding="utf-8", newline="")))
        assert result["rows"] == len(rows) == expected, (mask, result)
        for row in rows:
            active = next((token for token in ["safe", "suspicious", "critical"] if row[f"trivium-{token}"] == "1"), "none")
            assert not tokens or active in tokens, (tokens, row)
        web.drawer("Bulk annotation")
        before = len(web.trace())
        web.named("Preview", "#analysis-drawer")
        preview = wait_for(lambda: next((entry for entry in reversed(web.trace()[before:]) if entry["name"] == "bulk_preview" and "result" in entry), None), bool, "actual bulk preview")
        assert preview["result"]["count"] == expected and preview["request"]["flagFilter"] == wire, preview
        web.close_drawer()
        checked.append({"selection": tokens, "rows": expected, "export_rows": len(rows), "bulk_preview": expected})
    web.flags(["safe", "critical"])
    web.results(total // 2)
    web.drawer("Bulk annotation")
    web.write('input[aria-label="Bulk tag"]', "native-ui")
    before = len(web.trace())
    web.named("Add tag", "#analysis-drawer")
    wait_for(lambda: next((entry for entry in reversed(web.trace()[before:]) if entry["name"] == "bulk_update" and "result" in entry), None), bool, "bulk adds tags to applied result")
    web.refreshed(before)
    all_rows = web.command("query_project_rows", {"projectId": data["id"], "offset": 0, "limit": total})["rows"]
    assert all(("native-ui" in row["tags"]) == ((row["row_index"] + 1) % 4 in [0, 2]) for row in all_rows)
    before = len(web.trace())
    web.named("Undo last bulk", "#analysis-drawer")
    wait_for(lambda: next((entry for entry in reversed(web.trace()[before:]) if entry["name"] == "bulk_undo" and "result" in entry), None), bool, "bulk undo")
    web.refreshed(before)
    assert all("native-ui" not in row["tags"] for row in web.command("query_project_rows", {"projectId": data["id"], "offset": 0, "limit": total})["rows"])
    web.close_drawer()
    web.flags([])
    web.results(total)
    web.drawer("Bulk annotation")
    web.write('input[aria-label="Bulk note"]', "delayed bulk audit")
    before = len(web.trace())
    web.named("Preview", "#analysis-drawer")
    wait_for(lambda: next((entry for entry in reversed(web.trace()[before:]) if entry["name"] == "bulk_preview" and "result" in entry), None), bool, "delayed bulk preview")
    web.script("window.__workspaceHold={name:'bulk_update',projectId:arguments[0],ready:false,release:null};", data["id"])
    try:
        web.named("Apply", "#analysis-drawer")
        wait_for(lambda: web.script("return window.__workspaceHold.ready;"), bool, "real bulk completion held")
        web.close_drawer()
        web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
        wait_for(lambda: web.script("const rows=[...document.querySelectorAll('.data-row')];return rows.length>0&&rows.every(row=>row.querySelector('.flag-safe.selected')&&row.querySelector('.memo-button').textContent.includes('delayed bulk audit'));"), bool, "bulk completion refreshes rows after drawer unmount")
    finally:
        web.script("window.__workspaceHold?.release?.();delete window.__workspaceHold;")
    web.drawer("Bulk annotation")
    web.named("Undo last bulk", "#analysis-drawer")
    wait_for(lambda: web.script("return [...document.querySelectorAll('.data-row')].some(row=>row.querySelector('.cell.index')?.textContent.trim()==='1'&&row.querySelector('.flag-suspicious.selected'));"), bool, "undo restores original row flag")
    web.close_drawer()
    web.results(total)
    report["flag_subsets"] = checked
    report["bulk_completion_after_panel_close"] = True


def columns_and_header(web: WorkspaceView, data: dict[str, Any], root: Path, report: dict[str, Any]) -> None:
    web.write('input[aria-label="Search rows"]', "cohort:keep")
    expected = sum(i % 5 != 0 for i in range(1, data["total"] + 1))
    web.applied(search="cohort:keep")
    web.results(expected)
    before = web.query()
    web.named("Choose visible columns")
    web.named("Deselect all", "#visible-column-menu")
    wait_for(lambda: web.state()["headers"], lambda value: not value, "all data columns hidden")
    assert web.state()["search"] == "cohort:keep" and web.state()["total"] == expected
    web.write('input[aria-label="Find a column"]', "col_27")
    assert web.script("return document.querySelectorAll('#visible-column-menu li').length;") == 1
    web.click(web.element('#visible-column-menu li input'))
    wait_for(lambda: web.state()["headers"], lambda value: value == ["col_27"], "one matching column visible")
    web.named("Select all", "#visible-column-menu")
    wait_for(web.visible_column_count, lambda value: value == 27, "all27columns configured visible")
    web.write('input[aria-label="Find a column"]', "")
    assert web.script("return document.querySelectorAll('#visible-column-menu li').length;") == 27
    assert web.script("return [...document.querySelectorAll('#visible-column-menu li input')].every(e=>e.checked);")
    web.named("Done", "#visible-column-menu")
    after = web.query()
    assert all(before.get(key) == after.get(key) for key in ["search", "searchColumns", "condition", "flagFilter"]), (before, after)
    sha = web.element('[aria-label="Copy full SHA-256"]')
    assert data["sha"] in web.script("return arguments[0].title;", sha)
    assert not web.script("return !!document.querySelector('.brand-icon');")
    web.click(sha)
    copy = wait_for(lambda: web.script("return document.querySelector('.copy-status')?.textContent;"), bool, "SHA copy result")
    assert copy == "SHA-256 copied", copy
    data["source"].rename(root / "original-source-moved.csv")
    imports = sum(entry["name"] == "create_project" for entry in web.trace())
    checkpoint = len(web.trace())
    web.named("Workspace options")
    web.named("Reload saved project", ".workspace-options")
    wait_for(lambda: next((entry for entry in reversed(web.trace()[checkpoint:]) if entry["name"] == "load_project" and entry["request"].get("projectId") == data["id"] and "result" in entry), None), bool, "saved project actually reloads")
    web.refreshed(checkpoint)
    web.results(expected)
    assert imports == sum(entry["name"] == "create_project" for entry in web.trace()), "Reload must not reimport the source"
    assert data["sha"] in web.script("return document.querySelector('[aria-label=\"Copy full SHA-256\"]').title;")
    web.write('input[aria-label="Search rows"]', "")
    web.applied(search="")
    web.results(data["total"])
    for label in ["source_id", "score", "cohort", "timestamp"]:
        header = web.script("return [...document.querySelectorAll('.data-header button')].find(e=>e.querySelector('.header-label')?.textContent.trim()===arguments[0]);", label)
        web.tap(header)
        web.applied(sortKey=data["columns"][label], sortDirection="asc")
        web.results(data["total"])
    horizontal = web.horizontal(250)
    web.horizontal(-250)
    first = web.script("return document.querySelector('.data-header button');")
    web.tap(first)
    web.applied(sortKey=data["columns"]["source_id"], sortDirection="asc")
    web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "native-keyboard", "actions": [
        action for _ in range(26) for action in [{"type": "keyDown", "value": "\ue004"}, {"type": "keyUp", "value": "\ue004"}]
    ]}]})
    focused = wait_for(lambda: web.script("const e=document.activeElement;return {label:e?.querySelector('.header-label')?.textContent.trim(),body:document.querySelector('.virtual-viewport').scrollLeft,header:document.querySelector('.table-header-scroll').scrollLeft};"),
                       lambda value: value.get("label") == "col_27" and value["body"] > 0 and value["body"] == value["header"], "keyboard reveals last of27columns")
    web.horizontal(-focused["body"])
    report["columns_header"] = {"columns": 27, "hidden_all_then_restored": True, "search_preserved": True, "sha_copy": copy, "reload_without_source": True,
                                "actual_header_sort_clicks": ["source_id", "score", "cohort", "timestamp"], "actual_horizontal_scroll_sync": horizontal,
                                "keyboard_last_column": focused}


def analysis(web: WorkspaceView, data: dict[str, Any], evidence: Path, report: dict[str, Any]) -> None:
    web.drawer("Column stats")
    web.choose('select[aria-label="Stats column"]', data["columns"]["cohort"])
    values = wait_for(lambda: web.script("return [...document.querySelectorAll('[aria-label=\"Most frequent values\"] .value-count-row')].map(e=>({value:e.querySelector('.value-label').textContent,count:Number(e.querySelector('.value-count').textContent.replaceAll(',',''))}));"), lambda value: len(value) == 2, "compact top values")
    assert sum(value["count"] for value in values) == data["total"], values
    assert web.script("return !document.querySelector('.rare-section').open;")
    web.choose('select[aria-label="Stats column"]', data["columns"]["score"])
    wait_for(lambda: web.script("return document.querySelectorAll('.column-histogram rect').length;"), lambda value: value > 0, "numeric SVG histogram")
    web.drawer("Time analysis")
    wait_for(lambda: web.script("return !!document.querySelector('.time-panel .time-section');"), bool, "time gaps/sessions/bursts")
    assert web.script("return document.querySelector('select[aria-label=\"Sort gaps\"]').value;") == "duration"
    titles = web.script("return [...document.querySelectorAll('.span-row')].map(e=>e.title);")
    assert titles and any(".123456" in title for title in titles), titles
    web.drawer("Timeline")
    web.choose('select[aria-label="Timeline column"]', data["columns"]["timestamp"])
    focused = wait_for(lambda: web.script("const e=document.querySelector('.timeline-summary');return e?{inside:Number(e.dataset.inWindow),outside:Number(e.dataset.outside),text:e.textContent}:null;"), lambda value: bool(value and 0 < value["outside"] < data["total"] / 4), "dense timeline initial focus")
    assert focused["inside"] + focused["outside"] == data["total"], focused
    web.screenshot(evidence / "timeline-focused.png")
    web.named("Show all events", "#analysis-drawer")
    wait_for(lambda: web.script("return Number(document.querySelector('.timeline-summary')?.dataset.outside??-1);"), lambda value: value == 0, "show all includes old outliers")
    buckets = web.script("return [...document.querySelectorAll('.timeline-bucket')].map(e=>({count:Number(e.dataset.count),safe:Number(e.dataset.safe),suspicious:Number(e.dataset.suspicious),critical:Number(e.dataset.critical),unflagged:Number(e.dataset.unflagged),title:e.querySelector('title').textContent}));")
    assert buckets and sum(bucket["count"] for bucket in buckets) == data["total"]
    assert all(bucket["count"] == sum(bucket[flag] for flag in ["safe", "suspicious", "critical", "unflagged"]) for bucket in buckets)
    assert all(sum(bucket[flag] for bucket in buckets) == data["total"] // 4 for flag in ["safe", "suspicious", "critical", "unflagged"])
    assert any(".123456" in bucket["title"] for bucket in buckets), "Hover titles retain exact time bounds"
    flagged = web.script("return document.querySelector('.table-meta-right').textContent;")
    def flag_totals() -> dict[str, int]:
        return web.script("return ['safe','suspicious','critical','unflagged'].reduce((s,flag)=>({...s,[flag]:[...document.querySelectorAll('.timeline-bucket')].reduce((n,e)=>n+Number(e.dataset[flag]),0)}),{});")
    for next_flag, safe, critical in [("Critical", 127, 129), ("Safe", 128, 128)]:
        button = web.script("return [...document.querySelectorAll('.data-row')].find(e=>e.querySelector('.cell.index')?.textContent.trim()==='4')?.querySelector('.flag-buttons button[aria-label=\"'+arguments[0]+'\"]');", next_flag)
        assert button, "Original row4 must be visible for a real flag edit"
        web.click(button)
        wait_for(flag_totals, lambda value: value["safe"] == safe and value["critical"] == critical, "open timeline refreshes same-flagged-count edit")
        assert web.script("return document.querySelector('.table-meta-right').textContent;") == flagged
    raw = web.command("query_project_rows", {"projectId": data["id"], "offset": 0, "limit": 1})["rows"][0]
    assert raw["cells"][data["columns"]["timestamp"]]["text"] == data["timestamps"][1]
    web.screenshot(evidence / "timeline-all-flags.png")
    web.close_drawer()
    before = sum(entry["name"] in HEAVY for entry in web.trace())
    web.write('input[aria-label="Search rows"]', "keep")
    web.applied(search="keep")
    web.results(sum(i % 5 != 0 for i in range(1, data["total"] + 1)))
    time.sleep(.4)
    assert before == sum(entry["name"] in HEAVY for entry in web.trace()), "Closed panels must not run heavy analysis"
    web.write('input[aria-label="Search rows"]', "")
    web.applied(search="")
    web.results(data["total"])
    web.flags(["safe", "critical"])
    web.write('input[aria-label="Search rows"]', "keep")
    web.applied(search="keep", flagFilter="safe|critical")
    filtered = sum(i % 4 in [0, 2] and i % 5 != 0 for i in range(1, data["total"] + 1))
    web.results(filtered)
    web.drawer("Timeline")
    wait_for(lambda: web.script("return !!document.querySelector('.timeline-summary');"), bool, "filtered timeline")
    web.named("Show all events", "#analysis-drawer")
    expected_flags = {"safe": sum(i % 4 == 0 and i % 5 != 0 for i in range(1, data["total"] + 1)),
                      "critical": sum(i % 4 == 2 and i % 5 != 0 for i in range(1, data["total"] + 1)), "suspicious": 0, "unflagged": 0}
    wait_for(flag_totals, lambda value: value == expected_flags, "timeline shares search and flag selections")
    web.close_drawer()
    web.flags([])
    web.write('input[aria-label="Search rows"]', "")
    web.applied(search="", flagFilter="all")
    web.results(data["total"])
    report["analysis"] = {"top_values": values, "only_active_drawer": True, "height_preserved": True,
                          "focused": focused, "all_event_count": sum(bucket["count"] for bucket in buckets), "flag_counts": {flag: sum(bucket[flag] for bucket in buckets) for flag in ["safe", "suspicious", "critical", "unflagged"]}, "raw_nanoseconds_preserved": True, "closed_heavy_requests": 0,
                          "same_flagged_count_edit_refresh": True, "filtered_flag_counts": expected_flags}


def timeline_gestures(web: WorkspaceView, data: dict[str, Any], report: dict[str, Any]) -> None:
    web.drawer("Timeline")
    wait_for(lambda: web.script("return !!document.querySelector('.timeline-summary');"), bool, "timeline before gestures")
    point = web.script("const r=document.querySelector('.timeline-svg').getBoundingClientRect();return {x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2),left:Math.round(r.left),width:Math.round(r.width)};")
    before = sum(entry["name"] == "histogram" for entry in web.trace())
    web.script("window.__workspaceWheelEvents=[];window.addEventListener('wheel',event=>{if(event.target.closest?.('.timeline-svg')&&event.deltaY!==0)window.__workspaceWheelEvents.push({delta:event.deltaY,trusted:event.isTrusted});},true);")
    wheel_supported = True
    try:
        web.request("POST", web.route("actions"), {"actions": [{"type": "wheel", "id": "native-wheel", "actions": [
            {"type": "scroll", "x": point["x"], "y": point["y"], "deltaX": 0, "deltaY": -20 * index, "duration": 0} for index in range(1, 11)
        ]}]})
    except AssertionError as error:
        if not any(word in str(error).lower() for word in ["unsupported", "unknown action", "wheel"]):
            raise
        wheel_supported = False
        report["unverified"].append(f"Native wheel action unavailable in WebKitWebDriver: {error}")
    if wheel_supported:
        events = web.script("return window.__workspaceWheelEvents;")
        assert len(events) == 10 and all(event["trusted"] for event in events), events
        wait_for(lambda: sum(entry["name"] == "histogram" for entry in web.trace()), lambda value: value > before, "wheel zoom native histogram")
        time.sleep(.3)
        calls = sum(entry["name"] == "histogram" for entry in web.trace()) - before
        assert calls == 1, f"One rapid10-eventwheelburst must request one histogram, observed {calls}"
    for action in ["Zoom in", "Pan left"]:
        before = len(web.trace())
        web.named(action, "#analysis-drawer")
        wait_for(lambda: next((entry for entry in reversed(web.trace()[before:]) if entry["name"] == "histogram"), None), lambda entry: bool(entry and "result" in entry), f"{action} completes")
    point = web.script("const r=document.querySelector('.timeline-svg').getBoundingClientRect();return {x:Math.round(r.left+r.width*.3),to:Math.round(r.left+r.width*.7),y:Math.round(r.top+r.height*.5)};")
    web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "native-mouse", "parameters": {"pointerType": "mouse"}, "actions": [
        {"type": "pointerMove", "x": point["x"], "y": point["y"], "duration": 0, "origin": "viewport"},
        {"type": "pointerDown", "button": 0}, {"type": "pointerMove", "x": point["to"], "y": point["y"], "duration": 150, "origin": "viewport"},
        {"type": "pointerUp", "button": 0}
    ]}]})
    wait_for(lambda: web.query().get("condition"), bool, "pointer drag selects a structured time range")
    web.close_drawer()
    web.drawer("Conditions")
    assert web.script("return document.querySelectorAll('.condition-row').length;") == 2
    values = web.script("return [...document.querySelectorAll('.condition-row input[aria-label=\"Value\"]')].map(e=>e.value);")
    assert all("." in value and len(value.split(".")[-1].removesuffix("Z")) == 9 for value in values), values
    while True:
        count = web.script("return document.querySelectorAll('.condition-row').length;")
        if not count:
            break
        web.named("Remove condition", ".condition-editor")
        wait_for(lambda: web.script("return document.querySelectorAll('.condition-row').length;"), lambda value: value == count - 1, "remove gesture condition")
    web.close_drawer()
    web.applied(condition=None)
    web.results(data["total"])
    report["timeline_gestures"] = {"native_wheel": wheel_supported, "native_nonzero_wheel_events": len(events) if wheel_supported else None,
                                   "zoom_and_pan": True, "native_pointer_drag": True, "exact_nanosecond_range_values": values}


def tab_workspace(web: WorkspaceView, data: dict[str, Any], root: Path, evidence: Path, report: dict[str, Any]) -> None:
    web.button("Last")
    web.results(data["total"])
    for flag in ["Critical", "Safe"]:
        button = web.script("return [...document.querySelectorAll('.data-row')].find(e=>e.querySelector('.cell.index')?.textContent.trim()==='512')?.querySelector('.flag-buttons button[aria-label=\"'+arguments[0]+'\"]');", flag)
        web.tap(button)
        wait_for(lambda: web.script("return [...document.querySelectorAll('.data-row')].find(e=>e.querySelector('.cell.index')?.textContent.trim()==='512')?.querySelector('.flag-buttons button.selected')?.getAttribute('aria-label');"), lambda value: value == flag, "last row flag remains reachable beside horizontal scrolling")
    memo = web.script("return [...document.querySelectorAll('.data-row')].find(e=>e.querySelector('.cell.index')?.textContent.trim()==='512')?.querySelector('.memo-button');")
    web.tap(memo)
    wait_for(lambda: web.script("return !!document.querySelector('[role=dialog][aria-label=\"Edit note for row 512\"]');"), bool, "last-row Add note opens")
    web.named("Cancel", ".memo-dialog")
    web.screenshot(evidence / "last-row-flags.png")
    web.button("First")
    web.results(data["total"])
    web.write('input[aria-label="Search rows"]', "keep")
    web.applied(search="keep")
    web.drawer("Conditions")
    web.named("Add condition", ".condition-editor")
    web.choose('.condition-row select[aria-label="Column"]', data["columns"]["score"])
    web.write('.condition-row input[aria-label="Value"]', "100")
    web.choose('.condition-row select[aria-label="Operator"]', "ge")
    web.named("Add group", ".condition-editor")
    web.choose('select[aria-label="Combine conditions"]', "or", 1)
    for index, value in enumerate(["keep", "drop"], 1):
        web.named("Add condition", ".condition-group .condition-group")
        web.choose('.condition-row select[aria-label="Column"]', data["columns"]["cohort"], index)
        web.write('.condition-row input[aria-label="Value"]', value, index)
        web.choose('.condition-row select[aria-label="Operator"]', "eq", index)
    conditions = web.script("return {kinds:[...document.querySelectorAll('select[aria-label=\"Combine conditions\"]')].map(e=>e.value),values:[...document.querySelectorAll('input[aria-label=\"Value\"]')].map(e=>e.value)};")
    assert conditions == {"kinds": ["and", "or"], "values": ["100", "keep", "drop"]}, conditions
    web.close_drawer()
    expected = sum(i >= 100 and i % 5 != 0 for i in range(1, data["total"] + 1))
    web.results(expected)
    header = web.script("return [...document.querySelectorAll('.data-header button')].find(e=>e.querySelector('.header-label')?.textContent.trim()==='score');")
    web.tap(header)
    web.applied(sortDirection="asc", sortKey=data["columns"]["score"])
    web.tap(header)
    web.applied(sortDirection="desc", sortKey=data["columns"]["score"])
    web.results(expected)
    web.named("Choose visible columns")
    web.click(web.element(f'#column-{data["columns"]["col_27"]}'))
    web.named("Done", "#visible-column-menu")
    web.write('.row-jump input[type="number"]', "242")
    web.button("Go")
    wait_for(web.state, lambda state: any(row["id"] == 242 and row["top"] >= state["top"] - 1 and row["bottom"] <= state["bottom"] + 1 for row in state.get("rows", [])), "original242fullyvisible")
    for row in [242, 243]:
        web.click(web.element(f'input[aria-label="Select row {row}"]'))
    web.horizontal(200)
    before = web.state()
    assert before["selectionTotal"] == 2 and sorted(before["selectedIds"]) == [242, 243], before
    source = root / f"secondary-{uuid.uuid4().hex[:8]}.json"
    source.write_text(json.dumps([{"source_id": f"r-{i:07d}", "other": "secondary marker"} for i in range(1, 257)]), encoding="utf-8")
    second = web.command("create_project", {"path": str(source)})
    second_id = second["meta"]["id"]
    open_project(web, second["meta"]["name"])
    web.results(256)
    assert web.state()["tabs"] == 2 and web.state()["views"] == 1
    web.write('input[aria-label="Search rows"]', "secondary")
    web.applied(projectId=second_id, search="secondary")
    web.results(256)
    web.tab(data["id"])
    web.applied(projectId=data["id"], search="keep", sortDirection="desc")
    web.results(expected)
    after = wait_for(web.state, lambda state: abs(state["scrollTop"] - before["scrollTop"]) <= 1 and abs(state["scrollLeft"] - before["scrollLeft"]) <= 1,
                     "tab restores actual vertical and horizontal DOM scroll positions")
    assert after["search"] == before["search"] and after["headers"] == before["headers"] and after["selected"] == before["selected"], (before, after)
    assert after["selectionTotal"] == 2 and sorted(after["selectedIds"]) == [242, 243], after
    assert abs(after["scrollTop"] - before["scrollTop"]) <= 1, (before, after)
    assert abs(after["scrollLeft"] - before["scrollLeft"]) <= 1 and before["scrollLeft"] == 200, (before, after)
    web.screenshot(evidence / "tabs-restored-table.png")
    web.drawer("Conditions")
    restored = web.script("return {kinds:[...document.querySelectorAll('select[aria-label=\"Combine conditions\"]')].map(e=>e.value),values:[...document.querySelectorAll('input[aria-label=\"Value\"]')].map(e=>e.value)};")
    assert restored == conditions, restored
    web.screenshot(evidence / "tabs-restored-conditions.png")
    web.close_drawer()
    web.request("POST", web.route("window/rect"), {"width": 320, "height": 827})
    web.tap(web.element(f'#project-tab-{data["id"]}'))
    def arrow(value: str) -> None:
        web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "native-keyboard", "actions": [
            {"type": "keyDown", "value": value}, {"type": "keyUp", "value": value}
        ]}]})
    arrow("\ue014")
    wait_for(lambda: web.state()["active"], lambda value: value == f"project-tab-{second_id}", "ArrowRight switches to second narrow tab")
    narrow = wait_for(lambda: web.script("const p=document.querySelector('.project-tabs'),e=p.querySelector('[aria-selected=true]'),r=e.getBoundingClientRect();return {left:r.left,right:r.right,width:innerWidth,overflow:p.scrollWidth>p.clientWidth,scroll:p.scrollLeft};"),
                      lambda value: value["overflow"] and value["left"] >= -1 and value["right"] <= value["width"] + 1, "selected overflow tab is revealed")
    web.tap(web.element(f'#project-tab-{second_id}'))
    web.screenshot(evidence / "narrow-tabs.png")
    arrow("\ue012")
    wait_for(lambda: web.state()["active"], lambda value: value == f"project-tab-{data["id"]}", "ArrowLeft switches back to first narrow tab")
    web.request("POST", web.route("window/rect"), {"width": 1280, "height": 827})
    web.applied(projectId=data["id"], search="keep")
    web.results(expected)
    web.script("window.__workspaceHold={name:'query_project_rows',projectId:arguments[0],search:'never-present',total:0,ready:false,release:null};", data["id"])
    try:
        web.write('input[aria-label="Search rows"]', "never-present")
        wait_for(lambda: web.script("return window.__workspaceHold.ready;"), bool, "real old-tab query response held")
        held = web.script("return {request:window.__workspaceHold.request,result:window.__workspaceHold.result};")
        assert held["request"]["search"] == "never-present" and held["result"]["total_filtered_rows"] == 0, held
        web.tab(second_id)
        web.applied(projectId=second_id, search="secondary")
        web.results(256)
        web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
        web.async_script("const done=arguments[arguments.length-1];requestAnimationFrame(()=>requestAnimationFrame(()=>done(true)));")
        state = web.results(256)
        assert state["active"] == f"project-tab-{second_id}" and state["search"] == "secondary", state
    finally:
        web.script("window.__workspaceHold?.release?.();delete window.__workspaceHold;")
    web.named(f'Close {data["filename"]}')
    wait_for(lambda: web.state()["tabs"], lambda count: count == 1, "inactive tab closes")
    saved = web.command("list_projects")
    assert {data["id"], second_id}.issubset({project["meta"]["id"] for project in saved})
    annotation = web.command("query_project_rows", {"projectId": data["id"], "offset": 9, "limit": 1})["rows"][0]
    assert annotation["flag"] == "critical" and annotation["memo"] == "Persist native audit", annotation
    open_project(web, data["name"])
    web.applied(projectId=data["id"], search="never-present")
    web.results(0)
    web.tab(second_id)
    web.results(256)
    web.named(f"Close {source.name}")
    wait_for(lambda: web.script("return document.querySelectorAll('[role=tab]').length;"), lambda value: value == 1, "active tab closes to remaining project")
    wait_for(lambda: web.state()["active"], lambda value: value == f'project-tab-{data["id"]}', "closing active tab selects remaining project")
    web.applied(projectId=data["id"], search="never-present")
    assert web.results(0)["views"] == 1
    assert len(web.command("list_projects")) == 2
    report["tabs"] = {"csv_and_json": True, "restored_search_condition_sort_columns_scroll_selection": True,
                      "restored_selected_original_ids": [242, 243], "late_native_response_ignored": True,
                      "closing_keeps_projects_and_annotations": True, "mounted_project_views": 1,
                      "last_row_actual_flag_change_restore": True, "last_row_add_memo": True, "narrow_keyboard_tab_reveal": narrow,
                      "held_old_query": held}



def pending_annotation_reopen(web: WorkspaceView, data: dict[str, Any], root: Path, report: dict[str, Any]) -> None:
    """Hold the native mutation before dispatch, then close and reopen its tab."""
    source = root / f"pending-write-{uuid.uuid4().hex[:8]}.csv"
    source.write_text("host,value\nalpha,one\nbeta,two\n", encoding="utf-8")
    project = web.command("create_project", {"path": str(source)})
    project_id = project["meta"]["id"]
    open_project(web, project["meta"]["name"])
    web.results(2)
    web.script("window.__workspaceBeforeCommit={name:'update_flag',projectId:arguments[0],rowIndex:0,ready:false,release:null};", project_id)
    try:
        button = web.script("return [...document.querySelectorAll('.data-row')].find(e=>e.querySelector('.cell.index')?.textContent.trim()==='1')?.querySelector('.flag-buttons button[aria-label=\"Safe\"]');")
        web.tap(button)
        wait_for(lambda: web.script("return window.__workspaceBeforeCommit.ready;"), bool, "annotation held before native dispatch")
        held = next(entry for entry in reversed(web.trace()) if entry["name"] == "update_flag" and entry["request"].get("projectId") == project_id)
        assert held["dispatched"] is False, held
        uncommitted = web.command("query_project_rows", {"projectId": project_id, "offset": 0, "limit": 1})
        assert uncommitted["rows"][0]["flag"] == "" and uncommitted["annotated_total"] == 0, uncommitted
        web.named(f"Close {source.name}")
        wait_for(lambda: web.state()["active"], lambda value: value == f'project-tab-{data["id"]}', "other case activates while annotation is blocked")
        web.applied(projectId=data["id"], search="never-present")
        web.results(0)

        # Select through the actual project menu without awaiting its close:
        # the UI intentionally waits for this case's pending mutation first.
        web.named("Open project menu")
        web.named("Refresh projects", ".workspace-drawer")
        card = wait_for(lambda: web.script("return [...document.querySelectorAll('.project-card')].find(e=>e.querySelector('.project-name')?.textContent.trim()===arguments[0])??null;", project["meta"]["name"]), bool, "pending case in saved project list")
        before = len(web.trace())
        web.click(card)
        wait_for(lambda: web.state()["active"], lambda value: value == f"project-tab-{project_id}", "reopened pending case selected")
        web.async_script("const done=arguments[arguments.length-1];requestAnimationFrame(()=>requestAnimationFrame(()=>done(true)));")
        time.sleep(.2)
        assert web.script("return !document.querySelector('.project-view')&&document.querySelector('main')?.textContent.includes('Loading project');"), "The replacement case must wait instead of querying stale annotations"
        assert not any(entry["name"] == "load_project" and entry["request"].get("projectId") == project_id for entry in web.trace()[before:]), "Pending annotation must settle before metadata-only native load is dispatched"
        web.script("window.__workspaceBeforeCommit.release();delete window.__workspaceBeforeCommit;")
        wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "project menu closes after annotation commit")
        web.results(2)
        wait_for(lambda: web.script("return [...document.querySelectorAll('.data-row')].find(e=>e.querySelector('.cell.index')?.textContent.trim()==='1')?.querySelector('.flag-safe.selected')!==null&&document.querySelector('.table-meta-right')?.textContent.trim()==='Flagged 1';"), bool, "reopened row and Flagged counter include the old tab's committed edit")
        loaded = next(entry for entry in web.trace()[before:] if entry["name"] == "load_project" and entry["request"].get("projectId") == project_id)
        assert loaded["metadataOnly"] is True and loaded["requestId"], loaded
        saved = web.command("query_project_rows", {"projectId": project_id, "offset": 0, "limit": 1})
        assert saved["rows"][0]["flag"] == "safe" and saved["annotated_total"] == 1, saved
        report["pending_annotation_reopen"] = {"held_before_native_dispatch": True, "different_case_independent": True,
            "reopened_case_waited": True, "saved_flag": "safe", "flagged_count": 1,
            "metadata_only_owned_load": True}
        web.named(f"Close {source.name}")
        web.results(0)
    finally:
        web.script("window.__workspaceBeforeCommit?.release?.();delete window.__workspaceBeforeCommit;")


def themes(app: NativeApp, root: Path, evidence: Path, report: dict[str, Any]) -> None:
    restored = []
    app.web.named("Workspace options")
    available = app.web.script("return [...document.querySelectorAll('select[aria-label=\"Theme\"] option')].map(e=>e.value);")
    app.web.named("Workspace options")
    assert available == ["trivium", "light", "dark", "toki", "sakura", "kohaku", "hisui", "nagi",
        "sora", "yukari", "sumire", "yozora", "shion", "kasumi"], available
    for theme in available:
        web = app.web
        web.named("Workspace options")
        if web.script("return document.documentElement.dataset.theme;") == theme:
            web.choose('select[aria-label="Theme"]', "dark" if theme != "dark" else "light")
        web.choose('select[aria-label="Theme"]', theme)
        wait_for(lambda: web.script("return document.documentElement.dataset.theme;"), lambda value: value == theme, f"{theme} CSS variables applied")
        def saved_themes() -> list[str]:
            try:
                return [json.loads(path.read_text()).get("theme") for path in root.rglob("theme.json")]
            except (OSError, json.JSONDecodeError):
                return []
        wait_for(saved_themes, lambda value: theme in value, f"{theme} written to native settings")
        if theme in {"light", "sakura", "sora", "kohaku", "hisui", "toki", "nagi"}:
            surfaces = web.script("const c=document.createElement('canvas').getContext('2d'),s=getComputedStyle(document.documentElement);return ['--bg','--surface'].map(name=>{c.clearRect(0,0,1,1);c.fillStyle=s.getPropertyValue(name);c.fillRect(0,0,1,1);return [...c.getImageData(0,0,1,1).data];});")
            assert all(colour == [255, 255, 255, 255] for colour in surfaces), (theme, surfaces)
        web.screenshot(evidence / f"theme-{theme}.png")
        app.end_session()
        web = app.start()
        wait_for(lambda: web.script("return document.documentElement.dataset.theme;"), lambda value: value == theme, f"{theme} survives actual native restart")
        restored.append(theme)
    report["themes"] = {"applied_and_restored_after_native_restart": restored}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--driver", default=shutil.which("tauri-driver") or "/workspace/.trivium-tools/bin/tauri-driver")
    parser.add_argument("--native-driver", default=native_driver_default())
    parser.add_argument("--output", type=Path, default=Path("target/native-workspace-ui"))
    args = parser.parse_args()
    args.binary.resolve(strict=True)
    evidence = args.output.resolve() / uuid.uuid4().hex[:12]
    evidence.mkdir(parents=True)
    report: dict[str, Any] = {"started_utc": datetime.now(timezone.utc).isoformat(), "passed": False,
                              "unverified": ["Native GTK export destination picker", "Native OS import picker (only its selected-path response is substituted)"], "stages": []}
    with tempfile.TemporaryDirectory(prefix="trivium-native-workspace-") as temporary:
        root = Path(temporary)
        app = NativeApp(args, root, evidence)
        try:
            web = app.start()
            data = fixture(web, root)
            open_project(web, data["name"])
            web.results(data["total"])
            report["default_viewport"] = {"window": [1280, 827], "table_height": web.view()["viewportHeight"]}
            web.screenshot(evidence / "default-table.png")
            for name, run in [
                ("feedback usability and sticky cells", lambda: verify_feedback(web, data, root, evidence, report)),
                ("refined imports, scope, grouping and styling", lambda: verify_refinements(web, data, root, evidence, report)),
                ("stable stripes while scrolling", lambda: verify_scroll_rendering(web, data, evidence, report)),
                ("virtualized wide table and unchanged rows", lambda: verify_wide_table(web, data, root, evidence, report)),
                ("saved arbitrary column pinning", lambda: verify_column_pinning(web, data, root, evidence, report)),
                ("compact scrollable row detail", lambda: verify_compact_detail(web, data, root, evidence, report)),
                ("full row detail and sparse flagged timeline", lambda: verify_detail_timeline(web, data, root, evidence, report)),
                ("flag filters and bulk/export", lambda: flag_filters(web, data, root, report)),
                ("columns and header", lambda: columns_and_header(web, data, root, report)),
                ("analysis drawers and timeline", lambda: analysis(web, data, evidence, report)),
                ("timeline wheel/pan/drag", lambda: timeline_gestures(web, data, report)),
                ("multiple project tabs", lambda: tab_workspace(web, data, root, evidence, report)),
                ("pending annotation close and reopen", lambda: pending_annotation_reopen(web, data, root, report)),
                ("all available themes and actual restarts", lambda: themes(app, root, evidence, report)),
            ]:
                print(f"CHECK: {name}", flush=True)
                run()
                report["stages"].append(name)
                print(f"PASS: {name}", flush=True)
            assert not any(session["js_errors"] for session in app.sessions + [app.web.diagnostics()])
            errors = [entry for session in app.sessions + [app.web.diagnostics()] for entry in session["ipc"] if "error" in entry]
            cancelled = [entry for entry in errors if entry["error"] == "Cancelled." and entry["name"] in HEAVY | {"query_project_rows", "load_project", "locate_project_row", "get_cell", "surrounding_rows", "group_tree", "bulk_preview"}]
            # The feedback scenario deliberately types one invalid timestamp;
            # its query must reject that exact literal rather than coerce it.
            expected_invalid = [entry for entry in errors if entry["name"] == "query_project_rows"
                and entry["request"].get("projectId") == data["id"]
                and entry["request"].get("condition") == {"column": data["columns"]["timestamp"],
                    "op": "ge", "values": ["not-a-timestamp"], "negated": False}
                and entry["error"] == "Datetime comparison requires a recognized datetime: not-a-timestamp"]
            assert all(entry in cancelled or entry in expected_invalid for entry in errors), errors
            report["expected_invalid_timestamp_queries"] = len(expected_invalid)
            report["cancelled_superseded_queries"] = len(cancelled)
            report["passed"] = True
        except Exception as error:
            report["error"] = str(error)
            if app.web.session:
                try:
                    report["last_view"] = app.web.state()
                    app.web.screenshot(evidence / "failure.png")
                except Exception:
                    pass
            raise
        finally:
            try:
                app.close()
            finally:
                report["sessions"] = app.sessions
                report["transport_retries"] = app.web.transport_retries
                (evidence / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Evidence: {evidence / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
