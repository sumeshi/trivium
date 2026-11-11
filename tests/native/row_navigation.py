"""Exercise row navigation in the actual Tauri/WebKit application.

Build with embedded assets, then run under Xvfb, for example:
  xvfb-run -a python3 tests/native/row_navigation.py --binary target/release/trivium-desktop

Requires tauri-driver and WebKitWebDriver. Uses only Python's standard library.
Every input, project and annotation belongs to an isolated temporary profile.
The session starts through tauri-driver; subsequent commands go directly to its
owned WebKitWebDriver to avoid intermittent EOFs in tauri-driver's HTTP proxy.
"""
from __future__ import annotations

import argparse
import base64
import csv
from datetime import datetime, timezone
import json
from http.client import RemoteDisconnected
import math
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import uuid


ELEMENT = "element-6066-11e4-a52e-4f735466cecf"


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def wait_for(read: Callable[[], Any], ready: Callable[[Any], bool], label: str, timeout: float = 60) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = read()
        if ready(last):
            return last
        time.sleep(.05)
    raise AssertionError(f"Timed out: {label}; last observation: {json.dumps(last, ensure_ascii=False)}")


class WebView:
    def __init__(self, port: int):
        self.origin = f"http://127.0.0.1:{port}"
        self.session: str | None = None
        self.transport_retries: list[str] = []

    def request(self, method: str, path: str, body: Any = None) -> Any:
        encoded = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(self.origin + path, encoded, method=method,
                          headers={"Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=600) as response:
                value = json.load(response).get("value")
        except HTTPError as error:
            raise AssertionError(f"WebDriver {method} {path}: {error.read(65536).decode('utf-8', errors='replace')}") from error
        if isinstance(value, dict) and value.get("error"):
            raise AssertionError(f"WebDriver error: {value}")
        return value

    def route(self, suffix: str) -> str:
        assert self.session is not None
        return f"/session/{self.session}/{suffix}"

    def script(self, source: str, *args: Any) -> Any:
        return self.request("POST", self.route("execute/sync"), {"script": source, "args": list(args)})

    def async_script(self, source: str, *args: Any) -> Any:
        return self.request("POST", self.route("execute/async"), {"script": source, "args": list(args)})

    def command(self, name: str, payload: dict[str, Any] | None = None, *, request: bool = False) -> Any:
        args = {} if payload is None else {"request" if request else "payload": payload}
        value = self.async_script(
            "const done=arguments[arguments.length-1];"
            "window.__TAURI_INTERNALS__.invoke(arguments[0],arguments[1])"
            ".then(result=>done({ok:true,result}),error=>done({ok:false,error:String(error)}));",
            name, args,
        )
        assert value["ok"], (name, value)
        return value.get("result")

    def element(self, selector: str) -> dict[str, str]:
        return self.request("POST", self.route("element"), {"using": "css selector", "value": selector})

    def click(self, element: dict[str, str]) -> None:
        self.request("POST", self.route(f"element/{element[ELEMENT]}/click"), {})

    def input(self, selector: str, text: str, *, enter: bool = False) -> None:
        element = self.element(selector)
        self.request("POST", self.route(f"element/{element[ELEMENT]}/clear"), {})
        self.request("POST", self.route(f"element/{element[ELEMENT]}/value"),
                     {"text": text + ("\ue007" if enter else "")})

    def button(self, text: str, container: str = ".row-jump") -> None:
        element = wait_for(
            lambda: self.script(
                "return [...document.querySelectorAll(arguments[0]+' button')]"
                ".find(e=>e.textContent.trim()===arguments[1]&&!e.disabled&&e.getClientRects().length)??null;",
                container, text,
            ), bool, f"enabled {text} button",
        )
        self.click(element)

    def view(self) -> dict[str, Any]:
        for attempt in range(3):
            try:
                return self._view()
            except (OSError, RemoteDisconnected) as error:
                if attempt == 2:
                    raise
                self.transport_retries.append(str(error))
                time.sleep(.1)
        raise AssertionError("Unreachable")

    def _view(self) -> dict[str, Any]:
        return self.script(r"""
          const viewport=document.querySelector('.virtual-viewport');
          const count=document.querySelector('.table-meta-left strong');
          if(!viewport||!count)return {ready:false};
          const box=viewport.getBoundingClientRect();
          const top=box.top+viewport.clientTop,bottom=top+viewport.clientHeight;
          const rows=[...viewport.querySelectorAll('.data-row')].map(row=>{
            const r=row.getBoundingClientRect();
            const text=row.querySelector('.cell.index')?.textContent.trim()??'';
            const parsed=text.replaceAll(',','');
            return {id:/^\d+$/.test(parsed)?Number(parsed):null,top:r.top,bottom:r.bottom,height:r.height,
              source:row.querySelector('.cell.stickyData')?.textContent.trim()??'',
              flags:[...row.querySelectorAll('.flag-buttons button')].map(e=>e.getAttribute('aria-label')),
              memo:row.querySelector('.memo-button')?.textContent.trim()??'',
              critical:!!row.querySelector('.flag-critical.selected')};
          });
          const selected=[...document.querySelectorAll('.data-header button[aria-pressed=true]')]
            .map(e=>e.textContent.trim());
          return {ready:true,total:Number(count.textContent.replaceAll(',','')),top,bottom,
            viewportHeight:viewport.clientHeight,scrollTop:viewport.scrollTop,
            scrollHeight:viewport.scrollHeight,rows,selected,
            search:document.querySelector('input[type=search]')?.value??'',
            goInput:document.querySelector('.row-jump input')?.value??'',
            toast:document.querySelector('.toast[role=status],.toast[role=alert]')?.textContent.trim()??''};
        """)

    def result(self, count: int, target: int | None = None, stride: int = 1) -> dict[str, Any]:
        def good(view: dict[str, Any]) -> bool:
            if not view.get("ready") or view["total"] != count or view["viewportHeight"] <= 0:
                return False
            rows = view["rows"]
            if not rows or any(row["id"] is None for row in rows):
                return False
            visible = [r for r in rows if r["bottom"] > view["top"] + .5 and r["top"] < view["bottom"] - .5]
            if not visible:
                return False
            if target is not None and not any(
                r["id"] == target and r["top"] >= view["top"] - 1 and r["bottom"] <= view["bottom"] + 1
                for r in rows
            ):
                return False
            if visible[0]["top"] > view["top"] + 1:
                return False
            if count * 32 >= view["viewportHeight"] and visible[-1]["bottom"] < view["bottom"] - 1:
                return False
            return True

        view = wait_for(self.view, good, f"{count} results with original #{target} visible")
        ids = [row["id"] for row in view["rows"]]
        assert len(ids) == len(set(ids)), view
        assert all(right - left == stride for left, right in zip(ids, ids[1:])), view
        assert len(ids) <= math.ceil(view["viewportHeight"] / 32) + 64, view
        assert view["scrollHeight"] <= 8_000_002, view
        for index, row in enumerate(view["rows"]):
            assert abs(row["height"] - 32) <= 1, row
            assert row["source"] == f"r-{row['id']:07d}", row
            assert sorted(row["flags"]) == ["Critical", "Safe", "Suspicious"], row
            assert row["memo"], row
            if index:
                assert abs(row["top"] - view["rows"][index - 1]["bottom"]) <= 1, view
        return view

    def go(self, number: int | str, count: int, stride: int = 1, *, enter: bool = False) -> dict[str, Any]:
        self.input('.row-jump input[type="number"]', str(number), enter=enter)
        if not enter:
            self.button("Go")
        return self.result(count, int(number), stride)

    def instrument(self) -> None:
        assert self.script("""
          window.__navigationErrors=[];window.__navigationIpc=[];
          addEventListener('error',e=>window.__navigationErrors.push(e.message));
          addEventListener('unhandledrejection',e=>window.__navigationErrors.push(String(e.reason)));
          const original=window.fetch;
          const wrapped=(input,options,...rest)=>{
            const name=typeof input==='string'&&input.startsWith('ipc://localhost/')
              ?decodeURIComponent(new URL(input).pathname.slice(1)):'';
            const promise=original.call(window,input,options,...rest);
            if(!['query_project_rows','locate_project_row','create_project','load_project','export_project'].includes(name))
              return promise;
            let args={};
            try{args=JSON.parse(typeof options.body==='string'?options.body:new TextDecoder().decode(options.body));}catch{}
            const payload=args?.payload??args?.request??{};
            const entry={name,started:performance.now(),request:Object.fromEntries(
              ['projectId','search','sortKey','sortDirection','offset','limit','rowIndex','scope']
              .filter(key=>key in payload).map(key=>[key,payload[key]]))};
            window.__navigationIpc.push(entry);
            if(window.__navigationIpc.length>300)window.__navigationIpc.shift();
            return promise.then(async response=>{
              const result=await response.clone().json();
              entry.elapsed_ms=performance.now()-entry.started;
              if(response.headers.get('Tauri-Response')==='ok'){
                entry.result={total_rows:result?.total_rows,total_filtered_rows:result?.total_filtered_rows,
                  position:result?.position,rows:Array.isArray(result?.rows)?result.rows.length:result?.rows};
              }else entry.error=String(result);
              if(name==='locate_project_row'&&window.__navigationHold){
                return new Promise(resolve=>{
                  window.__navigationHold.ready=true;
                  window.__navigationHold.release=()=>resolve(response);
                });
              }
              return response;
            },error=>{entry.error=String(error);throw error;});
          };
          window.fetch=wrapped;
          return window.fetch===wrapped;
        """), "Native IPC fetch interception must be writable"

    def diagnostics(self) -> dict[str, Any]:
        return self.script("return {js_errors:window.__navigationErrors??[],ipc:window.__navigationIpc??[]};")

    def screenshot(self, path: Path) -> None:
        path.write_bytes(base64.b64decode(self.request("GET", self.route("screenshot"))))


def open_project(web: WebView, name: str) -> None:
    if not web.script("return !!document.querySelector('.workspace-drawer');"):
        web.click(web.element('[aria-label="Open project menu"]'))
    web.click(web.element('[aria-label="Refresh projects"]'))
    card = wait_for(lambda: web.script(
        "return [...document.querySelectorAll('.project-card')]"
        ".find(e=>e.querySelector('.project-name')?.textContent.trim()===arguments[0])??null;", name,
    ), bool, f"project {name}")
    web.click(card)
    wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "project menu closes")


def rejection(web: WebView, number: str, expected_count: int, stride: int) -> None:
    wait_for(lambda: web.view().get("toast", ""), lambda value: not value, "previous notification clears")
    before = web.result(expected_count, stride=stride)
    web.input('.row-jump input[type="number"]', number)
    web.button("Go")
    notice = wait_for(lambda: web.view().get("toast", ""), bool, "rejected jump is explained")
    after = web.result(expected_count, stride=stride)
    assert after["search"] == before["search"] and after["selected"] == before["selected"], (before, after)
    assert abs(after["scrollTop"] - before["scrollTop"]) <= 1, (number, notice, before, after)
    assert [r["id"] for r in after["rows"]] == [r["id"] for r in before["rows"]], (before, after)


def stale_lookup(web: WebView, query: str, target: int) -> None:
    """Delay a real native response; a newer visible query must win."""
    web.script("""
      window.__navigationHold={ready:false,release:null};
    """)
    try:
        web.input('.row-jump input[type="number"]', str(target))
        web.button("Go")
        wait_for(lambda: web.script("return window.__navigationHold.ready;"), bool, "held native lookup response")
        web.input('input[placeholder="Search rows"]', query)
        web.result(1, 1)
        web.script("window.__navigationHold.release();delete window.__navigationHold;")
        web.async_script("const done=arguments[arguments.length-1];requestAnimationFrame(()=>requestAnimationFrame(()=>done(true)));")
        after = web.result(1, 1)
        assert after["search"] == query, after
    finally:
        web.script("if(window.__navigationHold){window.__navigationHold.release?.();delete window.__navigationHold;}")


def scenario(web: WebView, directory: Path, total: int, progress: dict[str, Any], evidence: Path) -> dict[str, Any]:
    def stage(label: str) -> None:
        progress["stage"] = label
        print(f"CHECK: {total:,} rows: {label}", flush=True)

    stage("generate fixture")
    source = directory / f"navigation-{total}-{uuid.uuid4().hex[:8]}.csv"
    with source.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["source_id", "score", "cohort"])
        writer.writerows((f"r-{i:07d}", i, "keep" if i % 2 == 0 else "drop") for i in range(1, total + 1))
    progress["input_bytes"] = source.stat().st_size
    stage("native import")
    started = time.monotonic()
    summary = web.command("create_project", {"path": str(source)})
    assert web.script("return window.__navigationIpc.some(entry=>entry.name==='create_project'&&entry.result);"), "Native IPC trace must record the actual import"
    project_id = summary["meta"]["id"]
    assert summary["meta"]["total_records"] == total, summary
    imported_seconds = round(time.monotonic() - started, 3)
    middle = (total + 1) // 2
    memo = "Native navigation preserves this original row."
    web.command("update_flag", {"projectId": project_id, "row_index": middle - 1, "flag": "critical", "memo": memo})
    detail = web.command("load_project", {"projectId": project_id}, request=True)
    score = next(column["id"] for column in detail["columns_meta"] if column["label"] == "score")
    observations: list[dict[str, Any]] = []
    progress.update(import_seconds=imported_seconds, navigation=observations)
    stage("open project")
    open_project(web, summary["meta"]["name"])
    web.result(total, 1)
    for target in [1, middle, total]:
        stage(f"Go original #{target:,}")
        before = time.monotonic()
        view = web.go(target, total)
        observations.append({"action": "Go", "original_row": target, "seconds": round(time.monotonic() - before, 3),
                             "scroll_top": view["scrollTop"], "viewport_height": view["viewportHeight"],
                             "rendered_rows": len(view["rows"])})
        if total == 2_000_000 and target in [middle, total]:
            web.screenshot(evidence / f"rows-{total}-go-{target}.png")
        if target == middle:
            marked = next(row for row in view["rows"] if row["id"] == middle)
            assert marked["critical"] and memo in marked["memo"], marked
    stage("First / Last unfiltered")
    web.button("First")
    web.result(total, 1)
    web.button("Last")
    web.result(total, total)

    stage("search keep")
    web.input('input[placeholder="Search rows"]', "keep")
    filtered = total // 2
    web.result(filtered, stride=2)
    web.button("First")
    web.result(filtered, 2, 2)
    header = web.script("return [...document.querySelectorAll('.data-header button')].find(e=>e.querySelector('.header-label')?.textContent.trim()==='score');")
    assert header, "score column header not found"
    stage("numeric score sort ascending")
    web.click(header)
    web.result(filtered, 2, 2)
    stage("numeric score sort descending")
    web.click(header)
    last_even = total - total % 2
    web.result(filtered, last_even, -2)
    original = (total * 3 // 4) // 2 * 2
    assert original > filtered
    stage(f"filtered descending Go original #{original:,}")
    web.go(original, filtered, -2)
    web.button("First")
    web.result(filtered, last_even, -2)
    web.button("Last")
    web.result(filtered, 2, -2)
    stage("rejected jumps preserve query and viewport")
    rejection(web, "1", filtered, -2)
    if total == 5199:
        rejection(web, str(total + 1), filtered, -2)
        rejection(web, "1.5", filtered, -2)
        web.go(original, filtered, -2, enter=True)

    query = {"search": "keep", "searchColumns": [], "condition": None, "flagFilter": "all",
             "bookmarkedOnly": False, "sortKey": score, "sortDirection": "desc"}
    located = web.command("locate_project_row", {"projectId": project_id, "rowIndex": original - 1, **query})
    assert located["position"] == (last_even - original) // 2, located
    if total == 5199:
        stage("annotation / selected export compatibility")
        exported = directory / "selected-navigation.csv"
        result = web.command("export_project", {"projectId": project_id, **query, "destination": str(exported),
                             "scope": "selected", "rowIndices": [middle - 1], "includeAnnotations": True})
        assert result["rows"] == 1, result
        with exported.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == 1 and rows[0]["source_id"] == f"r-{middle:07d}", rows
        assert rows[0]["trivium-critical"] == "1" and rows[0]["trivium-memo"] == memo, rows
        stage("stale real lookup response")
        stale_lookup(web, "r-0000001", original)
    errors = web.script("return window.__navigationErrors??[];")
    assert not errors, errors
    progress.update(filtered_results=filtered, filtered_go_original_row=original, passed=True, stage="complete")
    return progress


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--driver", default=shutil.which("tauri-driver") or "/workspace/.trivium-tools/bin/tauri-driver")
    parser.add_argument("--native-driver", default=shutil.which("WebKitWebDriver") or "/usr/bin/WebKitWebDriver")
    parser.add_argument("--rows", nargs="+", type=int, default=[5199, 2_000_000])
    parser.add_argument("--output", type=Path, default=Path("target/native-row-navigation"))
    args = parser.parse_args()
    if any(rows < 100 or rows > 2_000_000 for rows in args.rows):
        parser.error("--rows must be between 100 and 2000000; this is a bounded navigation check")
    binary = args.binary.resolve(strict=True)
    evidence = args.output.resolve() / uuid.uuid4().hex[:12]
    evidence.mkdir(parents=True)
    report: dict[str, Any] = {"started_utc": datetime.now(timezone.utc).isoformat(), "binary": str(binary), "results": []}
    with tempfile.TemporaryDirectory(prefix="trivium-native-navigation-") as temporary:
        root = Path(temporary)
        environment = dict(os.environ, XDG_DATA_HOME=str(root / "data"), XDG_CONFIG_HOME=str(root / "config"),
                           XDG_CACHE_HOME=str(root / "cache"), XDG_RUNTIME_DIR=str(root),
                           GSETTINGS_BACKEND="memory", NO_AT_BRIDGE="1")
        port = free_port()
        native_port = free_port()
        while native_port == port:
            native_port = free_port()
        web = WebView(port)
        with (evidence / "driver.log").open("w") as log:
            driver = subprocess.Popen([args.driver, "--port", str(port), "--native-port", str(native_port),
                                       "--native-driver", args.native_driver], env=environment,
                                      stdout=log, stderr=log, start_new_session=True)
            try:
                def ready() -> bool:
                    if driver.poll() is not None:
                        raise AssertionError(f"tauri-driver exited; inspect {evidence / 'driver.log'}")
                    try:
                        web.request("GET", "/status")
                        return True
                    except (OSError, URLError):
                        return False
                wait_for(ready, bool, "tauri-driver starts", 20)
                session = web.request("POST", "/session", {"capabilities": {"alwaysMatch": {
                    "tauri:options": {"application": str(binary)}}}})
                web.session = session["sessionId"]
                web.origin = f"http://127.0.0.1:{native_port}"
                report["webdriver_transport"] = "tauri-driver session, direct owned WebKitWebDriver commands"
                web.request("POST", web.route("timeouts"), {"script": 600000, "pageLoad": 60000, "implicit": 0})
                web.request("POST", web.route("window/rect"), {"width": 1280, "height": 827})
                wait_for(lambda: web.script("return typeof window.__TAURI_INTERNALS__?.invoke==='function';"), bool, "Tauri IPC ready")
                assert ":5173" not in web.script("return location.href;"), "Use an embedded custom-protocol build"
                web.instrument()
                for rows in args.rows:
                    result = {"rows": rows, "passed": False}
                    report["results"].append(result)
                    scenario(web, root, rows, result, evidence)
                    print(f"PASS: real Tauri/WebKit {rows:,} rows; source-number Go, First/Last, filtered descending scope and geometry", flush=True)
                report["passed"] = True
            except Exception as error:
                report["passed"] = False
                report["error"] = str(error)
                if web.session:
                    try:
                        report["last_view"] = web.view()
                        web.screenshot(evidence / "failure.png")
                    except Exception:
                        pass
                raise
            finally:
                if web.session:
                    try:
                        report.update(web.diagnostics())
                    except Exception:
                        pass
                report["transport_retries"] = web.transport_retries
                (evidence / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
                if web.session:
                    try:
                        web.request("DELETE", f"/session/{web.session}")
                    except Exception:
                        pass
                if driver.poll() is None:
                    os.killpg(driver.pid, signal.SIGTERM)
                    try:
                        driver.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(driver.pid, signal.SIGKILL)
                        driver.wait(timeout=5)
    print(f"Evidence: {evidence / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
