"""Native embedded-app rendering benchmark; immutable binaries, synthetic data."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from workspace_ui import NativeApp
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS, native_driver_default


INSTRUMENT = r"""
window.__workspaceErrors=[];window.__workspaceIpc=[];window.__TRIVIUM_PERF__={pages:[]};
addEventListener('error',e=>window.__workspaceErrors.push(e.message));
addEventListener('unhandledrejection',e=>window.__workspaceErrors.push(String(e.reason)));
const p=window.__renderBenchmark={active:false,frames:[],handlers:[],flushes:[],mutations:0,added:0,removed:0,attributes:0,text:0,childLists:0,queries:[]};
const fetch=window.fetch;
const wrapped=(input,options,...rest)=>{
 __IPC_COMMAND__
 if(!name||name==='active_jobs'||name.startsWith('plugin:'))return fetch.call(window,input,options,...rest);
 let args={};try{args=JSON.parse(typeof options.body==='string'?options.body:new TextDecoder().decode(options.body));}catch{}
 const entry={name,request:args?.payload??args?.request??args??{},started:performance.now()};
 window.__workspaceIpc.push(entry);if(p.active&&name==='query_project_rows')p.queries.push(entry);
 return fetch.call(window,input,options,...rest).then(response=>{
  entry.fetch_received=performance.now();entry.roundtrip_ms=entry.fetch_received-entry.started;
  const json=response.json.bind(response);
  response.json=function(...args){
   const decode=performance.now();
   return json(...args).then(value=>{
    entry.native_resolved=performance.now();entry.json_decode_ms=entry.native_resolved-decode;entry.elapsed_ms=entry.native_resolved-entry.started;
    if(response.headers.get('Tauri-Response')==='ok')entry.result={rows:value?.rows?.length,total_rows:value?.total_rows,total_filtered_rows:value?.total_filtered_rows};
    else entry.error=String(value);
    if(name==='query_project_rows'){
     entry.columns=value?.rows?.[0]?.cells?Object.keys(value.rows[0].cells).length:0;
     requestAnimationFrame(()=>requestAnimationFrame(()=>{entry.next_paint_proxy_ms=performance.now()-entry.native_resolved;}));
    }
    return value;
   });
  };
  return response;
 },error=>{entry.error=String(error);throw error;});
};window.fetch=wrapped;
const add=EventTarget.prototype.addEventListener,remove=EventTarget.prototype.removeEventListener,wrappers=new WeakMap();
EventTarget.prototype.addEventListener=function(type,callback,options){
 if(type==='scroll'&&typeof callback==='function'){
  let wrapper=wrappers.get(callback);
  if(!wrapper){wrapper=function(event){const start=performance.now();try{return callback.call(this,event);}finally{
   if(p.active&&this.classList?.contains('virtual-viewport')){p.handlers.push(performance.now()-start);queueMicrotask(()=>{if(p.active)p.flushes.push(performance.now()-start);});}
  }};wrappers.set(callback,wrapper);}
  return add.call(this,type,wrapper,options);
 }return add.call(this,type,callback,options);
};
EventTarget.prototype.removeEventListener=function(type,callback,options){return remove.call(this,type,type==='scroll'?(wrappers.get(callback)??callback):callback,options);};
let previous=0;const frame=t=>{if(p.active&&previous)p.frames.push(t-previous);previous=p.active?t:0;requestAnimationFrame(frame);};requestAnimationFrame(frame);
p.longTasksSupported=PerformanceObserver.supportedEntryTypes?.includes('longtask')??false;
return window.fetch===wrapped;
""".replace("__IPC_COMMAND__", IPC_COMMAND_JS)

START = r"""
const p=window.__renderBenchmark;
p.active=false;p.frames=[];p.handlers=[];p.flushes=[];p.queries=[];p.mutations=p.added=p.removed=p.attributes=p.text=p.childLists=0;
p.observer?.disconnect();
p.observer=new MutationObserver(records=>{if(!p.active)return;for(const r of records){p.mutations++;p.added+=r.addedNodes.length;p.removed+=r.removedNodes.length;if(r.type==='attributes')p.attributes++;else if(r.type==='characterData')p.text++;else p.childLists++;}});
p.observer.observe(document.querySelector('.virtual-inner'),{attributes:true,characterData:true,childList:true,subtree:true});
p.active=true;p.started=performance.now();return p.started;
"""

END = r"""
const p=window.__renderBenchmark;p.active=false;
const stats=values=>{const a=values.slice().sort((a,b)=>a-b),q=f=>a[Math.min(a.length-1,Math.floor(a.length*f))]??0;return {count:a.length,p50:q(.5),p95:q(.95),p99:q(.99),max:a.at(-1)??0,over25:a.filter(v=>v>25).length,over50:a.filter(v=>v>50).length};};
return {started:p.started,finished:performance.now(),frames:stats(p.frames),scroll_handler_ms:stats(p.handlers),scroll_event_microtask_ms:stats(p.flushes),mutations:p.mutations,added:p.added,removed:p.removed,
 mutation_types:{attributes:p.attributes,text:p.text,child_lists:p.childLists},
 queries:p.queries,dom_rows:document.querySelectorAll('.data-row').length,data_cells:document.querySelectorAll('.data-row button[data-column-id]').length,
 header_columns:document.querySelectorAll('.data-header .column-heading').length,all_cells:document.querySelectorAll('.data-row .cell').length,
 long_tasks_api:p.longTasksSupported,viewport:{width:document.querySelector('.virtual-viewport').clientWidth,height:document.querySelector('.virtual-viewport').clientHeight},
 perf:window.__TRIVIUM_PERF__??null};
"""


def settle(web):
    web.async_script("const done=arguments[arguments.length-1];requestAnimationFrame(()=>requestAnimationFrame(()=>done(true)));")


def idle_queries(web):
    wait_for(lambda: web.script("return window.__workspaceIpc.filter(e=>e.name==='query_project_rows'&&!('elapsed_ms' in e)&&!e.error).length;"),
        lambda count: count == 0, "native pages finish before a cache-warm render sweep")
    settle(web)


def make_fixture(path: Path, rows: int, columns: int) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    names = ["source_id", "timestamp", "score", "host", "path"] + [f"col_{i:03d}" for i in range(6, columns + 1)]
    temporary = path.with_suffix(".creating")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        out = csv.writer(stream)
        out.writerow(names)
        for row in range(1, rows + 1):
            values = [f"r-{row:07d}", "2026-11-14T00:00:00.123456789Z", row % 997, f"host-{row % 67:02d}", f"/evidence/group-{row % 9:02d}/file-{row:07d}"]
            values.extend(f"evidence-c{i:03d}-r{row:07d}" for i in range(6, columns + 1))
            out.writerow(values)
    temporary.replace(path)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def run_case(web, case, total, columns, evidence, repeats, steps, report):
    open_project(web, case["meta"]["name"])
    web.results(total)
    settle(web)
    project_id = case["meta"]["id"]
    for row, flag in ((5, "safe"), (9, "suspicious"), (14, "critical")):
        web.command("update_flag", {"projectId": project_id, "row_index": row - 1, "flag": flag})
    scenario = {"rows": total, "columns": columns, "fixture_sha256": case["meta"].get("source_sha256"), "measurements": [], "navigation": []}
    report["cases"].append(scenario)
    web.screenshot(evidence / f"{total}x{columns}-initial.png")
    # The capped 2M-row spacer advances about8 logical rows per32px input.
    # Populate actual frontend pages0..3; direct engine reads do not do this.
    warm = min(600, total)
    web.input('.row-jump input[type="number"]', str(warm))
    web.button("Go")
    wait_for(web.view, lambda state: any(row["id"] == warm for row in state.get("rows", [])), "frontend cache warm-up reaches row600")
    web.results(total)
    idle_queries(web)
    for axis in ("vertical", "horizontal"):
        for repeat in range(repeats):
            web.button("First")
            web.results(total)
            web.script("const e=document.querySelector('.virtual-viewport');e.scrollLeft=0;")
            idle_queries(web)
            web.script(START)
            started = time.monotonic()
            observed_range = web.async_script(r"""
             const axis=arguments[0],steps=arguments[1],done=arguments[arguments.length-1],e=document.querySelector('.virtual-viewport');
             const maximum=e.scrollWidth-e.clientWidth;let index=0;
             const range={physical_max:0,horizontal_max:0,minimum_original:Infinity,maximum_original:0,input_overrides:[]};let expected=null;
             const check=()=>{const actual=axis==='vertical'?e.scrollTop:e.scrollLeft;if(expected!==null&&Math.abs(actual-expected)>1&&range.input_overrides.length<8)range.input_overrides.push({step:index,expected,actual});};
             const sample=()=>{const h=e.querySelector('.virtual-inner'),first=Number(h?.firstElementChild?.querySelector('.cell.index')?.textContent),last=Number(h?.lastElementChild?.querySelector('.cell.index')?.textContent);
              range.physical_max=Math.max(range.physical_max,e.scrollTop);range.horizontal_max=Math.max(range.horizontal_max,e.scrollLeft);if(Number.isFinite(first))range.minimum_original=Math.min(range.minimum_original,first);if(Number.isFinite(last))range.maximum_original=Math.max(range.maximum_original,last);};
             const run=()=>{check();if(axis==='vertical')e.scrollTop+=(index<steps/2?1:-1)*32;
              else e.scrollLeft=maximum*(index<steps/2?(index+1)/(steps/2):Math.max(0,(steps-index-1)/(steps/2)));
              expected=axis==='vertical'?e.scrollTop:e.scrollLeft;sample();if(++index<steps)requestAnimationFrame(run);else requestAnimationFrame(()=>requestAnimationFrame(()=>{check();sample();done(range);}));};
             requestAnimationFrame(run);
            """, axis, steps)
            settle(web)
            result = web.script(END)
            result.update(axis=axis, repeat=repeat, wall_seconds=time.monotonic() - started, observed_range=observed_range,
                input="rAF-driven native scroll offsets; not trusted-wheel input", steps=steps)
            scenario["measurements"].append(result)
            assert not result["queries"], "A cache-warm sweep unexpectedly requested native pages"
            assert not observed_range["input_overrides"], f"The app overwrote the requested {axis} trajectory: {observed_range['input_overrides']}"
            print(json.dumps({"rows": total, "columns": columns, **{k: v for k, v in result.items() if k not in {"queries", "perf"}}}), flush=True)
            (evidence / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    for fraction in (.25, .5, .75, 1, .25):
        target = max(1, round(total * fraction))
        before = len(web.trace())
        web.script(START)
        web.input('.row-jump input[type="number"]', str(target))
        started = time.monotonic()
        web.button("Go")
        wait_for(web.view, lambda state: any(row["id"] == target and row["top"] >= state["top"] - 1 and row["bottom"] <= state["bottom"] + 1 for row in state.get("rows", [])), f"original row{target} is fully visible")
        web.results(total)
        settle(web)
        result = web.script(END)
        queries = [entry for entry in web.trace()[before:] if entry["name"] == "query_project_rows"]
        nav = {"target": target, "click_to_visible_proxy_ms": (time.monotonic() - started) * 1000,
            "queries": queries, "dom_rows": result["dom_rows"], "data_cells": result["data_cells"], "perf": result["perf"]}
        scenario["navigation"].append(nav)
    web.screenshot(evidence / f"{total}x{columns}-navigated.png")
    (evidence / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"CASE COMPLETE {total}x{columns}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--cases", default="5199x27,2000000x7,100000x120", help="Comma-separated rowsxcolumns fixtures")
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--steps", type=int, default=120)
    parser.add_argument("--profile", help="Explicit build label for a binary copied outside target/debug or target/release")
    parser.add_argument("--fixtures", type=Path, help="Reuse synthetic CSVs here; default: generate in the temporary isolated profile")
    parser.add_argument("--driver", default=shutil.which("tauri-driver") or "tauri-driver")
    parser.add_argument("--native-driver", default=native_driver_default())
    parser.add_argument("--width", type=int, default=1280)
    parser.add_argument("--height", type=int, default=827)
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    if args.steps < 2 or args.steps % 2 or args.repeats < 0:
        parser.error("--steps must be a positive even count; --repeats must be nonnegative")
    args.profile = args.profile or f"{args.binary.parent.name} Rust / embedded production frontend"
    evidence = args.out.resolve() / uuid.uuid4().hex[:12]
    evidence.mkdir(parents=True)
    report = {"started_utc": datetime.now(timezone.utc).isoformat(), "binary": str(args.binary.resolve()),
        "binary_sha256": digest(args.binary), "build": args.profile,
        "method": "Isolated embedded native app, same synthetic fixtures per comparison. In-place fetch/JSON observation never clones or parses a response twice. rAF-driven native scroll offsets measure frame cadence, not trusted-wheel latency. Two-rAF completion is a presentation proxy, not a GPU fence. The 8Mpx spacer cap maps a32px physical step to about8 logical rows at2M rows; compare identical input within each fixture, not between row counts.",
        "cases": [], "passed": False}
    with tempfile.TemporaryDirectory(prefix="trivium-render-benchmark-") as tmp:
        root = Path(tmp)
        fixtures = args.fixtures.resolve() if args.fixtures else root / "fixtures"
        app = NativeApp(args, root, evidence)
        app.web.instrument = lambda: app.web.script(INSTRUMENT)
        try:
            web = app.start()
            report["application_build"] = web.script("return document.querySelector('.build-identity')?.textContent??null;")
            web.request("POST", web.route("window/rect"), {"width": args.width, "height": args.height})
            report["viewport"] = web.script("return {width:innerWidth,height:innerHeight,device_scale:devicePixelRatio};")
            for item in args.cases.split(","):
                rows, columns = map(int, item.split("x"))
                source = fixtures / f"render-{rows}x{columns}.csv"
                make_fixture(source, rows, columns)
                started = time.monotonic()
                case = web.command("create_project", {"path": str(source)})
                assert case["meta"]["total_records"] == rows, (source.name, case["meta"]["total_records"], rows)
                print(f"IMPORTED {rows}x{columns} in {time.monotonic()-started:.3f}s", flush=True)
                run_case(web, case, rows, columns, evidence, args.repeats, args.steps, report)
                web.close_drawer()
                web.named(f"Close {source.name}")
                web.command("delete_project", {"projectId": case["meta"]["id"]}, request=True)
            assert not web.diagnostics()["js_errors"], web.diagnostics()["js_errors"]
            report["passed"] = True
        except Exception as error:
            report["error"] = str(error)
            raise
        finally:
            if app.web.session:
                report["diagnostics"] = app.web.diagnostics()
            app.close()
            (evidence / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"REPORT {evidence/'report.json'}", flush=True)


if __name__ == "__main__":
    main()
