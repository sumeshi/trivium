"""Real WebView flag reconciliation with controlled rejected IPC writes.

Successful writes and persisted readback use the actual Rust backend. Only
the selected failed writes return the native IPC error response shape.
"""
from pathlib import Path
import uuid

from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS


def verify_annotation_failures(web, data, root: Path, report: dict) -> None:
    outer_query, outer_total = web.query(), web.state()["total"]
    outcomes = []
    try:
        for first, second in [(True, True), (False, True), (True, False), (False, False)]:
            source = root / f"flag-failures-{uuid.uuid4().hex[:8]}.csv"
            source.write_text("source_id,event\nr-0000000,synthetic\n", encoding="utf-8")
            case = web.command("create_project", {"path": str(source)})
            owner = case["meta"]["id"]
            open_project(web, case["meta"]["name"])
            web.results(1)
            wait_for(lambda: web.script("return !document.querySelector('.toast.error');"), bool, "previous error notification clears")
            web.script("""
              window.__flagFailureFetch=window.fetch;
              window.__flagFailure={results:arguments[0],calls:0,done:0,release:null,errorVisible:false};
              const observed=window.__flagFailure;
              observed.observer=new MutationObserver(()=>{
                if([...document.querySelectorAll('.toast.error')].some(e=>/flag|annotation/i.test(e.textContent)))
                  observed.errorVisible=true;
              });
              observed.observer.observe(document.body,{subtree:true,childList:true,characterData:true,attributes:true});
              window.fetch=(input,options,...rest)=>{
            """ + IPC_COMMAND_JS + """
                if(name!=='update_flag')return window.__flagFailureFetch(input,options,...rest);
                const state=window.__flagFailure,index=state.calls++;
                const perform=()=>{
                  const request=state.results[index]
                    ?window.__flagFailureFetch(input,options,...rest)
                    :Promise.resolve(new Response(JSON.stringify('Synthetic annotation write failure'),
                      {headers:{'Content-Type':'application/json','Tauri-Response':'error'}}));
                  return request.finally(()=>state.done++);
                };
                return index===0?new Promise((resolve,reject)=>{
                  state.release=()=>perform().then(resolve,reject);
                }):perform();
              };
            """, [first, second])
            try:
                web.named("Safe", ".data-row")
                wait_for(lambda: web.script("return !!window.__flagFailure.release;"), bool, "first flag write is pending")
                web.named("Critical", ".data-row")
                assert web.script("return document.querySelector('.data-row .flag-critical').classList.contains('selected');")
                web.script("window.__flagFailure.release();")
                expected = "critical" if second else "safe" if first else ""
                expected_count = int(bool(expected))
                wait_for(lambda: web.script("""
                  return {done:window.__flagFailure.done,
                    flag:document.querySelector('.data-row .flag-buttons .selected')?.getAttribute('aria-label')?.toLowerCase()??'',
                    count:document.querySelector('.table-meta-right')?.textContent.trim()};
                """), lambda value: value == {"done": 2, "flag": expected, "count": f"Flagged {expected_count}"},
                    "settled flags and counts use committed writes")
                persisted = web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 1})
                assert (persisted["rows"][0]["flag"] or "") == expected, persisted
                assert persisted["annotated_total"] == persisted["annotated_filtered"] == expected_count, persisted
                if not (first and second):
                    wait_for(lambda: web.script("return window.__flagFailure.errorVisible;"), bool, "write failure is shown to the user")
                outcomes.append({"first_success": first, "second_success": second,
                                 "persisted_flag": expected, "flagged": expected_count})
            finally:
                web.script("window.__flagFailure.observer.disconnect();window.fetch=window.__flagFailureFetch;delete window.__flagFailureFetch;delete window.__flagFailure;")
                open_project(web, data["name"])
                web.results(outer_total)
                web.named(f"Close {source.name}")
                delete_fixture_project(web, owner)
        report["annotation_failures"] = {"outcomes": outcomes, "failure_mode": "injected IPC error response in real WebView"}
    finally:
        web.close_drawer()
        open_project(web, data["name"])
        web.applied(**outer_query)
        web.results(outer_total)
