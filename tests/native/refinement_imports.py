"""Import-form regression with only the native OS picker result substituted.

The chosen path is returned through the actual dialog IPC response. Project
creation, import, metadata, rows and cleanup use the real native engine.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import uuid

from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS


def _pick(web, result: str | list[str] | None) -> None:
    web.script("window.__refinementDialog.path=arguments[0];window.__refinementDialog.ready=true;", result)
    web.click(web.node(".workspace-drawer .choose-file"))


def verify_import_identity(web, data, root: Path, report: dict) -> None:
    detail = web.command("load_project", {"projectId": data["id"]}, request=True)
    title = web.script("return document.querySelector('.project-title').title;")
    assert title == detail["project"]["meta"]["source_path"] and data["filename"] in title, title
    web.named("Open project menu")
    recent = web.script("return [...document.querySelectorAll('.project-card')].find(e=>e.querySelector('.project-name')?.textContent.trim()===arguments[0])?.querySelector('.project-hash');", data["name"])
    label = web.script("return {text:arguments[0].textContent.trim(),title:arguments[0].title};", recent)
    assert data["sha"] in label["title"] and not label["text"].lower().startswith("sha"), label
    assert data["sha"].startswith(label["text"].rstrip("…")), label
    assert web.script("return document.querySelector('.choose-file').textContent.trim();") == "Choose files"
    assert not web.script("return !!document.querySelector('.import-heading,#import-formats');"), "Import uses one file chooser"
    assert not web.script("return [...document.querySelectorAll('.import-help')].some(e=>e.textContent.includes('read-only'));")
    assert web.script("return document.querySelector('.import-preview').compareDocumentPosition(document.querySelector('.import-options')) & Node.DOCUMENT_POSITION_FOLLOWING;")
    assert not web.script("return !!document.querySelector('[aria-label=\"CSV delimiter\"]');"), "CSV controls stay hidden until CSV is selected"
    web.named("Close workspace menu", ".workspace-drawer")
    wait_for(lambda: web.script("return !document.querySelector('.drawer-backdrop');"), bool, "initial project menu closes")

    assert web.script("""
      const previous=window.fetch;
      window.__refinementDialog={path:null,ready:false,count:0,multiple:[],restore:()=>{window.fetch=previous;}};
      window.fetch=function(input,options,...rest){
    """ + IPC_COMMAND_JS + """
        if(name==='plugin:dialog|open'&&window.__refinementDialog.ready){
          let args={};
          try{args=JSON.parse(typeof options.body==='string'?options.body:new TextDecoder().decode(options.body));}catch{}
          window.__refinementDialog.multiple.push(args.options?.multiple===true);
          const path=window.__refinementDialog.path;window.__refinementDialog.ready=false;window.__refinementDialog.count++;
          return Promise.resolve(new Response(JSON.stringify(path),{headers:{'Tauri-Response':'ok','Content-Type':'application/json'}}));
        }
        return previous.call(window,input,options,...rest);
      };return true;
    """), "Substitute only the OS picker response"
    created: list[tuple[str, Path]] = []
    source_paths: list[Path] = []
    checks = []
    cancelled_selection = False
    batch_check = {}
    sources = [
        ("csv", "host\tmessage\nx\talpha\ny\tbeta\n"),
        ("json", json.dumps([{"host": "x", "message": "alpha"}, {"host": "y", "message": "beta"}])),
    ]
    try:
        for extension, text in sources:
            source = root / f"refinement-import-{uuid.uuid4().hex[:8]}.{extension}"
            source_paths.append(source)
            source.write_text(text, encoding="utf-8")
            before = source.read_bytes()
            web.named("Open project menu")
            _pick(web, str(source))
            wait_for(lambda: web.script("return document.querySelector('.pending-file')?.textContent.trim();"), lambda value: value == source.name, "native picker path populates the import form")
            controls = web.script("return !!document.querySelector('[aria-label=\"CSV delimiter\"]');")
            assert controls == (extension == "csv"), (extension, controls)
            if controls:
                web.choose('select[aria-label="CSV delimiter"]', "\t")
                # The drawer can still be moving; target its full checkbox label.
                web.tap(web.node('.restore-annotations'))
                wait_for(lambda: web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').checked;"),
                         bool, "annotation restoration is selected before cancelling the picker")
                trace_start = web.checkpoint()
                before_count = web.script("return window.__refinementDialog.count;")
                _pick(web, None)
                wait_for(lambda: web.script("return window.__refinementDialog.count;"),
                         lambda count: count == before_count + 1, "native picker cancellation returns")
                pending = web.script("""
                  return {name:document.querySelector('.pending-file').textContent.trim(),
                    delimiter:document.querySelector('[aria-label="CSV delimiter"]').value,
                    restore:document.querySelector('[aria-label="Restore Trivium annotations"]').checked};
                """)
                assert pending == {"name": source.name, "delimiter": "\t", "restore": True}, pending
                assert not any(entry["name"] == "create_project" for entry in web.trace(after=trace_start))
                cancelled_selection = True
            web.named("Import and open", ".workspace-drawer")
            wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "real native import opens the saved case")
            web.results(2)
            projects = web.command("list_projects")
            meta = next(item["meta"] for item in projects if item["meta"].get("source_file_name") == source.name)
            created.append((meta["id"], source))
            web.applied(projectId=meta["id"])
            loaded = web.command("load_project", {"projectId": meta["id"]}, request=True)
            assert [column["label"] for column in loaded["columns_meta"]] == ["host", "message"], loaded["columns_meta"]
            assert meta["source_sha256"] == hashlib.sha256(before).hexdigest() and source.read_bytes() == before
            assert web.script("return document.querySelector('.project-title').title;") == meta["source_path"]
            checks.append({"format": extension, "csv_controls_visible": controls, "real_import_rows": 2, "source_unchanged": True})
            web.tab(data["id"])
            web.results(data["total"])
            web.named(f"Close {source.name}")
            delete_fixture_project(web, meta["id"])

        token = uuid.uuid4().hex[:8]
        batch_csv = root / f"refinement-batch-{token}.csv"
        batch_bad = root / f"refinement-batch-{token}-broken.json"
        batch_ndjson = root / f"refinement-batch-{token}.ndjson"
        batch_sources = [batch_csv, batch_bad, batch_ndjson]
        source_paths.extend(batch_sources)
        batch_csv.write_text("host\tmessage\nx\talpha\ny\tbeta\n", encoding="utf-8")
        batch_bad.write_text('{"host":}', encoding="utf-8")
        batch_ndjson.write_text('{"host":"x","message":"alpha"}\n{"host":"y","message":"beta"}\n', encoding="utf-8")
        originals = {source: source.read_bytes() for source in batch_sources}
        web.named("Open project menu")
        _pick(web, [str(source) for source in batch_sources])
        expected_names = [source.name for source in batch_sources]
        wait_for(lambda: web.script("return [...document.querySelectorAll('.pending-files li')].map(e=>e.textContent.trim());"),
                 lambda names: names == expected_names, "multiple native picker paths populate the import form in order")
        web.choose('select[aria-label="CSV delimiter"]', "\t")
        web.tap(web.node('.restore-annotations'))
        wait_for(lambda: web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').checked;"),
                 bool, "annotation restoration is selected before importing the batch")
        start = web.checkpoint()
        web.named("Import and open", ".workspace-drawer")
        settled = wait_for(lambda: web.script("""
          const button=document.querySelector('.workspace-drawer .button.primary');
          return {pending:document.querySelector('.pending-file')?.textContent.trim(),
            failures:[...document.querySelectorAll('.import-failures li')].map(e=>e.textContent.trim()),
            ready:!!button&&!button.disabled&&button.textContent.trim()==='Import and open'};
        """), lambda value: value["ready"] and value["pending"] == batch_bad.name and len(value["failures"]) == 1,
            "mixed batch completes later imports and retains only the failed file for retry")
        failure = settled["failures"][0]
        assert failure.startswith(f"{batch_bad.name}:") and len(failure) > len(batch_bad.name) + 3, failure
        assert batch_csv.name not in failure and batch_ndjson.name not in failure, failure
        requests = [entry for entry in web.trace(after=start) if entry["name"] == "create_project"]
        assert [entry["request"]["path"] for entry in requests] == [str(source) for source in batch_sources], requests
        assert [bool(entry.get("error")) for entry in requests] == [False, True, False], requests
        assert requests[1]["error"] == "Failed to parse JSON: expected value at line 1 column 9", requests[1]
        report.setdefault("boundary_expected_errors", []).append(
            {key: requests[1][key] for key in ("name", "request", "error")})
        assert all(later["started"] >= earlier["decode_done"] for earlier, later in zip(requests, requests[1:])), requests
        for request, csv in zip(requests, [True, False, False]):
            assert request["request"]["restore_annotations"] is csv, request
            assert request["request"]["delimiter"] == ("\t" if csv else ""), request
        projects = web.command("list_projects")
        imported = []
        for source in [batch_csv, batch_ndjson]:
            meta = next(item["meta"] for item in projects if item["meta"].get("source_file_name") == source.name)
            created.append((meta["id"], source))
            assert meta["total_records"] == 2 and meta["source_sha256"] == hashlib.sha256(originals[source]).hexdigest(), meta
            assert web.script("return !!document.querySelector(arguments[0]);", f'#project-tab-{meta["id"]}'), meta
            rows = web.command("query_project_rows", {"projectId": meta["id"], "offset": 0, "limit": 10})["rows"]
            assert len(rows) == 2 and all(not row["flag"] for row in rows), rows
            imported.append(meta)
        assert not any(item["meta"].get("source_file_name") == batch_bad.name for item in projects), projects
        assert web.script("return document.querySelector('[role=tab][aria-selected=true]')?.id;") == f'project-tab-{imported[-1]["id"]}'
        assert all(source.read_bytes() == original for source, original in originals.items())
        batch_check = {"selected_files": 3, "successful_files": 2, "failed_files": 1,
            "sequential_native_imports": True, "later_file_imported_after_error": True,
            "successful_tabs_open": True, "failed_only_retry": True, "csv_options_not_applied_to_json": True}
        web.named("Close workspace menu", ".workspace-drawer")
        wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "mixed batch status menu closes")
        web.applied(projectId=imported[-1]["id"])
        web.results(2)
        assert web.script("return window.__refinementDialog.count;") == 4
        assert web.script("return window.__refinementDialog.multiple.every(Boolean);")
    finally:
        web.script("window.__refinementDialog?.restore();delete window.__refinementDialog;")
        open_project(web, data["name"])
        web.results(data["total"])
        known = {project_id for project_id, _ in created}
        sources_by_name = {source.name: source for source in source_paths}
        for project in web.command("list_projects"):
            meta = project["meta"]
            if meta.get("source_file_name") in sources_by_name and meta["id"] not in known:
                created.append((meta["id"], sources_by_name[meta["source_file_name"]]))
        for project_id, source in created:
            if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{project_id}"):
                web.named(f"Close {source.name}")
            if any(item["meta"]["id"] == project_id for item in web.command("list_projects")):
                delete_fixture_project(web, project_id)
        for source in source_paths:
            source.unlink(missing_ok=True)
    report["import_identity"] = {"header_full_source_path": True, "recent_short_hash_without_prefix": label,
        "file_picker_label": "Choose files", "preview_before_import_options": True, "cases": checks,
        "cancel_preserves_selection_and_options": cancelled_selection, "mixed_batch": batch_check,
        "unverified": "Native OS file-picker interaction; its selected-path response alone is substituted"}
