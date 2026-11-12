"""Import-form regression with only the native OS picker result substituted.

The chosen path is returned through the actual dialog IPC response. Project
creation, import, metadata, rows and cleanup use the real native engine.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import uuid

from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS


def verify_import_identity(web, data, root: Path, report: dict) -> None:
    detail = web.command("load_project", {"projectId": data["id"]}, request=True)
    title = web.script("return document.querySelector('.project-title').title;")
    assert title == detail["project"]["meta"]["source_path"] and data["filename"] in title, title
    web.named("Open project menu")
    recent = web.script("return [...document.querySelectorAll('.project-card')].find(e=>e.querySelector('.project-name')?.textContent.trim()===arguments[0])?.querySelector('.project-hash');", data["name"])
    label = web.script("return {text:arguments[0].textContent.trim(),title:arguments[0].title};", recent)
    assert data["sha"] in label["title"] and not label["text"].lower().startswith("sha"), label
    assert data["sha"].startswith(label["text"].rstrip("…")), label
    formats = web.script("return document.querySelector('#import-formats').textContent.trim();")
    assert formats == "CSV / JSON / JSONL / NDJSON", formats
    assert "read-only" in web.script("return document.querySelector('.import-help').textContent;")
    assert not web.script("return !!document.querySelector('[aria-label=\"CSV delimiter\"]');"), "CSV controls stay hidden until CSV is selected"
    web.named("Close workspace menu", ".workspace-drawer")
    wait_for(lambda: web.script("return !document.querySelector('.drawer-backdrop');"), bool, "initial project menu closes")

    assert web.script("""
      const previous=window.fetch;
      window.__refinementDialog={path:null,count:0,restore:()=>{window.fetch=previous;}};
      window.fetch=function(input,options,...rest){
    """ + IPC_COMMAND_JS + """
        if(name==='plugin:dialog|open'&&window.__refinementDialog.path!==null){
          const path=window.__refinementDialog.path;window.__refinementDialog.path=null;window.__refinementDialog.count++;
          return Promise.resolve(new Response(JSON.stringify(path),{headers:{'Tauri-Response':'ok','Content-Type':'application/json'}}));
        }
        return previous.call(window,input,options,...rest);
      };return true;
    """), "Substitute only the OS picker response"
    created: list[tuple[str, Path]] = []
    checks = []
    sources = [
        ("csv", "host\tmessage\nx\talpha\ny\tbeta\n"),
        ("json", json.dumps([{"host": "x", "message": "alpha"}, {"host": "y", "message": "beta"}])),
    ]
    try:
        for extension, text in sources:
            source = root / f"refinement-import-{uuid.uuid4().hex[:8]}.{extension}"
            source.write_text(text, encoding="utf-8")
            before = source.read_bytes()
            web.script("window.__refinementDialog.path=arguments[0];", str(source))
            web.named("Open project menu")
            web.named("Choose data file", ".workspace-drawer")
            wait_for(lambda: web.script("return document.querySelector('.pending-file')?.textContent.trim();"), lambda value: value == source.name, "native picker path populates the import form")
            controls = web.script("return !!document.querySelector('[aria-label=\"CSV delimiter\"]');")
            assert controls == (extension == "csv"), (extension, controls)
            if controls:
                web.choose('select[aria-label="CSV delimiter"]', "\t")
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
            web.command("delete_project", {"projectId": meta["id"]}, request=True)
        assert web.script("return window.__refinementDialog.count;") == 2
    finally:
        web.script("window.__refinementDialog?.restore();delete window.__refinementDialog;")
        for project_id, source in created:
            if any(item["meta"]["id"] == project_id for item in web.command("list_projects")):
                web.command("delete_project", {"projectId": project_id}, request=True)
        open_project(web, data["name"])
        web.results(data["total"])
    report["import_identity"] = {"header_full_source_path": True, "recent_short_hash_without_prefix": label,
        "formats": formats, "read_only_notice": True, "cases": checks,
        "unverified": "Native OS file-picker interaction; its selected-path response alone is substituted"}
