"""GUI CSV export/import roundtrip; only native picker path replies are supplied."""
from __future__ import annotations

import csv
import json
from pathlib import Path
import uuid

from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS


def _pick(web, source: Path) -> None:
    web.script("window.__annotationRestorePicker.path=arguments[0];", str(source))
    web.click(web.node(".workspace-drawer .choose-file"))
    wait_for(lambda: web.script("return document.querySelector('.pending-file')?.textContent.trim();"),
             lambda value: value == source.name, "selected file populates the import form")


def verify_annotation_restore(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    outer_query, outer_total = web.query(), web.state()["total"]
    source = root / f"annotation-source-{uuid.uuid4().hex[:8]}.csv"
    records = [
        ["r-0000001", '<CommandLine>whoami</CommandLine>\n日本語, "quoted"', "source memo", "source critical"],
        ["r-0000002", "9007199254740993", "", "ordinary data"],
    ]
    fields = ["source_id", "message", "trivium-memo", "trivium-critical"]
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle); writer.writerow(fields); writer.writerows(records)
    original = source.read_bytes()
    exported = root / f"annotation-export-{uuid.uuid4().hex[:8]}.csv"
    selected_json = root / "ordinary-annotation-fields.json"
    selected_json.write_text(json.dumps({"trivium-memo": "ordinary data"}), encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    owner = case["meta"]["id"]
    annotations = [
        {"flag": "critical", "memo": 'Sender: <alice@example.org>\n<CommandLine>whoami</CommandLine>\n日本語, "quoted"  ',
         "tags": ["alpha;beta", 'quote"tag', "日本語"], "bookmarked": True},
        {"flag": "safe", "memo": " \t ", "tags": [], "bookmarked": False},
    ]
    for index, annotation in enumerate(annotations):
        web.command("update_flag", {"projectId": owner, "row_index": index, **annotation})
    created: list[tuple[str, str]] = [(owner, source.name)]
    web.script("""
      const previous=window.fetch;
      window.__annotationRestorePicker={path:null,restore:()=>{window.fetch=previous;}};
      window.fetch=function(input,options,...rest){
    """ + IPC_COMMAND_JS + """
        if((name==='plugin:dialog|open'||name==='plugin:dialog|save')&&window.__annotationRestorePicker.path!==null){
          const path=window.__annotationRestorePicker.path;window.__annotationRestorePicker.path=null;
          return Promise.resolve(new Response(JSON.stringify(path),{headers:{'Tauri-Response':'ok','Content-Type':'application/json'}}));
        }
        return previous.call(window,input,options,...rest);
      };
    """)
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"])
        web.results(2)
        web.named("Export rows")
        assert web.script("return document.querySelector('.export-annotations input').checked;")
        web.script("window.__annotationRestorePicker.path=arguments[0];", str(exported))
        web.named("Export…", ".export-dialog")
        wait_for(lambda: exported.is_file(), bool, "GUI export writes the annotated CSV")
        wait_for(lambda: web.script("return !document.querySelector('.export-dialog');"), bool, "export dialog completes")
        exported_bytes = exported.read_bytes()

        imported = []
        for restore in [False, True]:
            web.named("Open project menu")
            _pick(web, exported)
            assert not web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').checked;"), "CSV restore defaults off for every selected file"
            if restore:
                web.click(web.node('[aria-label="Restore Trivium annotations"]'))
                # Opt-in must not leak to another selected file or JSON input.
                _pick(web, selected_json)
                assert not web.script("return !!document.querySelector('[aria-label=\"Restore Trivium annotations\"]');")
                _pick(web, exported)
                assert not web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').checked;")
                web.click(web.node('[aria-label="Restore Trivium annotations"]'))
                web.screenshot(evidence / "csv-annotation-restore-option.png")
            start = len(web.trace())
            web.named("Import and open", ".workspace-drawer")
            wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "GUI import opens the saved investigation")
            known = {project_id for project_id, _ in created}
            project_id = next(item["meta"]["id"] for item in web.command("list_projects")
                if item["meta"].get("source_file_name") == exported.name and item["meta"]["id"] not in known)
            created.append((project_id, exported.name))
            web.applied(projectId=project_id)
            web.results(2)
            request = next(entry for entry in web.trace()[start:] if entry["name"] == "create_project")
            assert request["request"]["restore_annotations"] is restore, request
            loaded = web.command("load_project", {"projectId": project_id}, request=True)
            rows = web.command("query_project_rows", {"projectId": project_id, "offset": 0, "limit": 10})["rows"]
            for row, expected in zip(rows, annotations):
                for field in ["flag", "memo", "tags", "bookmarked"]:
                    actual = (row[field] or "") if field in {"flag", "memo"} else row[field]
                    assert actual == (expected[field] if restore else {"flag": "", "memo": "", "tags": [], "bookmarked": False}[field]), (field, row)
            assert loaded["project"]["meta"]["flagged_records"] == (2 if restore else 0)
            # Colliding source names remain independent data columns in either mode.
            data_columns = loaded["columns_meta"][-len(fields):]
            assert [column["label"] for column in data_columns] == fields, data_columns
            for row, expected in zip(rows, records):
                assert [row["cells"][column["id"]]["text"] for column in data_columns] == expected
            imported.append({"restore": restore, "flagged": loaded["project"]["meta"]["flagged_records"],
                             "annotations_exact": True, "colliding_source_columns_preserved": True})
        assert source.read_bytes() == original and exported.read_bytes() == exported_bytes
        report["annotation_restore"] = {"gui_export_import": True, "cases": imported,
            "csv_only_explicit_opt_in": True, "source_files_unchanged": True,
            "unverified": "Native picker interaction; only selected paths are substituted"}
    finally:
        web.script("window.__annotationRestorePicker?.restore();delete window.__annotationRestorePicker;")
        web.close_drawer()
        open_project(web, data["name"])
        web.applied(**outer_query)
        web.results(outer_total)
        for project_id, filename in created:
            if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{project_id}"):
                web.named(f"Close {filename}")
            web.command("delete_project", {"projectId": project_id}, request=True)
