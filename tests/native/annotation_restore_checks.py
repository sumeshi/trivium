"""GUI CSV export/import roundtrip; only native picker path replies are supplied."""
from __future__ import annotations

import csv
import json
from pathlib import Path
import uuid

from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS


def _pick(web, source: Path) -> None:
    web.script("window.__annotationRestorePicker.path=arguments[0];", str(source))
    web.click(web.node(".workspace-drawer .choose-file"))
    wait_for(lambda: web.script("return document.querySelector('.pending-file')?.textContent.trim();"),
             lambda value: value == source.name, "selected file populates the import form")


def _displayed_annotations(web, annotations: list[dict]) -> None:
    expected = [{"row": index, "flag": entry["flag"], "memo": entry["memo"] or "",
                 "title": entry["memo"] or "Add note", "tags": len(entry["tags"]), "bookmarked": entry["bookmarked"]}
                for index, entry in enumerate(annotations)]
    wait_for(lambda: web.script("""
      return [...document.querySelectorAll('.data-row:not(.loading)')].map(row=>({
        row:Number(row.dataset.rowIndex),flag:row.querySelector('.flag-buttons .selected')?.getAttribute('aria-label').toLowerCase()??'',
        memo:row.querySelector('.memo-text')?.textContent??'',title:row.querySelector('.memo-button').title,
        tags:Number(row.querySelector('.row-tags-button span')?.textContent??0),bookmarked:row.classList.contains('bookmarked')}));
    """), lambda rows: rows == expected, "actual table flags, notes, tags and bookmarks match the restored annotations")


def _source_display(web, columns: list[dict], first_record: list[str], total: int) -> None:
    # Keep one source field visible and inspect the actual full-record dialog.
    web.named("Choose visible columns")
    web.named("Deselect all", "#visible-column-menu")
    web.click(web.element(f'#column-{columns[0]["id"]}'))
    web.named("Done", "#visible-column-menu")
    web.results(total)
    web.tap(web.node(f'.data-row[data-row-index="0"] [data-column-id="{columns[0]["id"]}"]'))
    for column, value in zip(columns, first_record):
        selector = f'.row-detail-field[data-column-id="{column["id"]}"]'
        web.script("document.querySelector(arguments[0]).scrollIntoView({block:'nearest'});", selector)
        wait_for(lambda: web.script("const e=document.querySelector(arguments[0])?.querySelector('.row-field-value');return e?e.dataset.valueKind==='empty'?'':e.textContent:null;", selector),
                 lambda actual: actual == value, f"restored full-record field {column['label']} displays the exact source text")
    web.named("Close", ".row-detail-dialog")


def _export_gui(web, destination: Path, scope: str = "filtered") -> None:
    web.named("Export rows")
    assert web.script("return document.querySelector('.export-annotations input').checked;")
    web.tap(web.node(f'.export-section input[type="radio"][value="{scope}"]'))
    web.script("window.__annotationRestorePicker.path=arguments[0];", str(destination))
    web.named("Export…", ".export-dialog")
    wait_for(destination.is_file, bool, "GUI export writes the annotated CSV")
    wait_for(lambda: web.script("return !document.querySelector('.export-dialog');"), bool, "GUI export finishes")


def verify_annotation_restore(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    outer_query, outer_total = web.query(), web.state()["total"]
    source = root / f"annotation-source-{uuid.uuid4().hex[:8]}.csv"
    records = [
        ["r-0000001", '<CommandLine>whoami</CommandLine>\r\n日本語, "quoted"', "source memo", "source critical"],
        ["r-0000002", "9007199254740993", "", "ordinary data"],
        ["r-0000003", "2026-11-14T00:00:00.123456789Z", "source note only", "source suspicious"],
        ["r-0000004", "<alice@example.org>", "untagged source note", ""],
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
        {"flag": "critical", "memo": 'Sender: <alice@example.org>\r\n<CommandLine>whoami</CommandLine>\r\n日本語, "quoted"  ',
         "tags": ["alpha;beta", 'quote"tag', "日本語"], "bookmarked": True},
        {"flag": "safe", "memo": " \t ", "tags": [], "bookmarked": False},
        {"flag": "suspicious", "memo": "調査中\r\n<XML>keep</XML>", "tags": ["check;again", "再調査"], "bookmarked": True},
        {"flag": "", "memo": "", "tags": ["unflagged"], "bookmarked": False},
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
        const next=window.__annotationRestoreHoldNextQuery;
        if(next&&name==='query_project_rows'){
          let args={};try{args=JSON.parse(typeof options.body==='string'?options.body:new TextDecoder().decode(options.body));}catch{}
          const projectId=(args.payload??args.request??{}).projectId;
          if(projectId&&!next.excluded.includes(projectId)){
            window.__workspaceHold={name,projectId,ready:false,release:null};
            delete window.__annotationRestoreHoldNextQuery;
          }
        }
        return previous.call(window,input,options,...rest);
      };
    """)
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"])
        web.results(len(records))
        _export_gui(web, exported)
        exported_bytes = exported.read_bytes()
        (evidence / "csv-source.csv").write_bytes(original)
        (evidence / "csv-export-first.csv").write_bytes(exported_bytes)

        imported = []
        for restore in [False, True]:
            web.named("Open project menu")
            _pick(web, exported)
            assert not web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').checked;"), "CSV restore defaults off for every selected file"
            if restore:
                web.tap(web.node('.restore-annotations'))
                wait_for(lambda: web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').checked;"),
                         bool, "annotation restoration is selected before choosing another file")
                # Opt-in must not leak to another selected file or JSON input.
                _pick(web, selected_json)
                assert not web.script("return !!document.querySelector('[aria-label=\"Restore Trivium annotations\"]');")
                _pick(web, exported)
                assert not web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').checked;")
                web.tap(web.node('.restore-annotations'))
                wait_for(lambda: web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').checked;"),
                         bool, "annotation restoration is selected before importing the annotated CSV")
                web.screenshot(evidence / "csv-annotation-restore-option.png")
            start = web.checkpoint()
            if restore:
                assert web.script("return !window.__workspaceHold;"), "The restore check must own its response hold"
                web.script("window.__annotationRestoreHoldNextQuery={excluded:arguments[0]};",
                           [data["id"], *[project_id for project_id, _ in created]])
            web.named("Import and open", ".workspace-drawer")
            wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "GUI import opens the saved investigation")
            known = {project_id for project_id, _ in created}
            project_id = next(item["meta"]["id"] for item in web.command("list_projects")
                if item["meta"].get("source_file_name") == exported.name and item["meta"]["id"] not in known)
            created.append((project_id, exported.name))
            if restore:
                wait_for(lambda: web.script("return !!window.__workspaceHold?.ready;"), bool,
                         "the restored CSV's actual first-page response is held")
                assert web.script("return window.__workspaceHold.request.projectId;") == project_id
                pending = web.script("""
                  const viewport=document.querySelector('.virtual-viewport');
                  return {rows:document.querySelectorAll('.data-row').length,
                    viewportHeight:viewport?.getBoundingClientRect().height??0,
                    viewportWidth:viewport?.getBoundingClientRect().width??0,
                    spacer:!!document.querySelector('.virtual-spacer'),
                    status:document.querySelector('.table-query-state[role=status]')?.textContent??''};
                """)
                assert pending["rows"] == 0, ("Unknown restored results must not render placeholder records", pending)
                assert pending["viewportHeight"] > 0 and pending["viewportWidth"] > 0 and pending["spacer"], pending
                assert "Loading rows" in pending["status"], pending
                web.screenshot(evidence / "csv-annotation-restore-loading.png")
                web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
            web.applied(projectId=project_id)
            web.results(len(records))
            request = next(entry for entry in web.trace(after=start) if entry["name"] == "create_project")
            assert request["request"]["restore_annotations"] is restore, request
            loaded = web.command("load_project", {"projectId": project_id}, request=True)
            rows = web.command("query_project_rows", {"projectId": project_id, "offset": 0, "limit": 10})["rows"]
            assert [row["row_index"] for row in rows] == list(range(len(records))), "Restore must not add source records"
            assert web.script("return [...document.querySelectorAll('.data-row:not(.loading)')].map(row=>Number(row.dataset.rowIndex));") == list(range(len(records)))
            assert not web.script("return !!document.querySelector('.data-row.loading');")
            for row, expected in zip(rows, annotations):
                for field in ["flag", "memo", "tags", "bookmarked"]:
                    actual = (row[field] or "") if field in {"flag", "memo"} else row[field]
                    assert actual == (expected[field] if restore else {"flag": "", "memo": "", "tags": [], "bookmarked": False}[field]), (field, row)
            assert loaded["project"]["meta"]["flagged_records"] == (3 if restore else 0)
            # Colliding source names remain independent data columns in either mode.
            data_columns = loaded["columns_meta"][-len(fields):]
            assert [column["label"] for column in data_columns] == fields, data_columns
            for row, expected in zip(rows, records):
                assert [row["cells"][column["id"]]["text"] for column in data_columns] == expected
            expected_display = annotations if restore else [{"flag": "", "memo": "", "tags": [], "bookmarked": False} for _ in records]
            _displayed_annotations(web, expected_display)
            _source_display(web, data_columns, records[0], len(records))
            imported.append({"restore": restore, "flagged": loaded["project"]["meta"]["flagged_records"],
                             "annotations_exact": True, "colliding_source_columns_preserved": True})

        # Change the restored investigation through the real table/editor, then
        # export/import it again. Old annotation columns remain source data;
        # the new leading annotation block must win even when names are suffixed.
        latest = [dict(entry) for entry in annotations]
        latest[0] = {**latest[0], "flag": "suspicious", "memo": 'Updated <bob@example.org>\n<CommandLine>hostname</CommandLine>\n再調査, "new"  '}
        start = web.checkpoint()
        web.named("Suspicious", '.data-row[data-row-index="0"]')
        wait_for(lambda: [entry for entry in web.trace(after=start) if entry["name"] == "update_flag" and "result" in entry],
                 bool, "the edited restored flag is committed by the native engine")
        web.tap(web.node('.data-row[data-row-index="0"] .memo-button'))
        wait_for(lambda: web.script("return !!document.querySelector('.memo-dialog textarea');"), bool, "restored note editor opens")
        web.script("const e=document.querySelector('.memo-dialog textarea');e.value=arguments[0];e.dispatchEvent(new Event('input',{bubbles:true}));", latest[0]["memo"])
        web.named("Save", ".memo-dialog")
        wait_for(lambda: web.script("return !document.querySelector('.memo-dialog');"), bool, "edited restored note is saved")
        _displayed_annotations(web, latest)
        second = root / f"annotation-export-second-{uuid.uuid4().hex[:8]}.csv"
        _export_gui(web, second)
        (evidence / "csv-export-second.csv").write_bytes(second.read_bytes())
        with exported.open(encoding="utf-8", newline="") as handle:
            first_csv = list(csv.reader(handle))
        with second.open(encoding="utf-8", newline="") as handle:
            second_csv = list(csv.reader(handle))
        assert len(first_csv) == len(second_csv) == len(records) + 1
        for first_row, second_row, current in zip(first_csv[1:], second_csv[1:], latest):
            assert second_row[:6] == ["1" if current["flag"] == flag else "0" for flag in ("safe", "suspicious", "critical")] + [current["memo"], json.dumps(current["tags"], ensure_ascii=False, separators=(",", ":")), "1" if current["bookmarked"] else "0"]
            assert second_row[6:12] == first_row[:6], "The earlier annotation block remains ordinary source data"
        assert second_csv[1][:6] != first_csv[1][:6], "This roundtrip must distinguish edited annotations from the stale source block"
        web.named("Open project menu")
        _pick(web, second)
        web.tap(web.node('.restore-annotations'))
        wait_for(lambda: web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').checked;"), bool, "second GUI restore is explicitly enabled")
        start = web.checkpoint()
        web.named("Import and open", ".workspace-drawer")
        wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "second exported CSV opens as an investigation")
        second_id = next(item["meta"]["id"] for item in web.command("list_projects") if item["meta"].get("source_file_name") == second.name)
        created.append((second_id, second.name))
        web.applied(projectId=second_id)
        web.results(len(records))
        assert next(entry for entry in web.trace(after=start) if entry["name"] == "create_project")["request"]["restore_annotations"] is True
        loaded = web.command("load_project", {"projectId": second_id}, request=True)
        all_column_ids = [column["id"] for column in loaded["columns_meta"]]
        rows = web.command("query_project_rows", {"projectId": second_id, "offset": 0, "limit": 10})["rows"]
        assert [row["row_index"] for row in rows] == list(range(len(records)))
        for row, current in zip(rows, latest):
            assert all((row[field] or "" if field in {"flag", "memo"} else row[field]) == value for field, value in current.items()), row
        data_columns = loaded["columns_meta"][-len(fields):]
        assert [column["label"] for column in data_columns] == fields
        assert [[row["cells"][column["id"]]["text"] for column in data_columns] for row in rows] == records
        _displayed_annotations(web, latest)
        _source_display(web, data_columns, records[0], len(records))
        web.screenshot(evidence / "csv-annotation-second-restore.png")

        # A restored investigation must be durable, not merely visible in the
        # import response. Keep a filtered, sorted, one-column view, close it,
        # and reopen from Recent before exporting either result scope.
        search = "r-0000001|r-0000003"
        web.write('input[aria-label="Search rows"]', search)
        web.applied(projectId=second_id, search=search)
        web.results(2)
        sort_column = data_columns[0]["id"]
        for direction in ["asc", "desc"]:
            web.tap(web.node(f'.column-heading[data-column-id="{sort_column}"] .column-sort'))
            web.applied(sortKey=sort_column, sortDirection=direction)
            web.results(2)
        saved_query = web.query()
        hidden = web.command("load_project", {"projectId": second_id}, request=True)["hidden_columns"]
        expected_order = [2, 0]
        wait_for(lambda: web.script("return [...document.querySelectorAll('.data-row:not(.loading)')].map(e=>Number(e.dataset.rowIndex));"),
                 lambda indices: indices == expected_order, "restored CSV follows the filtered descending source order")
        web.named(f"Close {second.name}")
        open_project(web, loaded["project"]["meta"]["name"])
        web.applied(**saved_query)
        web.results(2)
        reopened = web.command("load_project", {"projectId": second_id}, request=True)
        reopened_rows = web.command("query_project_rows", {"projectId": second_id, "offset": 0, "limit": 10})["rows"]
        assert reopened["columns_meta"] == loaded["columns_meta"]
        assert reopened["hidden_columns"] == hidden and web.visible_column_count() == 1
        # List pages intentionally omit hidden fields. Read each full cell via
        # the detail API to check persistence without changing the saved view.
        assert len(reopened_rows) == len(rows)
        for before, after in zip(rows, reopened_rows):
            assert set(after["cells"]) == {sort_column}, "Reopen preserves the visible-column projection"
            full_cells = {column: web.command("get_cell", {"projectId": second_id,
                "rowIndex": after["row_index"], "columnId": column}) for column in all_column_ids}
            assert {**after, "cells": full_cells} == before, "Closing and reopening retains every annotation and full source cell"
        assert web.query() == saved_query
        wait_for(lambda: web.script("return [...document.querySelectorAll('.data-row:not(.loading)')].map(e=>Number(e.dataset.rowIndex));"),
                 lambda indices: indices == expected_order, "the reopened investigation retains its filtered descending view")

        scoped_exports = []
        for scope, original_indices in [("filtered", expected_order), ("selected", [0])]:
            if scope == "selected":
                web.tap(web.node('.data-row[data-row-index="0"] input[aria-label="Select row 1"]'))
                wait_for(lambda: web.state()["selectedIds"], lambda selected: selected == [1],
                         "the selected export contains only the explicit source row")
            destination = root / f"annotation-{scope}-{uuid.uuid4().hex[:8]}.csv"
            checkpoint = web.checkpoint()
            _export_gui(web, destination, scope)
            calls = [entry for entry in web.trace(after=checkpoint) if entry["name"] == "export_project" and "result" in entry]
            assert len(calls) == 1 and calls[0]["request"]["scope"] == scope
            assert calls[0]["request"]["search"] == search and calls[0]["request"]["sortDirection"] == "desc"
            assert calls[0]["result"]["rows"] == len(original_indices)
            with destination.open(encoding="utf-8", newline="") as handle:
                scoped_csv = list(csv.reader(handle))
            assert [row[-len(fields):] for row in scoped_csv[1:]] == [records[index] for index in original_indices], "CSV scope/order must match the applied query or explicit selection"
            scoped_exports.append((destination, original_indices))

        # Read both actual saved files back through the GUI. After import the
        # row IDs are new positions, but annotations must follow source values.
        restored_scopes = []
        for destination, original_indices in scoped_exports:
            web.named("Open project menu")
            _pick(web, destination)
            web.tap(web.node('.restore-annotations'))
            wait_for(lambda: web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').checked;"),
                     bool, "subset CSV restoration is explicitly enabled")
            web.named("Import and open", ".workspace-drawer")
            wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "the exported subset opens as an investigation")
            restored = next(item for item in web.command("list_projects") if item["meta"].get("source_file_name") == destination.name)
            restored_id = restored["meta"]["id"]
            created.append((restored_id, destination.name))
            web.applied(projectId=restored_id)
            web.results(len(original_indices))
            restored_detail = web.command("load_project", {"projectId": restored_id}, request=True)
            restored_rows = web.command("query_project_rows", {"projectId": restored_id, "offset": 0, "limit": 10})["rows"]
            restored_columns = restored_detail["columns_meta"][-len(fields):]
            assert [column["label"] for column in restored_columns] == fields
            assert [row["row_index"] for row in restored_rows] == list(range(len(original_indices)))
            assert [[row["cells"][column["id"]]["text"] for column in restored_columns] for row in restored_rows] == [records[index] for index in original_indices]
            for row, index in zip(restored_rows, original_indices):
                assert all((row[field] or "" if field in {"flag", "memo"} else row[field]) == value for field, value in latest[index].items()), row
            _displayed_annotations(web, [latest[index] for index in original_indices])
            restored_scopes.append({"scope": "filtered" if len(original_indices) == 2 else "selected", "source_rows": original_indices, "exact_annotations": True})
        assert source.read_bytes() == original and exported.read_bytes() == exported_bytes
        report["annotation_restore"] = {"gui_export_import": True, "cases": imported,
            "csv_only_explicit_opt_in": True, "source_files_unchanged": True,
            "first_restored_page_has_no_placeholder_records": True,
            "all_four_flag_states": True, "gui_edit_reexport_restore_uses_latest_annotations": True,
            "actual_table_and_full_record_values_checked": True,
            "reopened_case_retains_annotations_columns_and_filtered_view": True,
            "gui_filtered_and_selected_export_restore": restored_scopes,
            "unverified": "Native picker interaction; only selected paths are substituted"}
    finally:
        web.script("window.__workspaceHold?.release?.();delete window.__workspaceHold;delete window.__annotationRestoreHoldNextQuery;")
        web.script("window.__annotationRestorePicker?.restore();delete window.__annotationRestorePicker;")
        web.close_drawer()
        open_project(web, data["name"])
        web.applied(**outer_query)
        web.results(outer_total)
        for project_id, filename in created:
            if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{project_id}"):
                web.named(f"Close {filename}")
            delete_fixture_project(web, project_id)
