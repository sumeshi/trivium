"""Actual named-pattern filtering and extraction tabs, using synthetic logs.

Only the native Save dialog's chosen path is supplied by the harness. Imports,
matching, row membership, extraction, navigation and CSV creation are real IPC.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
import uuid

from column_pin_checks import _key
from column_presentation_checks import _action
from feedback_checks import _settle
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS


def _extract_panel(web):
    web.close_drawer()
    web.named("Open Filter")
    web.tap(web.node("#tool-tab-extract"))
    wait_for(lambda: web.script("return !!document.querySelector('.extract-panel');"), bool, "Extract tool opens")


def _pattern(web, name):
    if not web.script("return !!document.querySelector('#search-pattern-menu');"):
        web.named("Choose search patterns")
    button = wait_for(lambda: web.script("""
      return [...document.querySelectorAll('#search-pattern-menu .pattern-select')]
        .find(e=>e.querySelector('strong')?.textContent.trim()===arguments[0])??null;
    """, name), bool, "named search pattern is available")
    web.tap(button)


def _custom_pattern(web, name):
    web.named("Choose search patterns")
    web.named("New pattern", "#search-pattern-menu")
    web.write('[aria-label="Pattern name"]', name)
    web.write('[aria-label="Pattern expression"]', "TOKEN-[0-9]{2}")
    checkpoint = len(web.trace())
    web.named("Save pattern", "#search-pattern-menu")
    entry = wait_for(lambda: next((item for item in reversed(web.trace()[checkpoint:]) if item["name"] == "save_format"), None),
        lambda item: bool(item and "result" in item), "custom pattern saves through the actual Rust catalog")
    definition = entry["result"]["definition"]
    _pattern(web, name); web.results(1)
    assert web.script("return [...document.querySelectorAll('.data-row:not(.loading)')].map(e=>Number(e.dataset.rowIndex));") == [2]
    frozen = web.query()
    web.named("Choose search patterns")
    web.named(f"Edit {name}", "#search-pattern-menu")
    changed = {**definition, "pattern": "TOKEN-99"}
    web.write('[aria-label="Pattern definition JSON"]', json.dumps(changed))
    checkpoint = len(web.trace())
    web.named("Save pattern", "#search-pattern-menu")
    wait_for(lambda: next((item for item in reversed(web.trace()[checkpoint:]) if item["name"] == "save_format"), None),
             lambda item: bool(item and "result" in item), "editing the reusable pattern completes")
    # Beyond the real 160ms query debounce: catalog edits must not silently
    # replace the definition already applied to this investigation.
    web.async_script("const done=arguments[arguments.length-1];setTimeout(()=>done(true),240);")
    web.results(1)
    assert web.query() == frozen
    web.named("Close search patterns")
    web.named(f"Remove pattern {name}"); web.results(3)
    _pattern(web, name); web.results(0)
    web.named(f"Remove pattern {name}"); web.results(3)
    return definition["id"]


def _value_rows(web):
    return web.script("""
      return Object.fromEntries([...document.querySelectorAll('.extract-view .value-table tbody tr')].map(row=>{
        const cells=[...row.querySelectorAll('td')].map(e=>e.textContent.trim());
        return [cells[0],{count:Number(cells[1].replaceAll(',','')),rows:Number(cells[2].replaceAll(',','')),scope:cells[3]}];
      }));
    """)


def _ranking(web, values):
    return wait_for(lambda: _value_rows(web), lambda rows: set(rows) == set(values), "complete native extraction ranking")


def _scan(web, project_id):
    checkpoint = len(web.trace())
    web.named("IPv4", ".extract-panel")
    entry = wait_for(lambda: next((item for item in reversed(web.trace()[checkpoint:])
        if item["name"] == "extract_values" and item["request"].get("projectId") == project_id), None),
        lambda value: bool(value and ("result" in value or "error" in value)), "real extraction completes")
    assert "result" in entry and "error" not in entry, entry
    result = entry["result"]
    result_id = result["resultId"]
    wait_for(lambda: web.script("return document.getElementById(arguments[0])?.getAttribute('aria-selected');", f"extract-tab-{result_id}"),
        lambda selected: selected == "true", "extracted values open as a peer tab")
    wait_for(lambda: web.script("return !!document.querySelector('.extract-view .value-table');"), bool, "extracted value table is visible")
    return entry


def _tab(web, result_id):
    web.tap(web.node(f"#extract-tab-{result_id}"))
    wait_for(lambda: web.script("return !!document.querySelector('.extract-view .value-table');"), bool, "analysis tab reloads its ranking page")


def _row_action(web, value, label):
    button = wait_for(lambda: web.script("""
      const row=[...document.querySelectorAll('.extract-view .value-table tbody tr')]
        .find(row=>row.querySelector('.value-button')?.textContent.trim()===arguments[0]);
      return [...(row?.querySelectorAll('button')??[])].find(e=>e.getAttribute('aria-label')===arguments[1])??null;
    """, value, label), bool, "extraction row action")
    web.tap(button)


def _select_value(web, value):
    button = wait_for(lambda: web.script("""
      return [...document.querySelectorAll('.extract-view .value-button')].find(e=>e.textContent.trim()===arguments[0])??null;
    """, value), bool, "extracted value is selectable")
    web.tap(button)
    return wait_for(lambda: web.script("""
      return [...document.querySelectorAll('.source-table tbody tr')].map(row=>{
        const cells=[...row.querySelectorAll('td')];return {row:cells[0].textContent.trim(),column:cells[1].textContent.trim()};
      });
    """), bool, "original occurrences load")


def _export(web, destination):
    web.script("""
      const previous=window.fetch;
      window.__extractPicker={path:arguments[0],count:0,restore:()=>{window.fetch=previous;}};
      window.fetch=(input,options,...rest)=>{
    """ + IPC_COMMAND_JS + """
        if(name==='plugin:dialog|save'&&window.__extractPicker.path!==null){
          const path=window.__extractPicker.path;window.__extractPicker.path=null;window.__extractPicker.count++;
          return Promise.resolve(new Response(JSON.stringify(path),{headers:{'Tauri-Response':'ok','Content-Type':'application/json'}}));
        }
        return previous.call(window,input,options,...rest);
      };
    """, str(destination))
    checkpoint = len(web.trace())
    web.named("Export extracted values to CSV", ".extract-view")
    entry = wait_for(lambda: next((item for item in reversed(web.trace()[checkpoint:]) if item["name"] == "export_extract"), None),
        lambda value: bool(value and ("result" in value or "error" in value)), "GUI extraction export writes CSV")
    assert "result" in entry and "error" not in entry, entry
    assert entry["request"]["allowOverwrite"] is False
    assert web.script("return window.__extractPicker.count;") == 1
    web.script("window.__extractPicker.restore();delete window.__extractPicker;")
    with destination.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def verify_extract(web, data, root: Path, evidence: Path, report: dict):
    source = root / f"extract-{uuid.uuid4().hex[:8]}.csv"
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["first", "second", "cohort"])
        writer.writerows([
            ["10.0.0.1,10.0.0.1,192.0.2.1", "10.0.0.1 8.8.8.8", "keep"],
            ["10.0.0.10", "", "drop"],
            ["no address TOKEN-42", "", "keep"],
        ])
    original = source.read_bytes()
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    results = []
    custom_id = None
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"])
        web.results(3)
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        columns = {item["label"]: item["id"] for item in detail["columns_meta"]}
        _pattern(web, "IPv4")
        web.results(2)
        assert web.script("return [...document.querySelectorAll('.data-row:not(.loading)')].map(e=>Number(e.dataset.rowIndex));") == [0, 1]
        assert web.script("return document.querySelector('[aria-label=\"Search rows\"]').value;") == ""
        web.named("Choose search columns")
        checkbox = web.script("return [...document.querySelectorAll('#search-column-menu label')].find(e=>e.textContent.trim()==='cohort')?.querySelector('input');")
        web.tap(checkbox)
        web.results(0)
        web.named("All data columns", "#search-column-menu")
        web.results(2)
        web.named("Done", "#search-column-menu")
        web.named("Remove pattern IPv4")
        web.results(3)
        custom_id = _custom_pattern(web, f"Native token {project_id[:8]}")
        _action(web, columns["second"], "Hide this column")
        assert not web.script("return !!document.querySelector(arguments[0]);", f'.data-header [data-column-id="{columns["second"]}"]')
        web.write('[aria-label="Search rows"]', "cohort:keep")
        _key(web, "\ue007")
        web.applied(search="cohort:keep"); web.results(2)
        saved_query = web.query()
        project_ids = {item["meta"]["id"] for item in web.command("list_projects")}

        _extract_panel(web)
        assert web.script("return document.querySelector('[aria-label=\"Extraction rows\"]').value;") == "filtered"
        web.named("Choose extraction columns", ".extract-panel")
        web.named("Deselect all", ".extract-panel")
        wait_for(lambda: web.script("return document.querySelectorAll('.extract-panel .pattern-button').length;"),
                 lambda count: count > 0, "extraction pattern buttons are loaded")
        assert web.script("return [...document.querySelectorAll('.extract-panel .pattern-button')].every(e=>e.disabled);")
        web.named("Select all", ".extract-panel")
        first = _scan(web, project_id)
        results.append(first["result"]["resultId"])
        assert set(first["request"]["columns"]) == set(columns.values())
        assert first["request"]["search"] == "cohort:keep" and first["request"]["scope"] == "filtered"
        assert first["result"]["sourceRows"] == 2 and first["result"]["matchedRows"] == 1
        assert first["result"]["totalOccurrences"] == 5 and first["result"]["uniqueValues"] == 3
        ranked = _ranking(web, ["10.0.0.1", "192.0.2.1", "8.8.8.8"])
        assert ranked["10.0.0.1"] == {"count": 3, "rows": 1, "scope": "Private"}
        assert ranked["192.0.2.1"]["scope"] == "Special" and ranked["8.8.8.8"]["scope"] == "Public"
        assert {item["meta"]["id"] for item in web.command("list_projects")} == project_ids
        assert web.script("return document.querySelectorAll('[role=tablist] [role=tab][aria-selected=true]').length;") == 1
        web.tab(project_id); web.results(2)
        assert web.query() == saved_query

        _extract_panel(web)
        web.choose('[aria-label="Extraction rows"]', "all")
        second = _scan(web, project_id)
        results.append(second["result"]["resultId"])
        assert second["result"]["sourceRows"] == 3 and second["result"]["totalOccurrences"] == 6
        assert second["result"]["matchedRows"] == 2 and second["result"]["uniqueValues"] == 4
        all_values = _ranking(web, ["10.0.0.1", "10.0.0.10", "192.0.2.1", "8.8.8.8"])
        web.tab(project_id); web.results(2)
        _extract_panel(web)
        web.choose('[aria-label="Extraction rows"]', "all")
        web.named("Choose extraction columns", ".extract-panel")
        web.named("Deselect all", ".extract-panel")
        second_column = web.script("return [...document.querySelectorAll('#extraction-column-list label')].find(e=>e.textContent.trim()==='second')?.querySelector('input');")
        web.tap(second_column)
        third = _scan(web, project_id)
        results.append(third["result"]["resultId"])
        assert third["request"]["columns"] == [columns["second"]]
        selected_values = _ranking(web, ["10.0.0.1", "8.8.8.8"])
        assert all(value["count"] == value["rows"] == 1 for value in selected_values.values())
        assert third["result"]["totalOccurrences"] == 2 and third["result"]["matchedRows"] == 1
        _tab(web, results[1]); _ranking(web, all_values)
        occurrences = _select_value(web, "10.0.0.1")
        assert len(occurrences) == 3 and {entry["row"] for entry in occurrences} == {"#1"}
        assert {entry["column"] for entry in occurrences} == {"first", "second"}
        web.screenshot(evidence / "extract-ipv4-new-tab.png")

        web.tab(project_id); web.results(2)
        web.write('[aria-label="Search rows"]', ""); _key(web, "\ue007")
        web.applied(search=""); web.results(3)
        _tab(web, results[1]); _ranking(web, all_values)
        _row_action(web, "10.0.0.1", "Filter original rows by extracted value")
        web.results(1)
        assert web.script("return [...document.querySelectorAll('.data-row:not(.loading)')].map(e=>Number(e.dataset.rowIndex));") == [0]
        exact_query = web.query()
        assert '"equals": "10.0.0.1"' in json.dumps(exact_query["condition"])
        _tab(web, results[1]); _ranking(web, all_values)
        _select_value(web, "10.0.0.10")
        locate_start = len(web.trace())
        web.named("Go to original row 2", ".source-panel")
        wait_for(lambda: next((entry for entry in reversed(web.trace()[locate_start:]) if entry["name"] == "locate_project_row"), None),
                 lambda entry: bool(entry and ("result" in entry or "error" in entry)), "source row navigation finishes")
        web.results(1)
        assert web.query() == exact_query, "jumping to an excluded source row must preserve filters"

        _tab(web, results[1]); _ranking(web, all_values)
        destination = root / "trivium-extract-ipv4-native.csv"
        exported = _export(web, destination)
        expected = {value: {"Value": value, "Count": str(counts["count"]), "Rows": str(counts["rows"]), "Scope": counts["scope"]}
                    for value, counts in all_values.items()}
        assert {row["Value"]: row for row in exported} == expected, exported
        _tab(web, results[0]); _ranking(web, ["10.0.0.1", "192.0.2.1", "8.8.8.8"])
        assert source.read_bytes() == original
        report["extract"] = {"named_pattern_search": True, "shared_search_columns": True,
            "custom_pattern_save_and_edit": True, "applied_pattern_is_frozen": True,
            "filtered_and_all_row_scopes": True, "zero_columns_disabled": True,
            "all_columns_include_hidden": True, "selected_hidden_column_only": True,
            "adjacent_values_and_cross_column_counts": True, "classification": ["Private", "Public", "Special"],
            "peer_analysis_tabs": len(results), "recent_unchanged": True, "exact_captured_filter": True,
            "filtered_source_jump_preserves_rules": True, "frozen_result_after_source_query_change": True,
            "gui_export_matches_complete_ranking": True, "source_bytes_unchanged": True,
            "unverified": "Native Save dialog interaction; only its chosen path is supplied"}
        _settle(web)
    finally:
        web.script("window.__extractPicker?.restore();delete window.__extractPicker;")
        web.close_drawer()
        for result_id in results:
            button = web.script("return document.getElementById(arguments[0])?.parentElement.querySelector('[aria-label^=\"Close extraction\"]')??null;", f"extract-tab-{result_id}")
            if button:
                web.tap(button)
        web.tab(data["id"]); web.results(data["total"])
        if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{project_id}"):
            web.named(f"Close {source.name}")
        delete_fixture_project(web, project_id)
        if custom_id:
            web.command("delete_format", {"id": custom_id})
