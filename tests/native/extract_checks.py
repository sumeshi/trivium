"""Actual named-pattern filtering and extraction tabs, using synthetic logs.

Only the native Save dialog's chosen path is supplied by the harness. Imports,
matching, row membership, extraction, navigation and CSV creation are real IPC.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
import sys
import uuid

from column_pin_checks import _key
from column_presentation_checks import _action
from feedback_checks import _settle
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS, NormalWindowClose


def _extract_panel(web):
    web.close_drawer()
    web.named("Open Filter")
    web.tap(web.node("#tool-tab-extract"))
    wait_for(lambda: web.script("return !!document.querySelector('.extract-panel');"), bool, "Extract tool opens")


def _pattern(web, name):
    if not web.script("return !!document.querySelector('#search-pattern-menu');"):
        web.named("Choose search patterns")
    web.write('#search-pattern-menu [aria-label="Find a pattern"]', name)
    button = wait_for(lambda: web.script("""
      return [...document.querySelectorAll('#search-pattern-menu .pattern-select')]
        .find(e=>e.querySelector('strong')?.textContent.trim()===arguments[0])??null;
    """, name), bool, "named search pattern is available")
    web.tap(button)


def _custom_pattern(web, name):
    web.named("Choose search patterns")
    wait_for(lambda: web.script("""
      const picker=document.querySelector('#search-pattern-menu .pattern-picker');
      return !!picker&&!picker.textContent.includes('Loading patterns…');
    """), bool, "pattern catalog finishes loading before pointer input")
    web.async_script("const done=arguments[arguments.length-1];requestAnimationFrame(()=>requestAnimationFrame(()=>done(true)));")
    button = web.script("return [...document.querySelectorAll('#search-pattern-menu button')].find(e=>e.textContent.trim()==='New pattern');")
    web.tap(button)
    wait_for(lambda: web.script("return !!document.querySelector('#search-pattern-menu [aria-label=\"Pattern name\"]');"),
             bool, "New pattern opens its editor without dismissing the picker")
    web.write('[aria-label="Pattern name"]', name)
    web.write('[aria-label="Pattern expression"]', "TOKEN-[0-9]{2}")
    checkpoint = web.checkpoint()
    web.named("Save pattern", "#search-pattern-menu")
    entry = wait_for(lambda: next((item for item in reversed(web.trace(after=checkpoint)) if item["name"] == "save_format"), None),
        lambda item: bool(item and "result" in item), "custom pattern saves through the actual Rust catalog")
    definition = entry["request"]["definition"]
    custom = wait_for(lambda: web.script("""
      const button=[...document.querySelectorAll('#search-pattern-menu .pattern-select')]
        .find(e=>e.querySelector('strong')?.textContent.trim()===arguments[0]);
      return button?.textContent.includes('Custom')??false;
    """, name), bool, "user-created patterns are visibly marked Custom")
    assert custom
    _pattern(web, name); web.results(1)
    assert web.script("return [...document.querySelectorAll('.data-row:not(.loading)')].map(e=>Number(e.dataset.rowIndex));") == [2]
    frozen = web.query()
    web.named("Choose search patterns")
    web.write('#search-pattern-menu [aria-label="Find a pattern"]', name)
    button = web.script("return [...document.querySelectorAll('#search-pattern-menu button')].find(e=>e.getAttribute('aria-label')===arguments[0]);", f"Edit {name}")
    web.tap(button)
    wait_for(lambda: web.script("return !!document.querySelector('#search-pattern-menu [aria-label=\"Pattern definition JSON\"]');"),
             bool, "Edit keeps the selected pattern editor open")
    changed = {**definition, "pattern": "TOKEN-99", "examples": [{"text": "TOKEN-99", "values": ["TOKEN-99"]}]}
    web.write('[aria-label="Pattern definition JSON"]', json.dumps(changed))
    checkpoint = web.checkpoint()
    web.named("Save pattern", "#search-pattern-menu")
    wait_for(lambda: next((item for item in reversed(web.trace(after=checkpoint)) if item["name"] == "save_format"), None),
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


def _value_rows(web, columns):
    return web.script("""
      const ids=arguments[0],cell=(row,id)=>row.querySelector('[data-column-id="'+id+'"] .cell-text')?.textContent.trim()??'';
      return Object.fromEntries([...document.querySelectorAll('.data-row:not(.loading)')].map(row=>{
        const value=cell(row,ids.Value);return [value,{count:Number(cell(row,ids.Count).replaceAll(',','')),
          rows:Number(cell(row,ids.Rows).replaceAll(',',''))}];
      }));
    """, columns)


def _ranking(web, values, columns):
    web.results(len(values))
    return wait_for(lambda: _value_rows(web, columns), lambda rows: set(rows) == set(values), "ordinary CSV table displays extracted values")


def _scan(web, project_id, values):
    if web.script("return !!document.querySelector('#extraction-column-list');"):
        web.named("Close extraction columns", ".extract-panel")
    checkpoint = web.checkpoint()
    existing_tabs = set(web.script("return [...document.querySelectorAll('.project-tabs [role=tab]')].map(e=>e.id);"))
    web.write('.extract-panel [aria-label="Find an extraction pattern"]', "IPv4")
    _settle(web)
    web.script("""
      const bounds=()=>Object.fromEntries([
        ['search','.extract-panel [aria-label="Find an extraction pattern"]'],
        ['patterns','.extract-panel .pattern-list'],['table','.virtual-viewport'],['toolbar','.filters']
      ].map(([name,selector])=>{const r=document.querySelector(selector).getBoundingClientRect();
        return [name,{top:r.top,left:r.left,width:r.width,height:r.height}];}));
      const state=window.__extractLayout={samples:[bounds()],bounds,observer:null};
      state.observer=new MutationObserver(()=>state.samples.push(bounds()));
      state.observer.observe(document.querySelector('.extract-panel'),{attributes:true,subtree:true,childList:true});
    """)
    web.named("IPv4-like", ".extract-panel")
    entry = wait_for(lambda: next((item for item in reversed(web.trace(after=checkpoint))
        if item["name"] == "extract_values" and item["request"].get("projectId") == project_id), None),
        lambda value: bool(value and ("result" in value or "error" in value)), "real extraction creates a CSV")
    assert "result" in entry and "error" not in entry, entry
    new_tabs = wait_for(lambda: set(web.script("return [...document.querySelectorAll('.project-tabs [role=tab]')].map(e=>e.id);")) - existing_tabs,
                        bool, "generated CSV opens in a background file tab")
    assert len(new_tabs) == 1, new_tabs
    derived_id = new_tabs.pop().removeprefix("project-tab-")
    assert web.script("return document.querySelector('.project-tabs [role=tab][aria-selected=true]')?.id;") == f"project-tab-{project_id}"
    assert web.script("return !!document.querySelector('.extract-panel');"), "Extract remains open for another scan"
    assert not any(item["name"] in ("load_project", "query_project_rows")
                   and item["request"].get("projectId") == derived_id for item in web.trace(after=checkpoint)), "Background tabs retain metadata without loading row data"
    _settle(web)
    layout = web.script("""
      const s=window.__extractLayout;s.samples.push(s.bounds());s.observer.disconnect();
      delete window.__extractLayout;return s.samples;
    """)
    assert not web.script("return !!document.querySelector('.extract-panel .scan-progress');")
    assert len(layout) > 1, layout
    assert all(abs(sample[name][axis] - layout[0][name][axis]) <= 1
               for sample in layout for name in layout[0] for axis in layout[0][name]), "Extract must not move its controls, toolbar or table"
    detail = web.command("load_project", {"projectId": derived_id}, request=True)
    return {"id": derived_id, "request": entry["request"], "meta": detail["project"]["meta"], "expected_values": list(values)}


def _open_generated(web, case):
    derived_id = case["id"]
    web.tab(derived_id)
    web.applied(projectId=derived_id); web.results(len(case["expected_values"]))
    detail = web.command("load_project", {"projectId": derived_id}, request=True)
    columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
    assert list(columns) == ["Value", "Count", "Rows"], columns
    ranked = _ranking(web, case["expected_values"], columns)
    assert not web.script("return !!document.querySelector('.extract-view');"), "Results reuse the existing table"
    assert web.script("return document.querySelector('.applied-filter-status .temporary-result')?.textContent.includes('Temporary CSV');")
    case.update({"columns": columns, "values": ranked})


def _close_generated(web, case):
    button = web.script("return document.getElementById(arguments[0])?.parentElement.querySelector('.tab-close')??null;", f"project-tab-{case['id']}")
    assert button, case["id"]
    web.tap(button)
    wait_for(lambda: web.script("return !document.getElementById(arguments[0]);", f"project-tab-{case['id']}"),
             bool, "generated CSV tab closes")
    wait_for(lambda: Path(case["meta"]["source_path"]).exists(), lambda exists: not exists,
             "closing the temporary CSV removes its managed source copy")


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
    web.named("Export rows")
    web.tap(web.node('.export-dialog input[type="radio"][value="all"]'))
    checkbox = web.node('.export-dialog .export-annotations input')
    if web.script("return arguments[0].checked;", checkbox):
        web.tap(checkbox)
    checkpoint = web.checkpoint()
    web.named("Export…", ".export-dialog")
    entry = wait_for(lambda: next((item for item in reversed(web.trace(after=checkpoint)) if item["name"] == "export_project"), None),
        lambda value: bool(value and ("result" in value or "error" in value)), "normal CSV export writes extracted values")
    assert "result" in entry and "error" not in entry, entry
    assert entry["request"]["scope"] == "all" and entry["request"]["includeAnnotations"] is False
    assert web.script("return window.__extractPicker.count;") == 1
    web.script("window.__extractPicker.restore();delete window.__extractPicker;")
    with destination.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def verify_extract(app, data, root: Path, evidence: Path, report: dict):
    web = app.web
    source = root / f"extract-{uuid.uuid4().hex[:8]}.csv"
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["first", "second", "cohort", "network variants"])
        writer.writerows([
            ["10.0.0.1,10.0.0.1,192.0.2.1", "10.0.0.1 8.8.8.8", "keep",
             "10[.]0[.]0[.]1,192.0.2.1:443,192.0.2.0/24"],
            ["10.0.0.10", "", "drop", ""],
            ["no address TOKEN-42", "", "keep", ""],
        ])
    original = source.read_bytes()
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    generated = []
    custom_id = None
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"]); web.results(3)
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        columns = {item["label"]: item["id"] for item in detail["columns_meta"]}
        web.named("Choose search patterns")
        bundled = wait_for(lambda: web.script("""
          return [...document.querySelectorAll('#search-pattern-menu .pattern-row')]
            .filter(e=>e.querySelector('[aria-label^="Copy "]')).map(e=>e.querySelector('strong').textContent.trim());
        """), lambda names: "IPv4-like" in names, "built-in pattern catalogue is available")
        assert "IPv4-like" in bundled and "IPv6-like" in bundled, bundled
        assert not any("phone" in name.lower() or name.startswith('Defanged ') for name in bundled), bundled
        assert "Base64 candidate (32+ characters)" in bundled, bundled
        assert not any(name.startswith("Hex data candidate") for name in bundled), bundled
        assert not set(bundled).intersection({"BitLocker recovery-key candidate", "CVE ID", "JWT candidate",
            "PEM block candidate", "SSH public key", "IP endpoint", "IPv4 CIDR", "IPv6 CIDR"}), bundled
        presentation = web.script("""
          const button=[...document.querySelectorAll('#search-pattern-menu .pattern-select')]
            .find(e=>e.querySelector('strong')?.textContent.trim()==='IPv4-like'),
            expression=button.querySelector('.pattern-expression');
          return {custom:button.textContent.includes('Custom'),
            weight:Number(getComputedStyle(expression).fontWeight),
            regexSize:parseFloat(getComputedStyle(expression).fontSize),
            nameSize:parseFloat(getComputedStyle(button.querySelector('strong')).fontSize)};
        """)
        assert not presentation["custom"] and presentation["weight"] <= 400, presentation
        assert presentation["regexSize"] < presentation["nameSize"], presentation
        _pattern(web, "IPv4-like"); web.results(2)
        assert web.script("return [...document.querySelectorAll('.data-row:not(.loading)')].map(e=>Number(e.dataset.rowIndex));") == [0, 1]
        assert web.script("return document.querySelector('[aria-label=\"Search rows\"]').value;") == ""
        web.named("Choose search columns")
        checkbox = web.script("return [...document.querySelectorAll('#search-column-menu label')].find(e=>e.textContent.trim()==='cohort')?.querySelector('input');")
        web.tap(checkbox); web.results(0)
        web.named("All data columns", "#search-column-menu"); web.results(2)
        web.named("Done", "#search-column-menu")
        web.named("Remove pattern IPv4-like"); web.results(3)
        custom_id = _custom_pattern(web, f"Native token {project_id[:8]}")
        _action(web, columns["second"], "Hide this column")
        assert not web.script("return !!document.querySelector(arguments[0]);", f'.data-header [data-column-id="{columns["second"]}"]')
        web.write('[aria-label="Search rows"]', "cohort:keep"); _key(web, "\ue007")
        web.applied(search="cohort:keep"); web.results(2)
        saved_query = web.query()
        project_ids = {item["meta"]["id"] for item in web.command("list_projects")}

        _extract_panel(web)
        presentation = web.script("""
          const panel=document.querySelector('.extract-panel'),body=panel.closest('.analysis-body'),
            source=panel.querySelector('.scope-setting'),select=source.querySelector('select'),
            trigger=panel.querySelector('.column-trigger'),search=panel.querySelector('[aria-label="Find an extraction pattern"]'),
            list=panel.querySelector('.pattern-list'),rect=e=>e.getBoundingClientRect(),padding=parseFloat(getComputedStyle(body).paddingBottom);
          return {help:panel.querySelector('.extract-help').textContent.trim(),label:source.childNodes[0].textContent.trim(),
            options:[...select.options].map(e=>e.textContent.trim()),sameRow:Math.abs(rect(select).top-rect(trigger).top)<=1,
            sourceWidth:rect(select).width,gap:rect(list).top-rect(search).bottom,
            bottom:rect(list).bottom,expectedBottom:rect(body).bottom-padding,
            outerScroll:body.scrollHeight>body.clientHeight+1};
        """)
        assert presentation["help"] == "Extract matching values into a background tab, grouped by value.", presentation
        assert presentation["label"] == "From" and presentation["options"] == ["Current results", "All rows"], presentation
        assert presentation["sameRow"] and presentation["sourceWidth"] <= 140 and presentation["gap"] >= 8, presentation
        assert abs(presentation["bottom"]-presentation["expectedBottom"]) <= 1 and not presentation["outerScroll"], presentation
        assert web.script("return document.querySelector('[aria-label=\"Extraction rows\"]').value;") == "filtered"
        assert web.script("""
          const buttons=[...document.querySelectorAll('.extract-panel .pattern-button')],
            ipv4=buttons.find(e=>e.getAttribute('aria-label')==='IPv4-like'),
            custom=buttons.find(e=>e.getAttribute('aria-label')===arguments[0]);
          return ipv4?.textContent.includes('10.0.0.1')&&!ipv4.textContent.includes('Custom')&&
            custom?.textContent.includes('Custom')&&custom.textContent.includes('TOKEN-99');
        """, f"Native token {project_id[:8]}"), "Extract shows example values and marks edited user definitions"
        assert web.script("""
          return [...document.querySelectorAll('.extract-panel .pattern-button')].every(button=>{
            const bounds=button.getBoundingClientRect(),sample=button.querySelector('.pattern-example');
            return !sample||sample.getBoundingClientRect().bottom<=bounds.bottom-2;
          });
        """), "Example captions must fit inside their buttons when the list scrolls"
        web.screenshot(evidence / "extract-pattern-examples.png")
        before_picker = web.script("const r=document.querySelector('.extract-panel .pattern-list').getBoundingClientRect();return {top:r.top,height:r.height};")
        web.named("Choose extraction columns", ".extract-panel")
        picker = web.script("""
          const menu=document.querySelector('#extraction-column-list'),body=menu.closest('.analysis-body'),
            m=menu.getBoundingClientRect(),b=body.getBoundingClientRect(),list=document.querySelector('.extract-panel .pattern-list').getBoundingClientRect();
          return {overlay:getComputedStyle(menu).position,left:m.left,right:m.right,bodyLeft:b.left,bodyRight:b.right,
            list:{top:list.top,height:list.height}};
        """)
        assert picker["overlay"] == "absolute" and picker["list"] == before_picker, picker
        assert picker["left"] >= picker["bodyLeft"] and picker["right"] <= picker["bodyRight"], picker
        _key(web, "\ue00c")
        wait_for(lambda: web.script("return !document.querySelector('#extraction-column-list')&&!!document.querySelector('.extract-panel')&&document.activeElement.getAttribute('aria-label')==='Choose extraction columns';"),
                 bool, "Escape closes only the extraction column popover and restores its trigger focus")
        web.named("Choose extraction columns", ".extract-panel")
        web.named("Deselect all", ".extract-panel")
        wait_for(lambda: web.script("return document.querySelectorAll('.extract-panel .pattern-button').length;"),
                 lambda count: count > 0, "extraction pattern buttons are loaded")
        assert web.script("return [...document.querySelectorAll('.extract-panel .pattern-button')].every(e=>e.disabled);")
        web.named("Select all", ".extract-panel")
        variants = {"10[.]0[.]0[.]1": {"count": 1, "rows": 1},
            "192.0.2.1:443": {"count": 1, "rows": 1}, "192.0.2.0/24": {"count": 1, "rows": 1}}
        first = _scan(web, project_id, ["10.0.0.1", "192.0.2.1", "8.8.8.8", *variants])
        generated.append(first)
        assert set(first["request"]["columns"]) == set(columns.values())
        assert first["request"]["search"] == "cohort:keep" and first["request"]["scope"] == "filtered"
        assert {item["meta"]["id"] for item in web.command("list_projects")} == project_ids
        assert Path(first["meta"]["source_path"]).is_file()
        assert web.script("return document.querySelectorAll('.project-tabs [role=tab][aria-selected=true]').length;") == 1
        assert web.query() == saved_query
        assert web.script("return document.querySelector('[aria-label=\"Extraction rows\"]').value;") == "filtered"
        web.choose('[aria-label="Extraction rows"]', "all")
        second = _scan(web, project_id, ["10.0.0.1", "10.0.0.10", "192.0.2.1", "8.8.8.8", *variants])
        generated.append(second)
        assert web.script("return document.querySelector('[aria-label=\"Extraction rows\"]').value;") == "all"
        assert web.query() == saved_query
        _open_generated(web, first)
        assert first["values"] == {"10.0.0.1": {"count": 3, "rows": 1},
            "192.0.2.1": {"count": 1, "rows": 1}, "8.8.8.8": {"count": 1, "rows": 1}, **variants}
        web.tab(project_id); web.results(2)
        assert web.query() == saved_query
        assert web.script("return !!document.querySelector('.extract-panel');"), "Returning to the source restores Extract"
        assert web.script("return document.querySelector('[aria-label=\"Extraction rows\"]').value;") == "all"
        _open_generated(web, second)
        assert sum(value["count"] for value in second["values"].values()) == 9
        web.tap(web.node(f'.data-header [data-column-id="{second["columns"]["Count"]}"] .column-sort'))
        web.applied(sortKey=second["columns"]["Count"], sortDirection="asc"); web.results(7)
        web.tap(web.node(f'.data-header [data-column-id="{second["columns"]["Count"]}"] .column-sort'))
        web.applied(sortKey=second["columns"]["Count"], sortDirection="desc"); web.results(7)
        # Virtual row slots are recycled; DOM insertion order is not the
        # current visual/result order. Check the actual first result position.
        wait_for(lambda: web.script("""
          return document.querySelector('.data-row[data-position="0"] [data-column-id="'+arguments[0]+'"] .cell-text')?.textContent.trim()??null;
        """, second["columns"]["Value"]), lambda value: value == "10.0.0.1", "numeric Count descending puts the most frequent value first")
        web.write('[aria-label="Search rows"]', "Value:8.8.8.8"); _key(web, "\ue007")
        web.applied(search="Value:8.8.8.8"); web.results(1)
        destination = root / "trivium-extract-ipv4-native.csv"
        exported = _export(web, destination)
        expected = {value: {"Value": value, "Count": str(counts["count"]), "Rows": str(counts["rows"])}
                    for value, counts in second["values"].items()}
        assert {row["Value"]: row for row in exported} == expected, exported
        web.screenshot(evidence / "extract-ipv4-csv-tab.png")

        web.tab(project_id); web.results(2)
        assert web.query() == saved_query
        assert web.script("return !!document.querySelector('.extract-panel');")
        assert web.script("return document.querySelector('[aria-label=\"Extraction rows\"]').value;") == "all"
        web.named("Choose extraction columns", ".extract-panel")
        web.named("Deselect all", ".extract-panel")
        second_column = web.script("return [...document.querySelectorAll('#extraction-column-list label')].find(e=>e.textContent.trim()==='second')?.querySelector('input');")
        web.tap(second_column)
        third = _scan(web, project_id, ["10.0.0.1", "8.8.8.8"])
        generated.append(third)
        assert third["request"]["columns"] == [columns["second"]]
        _open_generated(web, third)
        assert all(value["count"] == value["rows"] == 1 for value in third["values"].values())
        web.tab(project_id); web.results(2)
        assert web.script("return !!document.querySelector('.extract-panel');")
        assert web.script("return document.querySelector('[aria-label=\"Choose extraction columns\"] .column-count').textContent.trim();") == f"1/{len(columns)}"
        assert web.script("return document.querySelector('[aria-label=\"Extraction rows\"]').value;") == "all"
        _close_generated(web, third)
        web.tab(second["id"]); web.results(1)
        assert source.read_bytes() == original
        before_close = web.diagnostics()
        target = NormalWindowClose(app.application)
        target.request()
        wait_for(target.exited, bool, "normal window close removes all remaining temporary CSVs before exiting")
        app.end_session(captured_diagnostics=before_close)
        web = app.start()
        assert all(not Path(item["meta"]["source_path"]).exists() for item in generated)
        assert {item["meta"]["id"] for item in web.command("list_projects")} == project_ids
        assert source.read_bytes() == original and source.is_file()
        report["extract"] = {"named_pattern_search": True, "shared_search_columns": True,
            "subtle_search_expression": True, "example_values_and_custom_markers": True,
            "custom_pattern_save_and_edit": True, "applied_pattern_is_frozen": True,
            "filtered_and_all_row_scopes": True, "zero_columns_disabled": True,
            "compact_from_and_columns_row": True, "column_popover_preserves_list_layout": True,
            "column_popover_escape_keeps_extract_open": True, "pattern_list_fills_drawer_height": True,
            "combined_network_formats_keep_distinct_raw_values": True,
            "all_columns_include_hidden": True, "selected_hidden_column_only": True,
            "adjacent_values_and_cross_column_counts": True, "output_columns": ["Value", "Count", "Rows"],
            "ordinary_csv_tabs": len(generated), "recent_unchanged": True,
            "successive_background_result_tabs": True, "source_extract_panel_and_inputs_restored": True,
            "stable_extract_panel_toolbar_and_table_bounds": True,
            "ordinary_numeric_sort_and_search": True, "standard_gui_export_complete_csv": True,
            "closed_tab_source_copy_removed": True, "normal_exit_cleans_active_and_inactive_csvs": True,
            "same_profile_restart": True, "source_bytes_unchanged": True,
            "unverified": "Native Save dialog interaction; only its chosen path is supplied"}
        _settle(web)
    finally:
        failed = sys.exc_info()[0] is not None
        if web.session:
            web.script("window.__extractLayout?.observer?.disconnect();delete window.__extractLayout;")
            web.script("window.__extractPicker?.restore();delete window.__extractPicker;")
        if web.session and not failed:
            web.close_drawer()
            for item in generated:
                if web.script("return !!document.getElementById(arguments[0]);", f"project-tab-{item['id']}"):
                    _close_generated(web, item)
            # The restarted app has no open tabs. Reopen the original saved
            # fixture before removing only this test's ordinary source case.
            open_project(web, data["name"])
            # Earlier scenarios intentionally save nonempty filters. Restoring
            # the original case must preserve them, including a valid 0 rows.
            web.applied(projectId=data["id"])
            if web.script("return !!document.getElementById(arguments[0]);", f"project-tab-{project_id}"):
                web.named(f"Close {source.name}")
            delete_fixture_project(web, project_id)
            if custom_id:
                web.command("delete_format", {"id": custom_id})
