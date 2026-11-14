"""Preset portability and explicit IOC annotation through the real bulk engine."""
from __future__ import annotations

import json
from pathlib import Path
import uuid

from column_pin_checks import _key
from column_presentation_checks import _focus_resize, _order, _reorder, _width
from feedback_checks import _settle
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS


def _picker(web, destination: Path) -> None:
    web.script("window.__presetIocPicker.path=arguments[0];", str(destination))


def _open_iocs(web) -> None:
    web.drawer("IOC rules")
    web.named("IOC Rules", ".filter-ioc")
    wait_for(lambda: web.script("return !!document.querySelector('.ioc-dialog');"), bool, "IOC editor opens")


def _rows(web, project_id: str) -> list[dict]:
    return web.command("query_project_rows", {"projectId": project_id, "offset": 0, "limit": 10})["rows"]


def _load_preset(web, source: Path) -> None:
    start = len(web.trace())
    _picker(web, source)
    web.named("Load preset file", "#analysis-drawer")
    wait_for(lambda: [entry for entry in web.trace()[start:]
                     if entry["name"] == "set_hidden_columns" and "result" in entry],
             bool, "the loaded preset's presentation is applied")
    wait_for(lambda: web.script("return document.querySelector('#analysis-drawer').textContent;"),
             lambda text: "Preset applied." in text, "preset application completes")
    _settle(web)


def _layout_presets(web, ids: dict[str, str], preset: dict, plain_path: Path, root: Path) -> dict:
    web.close_drawer()
    original_query = web.query()
    defaults = {column: _width(web, column)["header"] for column in ids.values()}
    source_order = [ids[name] for name in ["host", "message", "score"]]
    custom_order = [ids[name] for name in ["score", "host", "message"]]
    _reorder(web, ids["score"], ids["host"], custom_order)
    _focus_resize(web, ids["host"])
    _key(web, "\ue014", 2)
    custom_width = defaults[ids["host"]] + 20
    wait_for(lambda: _width(web, ids["host"]),
             lambda widths: all(abs(width - custom_width) <= 1 for width in widths.values()),
             "custom width affects both header and data cells")
    assert _order(web) == custom_order, _order(web)
    web.drawer("Presets")
    changed_path = root / "changed-layout-preset.json"
    _picker(web, changed_path)
    web.named("Save preset file", "#analysis-drawer")
    wait_for(changed_path.exists, bool, "the custom-layout preset is written")
    saved = json.loads(changed_path.read_text(encoding="utf-8"))
    assert saved["columnOrder"] == custom_order, saved
    assert saved["columnWidths"] == {ids["host"]: custom_width}, saved

    web.close_drawer()
    _reorder(web, ids["score"], ids["message"], source_order, after=True)
    _focus_resize(web, ids["host"])
    _key(web, "\ue014")
    wait_for(lambda: _width(web, ids["host"])["header"],
             lambda width: abs(width - custom_width - 10) <= 1, "the current layout differs from the saved preset")
    assert _order(web) == source_order, _order(web)
    web.drawer("Presets")
    _load_preset(web, changed_path)
    wait_for(lambda: _order(web), lambda order: order == custom_order, "loading the preset restores column order")
    wait_for(lambda: _width(web, ids["host"]),
             lambda widths: all(abs(width - custom_width) <= 1 for width in widths.values()),
             "loading the preset restores column width")

    # Earlier files omit the optional layout fields and must not reset a
    # user's current arrangement. Explicit empty values request defaults.
    _load_preset(web, plain_path)
    assert _order(web) == custom_order
    assert all(abs(width - custom_width) <= 1 for width in _width(web, ids["host"]).values())
    reset_path = root / "reset-layout-preset.json"
    reset_path.write_text(json.dumps({**preset, "columnOrder": [], "columnWidths": {}}), encoding="utf-8")
    _load_preset(web, reset_path)
    wait_for(lambda: _order(web), lambda order: order == source_order, "explicit empty order restores source order")
    wait_for(lambda: _width(web, ids["host"]),
             lambda widths: all(abs(width - defaults[ids["host"]]) <= 1 for width in widths.values()),
             "explicit empty widths restore automatic widths")
    assert web.query() == original_query, "Column layout presets do not change the search scope"
    return {"custom_order": custom_order, "changed_widths_only": saved["columnWidths"],
            "save_modify_load_restores_layout": True, "omitted_layout_preserves_current": True,
            "explicit_empty_layout_resets": True}


def verify_presets_iocs(web, data, root: Path, evidence: Path, report: dict) -> None:
    source = root / f"preset-ioc-{uuid.uuid4().hex[:8]}.csv"
    source.write_text("host,message,score\nalpha,beacon,1\nbeta,clear,2\ngamma,beacon,3\n", encoding="utf-8")
    original_source = source.read_bytes()
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    columns = case["meta"]["columns"]
    ids = {column["source"]: column["id"] for column in columns}
    web.command("update_flag", {"projectId": project_id, "row_index": 0, "flag": "safe", "memo": "human note", "tags": ["manual"], "bookmarked": True})
    open_project(web, case["meta"]["name"])
    web.results(3)
    assert web.script("""
      const previous=window.fetch;
      window.__presetIocPicker={path:null,calls:[],restore:()=>{window.fetch=previous;}};
      window.fetch=function(input,options,...rest){
    """ + IPC_COMMAND_JS + """
        if((name==='plugin:dialog|open'||name==='plugin:dialog|save')&&window.__presetIocPicker.path!==null){
          let args={};try{args=JSON.parse(typeof options.body==='string'?options.body:new TextDecoder().decode(options.body));}catch{}
          window.__presetIocPicker.calls.push({name,options:args.options});
          const path=window.__presetIocPicker.path;window.__presetIocPicker.path=null;
          return Promise.resolve(new Response(JSON.stringify(path),{headers:{'Tauri-Response':'ok','Content-Type':'application/json'}}));
        }
        return previous.call(window,input,options,...rest);
      };return true;
    """), "Only native picker responses are substituted"
    try:
        web.drawer("Presets")
        preset_path = root / "portable-preset.json"
        _picker(web, preset_path)
        web.named("Save preset file", "#analysis-drawer")
        wait_for(lambda: preset_path.exists(), bool, "preset is written by the native engine")
        wait_for(lambda: web.script("return document.querySelector('#analysis-drawer').textContent;"), lambda text: "Preset saved." in text, "preset save completes in the editor")
        saved = json.loads(preset_path.read_text(encoding="utf-8"))
        picker = web.script("return window.__presetIocPicker.calls[0];")
        assert picker["options"]["defaultPath"] == f"trivium-preset-{source.stem}.json", picker
        assert saved["version"] == 2 and [item["source"] for item in saved["columns"]] == ["host", "message", "score"], saved
        assert "columnOrder" not in saved and "columnWidths" not in saved, "Default layouts need no preset overrides"
        layout = _layout_presets(web, ids, saved, preset_path, root)
        # A reference the file cannot identify must not partially replace the view.
        invalid = {**saved, "condition": {"column": "not-known", "op": "eq", "values": ["alpha"], "negated": False},
            "hiddenColumns": [ids["score"]], "sortKey": ids["message"], "sortDirection": "desc"}
        invalid_path = root / "unknown-preset.json"
        invalid_path.write_text(json.dumps(invalid), encoding="utf-8")
        before = web.query()
        _picker(web, invalid_path)
        web.named("Load preset file", "#analysis-drawer")
        wait_for(lambda: web.script("return document.querySelector('#analysis-drawer').textContent;"),
            lambda text: "Unknown columns: not-known" in text, "unknown preset reference is rejected before applying")
        assert web.query() == before and web.visible_column_count() == 3
        valid = {**invalid, "condition": {"column": ids["host"], "op": "contains", "values": ["alpha"], "negated": False}}
        valid_path = root / "valid-preset.json"
        valid_path.write_text(json.dumps(valid), encoding="utf-8")
        _picker(web, valid_path)
        web.named("Load preset file", "#analysis-drawer")
        web.applied(condition=valid["condition"], sortKey=ids["message"], sortDirection="desc")
        web.results(1)
        assert web.visible_column_count() == 2
        # Reset query through the ordinary clear-all control. IOC scope is
        # deliberately all rows, even while the table has a narrower search.
        web.close_drawer()
        web.named("Clear all filters")
        web.results(3)
        web.write('input[aria-label="Search rows"]', "clear")
        web.applied(search="clear")
        web.results(1)
        _open_iocs(web)
        web.named("Add rule", ".ioc-dialog")
        web.write('input[aria-label="Rule tag 1"]', "IOC")
        web.write('input[aria-label="Rule query 1"]', "message:beacon")
        assert not web.script("return [...document.querySelectorAll('.ioc-rule-actions button')].find(e=>e.textContent.trim()==='Apply').disabled;")
        assert not web.script("return [...document.querySelectorAll('.ioc-dialog button')].some(e=>e.textContent.trim()==='Preview');")
        help_text = web.script("return document.querySelector('[aria-label=\"Rule query 1\"]').title;")
        assert all(term in help_text for term in ["AND", "OR", "-noise", '"failed login"', "column:value", "@tag:review", "@tag:*"]), help_text
        assert help_text == web.script("return document.querySelector('input[aria-label=\"Search rows\"]').title;"), "IOC queries share the main search's full help"
        web.named("Save rules", ".ioc-dialog")
        wait_for(lambda: web.script("return !document.querySelector('.ioc-dialog');"), bool, "saving definitions closes the editor")
        web.results(1)
        original = _rows(web, project_id)
        assert [row["flag"] for row in original] == ["safe", "", ""], original
        _open_iocs(web)
        assert web.script("return document.querySelector('[aria-label=\"Rule query 1\"]').value;") == "message:beacon"
        web.write('input[aria-label="Rule query 1"]', "")
        assert web.script("return [...document.querySelectorAll('.ioc-rule-actions button')].find(e=>e.textContent.trim()==='Apply').disabled;"), "A blank rule cannot apply"
        web.write('input[aria-label="Rule query 1"]', "message:beacon")
        start = len(web.trace())
        web.named("Apply", ".ioc-row")
        wait_for(lambda: web.script("return document.querySelector('.ioc-dialog').textContent;"), lambda text: "2 rows annotated." in text, "IOC apply stays open and reports its committed count")
        operations = [entry for entry in web.trace()[start:] if entry["name"] in {"bulk_preview", "bulk_update"}]
        assert [entry["name"] for entry in operations] == ["bulk_preview", "bulk_update"], operations
        assert all(entry["request"]["target"] == "filtered" and entry["request"]["search"] == "message:beacon"
                   for entry in operations), "A direct rule uses its own all-row query, independent of the current table search"
        assert operations[0]["result"]["count"] == 2
        applied = _rows(web, project_id)
        assert [row["flag"] for row in applied] == ["critical", "", "critical"], applied
        assert applied[0]["memo"] == "human note" and applied[0]["tags"] == ["manual", "IOC"] and applied[0]["bookmarked"], applied[0]
        web.named("Undo", ".ioc-footer")
        wait_for(lambda: web.script("return document.querySelector('.ioc-dialog').textContent;"), lambda text: "2 rows restored." in text, "IOC undo restores the complete previous annotation")
        assert _rows(web, project_id) == original
        export_path = root / "ioc-draft.csv"
        _picker(web, export_path)
        web.named("Export IOC rules", ".ioc-dialog")
        wait_for(lambda: export_path.exists(), bool, "native IOC export writes a guarded CSV")
        wait_for(lambda: web.script("return document.querySelector('.ioc-dialog').textContent;"), lambda text: "Rules exported." in text, "IOC export completes before closing")
        assert "critical,IOC,message:beacon" in export_path.read_text(encoding="utf-8")
        assert source.read_bytes() == original_source
        web.screenshot(evidence / "ioc-direct-apply-undo.png")
        web.named("Close IOC rules", ".ioc-dialog")
        report["presets_iocs"] = {"preset_default_name": picker["options"]["defaultPath"], "portable_column_metadata": True,
            "layout": layout,
            "unknown_reference_leaves_view_unchanged": True, "preset_rules_columns_sort_applied": True,
            "ioc_saved_without_annotations": True, "direct_apply_matches_all_rows": 2, "no_visible_preview": True,
            "query_help": help_text,
            "apply_preserves_other_fields": True, "undo_restores_complete_annotations": True, "native_csv_export": True}
    finally:
        web.script("window.__presetIocPicker?.restore();delete window.__presetIocPicker;")
        if web.script("return !!document.querySelector('.ioc-dialog');"):
            web.named("Close IOC rules", ".ioc-dialog")
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        web.named(f"Close {source.name}")
        web.command("delete_project", {"projectId": project_id}, request=True)
