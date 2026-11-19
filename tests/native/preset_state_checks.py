"""Literal preset conditions and column edits while native saves are pending."""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path

from row_navigation import open_project, wait_for


def _end_native_select(web) -> None:
    # Finish WebKit's native select interaction before subsequent XTest input.
    if os.name == "nt":
        return
    from ctypes import CDLL, c_int, c_ubyte, c_uint, c_ulong, c_void_p
    from feedback_checks import _settle
    from runtime import _x11_open

    x11, display = _x11_open()
    xtest = CDLL("libXtst.so.6")
    x11.XKeysymToKeycode.argtypes, x11.XKeysymToKeycode.restype = [c_void_p, c_ulong], c_ubyte
    xtest.XTestFakeKeyEvent.argtypes = [c_void_p, c_uint, c_int, c_ulong]
    try:
        escape = x11.XKeysymToKeycode(display, 0xFF1B)
        assert escape and xtest.XTestFakeKeyEvent(display, escape, 1, 0)
        assert xtest.XTestFakeKeyEvent(display, escape, 0, 0)
        x11.XSync(display, 0)
    finally:
        x11.XCloseDisplay(display)
    _settle(web)
    web.drawer("Conditions")


def _hidden(web, project_id: str) -> set[str]:
    return set(web.command("load_project", {"projectId": project_id}, request=True)["hidden_columns"])


def _view_hidden(web, project_id: str) -> set[str] | None:
    state = web.command("load_view_state", {"projectId": project_id}, request=True)["state"]
    return set(state["hiddenColumns"]) if state else None


def _saved_values(node: dict) -> list[list[str]]:
    if node["type"] == "condition":
        return [node.get("sourceValues")]
    return [values for child in node["children"] for values in _saved_values(child)]


def verify_preset_state(web, data: dict, case: dict, source: Path, ids: dict[str, str],
                        base: dict, base_path: Path, root: Path) -> dict:
    # Imported here because the caller owns the shared picker helpers.
    from preset_ioc_checks import _load_preset, _picker

    project_id = case["meta"]["id"]
    base_query = {"projectId": project_id, "condition": base.get("condition"),
        "sortKey": base.get("sortKey"), "sortDirection": "desc" if base.get("sortDirection") == "desc" else "asc"}
    literal = {"kind": "and", "children": [
        {"column": ids["host"], "op": "eq", "values": ["alpha", "gamma"], "negated": False},
        {"kind": "or", "children": [
            {"column": ids["message"], "op": "in", "values": ["beacon,clear", "beacon", " unused ", ""], "negated": False},
            {"column": ids["score"], "op": "contains", "values": [], "negated": False},
        ]},
    ]}
    literal_path = root / "literal-values-preset.json"
    literal_path.write_text(json.dumps({**base, "condition": literal}), encoding="utf-8")
    expected_values = [["alpha", "gamma"], ["beacon,clear", "beacon", " unused ", ""], []]
    checked = []
    try:
        web.drawer("Presets")
        _load_preset(web, literal_path)
        web.applied(condition=literal)
        web.results(2)
        exported = root / "literal-values-saved.json"
        _picker(web, exported)
        before_save = len(web.trace())
        web.named("Save preset file", "#analysis-drawer")
        wait_for(exported.exists, bool, "literal condition preset is saved")
        wait_for(lambda: web.script("return document.querySelector('#analysis-drawer')?.textContent??'';"),
                 lambda text: "Preset saved." in text, "literal preset save completes")
        assert json.loads(exported.read_text(encoding="utf-8"))["condition"] == literal
        saved_calls = [entry for entry in web.trace()[before_save:]
                       if entry["name"] == "save_preset" and "result" in entry]
        assert len(saved_calls) == 1 and saved_calls[0]["request"]["allowOverwrite"] is True

        def persisted_values():
            state = web.command("load_view_state", {"projectId": project_id}, request=True)["state"]
            return _saved_values(state["condition"]) if state else None

        wait_for(persisted_values, lambda values: values == expected_values,
                 "the autosaved editor keeps every literal value array")
        web.close_drawer()
        web.tab(data["id"])
        web.named(f"Close {source.name}")
        open_project(web, case["meta"]["name"])
        web.applied(condition=literal)
        web.results(2)
        web.drawer("Conditions")
        assert web.script("return document.querySelector('.condition-value').title;").startswith('Saved values: ["alpha","gamma"]')
        changed = copy.deepcopy(literal)
        changed["children"][0]["op"] = "in"
        web.choose('select[aria-label="Operator"]', "in")
        _end_native_select(web)
        web.applied(condition=changed)
        web.results(2)
        changed["children"][0]["values"] = ["alpha"]
        web.write('input[aria-label="Value"]', "alpha")
        web.applied(condition=changed)
        web.results(1)

        for index, (condition, expected_count) in enumerate([
            ({"column": ids["host"], "op": "in", "values": ["alpha,beta", "gamma", " beta ", ""], "negated": False}, 1),
            # An empty values array matches no rows. text_any also ignores an
            # empty needle, so negating contains [""] matches all three rows.
            ({"column": ids["host"], "op": "contains", "values": [], "negated": False}, 0),
            ({"column": ids["host"], "op": "contains", "values": [""], "negated": True}, 3),
        ]):
            path = root / f"literal-edge-{index}.json"
            path.write_text(json.dumps({**base, "condition": condition}), encoding="utf-8")
            web.drawer("Presets")
            _load_preset(web, path)
            web.applied(condition=condition)
            web.results(expected_count)
            checked.append({"values": condition["values"], "rows": expected_count})

        _load_preset(web, base_path)
        # Different conditions can produce the same count. Wait for the actual
        # query, beyond the filter debounce, before capturing its baseline.
        web.applied(**base_query)
        web.results(3)
        baseline_query = web.query()
        delayed_path = root / "delayed-column-preset.json"
        delayed_path.write_text(json.dumps({**base, "hiddenColumns": [ids["score"]]}), encoding="utf-8")
        web.script("window.__workspaceHold={name:'set_hidden_columns',projectId:arguments[0],ready:false};", project_id)
        _picker(web, delayed_path)
        web.named("Load preset file", "#analysis-drawer")
        wait_for(lambda: web.script("return !!window.__workspaceHold?.ready;"), bool,
                 "the native preset metadata response is held after commit")
        assert web.visible_column_count() == 2, "the entire preset must apply before waiting for metadata"
        actual_query = web.query()
        assert actual_query == baseline_query, {"actual": actual_query, "baseline": baseline_query}
        web.named("Choose visible columns")
        web.click(web.element(f'#column-{ids["host"]}'))
        web.named("Done", "#visible-column-menu")
        assert web.visible_column_count() == 1
        web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
        latest_hidden = {ids["score"], ids["host"]}
        wait_for(lambda: _hidden(web, project_id), lambda value: value == latest_hidden,
                 "the subsequent user column choice wins the native write queue")
        wait_for(lambda: _view_hidden(web, project_id), lambda value: value == latest_hidden,
                 "the same latest choice is stored in the saved view")
        assert web.visible_column_count() == 1
        actual_query = web.query()
        assert actual_query == baseline_query, {"actual": actual_query, "baseline": baseline_query}

        # Closing the panel during another save must not leave only the durable
        # hidden-column metadata updated while the visible investigation stays old.
        web.script("window.__workspaceHold={name:'set_hidden_columns',projectId:arguments[0],ready:false};", project_id)
        _picker(web, delayed_path)
        web.named("Load preset file", "#analysis-drawer")
        wait_for(lambda: web.script("return !!window.__workspaceHold?.ready;"), bool, "another preset save is held")
        assert web.visible_column_count() == 2
        web.close_drawer()
        web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
        wait_for(lambda: _hidden(web, project_id), lambda value: value == {ids["score"]},
                 "metadata matches the already applied view after its panel closes")
        wait_for(lambda: _view_hidden(web, project_id), lambda value: value == {ids["score"]},
                 "saved view remains consistent after closing the preset panel")
        assert web.visible_column_count() == 2
        actual_query = web.query()
        assert actual_query == baseline_query, {"actual": actual_query, "baseline": baseline_query}
        assert not web.script("return !!document.querySelector('#analysis-drawer');")
        return {"literal_arrays_saved_and_reopened": expected_values, "nested_rows": 2,
            "operator_edit_keeps_values": True, "text_edit_replaces_values": True,
            "edge_values": checked, "later_column_edit_wins_pending_save": True,
            "panel_close_preserves_consistent_view": True, "column_changes_preserve_query": True}
    finally:
        web.script("window.__workspaceHold?.release?.();delete window.__workspaceHold;")
        web.drawer("Presets")
        _load_preset(web, base_path)
        web.applied(**base_query)
        web.results(3)
