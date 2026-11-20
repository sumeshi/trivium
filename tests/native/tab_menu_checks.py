"""Real tab context actions preserve saved cases, drafts and the kept view."""
from __future__ import annotations

from pathlib import Path
import uuid

from column_pin_checks import _key
from feedback_checks import _settle
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from runtime import context_click


def _tabs(web) -> list[str]:
    return web.script("return [...document.querySelectorAll('.project-tabs [role=tab]')].map(e=>e.id.replace('project-tab-','')); ")


def _active(web) -> str:
    return web.script("return document.querySelector('.project-tabs [role=tab][aria-selected=true]')?.id?.replace('project-tab-','')??'';")


def _menu(web, owner: str, keyboard: bool = False) -> str:
    tab = web.node(f"#project-tab-{owner}")
    if keyboard:
        web.script("arguments[0].focus({preventScroll:true});", tab)
        web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "tab-context-keyboard", "actions": [
            {"type": "keyDown", "value": "\ue008"}, {"type": "keyDown", "value": "\ue03a"},
            {"type": "keyUp", "value": "\ue03a"}, {"type": "keyUp", "value": "\ue008"}]}]})
        mode = "WebDriver Shift+F10"
    else:
        point = web.script("const e=arguments[0],r=e.getBoundingClientRect(),x=Math.round(r.left+r.width/2),y=Math.round(r.top+r.height/2);return {x,y,hit:e.contains(document.elementFromPoint(x,y))};", tab)
        assert point.pop("hit"), point
        mode = context_click(web, point)
    wait_for(lambda: web.script("return [...document.querySelectorAll('.project-tab-context-menu [role=menuitem]')].map(e=>e.textContent.trim());"),
             lambda items: items == ["Close", "Close others", "Close tabs to the right", "Close all"], "tab context actions open")
    _settle(web)
    return mode


def verify_tab_close_menu(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    base = data["id"]
    web.close_drawer()
    assert _tabs(web) == [base], "The menu fixture starts with the base case alone"
    restore_query, restore_total = web.query(), web.state()["total"]
    def annotation():
        fields = ["flag", "memo", "tags", "bookmarked"]
        rows = web.command("query_project_rows", {"projectId": base, "offset": 0,
            "limit": min(data["total"], 1000), "columns": [data["columns"]["source_id"]]})["rows"]
        row = next((row for row in rows if any(row.get(key) for key in fields)), rows[0])
        return {"row_index": row["row_index"], **{key: row.get(key) for key in fields}}
    original_annotation = annotation()
    cases = []
    try:
        web.write('input[aria-label="Search rows"]', "source_id:r-0000001")
        web.applied(projectId=base, search="source_id:r-0000001")
        outer_query = web.query()
        outer_total = web.command("query_project_rows", {**outer_query, "offset": 0, "limit": 1})["total_filtered_rows"]
        web.results(outer_total)
        for label in ["b", "c"]:
            source = root / f"tab-menu-{label}-{uuid.uuid4().hex[:8]}.csv"
            source.write_bytes(f"host,event\n{label},one\n{label},two\n".encode())
            case = web.command("create_project", {"path": str(source)})
            cases.append({"id": case["meta"]["id"], "name": case["meta"]["name"], "source": source, "bytes": source.read_bytes()})
            open_project(web, case["meta"]["name"])
            web.results(2)
        b, c = cases
        expected = [base, b["id"], c["id"]]
        assert _tabs(web) == expected and _active(web) == c["id"]
        web.drawer("IOC rules")
        web.named("Add rule", ".ioc-panel")
        web.write('[aria-label="Rule query 1"]', "host:c")
        wait_for(lambda: web.script("return ![...document.querySelectorAll('.ioc-footer button')].find(e=>e.textContent.trim()==='Discard').disabled;"),
                 bool, "the IOC definition is an unsaved draft")
        pointer_mode = _menu(web, base)
        assert _active(web) == c["id"], "Context-clicking an inactive tab must not select it"
        web.screenshot(evidence / "project-tab-close-menu.png")
        _key(web, "\ue00c")
        wait_for(lambda: web.script("return !document.querySelector('.project-tab-context-menu')&&document.activeElement.id===arguments[0];", f"project-tab-{base}"),
                 bool, "Escape returns focus to the inactive context tab")
        assert _active(web) == c["id"]
        _menu(web, base)
        checkpoint = len(web.trace())
        web.named("Close others", ".project-tab-context-menu")
        wait_for(lambda: web.view().get("toast", ""), lambda text: "Save or discard your IOC rules" in text,
                 "an unsaved active IOC draft blocks the whole batch")
        assert _tabs(web) == expected and _active(web) == c["id"]
        assert not any(item["name"] in {"load_project", "delete_project"} for item in web.trace()[checkpoint:])
        web.named("Discard", ".ioc-panel")
        wait_for(lambda: web.script("return document.querySelectorAll('.ioc-row').length;"), lambda count: count == 0, "discard removes the unsaved IOC rule")

        web.tab(base)
        web.applied(**outer_query); web.results(outer_total)
        _menu(web, b["id"])
        assert _active(web) == base
        checkpoint = len(web.trace())
        web.named("Close tabs to the right", ".project-tab-context-menu")
        wait_for(lambda: _tabs(web), lambda tabs: tabs == [base, b["id"]], "Close right keeps the context tab and all tabs to its left")
        assert _active(web) == base and web.query() == outer_query
        assert not any(item["name"] == "load_project" for item in web.trace()[checkpoint:]), "Closing inactive right-hand tabs does not load another case"
        open_project(web, c["name"])
        web.results(2)

        _menu(web, base)
        assert _active(web) == c["id"]
        checkpoint = len(web.trace())
        web.named("Close others", ".project-tab-context-menu")
        wait_for(lambda: _tabs(web), lambda tabs: tabs == [base], "Close others keeps exactly the context tab")
        web.applied(**outer_query); web.results(outer_total)
        assert _active(web) == base and web.query() == outer_query
        calls = web.trace()[checkpoint:]
        assert not any(item["name"] == "load_project" and item["request"].get("projectId") in {b["id"], c["id"]} for item in calls), "A batch must not load a tab that it is closing"
        assert not any(item["name"] == "delete_project" for item in calls), "Closing saved tabs never deletes their cases"
        saved = {case["meta"]["id"] for case in web.command("list_projects")}
        assert set(expected).issubset(saved)
        assert all(case["source"].read_bytes() == case["bytes"] for case in cases)

        keyboard_mode = _menu(web, base, keyboard=True)
        disabled = web.script("return Object.fromEntries([...document.querySelectorAll('.project-tab-context-menu [role=menuitem]')].map(e=>[e.textContent.trim(),e.disabled]));")
        assert disabled["Close others"] and disabled["Close tabs to the right"] and not disabled["Close all"]
        web.named("Close all", ".project-tab-context-menu")
        wait_for(lambda: _tabs(web), lambda tabs: tabs == [], "Close all leaves no open tabs")
        _settle(web)
        assert set(expected).issubset({case["meta"]["id"] for case in web.command("list_projects")})
        open_project(web, data["name"])
        web.applied(**outer_query); web.results(outer_total)
        assert web.query() == outer_query and annotation() == original_annotation
        report["tab_close_menu"] = {"pointer": pointer_mode, "keyboard": keyboard_mode,
            "inactive_context_keeps_selection": True, "escape_restores_context_focus": True,
            "dirty_active_draft_blocks_entire_batch": True, "close_right_preserves_left_active_tab": True,
            "close_others_keeps_context": True, "closed_saved_cases_not_loaded_or_deleted": True,
            "close_all_then_recent_restores_filters_and_annotations": True}
    finally:
        if web.script("return !!document.querySelector('.project-tab-context-menu');"):
            _key(web, "\ue00c")
        if web.script("return !!document.querySelector('.ioc-panel')&&[...document.querySelectorAll('.ioc-footer button')].some(e=>e.textContent.trim()==='Discard'&&!e.disabled);"):
            web.named("Discard", ".ioc-panel")
        web.close_drawer()
        if base in _tabs(web):
            web.tab(base)
        else:
            open_project(web, data["name"])
        web.applied(projectId=base)
        if web.query()["search"] != restore_query["search"]:
            web.write('input[aria-label="Search rows"]', restore_query["search"])
        web.applied(**restore_query); web.results(restore_total)
        for case in cases:
            if case["id"] in _tabs(web):
                web.named(f"Close {case['source'].name}")
            if any(item["meta"]["id"] == case["id"] for item in web.command("list_projects")):
                delete_fixture_project(web, case["id"])
