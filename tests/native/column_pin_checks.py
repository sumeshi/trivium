"""Real header-menu column visibility and cumulative frozen-prefix checks."""
from __future__ import annotations

from pathlib import Path
import time
import uuid

from feedback_checks import _horizontal_to, _settle
from row_navigation import open_project, wait_for


_GEOMETRY = """
const viewport=document.querySelector('.virtual-viewport'),v=viewport.getBoundingClientRect();
const canvas=document.createElement('canvas');canvas.width=canvas.height=1;const c=canvas.getContext('2d');
const alpha=value=>{c.clearRect(0,0,1,1);c.fillStyle=value;c.fillRect(0,0,1,1);return c.getImageData(0,0,1,1).data[3];};
const sample=e=>{const r=e.getBoundingClientRect(),s=getComputedStyle(e);
 const x=Math.min(v.right-2,r.right-2),y=Math.round(r.top+r.height/2),visible=x>=Math.max(v.left,r.left)+1;
 return {id:e.dataset.columnId,left:r.left,width:r.width,right:r.right,position:s.position,cssLeft:parseFloat(s.left),
   pinned:e.classList.contains('stickyData'),edge:e.classList.contains('lastFrozenData'),alpha:alpha(s.backgroundColor),background:s.backgroundColor,transition:s.transition,
   visible,topmost:!visible||e.contains(document.elementFromPoint(x,y))};};
return {scroll:viewport.scrollLeft,viewport:{left:v.left,right:v.right},
 staticWidth:[...document.querySelectorAll('.data-header>.sticky:not([data-column-id])')].reduce((n,e)=>n+e.getBoundingClientRect().width,0),
 headers:[...document.querySelectorAll('.data-header>[data-column-id]')].map(sample),
 cells:[...document.querySelector('.data-row:not(.loading)').querySelectorAll(':scope>[data-column-id]')].map(sample),
 annotationHeaders:[...document.querySelectorAll('.data-header>.sticky:not([data-column-id])')].map(sample),
 annotationCells:[...document.querySelector('.data-row:not(.loading)').querySelectorAll(':scope>.sticky:not([data-column-id])')].map(sample)};
"""


def _geometry(web):
    _settle(web)
    return web.script(_GEOMETRY)


def _action(web, label: str, name: str) -> None:
    _horizontal_to(web, 0)
    web.named(f"Actions for column {label}")
    web.named(name, ".column-context-menu")
    wait_for(lambda: web.script("return !document.querySelector('.column-context-menu');"), bool, "column action closes menu")


def _saved(web, project_id: str, predicate, description: str):
    return wait_for(lambda: web.command("load_view_state", {"projectId": project_id}, request=True)["state"],
                    lambda state: state is not None and predicate(state), description)


def _prefix(web, expected: list[str], description: str):
    value = wait_for(lambda: _geometry(web),
        lambda state: [cell["id"] for cell in state["headers"] if cell["pinned"]] == expected,
        description)
    for key in ("headers", "cells"):
        pinned = [cell for cell in value[key] if cell["pinned"]]
        assert [cell["id"] for cell in pinned] == expected, value
        left = value["viewport"]["left"] + value["staticWidth"]
        for cell in pinned:
            assert cell["position"] == "sticky" and cell["alpha"] == 255 and cell["topmost"], cell
            assert abs(cell["left"] - left) <= 1, (description, left, cell)
            assert abs(cell["cssLeft"] - (left - value["viewport"]["left"])) <= 1, cell
            left += cell["width"]
        assert [cell["id"] for cell in pinned if cell["edge"]] == expected[-1:], value
    return value


def _unchanged_query(web, start: int, before: dict) -> None:
    time.sleep(.35)  # Beyond the real query debounce: visibility/pins cannot reload data.
    assert web.query() == before, (before, web.query())
    reads = [entry for entry in web.trace()[start:] if entry["name"] in {"query_project_rows", "locate_project_row"}]
    assert not reads, reads


def _key(web, key: str, count: int = 1) -> None:
    if count < 1:
        return
    web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "column-pin-keyboard", "actions": [
        action for _ in range(count) for action in ({"type": "keyDown", "value": key}, {"type": "keyUp", "value": key})]}]})


def _focus_column(web, index: int, column_id: str):
    web.script("document.querySelector('.data-header [data-column-id] .column-sort').focus();")
    _key(web, "\ue004", index)
    _settle(web)
    state = _geometry(web)
    active = web.script("return document.activeElement.closest('[data-column-id]')?.dataset.columnId;")
    assert active == column_id, {"logical_index": index, "expected": column_id, "active": active, "geometry": state}
    target = next(item for item in state["headers"] if item["id"] == column_id)
    assert target["left"] >= state["viewport"]["left"] + state["staticWidth"] - 1, state
    assert target["right"] <= state["viewport"]["right"] + 1 and target["topmost"], state
    return state


def _oversized_prefix(web, data, evidence: Path, checks: dict) -> None:
    saved = _saved(web, data["id"], lambda state: state.get("frozenThrough") == "" and not state.get("hiddenColumns"), "ordinary unfrozen view ready for wide-column fixture")
    wide = [data["columns"][name] for name in ("source_id", "score", "cohort", "timestamp")]
    saved = {**saved, "columnWidths": {**saved.get("columnWidths", {}), **dict.fromkeys(wide, 450)}}
    web.named(f"Close {data['filename']}")
    wait_for(lambda: [entry for entry in web.trace() if entry["name"] == "save_view_state" and entry["request"].get("projectId") == data["id"]
        and "result" not in entry and "error" not in entry], lambda pending: not pending, "wide fixture waits for old saved writes")
    web.command("save_view_state", {"projectId": data["id"], "state": saved})
    open_project(web, data["name"]); web.results(data["total"])
    _focus_column(web, 3, wide[-1])
    _key(web, "\ue015")  # The focused header's actual keyboard context-menu action.
    before, checkpoint = web.query(), len(web.trace())
    web.named("Freeze columns through this column", ".column-context-menu")
    _horizontal_to(web, 1600)
    panned = _geometry(web)
    assert [item["id"] for item in panned["headers"] if item["pinned"]] == wide, panned
    assert all(abs(item["width"] - 450) <= 1 for item in panned["headers"][:4]), panned
    assert sum(item["width"] for item in panned["headers"][:4]) + panned["staticWidth"] > panned["viewport"]["right"] - panned["viewport"]["left"], panned
    for key in ("headers", "cells"):
        end = next(item for item in panned[key] if item["id"] == wide[-1])
        assert end["left"] >= panned["viewport"]["left"] + panned["staticWidth"] - 1 and end["right"] <= panned["viewport"]["right"] + 1, end
        assert end["alpha"] == 255 and end["topmost"], end
    for key in ("annotationHeaders", "annotationCells"):
        left = panned["viewport"]["left"]
        for item in panned[key]:
            assert item["position"] == "sticky" and item["alpha"] == 255 and item["topmost"], item
            assert abs(item["left"] - left) <= 1, item
            left += item["width"]
    keyboard = [_focus_column(web, index, column_id) for index, column_id in enumerate(wide)]
    # The panned DOM contains only the horizontal window. Tab follows the
    # configured source order, including columns currently outside that window.
    following = next(column for column in data["columns"].values() if column not in wide)
    keyboard.append(_focus_column(web, 4, following))
    _unchanged_query(web, checkpoint, before)
    checks["oversized_prefix"] = {"requested_frozen_columns": wide, "trusted_wheel": panned,
        "first_configured_scrolling_column": following,
        "first_mounted_scrolling_column": next(item["id"] for item in panned["headers"] if not item["pinned"]),
        "keyboard_reveals_each_frozen_and_next_scrolling_column": keyboard}
    web.screenshot(evidence / "columns-oversized-frozen-prefix.png")


def verify_column_pinning(web, data, root: Path, evidence: Path, report: dict) -> None:
    web.close_drawer()
    web.results(data["total"])
    _horizontal_to(web, 0)
    original = _saved(web, data["id"], lambda state: True, "existing view available before column checks")
    columns = data["columns"]
    first, hidden, third, boundary = [columns[name] for name in ("source_id", "score", "cohort", "timestamp")]
    other = None
    checks = {}
    try:
        assert not original.get("hiddenColumns"), original
        assert original.get("frozenThrough") is None, "Existing views retain the original first-visible-column default"
        checks["legacy_default"] = _prefix(web, [first], "legacy view freezes first visible data column")
        before, checkpoint = web.query(), len(web.trace())
        _action(web, "score", "Hide this column")
        wait_for(lambda: {"headers": web.state()["headers"], "count": web.visible_column_count()},
            lambda state: "score" not in state["headers"] and state["count"] == 26,
            "header hide action removes only its column from the configured set")
        _unchanged_query(web, checkpoint, before)
        _saved(web, data["id"], lambda state: state.get("hiddenColumns") == [hidden], "hidden column persisted")

        checkpoint = len(web.trace())
        _action(web, "cohort", "Freeze columns through this column")
        _prefix(web, [first, third], "arbitrary third source column freezes visible prefix")
        _horizontal_to(web, 700)
        checks["third_boundary_hidden_width_omitted"] = _prefix(web, [first, third], "hidden preceding column leaves no frozen gap")
        _unchanged_query(web, checkpoint, before)

        checkpoint = len(web.trace())
        _action(web, "timestamp", "Freeze columns through this column")
        _saved(web, data["id"], lambda state: state.get("frozenThrough") == boundary, "fourth-column frozen boundary persisted")
        _horizontal_to(web, 700)
        checks["fourth_boundary"] = _prefix(web, [first, third, boundary], "timestamp and all visible predecessors remain fixed")
        moving = next(cell for cell in checks["fourth_boundary"]["headers"] if not cell["pinned"])
        assert moving["position"] != "sticky", moving
        _unchanged_query(web, checkpoint, before)
        web.screenshot(evidence / "columns-frozen-through-timestamp.png")

        checkpoint = len(web.trace())
        _action(web, "timestamp", "Hide this column")
        checks["hidden_boundary"] = _prefix(web, [first, third], "hiding boundary retains its visible frozen predecessors")
        _saved(web, data["id"], lambda state: state.get("frozenThrough") == boundary and boundary in state.get("hiddenColumns", []),
            "hidden boundary is retained in saved settings")
        web.named("Choose visible columns")
        web.click(web.script("return document.getElementById('column-'+arguments[0]);", boundary))
        web.named("Done", "#visible-column-menu")
        _prefix(web, [first, third, boundary], "showing frozen boundary restores it without changing other pins")
        _unchanged_query(web, checkpoint, before)

        web.named("Workspace options"); web.named("Reload saved project")
        web.results(data["total"])
        checks["reload"] = _prefix(web, [first, third, boundary], "saved reload restores hidden column and frozen boundary")
        assert "score" not in web.state()["headers"]
        source = root / f"pin-switch-{uuid.uuid4().hex[:8]}.json"
        source.write_text('[{"other":"tab"}]', encoding="utf-8")
        other = web.command("create_project", {"path": str(source)})
        open_project(web, other["meta"]["name"])
        web.results(1)
        web.tab(data["id"])
        web.results(data["total"])
        checks["tab_restore"] = _prefix(web, [first, third, boundary], "tab session restores frozen prefix")
        assert "score" not in web.state()["headers"]

        checkpoint, before = len(web.trace()), web.query()
        _action(web, "timestamp", "Unfreeze data columns")
        _horizontal_to(web, 700)
        checks["context_unfreeze"] = _prefix(web, [], "context menu unfreezes data while annotation columns remain fixed")
        assert checks["context_unfreeze"]["staticWidth"] > 0
        assert checks["context_unfreeze"]["headers"][0]["left"] < checks["context_unfreeze"]["viewport"]["left"] + checks["context_unfreeze"]["staticWidth"]
        _unchanged_query(web, checkpoint, before)

        _action(web, "timestamp", "Freeze columns through this column")
        web.named("Choose visible columns")
        web.named("Deselect all", "#visible-column-menu")
        wait_for(lambda: web.state()["headers"], lambda names: not names, "all data headers hidden")
        web.named("Unfreeze data columns", "#visible-column-menu")
        _saved(web, data["id"], lambda state: state.get("frozenThrough") == "", "unfreeze remains accessible with all columns hidden")
        web.named("Select all", "#visible-column-menu")
        web.named("Done", "#visible-column-menu")
        checks["all_hidden_unfreeze"] = _prefix(web, [], "showing columns after all-hidden unfreeze does not repin")
        assert web.visible_column_count() == 27
        checks["query_unchanged_by_visibility_and_freeze"] = True
        _oversized_prefix(web, data, evidence, checks)
        report["column_pinning"] = checks
    finally:
        # Closing clears the tab snapshot; restore only synthetic fixture settings
        # through the real native APIs, then verify an ordinary reopen.
        web.close_drawer()
        if web.script("return !!document.querySelector('#visible-column-menu');"):
            web.named("Done", "#visible-column-menu")
        web.named(f"Close {data['filename']}")
        wait_for(lambda: [entry for entry in web.trace() if entry["name"] == "save_view_state"
            and entry["request"].get("projectId") == data["id"] and "result" not in entry and "error" not in entry],
            lambda pending: not pending, "closed case finishes its saved-view writes")
        web.command("set_hidden_columns", {"projectId": data["id"], "hidden_columns": original.get("hiddenColumns", [])})
        web.command("save_view_state", {"projectId": data["id"], "state": original})
        if other:
            web.named(f"Close {source.name}")
            web.command("delete_project", {"projectId": other["meta"]["id"]}, request=True)
        open_project(web, data["name"])
        web.results(data["total"])
        _horizontal_to(web, 0)
