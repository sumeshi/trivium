"""Held native input must never activate a different row in a recycled DOM slot."""
from __future__ import annotations

import uuid

from feedback_checks import _settle
from row_navigation import wait_for


def _pointer(web, events: list[dict]) -> None:
    web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "recycled-input-pointer",
        "parameters": {"pointerType": "mouse"}, "actions": events}]})


def _space(web, pressed: bool) -> None:
    web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "recycled-input-space",
        "actions": [{"type": "keyDown" if pressed else "keyUp", "value": "\ue00d"}]}]})


def _state(web) -> dict:
    return web.script("""
      const s=window.__recycledInput,selected=[...document.querySelectorAll('.table-meta-left strong')][1];
      return {id:Number(s.row.dataset.rowIndex),checked:s.button.checked??null,
        flag:s.row.querySelector('.flag-buttons .selected')?.getAttribute('aria-label')??null,
        selected:Number(selected?.textContent.replaceAll(',','')??0),
        dialog:document.querySelector('[role=dialog][aria-label^="Edit note for row"]')?.getAttribute('aria-label')??null};
    """)


def _writes(web, start: int) -> list[dict]:
    return [entry for entry in web.trace()[start:] if entry["name"] == "update_flag"]


def _activate(web, keyboard: bool) -> None:
    if keyboard:
        web.script("window.__recycledInput.button.focus({preventScroll:true});")
        _space(web, True)
        _space(web, False)
    else:
        web.tap(web.script("return window.__recycledInput.button;"))
    _settle(web)


def verify_recycled_inputs(web, projectId: str, report: dict) -> None:
    checks = report["recycled_inputs"] = []
    try:
        for keyboard, control in [(False, "safe"), (False, "checkbox"), (False, "note"), (True, "safe"), (True, "checkbox")]:
            web.input('.row-jump input', "50", enter=True)
            wait_for(lambda: web.script('return !!document.querySelector(\'.data-row[data-row-index="49"] .flag-safe\');'),
                bool, "Go50 warms the first page before a whole-window input test")
            _settle(web)
            point = web.script("""
              const v=document.querySelector('.virtual-viewport').getBoundingClientRect();
              const row=[...document.querySelectorAll('.data-row')].find(e=>{
                const r=e.getBoundingClientRect();return e.dataset.rowIndex!==undefined&&r.top>=v.top+80&&r.bottom<=v.bottom;
              });
              const button=row.querySelector(arguments[0]==='safe'?'.flag-safe':arguments[0]==='checkbox'?'.select-cell input':'.memo-button');
              const r=button.getBoundingClientRect(),x=Math.round(r.left+r.width/2),y=Math.round(r.top+r.height/2);
              window.__recycledInput={row,button};
              if(arguments[1])button.focus({preventScroll:true});
              return {x,y,hit:button.contains(document.elementFromPoint(x,y))};
            """, control, keyboard)
            assert point.pop("hit"), "The pressed control must be reachable by the real pointer"
            before, start = _state(web), len(web.trace())
            if keyboard:
                _space(web, True)
            else:
                _pointer(web, [{"type": "pointerMove", **point, "origin": "viewport", "duration": 0}, {"type": "pointerDown", "button": 0}])
            try:
                # Keeping the exact pointer position is essential: moving it to
                # another cell would naturally cancel the click and hide this bug.
                web.request("POST", web.route("actions"), {"actions": [{"type": "wheel", "id": f"recycled-wheel-{uuid.uuid4().hex}",
                    "actions": [{"type": "scroll", **point, "deltaX": 0, "deltaY": 2304, "duration": 0}]}]})
                recycled = wait_for(lambda: web.script("""
                  const s=window.__recycledInput,h=document.elementFromPoint(arguments[0],arguments[1]);
                  return {id:Number(s.row.dataset.rowIndex),checked:s.button.checked??null,
                    same:s.button.isConnected&&s.button===h?.closest('button,input'),
                    focused:document.activeElement===s.button};
                """, point["x"], point["y"]), lambda value: value["id"] != before["id"], "the same DOM slot now holds another original row")
                _settle(web)
                assert recycled["same"], recycled
                if keyboard:
                    assert not recycled["focused"], recycled
            finally:
                _space(web, False) if keyboard else _pointer(web, [{"type": "pointerUp", "button": 0}])
            _settle(web)
            after = _state(web)
            assert not _writes(web, start), "Releasing a recycled control must not write any row's annotation"
            assert after["selected"] == before["selected"] and after["dialog"] is None, after
            if control == "checkbox":
                assert after["checked"] == recycled["checked"], "Cancelled checkbox activation must roll back its native checked state"
            fresh_start = len(web.trace())
            _activate(web, keyboard)
            if control == "safe":
                fresh = wait_for(lambda: _writes(web, fresh_start), bool, "a fresh flag activation still writes the current original row")
                assert fresh[0]["request"].get("projectId") == projectId, fresh
                assert fresh[0]["request"].get("row_index", fresh[0]["request"].get("rowIndex")) == after["id"], fresh
                # Restore only the flag through its normal UI, preserving notes,
                # tags, bookmarks and any annotation established by earlier tests.
                restore = after["flag"] or "Safe"
                restore_start = len(web.trace())
                web.tap(web.script("return [...window.__recycledInput.row.querySelectorAll('.flag-buttons button')].find(e=>e.getAttribute('aria-label')===arguments[0]);", restore))
                wait_for(lambda: _writes(web, restore_start), lambda values: bool(values and "result" in values[-1]), "the original flag is restored")
            elif control == "checkbox":
                wait_for(lambda: _state(web)["checked"], lambda value: value != after["checked"], "fresh checkbox input remains usable")
                _activate(web, keyboard)
                wait_for(lambda: _state(web), lambda value: value["checked"] == after["checked"] and value["selected"] == before["selected"], "the original selection is restored")
            else:
                wait_for(lambda: _state(web)["dialog"], lambda value: value == f"Edit note for row {after['id'] + 1}", "fresh Note input opens the current original row")
                web.named("Cancel", ".memo-dialog")
                wait_for(lambda: _state(web)["dialog"], lambda value: value is None, "the unchanged note draft closes")
            checks.append({"input": "Space" if keyboard else "pointer", "control": control, "origin": before["id"],
                "replacement": recycled["id"], "same_dom_node": True, "cancelled": True, "fresh_activation": True})
    finally:
        if web.script("return !!document.querySelector('.memo-dialog');"):
            web.named("Cancel", ".memo-dialog")
        web.script("delete window.__recycledInput;")
        web.button("First")
