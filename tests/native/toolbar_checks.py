"""Keep the investigation toolbar compact without losing narrow-screen access."""
from __future__ import annotations

from pathlib import Path

from feedback_checks import _settle
from row_navigation import wait_for


def _geometry(web) -> dict:
    return web.script("""
      const controls=[
        ['Back','.history-buttons [aria-label="Back"]'],
        ['Forward','.history-buttons [aria-label="Forward"]'],
        ['Bookmark','.bookmark-toggle'],['Flag','.flag-trigger'],
        ['Original row','.row-jump input'],['First','.row-jump [aria-label="First"]'],
        ['Last','.row-jump [aria-label="Last"]'],['Filter','#tool-group-filters'],
        ['Analyze','#tool-group-analysis'],['Annotate','#tool-group-annotations'],
        ['Search','.search-controls'],['Columns','.column-picker>button'],
        ['Export','.export-button']
      ];
      return {width:innerWidth,overflow:document.documentElement.scrollWidth>innerWidth,
        goButton:!!document.querySelector('.row-jump [aria-label="Go"]'),
        controls:controls.map(([name,selector])=>{
          const e=document.querySelector(selector),r=e.getBoundingClientRect();
          return {name,left:r.left,right:r.right,top:r.top,bottom:r.bottom,center:(r.top+r.bottom)/2};
        })};
    """)


def verify_toolbar(web, data, evidence: Path, report: dict) -> None:
    web.close_drawer()
    checks = []
    try:
        for width in [1280, 800, 320]:
            web.request("POST", web.route("window/rect"), {"width": width, "height": 827})
            _settle(web)
            state = _geometry(web)
            assert not state["overflow"] and not state["goButton"], state
            assert all(item["left"] >= -1 and item["right"] <= state["width"] + 1
                and item["bottom"] > item["top"] for item in state["controls"]), state
            if width == 1280:
                centers = [item["center"] for item in state["controls"]]
                assert max(centers) - min(centers) <= 1, state
                assert all(right["left"] >= left["right"] - 1
                    for left, right in zip(state["controls"], state["controls"][1:])), state
            elif width == 800:
                primary, secondary = state["controls"][:10], state["controls"][10:]
                assert max(item["center"] for item in primary) - min(item["center"] for item in primary) <= 1, state
                assert max(item["center"] for item in secondary) - min(item["center"] for item in secondary) <= 1, state
                assert min(item["top"] for item in secondary) >= max(item["bottom"] for item in primary), state
            else:
                web.tap(web.node('.flag-trigger'))
                menu = web.script("const r=document.querySelector('.flag-menu').getBoundingClientRect();return {left:r.left,right:r.right};")
                assert menu["left"] >= -1 and menu["right"] <= state["width"] + 1, menu
                web.tap(web.node('.flag-trigger'))
                state["flag_menu"] = menu
            web.screenshot(evidence / f"toolbar-{width}.png")
            checks.append(state)
        web.request("POST", web.route("window/rect"), {"width": 1280, "height": 827})
        web.input('.row-jump input[type="number"]', "242", enter=True)
        wait_for(web.state, lambda state: any(row["id"] == 242 and row["top"] >= state["top"] - 1
            and row["bottom"] <= state["bottom"] + 1 for row in state.get("rows", [])), "Enter jumps to the original row")
        web.button("Last")
        wait_for(web.state, lambda state: any(row["id"] == data["total"] for row in state.get("rows", [])), "Last remains accessible")
        report["toolbar"] = {"geometry": checks, "enter_jumps_to_original_row": True}
    finally:
        web.request("POST", web.route("window/rect"), {"width": 1280, "height": 827})
        web.button("First")
        web.results(data["total"])


def _tap_blank(web, selector: str) -> dict:
    point = web.script("""
      const root=document.querySelector(arguments[0]),r=root.getBoundingClientRect();
      const interactive='button,a,input,select,textarea,label,summary,[role=button],[role=tab],[role=menuitem],[contenteditable=true]';
      for(const fraction of [.5,.65,.35,.8,.2]){
        const x=Math.round(r.left+r.width*fraction),y=Math.round(r.top+r.height/2),hit=document.elementFromPoint(x,y);
        if(hit&&root.contains(hit)&&!hit.closest(interactive)&&!hit.closest('#analysis-drawer'))return {x,y,target:hit.className};
      }
      return null;
    """, selector)
    assert point, f"No unobstructed blank point in {selector}"
    web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "drawer-blank", "parameters": {"pointerType": "mouse"}, "actions": [
        {"type": "pointerMove", "x": point["x"], "y": point["y"], "duration": 0, "origin": "viewport"},
        {"type": "pointerDown", "button": 0}, {"type": "pointerUp", "button": 0}]}]})
    wait_for(lambda: web.script("return !document.querySelector('#analysis-drawer');"), bool, f"blank {selector} dismisses the panel")
    return point


def verify_drawer_interaction(web, data, evidence: Path, report: dict) -> None:
    """Panel choices remain reachable, while only unused outside areas dismiss."""
    web.close_drawer()
    baseline = web.view()["viewportHeight"]
    web.named("Open Analyze")
    tabs = web.script("return [...document.querySelectorAll('.analysis-tabs button')].map(e=>e.id);")
    assert tabs == ["tool-tab-timeline", "tool-tab-time", "tool-tab-stats", "tool-tab-groups"], tabs
    assert web.script("return document.querySelector('#tool-tab-timeline').getAttribute('aria-selected');") == "true", "Analyze initially opens Timeline"
    web.click(web.element('#tool-tab-stats'))
    web.close_drawer()
    web.named("Open Analyze")
    assert web.script("return document.querySelector('#tool-tab-stats').getAttribute('aria-selected');") == "true", "Reopening a category keeps its last chosen tool"
    web.close_drawer()
    switches = []
    try:
        for width in [1280, 800, 320]:
            web.request("POST", web.route("window/rect"), {"width": width, "height": 827})
            _settle(web)
            for label in ["Filter", "Analyze", "Annotate"]:
                trigger = web.node(f'button[aria-label="Open {label}"]')
                web.tap(trigger)  # Native hit testing catches controls covered by the drawer.
                selected = wait_for(lambda: web.script("return document.querySelector('#analysis-tool-label')?.textContent;"),
                                    lambda value: value == label, f"{label} switches with another panel open")
                bounds = web.script("const d=document.querySelector('#analysis-drawer').getBoundingClientRect(),body=document.querySelector('.analysis-body').getBoundingClientRect(),buttons=[...document.querySelectorAll('.filter-row button,.row-jump input')].filter(e=>e.getClientRects().length);return {top:d.top,bottom:d.bottom,height:d.height,bodyBottom:body.bottom,toolbarBottom:Math.max(...buttons.map(e=>e.getBoundingClientRect().bottom)),label:document.querySelector('#analysis-tool-label').textContent};")
                assert bounds["top"] >= bounds["toolbarBottom"] + 2 and bounds["height"] > 200, bounds
                assert bounds["bodyBottom"] <= bounds["bottom"] + 1, bounds
                switches.append({"width": width, "label": selected, **bounds})
            _tap_blank(web, '.project-title')
        web.request("POST", web.route("window/rect"), {"width": 1280, "height": 827})
        _settle(web)
        assert abs(web.view()["viewportHeight"] - baseline) <= 1
        blanks = []
        for selector in ['.table-meta', '.project-tabs', '.app-header']:
            web.named("Open Filter")
            blanks.append({"selector": selector, **_tap_blank(web, selector)})

        web.named("Open Filter")
        web.tap(web.node('input[aria-label="Search rows"]'))
        assert web.script("return !!document.querySelector('#analysis-drawer');"), "Search focus is meaningful, not a blank dismissal"
        web.write('input[aria-label="Search rows"]', "keep")
        web.applied(search="keep"); web.results(data["total"] - data["total"] // 5)
        web.named("Clear all filters")
        web.applied(search="", searchColumns=[], flagFilter="all", bookmarkedOnly=False, condition=None)
        web.results(data["total"])
        assert web.script("return !!document.querySelector('#analysis-drawer');"), "Clear filters must act without dismissing its panel"

        web.named("Choose visible columns")
        web.tap(web.node('input[aria-label="Find a column"]'))
        assert web.script("return !!document.querySelector('#analysis-drawer');"), "Column picker interactions keep the panel"
        web.named("Done", "#visible-column-menu")
        web.named("Search syntax help")
        web.named("Done", "#search-syntax-help")
        assert web.script("return !!document.querySelector('#analysis-drawer');"), "Other popovers are meaningful controls"

        web.tap(web.node('.data-row button[data-column-id]'))
        wait_for(lambda: web.script("return !!document.querySelector('.row-detail-dialog');"), bool, "Table cell opens the actual row detail")
        assert web.script("return !!document.querySelector('#analysis-drawer');"), "Opening row details does not dismiss the tool panel"
        web.named("Close", ".row-detail-dialog")
        assert web.script("return !!document.querySelector('#analysis-drawer');"), "Closing the row modal is not a blank outside click"

        selection = web.script("const e=document.querySelector('.project-title'),r=e.getBoundingClientRect();return {x:Math.round(r.left+3),end:Math.round(r.right-3),y:Math.round((r.top+r.bottom)/2)};")
        web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "drawer-title-selection", "parameters": {"pointerType": "mouse"}, "actions": [
            {"type": "pointerMove", "x": selection["x"], "y": selection["y"], "origin": "viewport"},
            {"type": "pointerDown", "button": 0},
            {"type": "pointerMove", "x": selection["end"], "y": selection["y"], "duration": 80, "origin": "viewport"},
            {"type": "pointerUp", "button": 0}]}]})
        assert web.script("return !!document.querySelector('#analysis-drawer');"), "Dragging across title text must not be treated as a blank click"
        web.script("getSelection()?.removeAllRanges();")
        _tap_blank(web, '.table-meta')
        report["drawer_interaction"] = {"analysis_tabs": tabs, "timeline_first": True, "remembered_tool": True,
            "reachable_switches": switches, "blank_targets": blanks, "controls_popovers_and_modals_preserved": True,
            "clear_filters_performs_its_action": True, "text_drag_does_not_dismiss": True}
        web.screenshot(evidence / "central-panel-controls.png")
    finally:
        web.script("getSelection()?.removeAllRanges();")
        web.request("POST", web.route("window/rect"), {"width": 1280, "height": 827})
        web.close_drawer()


def verify_condition_buttons(web, evidence: Path, report: dict) -> None:
    web.drawer("Conditions")
    try:
        _settle(web)
        buttons = web.script("""
          const root=document.querySelector('.condition-editor .group-toolbar'),c=document.createElement('canvas').getContext('2d');
          return ['Add condition','Add group'].map(name=>{
            const e=root.querySelector('[aria-label="'+name+'"]'),r=e.getBoundingClientRect(),s=getComputedStyle(e),hit=document.elementFromPoint(r.left+r.width/2,r.top+r.height/2);
            c.clearRect(0,0,1,1);c.fillStyle=s.borderTopColor;c.fillRect(0,0,1,1);
            return {name,tag:e.tagName,text:e.textContent.trim(),width:r.width,height:r.height,
              border:parseFloat(s.borderTopWidth),borderAlpha:c.getImageData(0,0,1,1).data[3],
              rect:{left:r.left,top:r.top,right:r.right,bottom:r.bottom},
              reachable:e.contains(hit),hit:hit?{tag:hit.tagName,classes:String(hit.className)}:null};
          });
        """)
        report["condition_layout"] = web.script("""
          return ['#analysis-drawer','.analysis-body','.filters','.project-view'].map(selector=>{
            const e=document.querySelector(selector),r=e.getBoundingClientRect(),s=getComputedStyle(e);
            return {selector,top:r.top,bottom:r.bottom,height:r.height,position:s.position,z:s.zIndex,
              cssTop:s.top,cssBottom:s.bottom,overflow:s.overflow,offsetParent:e.offsetParent?.className};
          });
        """)
        assert all(button["tag"] == "BUTTON" and button["width"] >= 40
            and 24 <= button["height"] <= 28 and button["border"] >= 1
            and button["borderAlpha"] > 0 and button["reachable"] for button in buttons), buttons
        assert "Condition" in buttons[0]["text"] and "Group" in buttons[1]["text"], buttons
        web.script("document.querySelector('.condition-editor [aria-label=\"Add condition\"]').focus();")
        web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "condition-button-focus", "actions": [
            {"type": "keyDown", "value": "\ue004"}, {"type": "keyUp", "value": "\ue004"},
            {"type": "keyDown", "value": "\ue008"}, {"type": "keyDown", "value": "\ue004"},
            {"type": "keyUp", "value": "\ue004"}, {"type": "keyUp", "value": "\ue008"}]}]})
        focus = web.script("const e=document.activeElement,s=getComputedStyle(e);return {name:e.getAttribute('aria-label'),visible:e.matches(':focus-visible'),outline:parseFloat(s.outlineWidth)};")
        assert focus["name"] == "Add condition" and focus["visible"] and focus["outline"] >= 2, focus
        web.screenshot(evidence / "condition-add-buttons.png")
        report["condition_buttons"] = {"buttons": buttons, "keyboard_focus": focus}
    except Exception:
        web.screenshot(evidence / "condition-buttons-failure.png")
        raise
    finally:
        web.close_drawer()
