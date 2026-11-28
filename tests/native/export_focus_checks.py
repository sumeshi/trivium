"""Keyboard interaction stays inside the actual native CSV export dialog."""
from row_navigation import wait_for
from column_pin_checks import _key as native_key


def _key(web, value: str, shift: bool = False) -> None:
    actions = [{"type": "keyDown", "value": value}, {"type": "keyUp", "value": value}]
    if shift:
        actions = [{"type": "keyDown", "value": "\ue008"}, *actions, {"type": "keyUp", "value": "\ue008"}]
    web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "export-focus", "actions": actions}]})


def verify_export_focus(web, report: dict) -> None:
    wait_for(lambda: web.script("return document.querySelector('.export-dialog')?.contains(document.activeElement);"),
        bool, "opening Export moves keyboard focus into its dialog")
    web.script("""window.__exportFocusKeys=0;window.__exportFocusObserved={tabs:0,escapes:0};
      window.__exportFocusListener=()=>window.__exportFocusKeys++;
      window.__exportFocusCapture=event=>{
        if(event.key==='Tab'||event.code==='Tab')window.__exportFocusObserved.tabs++;
        if(event.key==='Escape')window.__exportFocusObserved.escapes++;
      };
      window.addEventListener('keydown',window.__exportFocusCapture,true);
      window.addEventListener('keydown',window.__exportFocusListener);""")
    try:
        focused = set()
        for shift in [False, True]:
            for _ in range(12):
                _key(web, "\ue004", shift)
                assert web.script("return document.querySelector('.export-dialog').contains(document.activeElement);"), "Tab must not reach the background table"
                focused.add(web.script("return [...document.querySelectorAll('.export-dialog button,.export-dialog input')].indexOf(document.activeElement);"))
        assert web.script("return window.__exportFocusObserved.tabs;") == 24, "Every Tab must reach the owned WebView"
        assert len(focused) > 2, "Real Tab input must move among dialog controls"
        assert web.script("return window.__exportFocusKeys;") == 0, "Export keyboard input leaked into workspace shortcuts"
        native_key(web, "\ue00c")
        wait_for(lambda: web.script("return !document.querySelector('.export-dialog');"),
            bool, "Escape closes the export dialog")
        assert web.script("return window.__exportFocusObserved.escapes;") == 1, "A native Escape must reach the dialog"
        assert web.script("return document.activeElement===document.querySelector('[aria-label=\"Export rows\"]');"), "Closing Export must restore focus to its trigger"
        assert web.script("return window.__exportFocusKeys;") == 0
        report["export_keyboard_focus"] = {"initial_focus": True, "forward_and_backward_tabs": 24,
            "background_shortcuts_blocked": True, "escape_and_focus_return": True}
    finally:
        web.script("window.removeEventListener('keydown',window.__exportFocusCapture,true);window.removeEventListener('keydown',window.__exportFocusListener);delete window.__exportFocusKeys;delete window.__exportFocusListener;delete window.__exportFocusCapture;delete window.__exportFocusObserved;")
        if web.script("return !!document.querySelector('.export-dialog');"):
            web.named("Close", ".export-dialog")
