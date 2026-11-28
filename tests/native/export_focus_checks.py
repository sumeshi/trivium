"""Keyboard interaction stays inside the actual native CSV export dialog."""
from row_navigation import wait_for


def _key(web, value: str, shift: bool = False) -> None:
    actions = [{"type": "keyDown", "value": value}, {"type": "keyUp", "value": value}]
    if shift:
        actions = [{"type": "keyDown", "value": "\ue008"}, *actions, {"type": "keyUp", "value": "\ue008"}]
    web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "export-focus", "actions": actions}]})


def verify_export_focus(web, report: dict) -> None:
    wait_for(lambda: web.script("return document.querySelector('.export-dialog')?.contains(document.activeElement);"),
        bool, "opening Export moves keyboard focus into its dialog")
    web.script("""window.__exportFocusKeys=0;
      window.__exportFocusListener=()=>window.__exportFocusKeys++;
      window.addEventListener('keydown',window.__exportFocusListener);""")
    try:
        for shift in [False, True]:
            for _ in range(12):
                _key(web, "\ue004", shift)
                assert web.script("return document.querySelector('.export-dialog').contains(document.activeElement);"), "Tab must not reach the background table"
        assert web.script("return window.__exportFocusKeys;") == 0, "Export keyboard input leaked into workspace shortcuts"
        _key(web, "\ue00c")
        wait_for(lambda: web.script("return !document.querySelector('.export-dialog');"),
            bool, "Escape closes the export dialog")
        assert web.script("return document.activeElement===document.querySelector('[aria-label=\"Export rows\"]');"), "Closing Export must restore focus to its trigger"
        assert web.script("return window.__exportFocusKeys;") == 0
        report["export_keyboard_focus"] = {"initial_focus": True, "forward_and_backward_tabs": 24,
            "background_shortcuts_blocked": True, "escape_and_focus_return": True}
    finally:
        web.script("window.removeEventListener('keydown',window.__exportFocusListener);delete window.__exportFocusKeys;delete window.__exportFocusListener;")
        if web.script("return !!document.querySelector('.export-dialog');"):
            web.named("Close", ".export-dialog")
