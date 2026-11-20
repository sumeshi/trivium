"""Hex input, transient preview and managed theme saves in the real WebView."""
from __future__ import annotations

import json
from pathlib import Path

from row_navigation import wait_for


def _read(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _accent(web):
    return web.script("return getComputedStyle(document.documentElement).getPropertyValue('--accent').trim();")


def _choose_theme(web, value: str) -> None:
    # WebKit's option-click atom does not focus the select. After dynamically
    # adding a theme, focus first so its last user-selection state is refreshed,
    # as it is when a person opens the selector.
    web.script("document.querySelector('[aria-label=\"Appearance theme\"]').focus();")
    web.choose('[aria-label="Appearance theme"]', value)


def verify_theme_editor(web, definition: dict, managed: Path, directory: Path,
                        cleanup: list[Path], report: dict) -> dict:
    original = _read(managed)
    assert original == definition
    expected_id = f"user:{definition['id']}"
    web.named("Edit colors", ".appearance-dialog")
    wait_for(lambda: web.script("return !!document.querySelector('.theme-editor');"), bool, "theme color editor opens")
    action_sizes = web.script("return [...document.querySelectorAll('.theme-editor .dialog-actions > button')].map(e=>({label:e.textContent.trim(),height:e.getBoundingClientRect().height,font:getComputedStyle(e).fontSize,lineHeight:getComputedStyle(e).lineHeight}));")
    assert [item["label"] for item in action_sizes] == ["Reset colors", "Cancel editing", "Save theme"], action_sizes
    assert all(abs(item["height"] - 30) < .5 for item in action_sizes), action_sizes
    assert len({(item["font"], item["lineHeight"]) for item in action_sizes}) == 1, action_sizes
    web.write('[aria-label="Accent color code"]', "#88bbcc")
    wait_for(lambda: _accent(web), lambda value: value == "#88bbcc", "valid hex previews on the real application")
    assert _read(managed) == original, "Preview must not overwrite the saved theme"
    web.write('[aria-label="Accent color code"]', "#ab")
    assert web.script("return document.querySelector('[aria-label=\"Accent color code\"]').getAttribute('aria-invalid');") == "true"
    assert web.script("return [...document.querySelectorAll('.theme-editor button')].find(e=>e.textContent.trim()==='Save theme').disabled;")
    assert _accent(web) == "#88bbcc", "Partial hex input keeps the last valid preview"
    web.named("Reset colors", ".theme-editor")
    wait_for(lambda: _accent(web), lambda value: value == original["colors"]["accent"], "reset restores the initial draft palette")
    web.write('[aria-label="Accent color code"]', "#ddeeff")
    assert web.script("""
      const input=document.querySelector('[aria-label="Theme name"]');
      input.dispatchEvent(new CompositionEvent('compositionstart',{bubbles:true,data:'名'}));
      input.dispatchEvent(new KeyboardEvent('keydown',{bubbles:true,key:'Escape',isComposing:true}));
      const open=!!document.querySelector('.theme-editor');
      input.dispatchEvent(new CompositionEvent('compositionend',{bubbles:true,data:'名'}));
      return open;
    """), "IME cancellation must not discard the theme draft"
    web.named("Cancel editing", ".theme-editor")
    wait_for(lambda: _accent(web), lambda value: value == original["colors"]["accent"], "cancel restores the selected theme")
    assert not web.script("return !!document.querySelector('.theme-editor');")
    assert _read(managed) == original

    web.named("Edit colors", ".appearance-dialog")
    web.write('[aria-label="Accent color code"]', "#bbaadd")
    web.write('[aria-label="Table background color code"]', "#263443")
    web.named("Save theme", ".theme-editor")
    wait_for(lambda: web.script("return !document.querySelector('.theme-editor')&&!document.querySelector('[aria-label=\"Appearance theme\"]').disabled;"),
             bool, "edited theme is durably saved before the editor closes")
    edited = wait_for(lambda: _read(managed),
        lambda value: value and value["colors"].get("accent") == "#bbaadd" and value["colors"].get("surface") == "#263443",
        "hex colors reach the actual managed profile")
    assert edited["id"] == original["id"] and edited["base"] == original["base"]
    assert edited["colors"]["bg"] == original["colors"]["bg"]
    assert web.script("return document.querySelector('[aria-label=\"Appearance theme\"]').value;") == expected_id

    _choose_theme(web, "light")
    wait_for(lambda: web.script("""
      const select=document.querySelector('[aria-label="Appearance theme"]'),root=document.documentElement;
      return {value:select.value,ready:!select.disabled,base:root.dataset.theme,accent:root.style.getPropertyValue('--accent')};
    """), lambda value: value == {"value": "light", "ready": True, "base": "light", "accent": ""}, "built-in theme selection finishes and clears custom overrides")
    light_accent = _accent(web)
    web.named("Edit colors", ".appearance-dialog")
    web.write('[aria-label="Theme name"]', "Native color copy")
    web.write('[aria-label="Accent color code"]', "#557799")
    web.named("Save theme", ".theme-editor")
    custom_id = wait_for(lambda: web.script("return document.querySelector('[aria-label=\"Appearance theme\"]').value;"),
        lambda value: isinstance(value, str) and value.startswith("user:") and value != expected_id,
        "editing a built-in saves a separate custom theme")
    copy_path = directory / "themes" / f"{custom_id.removeprefix('user:')}.json"
    cleanup.append(copy_path)
    copied = wait_for(lambda: _read(copy_path), lambda value: value and value["name"] == "Native color copy", "new named theme reaches native storage")
    assert copied["base"] == "light" and copied["colors"]["accent"] == "#557799"
    wait_for(lambda: web.script("return !document.querySelector('[aria-label=\"Appearance theme\"]').disabled;"), bool, "new theme save finishes")
    _choose_theme(web, "light")
    wait_for(lambda: web.script("return document.documentElement.style.getPropertyValue('--accent');"),
             lambda value: value == "", "built-in selection clears the saved-copy override")
    assert _accent(web) == light_accent, "the built-in palette remains intact"
    wait_for(lambda: web.script("return !document.querySelector('[aria-label=\"Appearance theme\"]').disabled;"), bool, "built-in restore finishes")
    _choose_theme(web, expected_id)
    wait_for(lambda: _accent(web), lambda value: value == "#bbaadd", "saved edited theme can be selected again")
    wait_for(lambda: web.script("return !document.querySelector('[aria-label=\"Appearance theme\"]').disabled;"), bool, "custom restore finishes")
    report["theme_editor"] = {"hex_preview_without_persistence": True,
        "consistent_dialog_action_sizes": action_sizes,
        "invalid_partial_hex_blocks_save": True, "reset_and_cancel_restore": True,
        "composition_escape_retains_draft": True,
        "existing_custom_id_retained": True, "built_in_saved_as_named_copy": True,
        "built_in_palette_preserved": True,
        "unverified": "OS color-picker dialog and actual IME; hex input uses real keys, composition events are synthesized"}
    return edited
