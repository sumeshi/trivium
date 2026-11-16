"""Real appearance settings, managed image conversion and normal native restart.

Only OS file-picker replies are substituted. File reads/writes, image handling,
paint and close-drain use the actual application.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import struct
import uuid
import zlib

from feedback_checks import _horizontal_to, _settle
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS, NormalWindowClose
from storage_responsiveness_checks import _windows_local_app_data
from table_wallpaper_checks import verify_table_wallpaper
from theme_editor_checks import verify_theme_editor
from wallpaper_paint_checks import assert_planes


def _png(path: Path) -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        body = kind + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body))
    width, height = 256, 128
    pixels = b"".join(b"\0" + bytes(channel for x in range(width)
        for channel in (x, y * 2, (x + y) % 256, 0 if x < 32 else 255)) for y in range(height))
    image = (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0))
             + chunk(b"IDAT", zlib.compress(pixels)) + chunk(b"IEND", b""))
    path.write_bytes(image)
    return image


def _open(web) -> None:
    if web.script("return !!document.querySelector('.appearance-dialog');"):
        return
    web.close_drawer()
    if web.script("return !!document.querySelector('.workspace-drawer');"):
        web.named("Close workspace menu", ".workspace-drawer")
    if not web.script("return !!document.querySelector('.workspace-menu')?.open;"):
        web.named("Workspace options")
    web.named("Appearance…", ".workspace-options")
    wait_for(lambda: web.script("return !!document.querySelector('.appearance-dialog');"), bool, "appearance dialog opens")
    _settle(web)


def _close(web) -> None:
    if web.script("return !!document.querySelector('.appearance-dialog');"):
        web.named("Close appearance", ".appearance-dialog")
        wait_for(lambda: web.script("return !document.querySelector('.appearance-dialog');"), bool, "appearance dialog closes")


def _install_pickers(web) -> None:
    assert web.script("""
      const previous=window.fetch;
      window.__appearancePicker={queue:[],count:0,restore:()=>{window.fetch=previous;}};
      window.fetch=function(input,options,...rest){
    """ + IPC_COMMAND_JS + """
        const state=window.__appearancePicker;
        if((name==='plugin:dialog|open'||name==='plugin:dialog|save')&&state.queue.length){
          const next=state.queue.shift();
          if(next.name!==name)throw new Error('Unexpected appearance picker kind');
          state.count++;
          return Promise.resolve(new Response(JSON.stringify(next.path),
            {headers:{'Tauri-Response':'ok','Content-Type':'application/json'}}));
        }
        return previous.call(window,input,options,...rest);
      };return true;
    """), "Only the native appearance picker response is substituted"


def _pick(web, action: str, path: Path, *, save: bool = False) -> None:
    before = web.script("return window.__appearancePicker.count;")
    web.script("window.__appearancePicker.queue.push({name:arguments[0],path:arguments[1]});",
               "plugin:dialog|save" if save else "plugin:dialog|open", str(path))
    web.named(action, ".appearance-dialog")
    wait_for(lambda: web.script("return window.__appearancePicker.count;"), lambda count: count == before + 1,
             "appearance picker response is consumed")


def _profile_directory(web, root: Path) -> Path:
    resolved = web.async_script("""
      const done=arguments[arguments.length-1];
      window.__TAURI_INTERNALS__.invoke('plugin:path|resolve_directory',{directory:15})
        .then(path=>done({path}),()=>done({error:true}));
    """)
    assert isinstance(resolved.get("path"), str), "Native appearance settings directory resolves"
    directory = Path(resolved["path"]).resolve()
    if os.name == "nt":
        assert directory == (_windows_local_app_data() / "com.trivium.native-tests").resolve(), "Use only the owned Windows native-test namespace"
    else:
        assert directory.is_relative_to(root.resolve()), "Appearance preferences stay inside the isolated Linux profile"
    return directory


def _saved(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _strength(web, value: int) -> None:
    # Native range keys exercise the actual input producer without replacing
    # app state. Move from the current value so the layer stays mounted.
    current = web.script("return Number(document.querySelector('#wallpaper-strength').value);")
    web.script("document.querySelector('#wallpaper-strength').focus();")
    key = "\ue014" if value > current else "\ue012"
    actions = [action for _ in range(abs(value - current))
               for action in ({"type": "keyDown", "value": key}, {"type": "keyUp", "value": key})]
    if actions:
        web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "appearance-range", "actions": actions}]})
    wait_for(lambda: web.script("return Number(document.querySelector('#wallpaper-strength').value);"),
             lambda actual: actual == value, "native range keys update background strength")


def _set_enabled(web, enabled: bool) -> None:
    _open(web)
    checkbox = web.node('.appearance-dialog input[type="checkbox"]')
    if web.script("return arguments[0].checked;", checkbox) != enabled:
        web.tap(checkbox)
    _close(web)
    wait_for(lambda: web.script("return !!document.querySelector('.table-body-surface.has-wallpaper');"),
             lambda visible: visible == enabled, "wallpaper toggle affects only the table body")


def verify_appearance(app, data: dict, root: Path, evidence: Path, report: dict) -> None:
    web = app.web
    token = uuid.uuid4().hex[:12]
    theme_id = f"appearance-{token}"
    user_theme = f"user:{theme_id}"
    theme_source = root / f"{theme_id}.json"
    image_source = root / f"{theme_id}.png"
    exported = root / f"{theme_id}-export.json"
    definition = {"type": "trivium-theme", "version": 1, "id": theme_id, "name": "Appearance test", "base": "dark",
                  "colors": {"accent": "#aabbdd", "bg": "#15191f", "surface": "#202632"}}
    theme_source.write_text(json.dumps(definition), encoding="utf-8")
    theme_bytes = theme_source.read_bytes()
    image_bytes = _png(image_source)
    directory = _profile_directory(web, root)
    managed_theme = directory / "themes" / f"{theme_id}.json"
    extra_themes: list[Path] = []
    _install_pickers(web)
    restored = False
    try:
        open_project(web, data["name"])
        web.applied(projectId=data["id"])
        if not web.script("return document.querySelector('[aria-label=\"Clear all filters\"]').disabled;"):
            web.named("Clear all filters")
        web.applied(projectId=data["id"], search="", flagFilter="all", bookmarkedOnly=False, condition=None)
        web.results(data["total"])
        _horizontal_to(web, 0)
        assert not web.script("return !!document.querySelector('.wallpaper-layer');"), "Wallpaper is off by default"
        _open(web)
        _pick(web, "Import JSON…", theme_source)
        wait_for(lambda: web.script("""
          return {value:document.querySelector('[aria-label="Appearance theme"]').value,
            base:document.documentElement.dataset.theme,accent:getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()};
        """), lambda value: value == {"value": user_theme, "base": "dark", "accent": "#aabbdd"}, "imported theme applies actual root color overrides")
        wait_for(lambda: _saved(managed_theme), lambda value: value == definition, "custom theme reaches the owned native catalog")
        saved_theme = verify_theme_editor(web, definition, managed_theme, directory, extra_themes, report)
        _pick(web, "Export theme…", exported, save=True)
        wait_for(lambda: _saved(exported), lambda value: value == saved_theme, "exported theme preserves its stable ID and edited palette")
        _pick(web, "Choose image…", image_source)
        wait_for(lambda: web.script("return !!document.querySelector('.wallpaper-layer');"), bool, "managed native wallpaper becomes visible")
        wait_for(lambda: web.script("return document.querySelector('.appearance-dialog .actions button').disabled;"), lambda disabled: disabled is False, "native image operation finishes")
        decoded = web.async_script("""
          const done=arguments[arguments.length-1],layer=document.querySelector('.wallpaper-base'),
            raw=getComputedStyle(layer,'::before').backgroundImage.slice(4,-1),url=raw.replace(/^['"]|['"]$/g,''),image=new Image();
          image.onload=()=>done({width:image.naturalWidth,height:image.naturalHeight});image.onerror=()=>done({error:true});image.src=url;
        """)
        assert decoded == {"width": 256, "height": 128}, decoded
        assert not web.script("return !!document.querySelector('#wallpaper-fit')||document.querySelectorAll('.appearance-dialog input[type=checkbox]').length!==1;")
        assert web.script("return getComputedStyle(document.querySelector('.wallpaper-base'),'::before').backgroundSize;") == "cover"
        web.script("""
          const style=CSSStyleDeclaration.prototype.setProperty,surface=document.querySelector('.table-body-surface');
          window.__appearanceStyle={writes:0,surface,layers:[...surface.querySelectorAll('.wallpaper-layer')],restore:()=>{CSSStyleDeclaration.prototype.setProperty=style;}};
          CSSStyleDeclaration.prototype.setProperty=function(name,...args){
            if(this===surface.style&&name==='--wallpaper-image')window.__appearanceStyle.writes++;
            return style.call(this,name,...args);
          };
        """)
        _strength(web, 64)
        wait_for(lambda: web.script("return Number(getComputedStyle(document.querySelector('.wallpaper-base'),'::before').opacity);"),
                 lambda opacity: abs(opacity - .64) < .001, "fixed wallpaper opacity follows native range input")
        assert web.script("return window.__appearanceStyle.surface===document.querySelector('.table-body-surface')&&window.__appearanceStyle.layers.every((layer,index)=>layer===document.querySelectorAll('.wallpaper-layer')[index])&&window.__appearanceStyle.writes===0;"), "Strength changes must not remount or reassign the image data URL"
        web.script("window.__appearanceStyle.restore();delete window.__appearanceStyle;")
        _close(web)
        verify_table_wallpaper(web, data, root, evidence, report, lambda enabled: _set_enabled(web, enabled))
        _horizontal_to(web, 700)
        paint = assert_planes(web)
        web.screenshot(evidence / "appearance-wallpaper-pinned.png")
        assert theme_source.read_bytes() == theme_bytes and image_source.read_bytes() == image_bytes
        image_source.unlink()
        wait_for(lambda: _saved(directory / "appearance.json"), lambda value: value == {"version": 2, "enabled": True, "strength": 64}, "simplified background settings reach native storage")
        # Reopen a real v1 preference with contain/off-table settings. The image
        # copy remains owned by the app; only its old layout choices are retired.
        web.script("localStorage.removeItem('trivium:wallpaper');")
        (directory / "appearance.json").write_text(json.dumps({"version": 1, "enabled": True,
            "fit": "contain", "strength": 64, "behindTable": False}), encoding="utf-8")
        diagnostic = web.diagnostics()
        target = NormalWindowClose(app.application)
        target.request()
        wait_for(target.exited, bool, "normal OS close drains appearance saves before exiting")
        app.end_session(captured_diagnostics=diagnostic)
        web = app.start()
        wait_for(lambda: web.script("return getComputedStyle(document.documentElement).getPropertyValue('--accent').trim();"), lambda value: value == saved_theme["colors"]["accent"], "edited custom theme survives native restart")
        assert not web.script("return !!document.querySelector('.wallpaper-layer');"), "No open table means no wallpaper on the application chrome"
        open_project(web, data["name"]); web.applied(projectId=data["id"]); web.results(data["total"])
        wait_for(lambda: web.script("return !!document.querySelector('.table-body-surface.has-wallpaper');"), bool, "managed wallpaper survives without the source image")
        assert_planes(web)
        _open(web)
        state = web.script("""
          return {theme:document.querySelector('[aria-label="Appearance theme"]').value,
            strength:Number(document.querySelector('#wallpaper-strength').value),
            enabled:document.querySelector('.appearance-dialog input[type="checkbox"]').checked};
        """)
        assert state == {"theme": user_theme, "strength": 64, "enabled": True}, state
        restored = True
        report["appearance"] = {"custom_theme_import_export": True, "stable_theme_id": True,
            "actual_css_override": True, "managed_image_dimensions": decoded, "original_source_removed_before_restart": True,
            "normal_close_and_restart": True, "native_settings_restored": True,
            "shared_table_background_layers": paint["layers"], "slider_image_style_reassignments": 0,
            "legacy_preferences_migrate_to_body_cover": True, "source_files_unchanged": True,
            "unverified": "Native OS picker interaction; only selected-path responses are substituted"}
    finally:
        # Only the isolated profile's uniquely named custom theme is removed.
        try:
            web.script("window.__appearanceStyle?.restore();delete window.__appearanceStyle;window.__appearancePicker?.restore();delete window.__appearancePicker;")
            _open(web)
            web.choose('[aria-label="Appearance theme"]', "light")
            wait_for(lambda: web.script("return document.documentElement.dataset.theme;"), lambda value: value == "light", "appearance cleanup returns to Light")
            wait_for(lambda: web.script("return !document.querySelector('[aria-label=\"Appearance theme\"]').disabled;"),
                     bool, "appearance cleanup waits for the theme save")
            if web.script("return [...document.querySelectorAll('.appearance-dialog button')].some(e=>e.textContent.trim()==='Remove'&&!e.disabled);"):
                web.named("Remove", ".appearance-dialog")
            wait_for(lambda: web.script("return !document.querySelector('.wallpaper-layer');"), bool, "appearance cleanup removes the wallpaper")
            if (directory / "appearance.json").is_file():
                wait_for(lambda: _saved(directory / "appearance.json"), lambda value: value and value.get("enabled") is False, "cleanup saves the disabled background")
            wait_for(lambda: _saved(directory / "theme.json"), lambda value: value and value.get("theme") == "light", "cleanup Light selection is durable")
            _close(web)
            managed_theme.unlink(missing_ok=True)
            for path in extra_themes:
                path.unlink(missing_ok=True)
            open_project(web, data["name"])
            web.applied(projectId=data["id"])
            web.results(data["total"])
            _horizontal_to(web, 0)
        except Exception:
            if restored:
                raise
        finally:
            for source in (theme_source, image_source, exported):
                source.unlink(missing_ok=True)
