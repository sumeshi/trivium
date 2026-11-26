"""Exercise the actual renderer IPC file boundary, without granting test access."""
from __future__ import annotations

import os
from pathlib import Path
import uuid

from appearance_checks import _profile_directory


def _read(web, path: Path) -> dict:
    return web.async_script("""
      const done=arguments[arguments.length-1];
      window.__TAURI_INTERNALS__.invoke('plugin:fs|read_text_file',{path:arguments[0]})
        .then(value=>done({ok:true,text:new TextDecoder().decode(
          value instanceof ArrayBuffer?value:Uint8Array.from(value))}),
          error=>done({ok:false,denied:/forbidden|not allowed/i.test(String(error))}));
    """, str(path))


def _write(web, path: Path, text: str) -> dict:
    return web.async_script("""
      const done=arguments[arguments.length-1];
      window.__TAURI_INTERNALS__.invoke('plugin:fs|write_text_file',new TextEncoder().encode(arguments[1]),
        {headers:{path:encodeURIComponent(arguments[0]),options:'{}'}})
        .then(()=>done({ok:true}),error=>done({ok:false,denied:/forbidden|not allowed/i.test(String(error))}));
    """, str(path), text)


def _import(web, path: Path, kind: str, report: dict) -> dict:
    before = web.checkpoint()
    result = web.async_script("""
      const done=arguments[arguments.length-1];
      window.__TAURI_INTERNALS__.invoke('read_import_text',{path:arguments[0],kind:arguments[1]})
        .then(text=>done({ok:true,text}),error=>done({ok:false,
          denied:String(error)==='Choose the import file in the file picker first.'}));
    """, str(path), kind)
    if not result["ok"]:
        entries = [entry for entry in web.trace(after=before) if entry["name"] == "read_import_text"
                   and entry.get("error") == "Choose the import file in the file picker first."]
        assert len(entries) == 1, "The expected import refusal must be observed exactly once"
        report.setdefault("boundary_expected_errors", []).append({
            key: entries[0][key] for key in ("name", "request", "error")
        })
    return result


def verify_fs_scope(web, root: Path, report: dict) -> None:
    directory = _profile_directory(web, root)
    token = uuid.uuid4().hex[:10]
    themes = directory / "themes"
    themes.mkdir(exist_ok=True)
    external = root / f"fs-private-{token}.json"
    internal = directory / f"fs-private-{token}.json"
    sibling = root / f"fs-sibling-{token}.json"
    destination = root / f"fs-picked-{token}.json"
    profile = themes / f"fs-profile-{token}.json"
    nested = themes / f"fs-nested-{token}"
    nested_profile = nested / "private.json"
    non_json = themes / f"fs-profile-{token}.json.bak"
    linked = themes / f"fs-link-{token}.json"
    preference = directory / "table-text-size.json"
    old_preference = preference.read_bytes() if preference.exists() else None
    sentinel = '{"private":"not selected"}'
    profile_text = '{"catalog":"allowed"}'
    try:
        nested.mkdir()
        for path in (nested_profile, non_json):
            path.write_text(sentinel, encoding="utf-8")
            assert _read(web, path) == {"ok": False, "denied": True}
            assert _write(web, path, "changed") == {"ok": False, "denied": True}
            assert _import(web, path, "theme", report) == {"ok": False, "denied": True}
            assert path.read_text(encoding="utf-8") == sentinel
        for path in (external, internal, sibling):
            path.write_text(sentinel, encoding="utf-8")
            assert _read(web, path) == {"ok": False, "denied": True}
            assert _write(web, path, "changed") == {"ok": False, "denied": True}
            assert path.read_text(encoding="utf-8") == sentinel
            for kind in ("theme", "pattern"):
                assert _import(web, path, kind, report) == {"ok": False, "denied": True}

        # Only the actual preference files are general renderer-readable data.
        preference.write_text("100", encoding="utf-8")
        assert _read(web, preference) == {"ok": True, "text": "100"}
        assert _write(web, preference, "100") == {"ok": True}
        assert preference.read_text(encoding="utf-8") == "100"

        assert _write(web, profile, profile_text) == {"ok": True}
        assert profile.read_text(encoding="utf-8") == profile_text
        assert _read(web, profile) == {"ok": False, "denied": True}
        assert _import(web, profile, "theme", report) == {"ok": True, "text": profile_text}
        assert _import(web, profile, "pattern", report) == {"ok": False, "denied": True}

        symlink_checked = False
        if os.name != "nt":
            linked.symlink_to(external)
            assert _write(web, linked, "changed") == {"ok": False, "denied": True}
            assert _import(web, linked, "theme", report) == {"ok": False, "denied": True}
            assert external.read_text(encoding="utf-8") == sentinel
            symlink_checked = True
        native_pickers = False
        if os.name != "nt":
            from native_file_picker import choose_native_file

            picked = Path(choose_native_file(web, external))
            assert _read(web, picked) == {"ok": True, "text": sentinel}
            for kind in ("theme", "pattern"):
                assert _import(web, picked, kind, report) == {"ok": True, "text": sentinel}
            assert _write(web, destination, profile_text) == {"ok": False, "denied": True}
            assert not destination.exists()
            picked_destination = Path(choose_native_file(web, destination, save=True))
            assert _write(web, picked_destination, profile_text) == {"ok": True}
            assert _read(web, picked_destination) == {"ok": True, "text": profile_text}
            assert destination.read_text(encoding="utf-8") == profile_text
            # Picking a file must not grant the directory or neighboring files.
            assert _read(web, sibling) == {"ok": False, "denied": True}
            assert _write(web, sibling, "changed") == {"ok": False, "denied": True}
            assert _import(web, sibling, "theme", report) == {"ok": False, "denied": True}
            assert sibling.read_text(encoding="utf-8") == sentinel
            native_pickers = True
        report["fs_scope"] = {"unselected_reads_and_writes_denied": True,
            "unrelated_app_data_denied": True, "unselected_bounded_import_denied": True,
            "preference_read_write": True, "managed_theme_write_and_bounded_read": True,
            "managed_theme_is_not_pattern_import_permission": True,
            "nested_and_non_json_theme_paths_denied": True,
            "symlink_escape_denied": symlink_checked,
            "native_open_and_save_grants": native_pickers,
            "picked_file_does_not_grant_siblings": native_pickers,
            "unverified": "Windows native picker interaction" if not native_pickers else None}
    finally:
        for path in (linked, profile, external, internal, sibling, destination, nested_profile, non_json):
            path.unlink(missing_ok=True)
        if nested.exists():
            nested.rmdir()
        if old_preference is None:
            preference.unlink(missing_ok=True)
        else:
            preference.write_bytes(old_preference)
