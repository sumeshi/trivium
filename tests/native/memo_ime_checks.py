"""Lossless note editing in the real app and synthetic IME event-order checks.

CompositionEvent/KeyboardEvent probes exercise WebView handlers, not a native
Japanese input method. Actual OS IME operation remains an explicit manual check.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import shutil
import tempfile
import uuid

from column_pin_checks import _key
from feedback_checks import _settle
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from runtime import native_driver_default


def _draft(web, selector: str, value: str) -> None:
    # A pasted multiline note must not turn each newline into a save shortcut.
    web.script("const e=document.querySelector(arguments[0]);e.value=arguments[1];e.dispatchEvent(new Event('input',{bubbles:true}));", selector, value)


def _stored(web, owner: str, row: int = 0):
    return web.command("query_project_rows", {"projectId": owner, "offset": row, "limit": 1})["rows"][0]


def _editor(web, row: int = 0) -> None:
    web.tap(web.node(f'.data-row[data-row-index="{row}"] .memo-button'))
    wait_for(lambda: web.script("return !!document.querySelector('.memo-dialog textarea');"), bool, "note editor opens")


def _saved(web) -> None:
    wait_for(lambda: web.script("return !document.querySelector('.memo-dialog');"), bool, "note save completes")


def verify_memo_ime(app, data: dict, root: Path, evidence: Path, report: dict) -> None:
    web = app.web
    # Earlier full-suite stages may intentionally leave this case filtered to
    # zero rows. Restore its real investigation state, not its source row count.
    if web.state()["active"] != f'project-tab-{data["id"]}':
        open_project(web, data["name"])
    web.applied(projectId=data["id"])
    _settle(web)
    outer_query = web.query()
    outer_total = web.state()["total"]
    web.results(outer_total)
    source = root / f"memo-plain-text-{uuid.uuid4().hex[:8]}.csv"
    source.write_text("id,message\n1,first\n2,second\n3,third\n", encoding="utf-8")
    original = source.read_bytes()
    case = web.command("create_project", {"path": str(source)})
    owner = case["meta"]["id"]
    samples = [
        "Sender: <alice@example.org>",
        "<alice@example.org>",
        '<CommandLine>whoami & "hostname"</CommandLine>',
        '<img src=x onerror="window.__memoUnsafe=true"><script>window.__memoUnsafe=true</script>',
        '  first line\n\tsecond line, "quoted"\n \t',
        ' \t\n ',
    ]
    try:
        open_project(web, case["meta"]["name"])
        web.results(3)
        for note in samples:
            _editor(web)
            _draft(web, '.memo-dialog textarea', note)
            web.named("Save", ".memo-dialog")
            _saved(web)
            assert _stored(web, owner)["memo"] == note
            displayed = web.script("const e=document.querySelector('.data-row[data-row-index=\"0\"] .memo-button');return {text:e.querySelector('.memo-text')?.textContent,title:e.title,hasMemo:e.classList.contains('has-memo'),label:e.getAttribute('aria-label'),unsafe:!!window.__memoUnsafe,markup:e.querySelectorAll('img,script').length};")
            assert displayed == {"text": note, "title": note, "hasMemo": True,
                "label": f"Edit note for row 1: {note}", "unsafe": False, "markup": 0}, displayed
            _editor(web)
            assert web.script("return document.querySelector('.memo-dialog textarea').value;") == note
            web.named("Save", ".memo-dialog")
            _saved(web)
            assert _stored(web, owner)["memo"] == note, "Editing again must not remove XML or whitespace"

        # Preserve remote/API notes too. Textareas normalize CRLF for display,
        # but opening and saving without an input event must keep the stored bytes.
        crlf = '  remote <alice@example.org>\r\n\t<XML>value</XML> \r\n'
        web.command("update_flag", {"projectId": owner, "row_index": 1, "memo": crlf})
        app.end_session()
        web = app.start()
        open_project(web, case["meta"]["name"])
        web.results(3)
        _editor(web, 1)
        assert web.script("return document.querySelector('.memo-dialog textarea').value;") == crlf.replace("\r\n", "\n")
        web.named("Save", ".memo-dialog")
        _saved(web)
        assert _stored(web, owner, 1)["memo"] == crlf

        _editor(web)
        _draft(web, '.memo-dialog textarea', "日本語 <alice@example.org>")
        before = len(web.trace())
        probes = web.script("""
          const e=document.querySelector('.memo-dialog textarea');
          const key=options=>{const k=new KeyboardEvent('keydown',{key:'Enter',bubbles:true,cancelable:true,...options});e.dispatchEvent(k);return k.defaultPrevented;};
          const results=[key({isComposing:true}),key({keyCode:229})];
          e.dispatchEvent(new CompositionEvent('compositionstart',{bubbles:true}));results.push(key({}));
          const escape=new KeyboardEvent('keydown',{key:'Escape',bubbles:true,cancelable:true});e.dispatchEvent(escape);
          e.dispatchEvent(new CompositionEvent('compositionend',{bubbles:true,data:'日本語'}));results.push(key({}));
          return {prevented:results,editor:!!document.querySelector('.memo-dialog'),text:e.value};
        """)
        assert probes == {"prevented": [False] * 4, "editor": True, "text": "日本語 <alice@example.org>"}, probes
        assert not any(entry["name"] == "update_flag" for entry in web.trace()[before:])
        assert _stored(web, owner)["memo"] == samples[-1]

        tag_input = '.memo-dialog .tag-input'
        _draft(web, tag_input, "調査中")
        tag_probes = web.script("""
          const e=document.querySelector('.memo-dialog .tag-input');e.focus();
          const key=options=>{const k=new KeyboardEvent('keydown',{key:'Enter',bubbles:true,cancelable:true,...options});e.dispatchEvent(k);return k.defaultPrevented;};
          const results=[key({isComposing:true}),key({keyCode:229})];
          e.dispatchEvent(new CompositionEvent('compositionstart',{bubbles:true}));results.push(key({}));
          const comma=new KeyboardEvent('keydown',{key:',',isComposing:true,bubbles:true,cancelable:true});e.dispatchEvent(comma);
          e.dispatchEvent(new CompositionEvent('compositionend',{bubbles:true,data:'調査中'}));results.push(key({}));
          return {prevented:results,tags:document.querySelectorAll('.memo-dialog .tag-chip').length,draft:e.value};
        """)
        assert tag_probes == {"prevented": [False] * 4, "tags": 0, "draft": "調査中"}, tag_probes
        web.async_script("const done=arguments[arguments.length-1];setTimeout(()=>done(true),100);")
        _key(web, "\ue007")
        wait_for(lambda: web.script("return [...document.querySelectorAll('.memo-dialog .tag-chip')].map(e=>e.firstChild.textContent.trim());"),
                 lambda tags: tags == ["調査中"], "normal Enter commits one tag without saving the note")
        assert web.script("return !!document.querySelector('.memo-dialog');")

        # Trusted Shift+Enter inserts a newline; a later Enter saves normally.
        web.script("const e=document.querySelector('.memo-dialog textarea');e.focus();e.setSelectionRange(e.value.length,e.value.length);")
        web.request("POST", web.route("actions"), {"actions": [{"type": "key", "id": "memo-enter", "actions": [
            {"type": "keyDown", "value": "\ue008"}, {"type": "keyDown", "value": "\ue007"},
            {"type": "keyUp", "value": "\ue007"}, {"type": "keyUp", "value": "\ue008"},
        ]}]})
        final = "日本語 <alice@example.org>\n"
        assert web.script("return document.querySelector('.memo-dialog textarea').value;") == final
        assert not any(entry["name"] == "update_flag" for entry in web.trace()[before:])
        _key(web, "\ue007")
        _saved(web)
        assert _stored(web, owner)["memo"] == final and _stored(web, owner)["tags"] == ["調査中"]

        app.end_session()
        web = app.start()
        open_project(web, case["meta"]["name"])
        web.results(3)
        assert _stored(web, owner)["memo"] == final
        assert _stored(web, owner, 1)["memo"] == crlf
        _editor(web)
        assert web.script("return document.querySelector('.memo-dialog textarea').value;") == final
        web.screenshot(evidence / "memo-plain-text.png")
        web.named("Cancel", ".memo-dialog")
        destination = root / "notes-export.csv"
        web.command("export_project", {"projectId": owner, "destination": str(destination), "scope": "all", "includeAnnotations": True})
        with destination.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert rows[0]["trivium-memo"] == final and rows[1]["trivium-memo"] == crlf
        assert source.read_bytes() == original
        report["memo_ime"] = {"plain_text_cases": len(samples), "markup_not_executed": True,
            "native_restart": True, "remote_crlf_noop_edit": True, "csv_exact": True,
            "synthetic_composition_orders": ["isComposing", "keyCode229", "compositionstart", "compositionend-before-keydown"],
            "normal_enter_and_shift_enter": True, "real_os_japanese_ime": "unverified"}
    finally:
        if web.script("return !!document.querySelector('.memo-dialog');"):
            web.named("Cancel", ".memo-dialog")
        open_project(web, data["name"])
        web.applied(**outer_query)
        web.results(outer_total)
        if web.script("return !!document.querySelector(arguments[0]);", f'#project-tab-{owner}'):
            web.named(f"Close {source.name}")
        delete_fixture_project(web, owner)


def main() -> None:
    from workspace_ui import NativeApp, fixture

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--driver", default=shutil.which("tauri-driver") or "/workspace/.trivium-tools/bin/tauri-driver")
    parser.add_argument("--native-driver", default=native_driver_default())
    parser.add_argument("--output", type=Path, default=Path("target/native-memo-ime"))
    args = parser.parse_args()
    evidence = args.output.resolve() / uuid.uuid4().hex[:12]
    evidence.mkdir(parents=True)
    report = {"passed": False, "unverified": ["Actual Linux and Windows Japanese IME operation", "Native export destination picker"]}
    with tempfile.TemporaryDirectory(prefix="trivium-native-memo-") as temporary:
        root = Path(temporary)
        app = NativeApp(args, root, evidence)
        try:
            web = app.start()
            data = fixture(web, root, total=12)
            verify_memo_ime(app, data, root, evidence, report)
            assert not any(session["js_errors"] for session in app.sessions + [app.web.diagnostics()])
            report["passed"] = True
        finally:
            app.close()
            (evidence / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
