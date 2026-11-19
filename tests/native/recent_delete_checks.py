"""Recent deletion confirms intent and retains rows until real storage replies."""
from __future__ import annotations

from pathlib import Path
import uuid

from column_pin_checks import _key
from feedback_checks import _settle
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for


def _recent(web, name: str):
    return web.script("return [...document.querySelectorAll('.project-row')].find(row=>row.querySelector('.project-name')?.textContent.trim()===arguments[0])??null;", name)


def verify_recent_delete(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    outer_query, outer_total = web.query(), web.state()["total"]
    source = root / f"recent-delete-{uuid.uuid4().hex[:8]}.csv"
    original = b"host,message\nalpha,keep evidence\nbeta,second record\n"
    source.write_bytes(original)
    case = web.command("create_project", {"path": str(source)})
    owner, name = case["meta"]["id"], case["meta"]["name"]
    annotation = {"flag": "critical", "memo": "Keep until deletion is confirmed.", "tags": ["manual"], "bookmarked": True}
    web.command("update_flag", {"projectId": owner, "row_index": 0, **annotation})
    deleted = False
    try:
        web.close_drawer()
        open_project(web, name)
        web.results(2)
        web.named("Open project menu")
        wait_for(lambda: _recent(web, name), bool, "saved case appears in Recent")
        before = web.state()["active"]
        start = len(web.trace())
        web.named(f"Delete {name}", ".workspace-drawer")
        wait_for(lambda: web.script("return document.querySelector('.delete-project-dialog')?.getAttribute('role');"),
                 lambda role: role == "alertdialog", "trash opens an explicit confirmation")
        wait_for(lambda: web.script("return document.activeElement?.textContent.trim();"),
                 lambda text: text == "Cancel", "deletion defaults to Cancel")
        assert _recent(web, name) and web.state()["active"] == before
        assert not any(entry["name"] == "delete_project" for entry in web.trace()[start:]), "Opening confirmation does not begin deletion"
        web.screenshot(evidence / "recent-delete-confirmation.png")
        web.named("Cancel", ".delete-project-dialog")
        wait_for(lambda: web.script("return !document.querySelector('.delete-project-dialog');"), bool, "Cancel dismisses the confirmation")
        assert _recent(web, name) and web.state()["active"] == before
        assert not any(entry["name"] == "delete_project" for entry in web.trace()[start:])
        saved = web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 1})["rows"][0]
        assert all(saved[field] == value for field, value in annotation.items()), saved

        web.named(f"Delete {name}", ".workspace-drawer")
        _key(web, "\ue00c")
        wait_for(lambda: web.script("return !document.querySelector('.delete-project-dialog');"), bool, "Escape cancels deletion")
        assert _recent(web, name) and web.state()["active"] == before

        web.script("window.__workspaceHold={name:'list_projects',ready:false};")
        web.named("Refresh projects", ".workspace-drawer")
        wait_for(lambda: web.script("return !!window.__workspaceHold?.ready;"), bool, "real Recent refresh reply is held")
        assert _recent(web, name), "Refreshing Recent must retain existing rows"
        web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
        _settle(web)

        web.script("window.__workspaceHold={name:'delete_project',projectId:arguments[0],ready:false};", owner)
        web.named(f"Delete {name}", ".workspace-drawer")
        web.named("Delete", ".delete-project-dialog")
        wait_for(lambda: web.script("return !!window.__workspaceHold?.ready;"), bool, "real deletion reply is held after storage finishes")
        deleted = True
        pending = web.script("const row=arguments[0];return {busy:row?.getAttribute('aria-busy'),disabled:row?.querySelector('.project-card').disabled,trashDisabled:row?.querySelector('.delete-button').disabled,text:row?.textContent};", _recent(web, name))
        assert pending["busy"] == "true" and pending["disabled"] and pending["trashDisabled"] and "Deleting" in pending["text"], pending
        assert not web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{owner}"), "Only a confirmed delete closes its tab"
        assert not any(project["meta"]["id"] == owner for project in web.command("list_projects")), "The held reply follows the real storage deletion"
        refresh_start = len(web.trace())
        web.named("Refresh projects", ".workspace-drawer")
        wait_for(lambda: [entry for entry in web.trace()[refresh_start:] if entry["name"] == "list_projects" and "result" in entry],
                 bool, "the pending-delete refresh receives its real saved-project list")
        _settle(web)
        wait_for(lambda: web.script("return arguments[0]?.getAttribute('aria-busy');", _recent(web, name)),
                 lambda busy: busy == "true", "refresh retains the row awaiting deletion acknowledgment")
        web.screenshot(evidence / "recent-delete-pending.png")
        web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
        wait_for(lambda: _recent(web, name), lambda row: row is None, "only the acknowledged case leaves Recent")
        assert any(project["meta"]["id"] == data["id"] for project in web.command("list_projects"))
        assert source.read_bytes() == original, "Deleting a saved investigation leaves its original source intact"
        report["recent_delete"] = {"confirmation_before_mutation": True, "cancel_preserves_case_and_annotations": True,
            "escape_cancels": True, "recent_refresh_keeps_rows": True, "row_retained_until_delete_ack": True,
            "refresh_during_delete_keeps_pending_row": True, "source_unchanged": True}
    finally:
        web.script("window.__workspaceHold?.release?.();delete window.__workspaceHold;")
        if web.script("return !!document.querySelector('.delete-project-dialog');"):
            web.named("Cancel", ".delete-project-dialog")
            wait_for(lambda: web.script("return !document.querySelector('.delete-project-dialog');"), bool, "cleanup closes delete confirmation")
        web.close_drawer()
        open_project(web, data["name"])
        web.applied(**outer_query)
        web.results(outer_total)
        if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{owner}"):
            web.named(f"Close {source.name}")
        if not deleted and any(project["meta"]["id"] == owner for project in web.command("list_projects")):
            delete_fixture_project(web, owner)
