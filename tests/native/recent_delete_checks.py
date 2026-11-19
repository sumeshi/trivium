"""Recent deletion confirms intent and retains rows until real storage replies."""
from __future__ import annotations

from pathlib import Path
import uuid

from column_pin_checks import _key
from feedback_checks import _settle
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from storage_responsiveness_checks import _case_database


def _recent(web, name: str):
    return web.script("return [...document.querySelectorAll('.project-row')].find(row=>row.querySelector('.project-name')?.textContent.trim()===arguments[0])??null;", name)


def _failed_reload_retry(web, owner: str, root: Path, evidence: Path, report: dict) -> None:
    web.named("Close workspace menu", ".workspace-drawer")
    wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "Recent closes before reloading")
    source = _case_database(root, owner).with_name("data.parquet")
    assert source.is_file(), "The owned native-test case data file was not found"
    missing = source.with_name("data.parquet-unavailable")
    source.rename(missing)
    try:
        start = len(web.trace())
        web.named("Workspace options")
        web.named("Reload saved project", ".workspace-options")
        wait_for(lambda: web.script("return document.querySelector('.project-load-error[role=alert]')?.textContent??'';"),
                 lambda text: "Project data file missing." in text and "Retry" in text,
                 "an actual case I/O failure keeps a reason and retry control in the workspace")
        assert web.state()["active"] == f"project-tab-{owner}"
        assert not web.script("return !!document.querySelector('.project-view');"), "A failed reload must not show stale evidence"
        errors = [entry for entry in web.trace()[start:] if entry["name"] == "load_project"
                  and entry["request"].get("projectId") == owner and entry.get("error")]
        assert len(errors) == 1 and errors[0]["error"] == "Project data file missing.", errors
        report.setdefault("boundary_expected_errors", []).append({key: errors[0][key] for key in ("name", "request", "error")})
        web.screenshot(evidence / "project-load-error-retry.png")
        missing.rename(source)
        retry_start = len(web.trace())
        web.named("Retry loading project")
        web.applied(projectId=owner)
        web.results(2)
        calls = web.trace()[retry_start:]
        assert len([entry for entry in calls if entry["name"] == "load_project" and entry["request"].get("projectId") == owner]) == 1
        assert not any(entry["name"] in {"create_project", "list_projects"} for entry in calls), "Retry reopens the saved case without importing or rescanning Recent"
        assert not web.script("return !!document.querySelector('.project-load-error');")
        row = web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 1})["rows"][0]
        assert row["flag"] == "critical" and row["memo"] == "Keep until deletion is confirmed." and row["bookmarked"], row
        report["project_load_retry"] = {"actual_missing_case_data": True, "persistent_reason": True,
            "stale_rows_hidden": True, "explicit_retry_only": True, "annotations_preserved": True}
    finally:
        if missing.exists():
            missing.rename(source)
    web.named("Open project menu")


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
        _failed_reload_retry(web, owner, root, evidence, report)

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
