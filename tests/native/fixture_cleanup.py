"""Direct fixture deletion must follow the UI's final project writes."""
from __future__ import annotations

from row_navigation import wait_for


PROJECT_WRITES = frozenset({
    "save_view_state", "set_hidden_columns", "update_flag", "bulk_update", "bulk_undo",
    "save_iocs", "import_iocs", "save_color_rules",
})


def delete_fixture_project(web, project_id: str):
    """Finish tab teardown and its real IPC responses before deleting its data.

    This is test cleanup's direct native invocation, outside the application's
    normal delete action. Completed errors remain in the trace for the final
    strict validation; they are neither hidden nor accepted by this barrier.
    """
    wait_for(lambda: web.script("return !document.querySelector(arguments[0]);", f"#project-tab-{project_id}"),
             bool, "fixture tab closes before direct deletion")
    web.async_script("const done=arguments[arguments.length-1];requestAnimationFrame(()=>requestAnimationFrame(()=>done(true)));")

    def pending():
        # A completed write can dispatch its serialized successor in the same
        # JS microtask drain, before WebDriver's next trace-reading task runs.
        return sum(entry["name"] in PROJECT_WRITES and entry.get("request", {}).get("projectId") == project_id
                   and "result" not in entry and "error" not in entry for entry in web.trace())

    wait_for(pending, lambda count: count == 0, "closed fixture finishes its pending project saves")
    return web.command("delete_project", {"projectId": project_id}, request=True)
