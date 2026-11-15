"""Check production CSP in the actual native WebView, without network access."""
from row_navigation import wait_for


def verify_content_policy(web, report: dict) -> None:
    # WebDriver itself can evaluate scripts. A newly inserted page script must
    # still be blocked by the page's policy; its code contains only a marker.
    web.script("""
      window.__cspInlineRan=false;window.__cspViolations=[];
      window.__cspListener=e=>window.__cspViolations.push(e.effectiveDirective);
      addEventListener('securitypolicyviolation',window.__cspListener);
      const node=document.createElement('script');
      node.textContent='window.__cspInlineRan=true';document.head.append(node);node.remove();
    """)
    try:
        wait_for(lambda: web.script("return window.__cspViolations;"),
                 lambda values: "script-src-elem" in values or "script-src" in values,
                 "production policy blocks new inline page scripts")
        assert not web.script("return window.__cspInlineRan;")
        assert web.command("list_projects", {}, request=True), "Normal native IPC remains available"
        report["content_policy"] = {"untrusted_inline_blocked": True, "native_ipc_works": True}
    finally:
        web.script("removeEventListener('securitypolicyviolation',window.__cspListener);delete window.__cspListener;")
