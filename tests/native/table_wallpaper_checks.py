"""Real rows, cumulative column pins and native screenshot paint isolation."""
from __future__ import annotations

import csv
from pathlib import Path
import uuid

from column_presentation_checks import _action
from feedback_checks import _horizontal_to, _settle
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from wallpaper_paint_checks import assert_no_text_leak, assert_planes, capture, changed_pixels


def verify_table_wallpaper(web, data: dict, root: Path, evidence: Path, report: dict,
                           set_enabled) -> None:
    source = root / f"wallpaper-paint-{uuid.uuid4().hex[:10]}.csv"
    labels = [f"field_{index}" for index in range(12)]
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(labels)
        writer.writerows([f"cell-{column}-{row}-{'sample_' * 5}" for column in range(12)] for row in range(400))
    original = source.read_bytes()
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    try:
        open_project(web, case["meta"]["name"])
        web.applied(projectId=project_id); web.results(400)
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
        _horizontal_to(web, 700)
        ordinary = assert_no_text_leak(web)
        planes = assert_planes(web)
        web.screenshot(evidence / "wallpaper-fixed-cells.png")

        # A normal even row uses the same base surface with/without wallpaper.
        # Sample below the glyphs, inside the fixed note area; a changed stripe
        # or control must not stand in for proof that the image itself is visible.
        x, y, _, height = planes["rect"]
        row = planes["rowRect"]
        fixed_background = [x + 180, row[1] + row[3] - 4, planes["annotationWidth"] - 190, 2]
        enabled = capture(web)
        set_enabled(False)
        assert not web.script("return !!document.querySelector('.wallpaper-layer');")
        disabled = capture(web)
        visible_fixed = changed_pixels(web, enabled, disabled, fixed_background)
        assert isinstance(visible_fixed, int) and visible_fixed > 30, "The wallpaper must actually show through the fixed cells"
        set_enabled(True)

        # Pin enough columns to exceed the viewport, then pan the frozen prefix
        # beneath the always-fixed annotations using the real header action.
        web.script("document.querySelector('.data-header [data-column-id=\"'+arguments[0]+'\"] .column-sort').focus({preventScroll:true});", columns[labels[3]])
        _settle(web)
        _action(web, columns[labels[3]], "Freeze columns through this column")
        width = web.script("return document.querySelector('.virtual-viewport').clientWidth;")
        prefix = web.script("return [...document.querySelectorAll('.data-header>.sticky')].reduce((sum,e)=>sum+e.getBoundingClientRect().width,0);")
        assert prefix > width, (prefix, width)
        _horizontal_to(web, round(max(700, prefix - width + 160)))
        frozen = assert_no_text_leak(web, panned_frozen=True)
        web.screenshot(evidence / "wallpaper-oversized-fixed-prefix.png")

        # The background belongs to the body viewport, not to the virtual rows.
        # Recycling a cached vertical range must preserve both image planes.
        web.script("""
          const surface=document.querySelector('.table-body-surface');
          window.__wallpaperScrollPlanes=[...surface.querySelectorAll('.wallpaper-layer')];
          window.__wallpaperScrollImage=surface.style.getPropertyValue('--wallpaper-image');
          document.querySelector('.virtual-viewport').scrollTop=1000;
        """)
        wait_for(lambda: web.state()["scrollTop"], lambda top: top >= 999, "real viewport scrolls beneath the fixed wallpaper")
        _settle(web)
        assert web.script("""
          const surface=document.querySelector('.table-body-surface'),layers=[...surface.querySelectorAll('.wallpaper-layer')];
          const same=window.__wallpaperScrollPlanes.every((layer,index)=>layer===layers[index])&&
            surface.style.getPropertyValue('--wallpaper-image')===window.__wallpaperScrollImage;
          delete window.__wallpaperScrollPlanes;delete window.__wallpaperScrollImage;return same;
        """), "Virtual scrolling must not rebuild the wallpaper"
        assert source.read_bytes() == original
        report["table_wallpaper"] = {"body_only": True, "always_cover": True,
            "shared_background_planes": planes["layers"], "fixed_image_visibility_changed_pixels": visible_fixed,
            "scrolling_cells_hidden": ordinary, "panned_frozen_cells_hidden": frozen,
            "vertical_scroll_retains_image_planes": True, "source_bytes_unchanged": True}
    finally:
        open_project(web, data["name"])
        web.applied(projectId=data["id"])
        if web.script("return !!document.getElementById(arguments[0]);", f"project-tab-{project_id}"):
            web.named(f"Close {source.name}")
        delete_fixture_project(web, project_id)
