"""Cleanup waits for observed writes, including serialized followers."""
import unittest
from unittest.mock import patch

from fixture_cleanup import delete_fixture_project


def write(name, owner="fixture", **completion):
    return {"name": name, "request": {"projectId": owner}, **completion}


class FakeWeb:
    def __init__(self, snapshots, closed=True):
        self.snapshots = iter(snapshots)
        self.last = []
        self.closed = closed
        self.events = []
        self.command_calls = []

    def script(self, _source, selector):
        self.events.append(("closed", selector))
        return self.closed

    def async_script(self, _source):
        self.events.append(("teardown",))
        return True

    def trace(self):
        self.last = next(self.snapshots, self.last)
        self.events.append(("trace",))
        return self.last

    def command(self, name, payload, *, request):
        self.events.append(("delete",))
        self.command_calls.append((name, payload, request))
        return {"deleted": True}


class FixtureCleanupTests(unittest.TestCase):
    def test_delayed_write_then_serialized_follower_both_finish_before_delete(self):
        first = write("save_view_state")
        first_done = write("save_view_state", result=None)
        second = write("save_view_state")
        second_done = write("save_view_state", result=None)
        web = FakeWeb([[first], [first_done, second], [first_done, second_done]])
        result = delete_fixture_project(web, "fixture")
        self.assertEqual(result, {"deleted": True})
        self.assertEqual(web.events, [("closed", "#project-tab-fixture"), ("teardown",),
            ("trace",), ("trace",), ("trace",), ("delete",)])
        self.assertEqual(web.command_calls, [("delete_project", {"projectId": "fixture"}, True)])

    def test_waits_for_annotation_and_hidden_writes_but_not_other_projects_or_reads(self):
        unrelated = [write("save_view_state", owner="other"), write("query_project_rows")]
        for name in ["update_flag", "bulk_update", "bulk_undo", "set_hidden_columns", "save_iocs", "import_iocs", "save_color_rules"]:
            with self.subTest(name=name):
                web = FakeWeb([[*unrelated, write(name)], [*unrelated, write(name, result={})]])
                delete_fixture_project(web, "fixture")
                self.assertEqual(len([event for event in web.events if event == ("trace",)]), 2)
                self.assertEqual(len(web.command_calls), 1)

    def test_completed_failure_is_preserved_for_strict_final_validation(self):
        failed = write("save_view_state", error="original storage failure")
        web = FakeWeb([[failed]])
        delete_fixture_project(web, "fixture")
        self.assertEqual(web.last, [failed])
        self.assertEqual(web.last[0]["error"], "original storage failure")

    def test_open_tab_or_pending_save_cannot_reach_direct_delete(self):
        for web in [FakeWeb([[]], closed=False), FakeWeb([[write("save_view_state")]])]:
            with self.subTest(closed=web.closed):
                def immediate(read, ready, label):
                    value = read()
                    self.assertTrue(ready(value), label)
                    return value
                with patch("fixture_cleanup.wait_for", immediate), self.assertRaises(AssertionError):
                    delete_fixture_project(web, "fixture")
                self.assertFalse(web.command_calls)
                self.assertEqual(("trace",) in web.events, web.closed)


if __name__ == "__main__":
    unittest.main()
