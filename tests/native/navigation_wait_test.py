"""The native harness must not mistake an old visible page for a completed action."""
import unittest

from row_navigation import completed_ipc


class NavigationWaitTests(unittest.TestCase):
    def test_identical_source_and_ascending_rows_still_require_the_new_sort_response(self):
        expected = {"projectId": "case", "search": "keep", "sortKey": "score", "sortDirection": "asc", "offset": 0}
        old = {"started": 4, "request": {**expected, "sortKey": None}, "result": {"total_filtered_rows": 2599}}
        pending = {"started": 6, "request": expected}
        self.assertIsNone(completed_ipc([old, pending], 5, expected))
        pending["result"] = {"total_filtered_rows": 2599}
        self.assertIs(completed_ipc([old, pending], 5, expected), pending)

    def test_visible_target_does_not_accept_an_old_lookup_or_another_project(self):
        expected = {"projectId": "case", "rowIndex": 3897}
        old = {"started": 1, "request": expected, "result": {"position": 650}}
        other = {"started": 7, "request": {**expected, "projectId": "other"}, "result": {"position": 650}}
        pending = {"started": 8, "request": expected}
        self.assertIsNone(completed_ipc([old, other, pending], 5, expected))
        pending["result"] = {"position": 650}
        self.assertIs(completed_ipc([old, other, pending], 5, expected), pending)
        # WorkspaceView's bounded observer deliberately omits the position.
        pending["result"] = {}
        self.assertIs(completed_ipc([old, other, pending], 5, expected), pending)

    def test_newest_matching_request_wins_and_errors_are_not_retried(self):
        expected = {"rowIndex": 0}
        finished = {"started": 6, "request": expected, "result": {"position": 0}}
        latest = {"started": 9, "request": expected}
        self.assertIsNone(completed_ipc([finished, latest], 5, expected))
        latest["error"] = "native lookup failed"
        with self.assertRaisesRegex(AssertionError, "native lookup failed"):
            completed_ipc([finished, latest], 5, expected)
        # A timestamp cutoff remains valid after the bounded trace evicts old entries.
        self.assertIs(completed_ipc([finished], 5, expected), finished)


if __name__ == "__main__":
    unittest.main()
