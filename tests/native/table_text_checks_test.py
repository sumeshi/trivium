"""Density changes retain query identity and bounded page prefetch."""
import unittest

from table_text_checks import _check_density_requests


class DensityRequestTests(unittest.TestCase):
    query = {"projectId": "synthetic", "search": "cohort:keep", "condition": None}

    def request(self, offset=250, **changes):
        return {"name": "query_project_rows", "request": {**self.query, "offset": offset, "limit": 250, **changes}}

    def check(self, entries):
        _check_density_requests(self.query, entries, rendered_pages={0}, loaded_pages={0}, total=1000)

    def test_normal_adjacent_prefetch_does_not_need_to_be_rendered(self):
        self.check([self.request()])

    def test_off_range_page_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "bounded prefetch"):
            self.check([self.request(offset=750)])

    def test_wrong_query_and_source_lookup_are_rejected(self):
        for entry in [self.request(search="other"), {"name": "locate_project_row"}]:
            with self.subTest(entry=entry), self.assertRaises(AssertionError):
                self.check([entry])

    def test_duplicate_or_already_rendered_page_is_rejected(self):
        for entries in [[self.request(), self.request()], [self.request(offset=0)]]:
            with self.subTest(entries=entries), self.assertRaisesRegex(AssertionError, "must not reload"):
                self.check(entries)

    def test_page_size_and_alignment_are_preserved(self):
        for entry in [self.request(offset=251), self.request(limit=500)]:
            with self.subTest(entry=entry), self.assertRaises(AssertionError):
                self.check([entry])


if __name__ == "__main__":
    unittest.main()
