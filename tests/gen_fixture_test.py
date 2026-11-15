"""Benchmark fixture properties, independent of engine timing."""
import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "gen_fixture", Path(__file__).resolve().parents[1] / "scripts/gen_fixture.py"
)
fixture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fixture)


class FixtureTest(unittest.TestCase):
    def test_permuted_key_is_unique_and_independent_of_record_order(self):
        for count in (1, 27, 1000, 104813):
            rows = fixture.iter_rows(count, 42, "ascii", shuffle_sort=True)
            keys = [int(row["sort_key"]) for row in rows]
            self.assertEqual(sorted(keys), list(range(count)))
            if count > 27:
                self.assertGreater(sum(abs(a - b) for a, b in zip(keys, keys[1:])), count)

    def test_default_fixture_values_unchanged_by_optional_sort_key(self):
        normal = list(fixture.iter_rows(20, 42, "long"))
        shuffled = list(fixture.iter_rows(20, 42, "long", shuffle_sort=True))
        self.assertEqual(len(fixture.header_for("long")), 120)
        self.assertEqual(len(fixture.header_for("long", True)), 121)
        for row in shuffled:
            row.pop("sort_key")
        self.assertEqual(normal, shuffled)

    def test_long_csv_and_jsonl_have_identical_source_values(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / name for name in ("long.csv", "long.jsonl")]
            for path, output_format in zip(paths, ("csv", "jsonl")):
                fixture.write_output(path, fixture.iter_rows(4, 42, "long", True),
                                     "long", output_format, True)
            with paths[0].open(newline="") as source:
                csv_rows = list(csv.DictReader(source))
            json_rows = [json.loads(line) for line in paths[1].read_text().splitlines()]
            self.assertEqual(csv_rows, json_rows)
            self.assertGreater(len(json_rows[0]["detail01"]), 100)


if __name__ == "__main__":
    unittest.main()
