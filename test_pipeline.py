#!/usr/bin/env python3
"""Fast deterministic unit checks, independent of RustFS/network availability."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline import map_chunk


class MapperTest(unittest.TestCase):
    def test_filter_and_1_2_3_skill_counts(self):
        rows = [
            {"name": "Python Engineer", "key_skills": "Python; SQL; Linux"},
            {"name": "Backend Developer", "key_skills": "python;SQL; python"},
            {"name": "Java Engineer", "key_skills": "Java"},
        ]
        n, counts, _, hist, contributions = map_chunk(rows)
        self.assertEqual(n, 2)
        self.assertEqual(counts[0], {"linux": 1, "python": 2, "sql": 2})
        self.assertEqual(counts[1], {"linux + python": 1, "linux + sql": 1, "python + sql": 2})
        self.assertEqual(counts[2], {"linux + python + sql": 1})
        self.assertEqual(hist, {2: 1, 3: 1})
        self.assertEqual(contributions, [5, 4, 1])

    def test_mapper_failure_propagates(self):
        with self.assertRaisesRegex(RuntimeError, "injected mapper failure"):
            map_chunk([{"name": "Python", "key_skills": "Python"}], fail_at=0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
