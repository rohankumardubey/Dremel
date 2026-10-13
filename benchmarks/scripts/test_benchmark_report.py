#!/usr/bin/env python3
"""Report provenance checks independent of the engine executables."""
import json
import tempfile
import unittest
from pathlib import Path

from generate_benchmark_report import load_report_data


class ReportProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory(prefix="dremel-report-test-")
        self.addCleanup(self.workspace.cleanup)
        self.root = Path(self.workspace.name)
        (self.root / "environment.txt").write_text("rustc: old compiler\ndataset rows: 20000\n")

    def suite(self, path, data):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(data))

    def test_standalone_suite_uses_its_own_environment(self):
        self.suite("nested-operators/nested-operators.json", {
            "rustc": "current compiler", "rows": 100000, "iterations": 10, "queries": [],
        })
        report = load_report_data(self.root, ["nested-operators"])
        self.assertEqual(report["environment"]["Nested value operators rustc"], "current compiler")
        self.assertEqual(report["environment"]["Nested value operators rows"], "100000")
        self.assertNotIn("old compiler", report["environment"].values())
        self.assertNotIn("dataset rows", report["environment"])

    def test_baseline_retains_captured_baseline_environment(self):
        self.suite("comparison.json", {"queries": []})
        self.assertEqual(load_report_data(self.root, ["baseline"])["environment"], {
            "rustc": "old compiler", "dataset rows": "20000",
        })

    def test_multiple_feature_suites_keep_separate_provenance(self):
        self.suite("nested-sql/nested-sql.json", {"rustc": "compiler A", "rows": 500})
        self.suite("nested-operators/nested-operators.json", {"rustc": "compiler B", "rows": 100000})
        environment = load_report_data(self.root, ["nested-sql", "nested-operators"])["environment"]
        self.assertEqual(environment["Nested SQL rustc"], "compiler A")
        self.assertEqual(environment["Nested value operators rustc"], "compiler B")

    def test_absent_provenance_is_not_invented(self):
        self.suite("nested-operators/nested-operators.json", {"queries": []})
        self.assertEqual(load_report_data(self.root, ["nested-operators"])["environment"], {})


if __name__ == "__main__":
    unittest.main()
