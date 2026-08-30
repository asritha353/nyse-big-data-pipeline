"""Focused unit tests for Airflow helper invariants that need no live cluster."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "airflow" / "dags"))

from pipeline_helpers import (  # noqa: E402
    PipelineValidationError,
    assert_counts,
    parse_integer_lines,
    safe_archive_relative_path,
)


class PipelineHelperTests(unittest.TestCase):
    def test_parse_integer_lines_ignores_tool_logs(self) -> None:
        output = "WARN startup\n15120\nTime taken: 1.2 seconds\n20\n"
        self.assertEqual(parse_integer_lines(output), [15120, 20])

    def test_assert_counts_rejects_boundary_mismatch(self) -> None:
        with self.assertRaises(PipelineValidationError):
            assert_counts([15119, 20], [15120, 20], "HDFS")

    def test_safe_archive_path_accepts_expected_root(self) -> None:
        self.assertEqual(
            safe_archive_relative_path(
                "r_analysis/correlation_heatmap.png", "r_analysis"
            ),
            Path("correlation_heatmap.png"),
        )

    def test_safe_archive_path_rejects_traversal(self) -> None:
        self.assertIsNone(
            safe_archive_relative_path("r_analysis/../../secret", "r_analysis")
        )


if __name__ == "__main__":
    unittest.main()
