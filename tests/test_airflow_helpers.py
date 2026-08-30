"""Focused unit tests for Airflow helper invariants that need no live cluster."""

from __future__ import annotations

import io
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "airflow" / "dags"))

from pipeline_helpers import (  # noqa: E402
    PipelineValidationError,
    _copy_directory_from_container,
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

    def test_copy_directory_overwrites_without_removing_destination(self) -> None:
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w") as archive:
            payload = b"new content"
            member = tarfile.TarInfo("r_analysis/result.txt")
            member.size = len(payload)
            archive.addfile(member, io.BytesIO(payload))

        class FakeContainer:
            def get_archive(self, source: str):
                self.source = source
                return [archive_buffer.getvalue()], {}

        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "r_analysis"
            destination.mkdir()
            preserved = destination / "preserved.txt"
            preserved.write_text("keep", encoding="utf-8")
            result = destination / "result.txt"
            result.write_text("old content", encoding="utf-8")

            _copy_directory_from_container(
                FakeContainer(), "/project/output/r_analysis", destination, "r_analysis"
            )

            self.assertEqual(result.read_bytes(), b"new content")
            self.assertEqual(preserved.read_text(encoding="utf-8"), "keep")

    def test_copy_directory_repairs_root_owned_generated_outputs(self) -> None:
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w") as archive:
            payload = b"new content"
            member = tarfile.TarInfo("r_analysis/result.txt")
            member.size = len(payload)
            archive.addfile(member, io.BytesIO(payload))

        class FakeContainers:
            def get(self, container_id: str):
                self.container_id = container_id
                return self

            def exec_run(self, command, **kwargs):
                self.command = command
                self.kwargs = kwargs
                return type("Result", (), {"exit_code": 0})()

        class FakeContainer:
            def __init__(self):
                self.client = type("Client", (), {"containers": FakeContainers()})()

            def get_archive(self, source: str):
                return [archive_buffer.getvalue()], {}

        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "r_analysis"
            destination.mkdir()
            result = destination / "result.txt"

            real_write_bytes = Path.write_bytes
            attempts = 0

            def deny_first_write(path: Path, payload: bytes) -> int:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise PermissionError("root-owned output")
                return real_write_bytes(path, payload)

            with (
                patch.dict("os.environ", {"HOSTNAME": "airflow-scheduler"}),
                patch.object(Path, "write_bytes", deny_first_write),
            ):
                container = FakeContainer()
                _copy_directory_from_container(
                    container, "/project/output/r_analysis", destination, "r_analysis"
                )

            self.assertEqual(result.read_bytes(), b"new content")
            self.assertEqual(attempts, 2)
            self.assertEqual(
                container.client.containers.command[:3], ["chown", "-R", "50000:0"]
            )
            self.assertEqual(container.client.containers.kwargs["user"], "0")


if __name__ == "__main__":
    unittest.main()
