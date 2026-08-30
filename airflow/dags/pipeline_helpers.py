"""Execution helpers for the NYSE Airflow DAG.

The helpers deliberately call the existing project scripts and containers. Airflow
owns orchestration, retries, and quality gates; Spark, Hive, Sqoop, and R retain
ownership of their existing processing logic.
"""

from __future__ import annotations

import io
import os
import re
import shutil
import subprocess
import sys
import tarfile
import uuid
from pathlib import Path, PurePosixPath
from typing import Sequence


EXPECTED_PRICE_ROWS = 15_120
EXPECTED_SECURITY_ROWS = 20
EXPECTED_SYMBOLS = 20
EXPECTED_MONTHLY_SECTOR_ROWS = 180


class PipelineValidationError(RuntimeError):
    """Raised when an orchestration quality gate fails."""


def project_root() -> Path:
    return Path(os.environ.get("NYSE_PROJECT_ROOT", "/opt/project")).resolve()


def parse_integer_lines(output: str) -> list[int]:
    """Return lines that contain only an integer, ignoring tool log messages."""

    return [
        int(line.strip())
        for line in output.splitlines()
        if re.fullmatch(r"-?\d+", line.strip())
    ]


def assert_counts(actual: Sequence[int], expected: Sequence[int], label: str) -> None:
    if list(actual) != list(expected):
        raise PipelineValidationError(
            f"{label} counts did not match: expected {list(expected)}, got {list(actual)}"
        )


def safe_archive_relative_path(member_name: str, root_name: str) -> Path | None:
    """Map a tar member below its expected root without allowing path traversal."""

    parts = [part for part in PurePosixPath(member_name).parts if part not in ("", ".")]
    if not parts or parts[0] != root_name or ".." in parts:
        return None
    relative_parts = parts[1:]
    return Path(*relative_parts) if relative_parts else Path()


def _docker_client():
    import docker

    return docker.from_env()


def _decode(value: bytes | None) -> str:
    return value.decode("utf-8", errors="replace") if value else ""


def _redact(text: str) -> str:
    password = os.environ.get("POSTGRES_PASSWORD", "")
    return text.replace(password, "***") if password else text


def _exec_container(container, command: Sequence[str], *, print_output: bool = True) -> str:
    result = container.exec_run(list(command), demux=True)
    stdout_bytes, stderr_bytes = result.output
    stdout = _decode(stdout_bytes)
    stderr = _decode(stderr_bytes)

    if print_output and stdout:
        print(stdout)
    if print_output and stderr:
        print(stderr)
    if result.exit_code != 0:
        detail = _redact(stderr or stdout).strip()
        raise RuntimeError(
            f"Container command failed with exit code {result.exit_code}: {detail[-4000:]}"
        )
    return stdout


def _master_exec(shell_command: str, *, print_output: bool = True) -> str:
    client = _docker_client()
    try:
        master = client.containers.get(
            os.environ.get("HADOOP_MASTER_CONTAINER", "infrastructure-master-1")
        )
        return _exec_container(
            master,
            ["bash", "-lc", shell_command],
            print_output=print_output,
        )
    finally:
        client.close()


def _put_file(container, source: Path, destination_directory: str) -> None:
    _exec_container(container, ["mkdir", "-p", destination_directory], print_output=False)
    archive_buffer = io.BytesIO()
    with tarfile.open(fileobj=archive_buffer, mode="w") as archive:
        archive.add(source, arcname=source.name)
    archive_buffer.seek(0)
    if not container.put_archive(destination_directory, archive_buffer.getvalue()):
        raise RuntimeError(f"Could not copy {source.name} into {container.name}")


def _copy_file_from_container(container, source: str, destination: Path) -> None:
    stream, _ = container.get_archive(source)
    archive_bytes = b"".join(stream)
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:*") as archive:
        file_members = [member for member in archive.getmembers() if member.isfile()]
        if len(file_members) != 1:
            raise RuntimeError(f"Expected one file in archive for {source}")
        extracted = archive.extractfile(file_members[0])
        if extracted is None:
            raise RuntimeError(f"Could not read archived file {source}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(extracted.read())


def _copy_directory_from_container(
    container, source: str, destination: Path, root_name: str
) -> None:
    stream, _ = container.get_archive(source)
    archive_bytes = b"".join(stream)
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)

    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:*") as archive:
        for member in archive.getmembers():
            relative = safe_archive_relative_path(member.name, root_name)
            if relative is None or relative == Path() or not member.isfile():
                continue
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            extracted = archive.extractfile(member)
            if extracted is None:
                raise RuntimeError(f"Could not read archived member {member.name}")
            target.write_bytes(extracted.read())


def preflight() -> dict[str, str]:
    root = project_root()
    required_files = [
        root / "data" / "raw" / "prices.csv",
        root / "data" / "raw" / "securities.csv",
        root / "scripts" / "prepare_nyse_data.py",
        root / "spark" / "process_nyse.py",
        root / "spark" / "validate_nyse.py",
        root / "hive" / "setup_hive.sql",
        root / "r" / "analyze_nyse.R",
    ]
    missing = [str(path) for path in required_files if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Required pipeline files are missing: {missing}")

    client = _docker_client()
    try:
        client.ping()
        network_name = os.environ.get("HADOOP_NETWORK", "infrastructure_sparknet")
        client.networks.get(network_name)

        for container_name in (
            os.environ.get("HADOOP_MASTER_CONTAINER", "infrastructure-master-1"),
            "nyse-postgres",
        ):
            container = client.containers.get(container_name)
            container.reload()
            if container.status != "running":
                raise RuntimeError(f"Required container is not running: {container_name}")

        for image_name in (
            os.environ.get("SQOOP_IMAGE", "nyse-sqoop:1.4.7"),
            os.environ.get("R_IMAGE", "r-base:4.5.1"),
        ):
            client.images.get(image_name)
    finally:
        client.close()

    return {"project_root": str(root), "network": network_name}


def prepare_staging_files() -> dict[str, int]:
    root = project_root()
    result = subprocess.run(
        [sys.executable, str(root / "scripts" / "prepare_nyse_data.py")],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout)

    import pandas as pd

    prices = pd.read_csv(root / "data" / "processed" / "prices_staging.csv")
    securities = pd.read_csv(root / "data" / "processed" / "securities_staging.csv")
    assert_counts(
        [len(prices), len(securities), prices["symbol"].nunique()],
        [EXPECTED_PRICE_ROWS, EXPECTED_SECURITY_ROWS, EXPECTED_SYMBOLS],
        "Prepared CSV",
    )
    if prices.isna().any().any():
        raise PipelineValidationError("Prepared prices contain null values")
    if prices.duplicated().any():
        raise PipelineValidationError("Prepared prices contain duplicate rows")
    if (prices["open"] <= 0).any() or (prices["high"] < prices["low"]).any():
        raise PipelineValidationError("Prepared prices violate market-value checks")

    return {"price_rows": len(prices), "security_rows": len(securities)}


def load_postgres_staging() -> dict[str, int]:
    import psycopg2

    root = project_root()
    connection = psycopg2.connect(
        host=os.environ.get("NYSE_POSTGRES_HOST", "nyse-postgres"),
        port=int(os.environ.get("NYSE_POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "nyse"),
        user=os.environ.get("POSTGRES_USER", "nyseuser"),
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute((root / "postgres" / "create_staging_tables.sql").read_text())
            cursor.execute("TRUNCATE staging_prices, staging_securities")
            with (root / "data" / "processed" / "prices_staging.csv").open() as file:
                cursor.copy_expert(
                    "COPY staging_prices FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", file
                )
            with (root / "data" / "processed" / "securities_staging.csv").open() as file:
                cursor.copy_expert(
                    "COPY staging_securities FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                    file,
                )
            cursor.execute("SELECT COUNT(*), COUNT(DISTINCT symbol) FROM staging_prices")
            price_rows, symbols = cursor.fetchone()
            cursor.execute("SELECT COUNT(*) FROM staging_securities")
            security_rows = cursor.fetchone()[0]
    finally:
        connection.close()

    assert_counts(
        [price_rows, security_rows, symbols],
        [EXPECTED_PRICE_ROWS, EXPECTED_SECURITY_ROWS, EXPECTED_SYMBOLS],
        "PostgreSQL staging",
    )
    return {
        "price_rows": price_rows,
        "security_rows": security_rows,
        "symbols": symbols,
    }


def run_sqoop_import(table: str, target_directory: str) -> str:
    allowed = {
        "staging_prices": "/data/nyse/prices_raw",
        "staging_securities": "/data/nyse/securities_raw",
    }
    if allowed.get(table) != target_directory:
        raise ValueError(f"Unsupported Sqoop target: {table} -> {target_directory}")

    password = os.environ["POSTGRES_PASSWORD"]
    command = [
        "sqoop",
        "import",
        "-D",
        "mapreduce.framework.name=local",
        "-D",
        "fs.defaultFS=hdfs://master:8020",
        "--connect",
        f"jdbc:postgresql://{os.environ.get('NYSE_POSTGRES_HOST', 'nyse-postgres')}:5432/{os.environ.get('POSTGRES_DB', 'nyse')}",
        "--username",
        os.environ.get("POSTGRES_USER", "nyseuser"),
        "--password",
        password,
        "--driver",
        "org.postgresql.Driver",
        "--table",
        table,
        "--target-dir",
        target_directory,
        "--delete-target-dir",
        "--num-mappers",
        "1",
        "--as-textfile",
    ]

    client = _docker_client()
    try:
        try:
            output = client.containers.run(
                os.environ.get("SQOOP_IMAGE", "nyse-sqoop:1.4.7"),
                command=command,
                network=os.environ.get("HADOOP_NETWORK", "infrastructure_sparknet"),
                remove=True,
                stdout=True,
                stderr=True,
            )
        except Exception as error:
            raise RuntimeError(_redact(str(error))) from None
    finally:
        client.close()

    print(_decode(output)[-4000:])
    return target_directory


def validate_hdfs_raw() -> dict[str, int]:
    prices = parse_integer_lines(
        _master_exec("hdfs dfs -cat /data/nyse/prices_raw/part-* | wc -l")
    )[-1]
    securities = parse_integer_lines(
        _master_exec("hdfs dfs -cat /data/nyse/securities_raw/part-* | wc -l")
    )[-1]
    assert_counts(
        [prices, securities],
        [EXPECTED_PRICE_ROWS, EXPECTED_SECURITY_ROWS],
        "HDFS raw",
    )
    return {"price_rows": prices, "security_rows": securities}


def _copy_project_file_to_master(relative_path: str) -> str:
    source = project_root() / relative_path
    destination_directory = "/tmp/airflow-nyse"
    client = _docker_client()
    try:
        master = client.containers.get(
            os.environ.get("HADOOP_MASTER_CONTAINER", "infrastructure-master-1")
        )
        _put_file(master, source, destination_directory)
    finally:
        client.close()
    return f"{destination_directory}/{source.name}"


def run_spark_processing() -> str:
    script = _copy_project_file_to_master("spark/process_nyse.py")
    output = _master_exec(f"spark-submit --master spark://master:7077 {script}")
    if "CLEANED ROW COUNT: 15120" not in output:
        raise PipelineValidationError("Spark processing did not report 15,120 rows")
    return "/data/nyse/prices_clean"


def validate_spark_output() -> str:
    script = _copy_project_file_to_master("spark/validate_nyse.py")
    output = _master_exec(f"spark-submit --master spark://master:7077 {script}")
    if "SPARK QUALITY GATE PASSED" not in output:
        raise PipelineValidationError("Independent Spark quality gate did not pass")
    return "spark_quality_passed"


def run_hive_script(filename: str) -> str:
    allowed = {
        "setup_hive.sql",
        "validate_hive.sql",
        "analytics.sql",
        "export_for_r.sql",
    }
    if filename not in allowed:
        raise ValueError(f"Unsupported Hive script: {filename}")
    script = _copy_project_file_to_master(f"hive/{filename}")
    _master_exec(f"hive -f {script}")
    return filename


def validate_hive_quality() -> dict[str, int]:
    run_hive_script("validate_hive.sql")
    queries = """
USE nyse;
SELECT COUNT(*) FROM prices_clean;
SELECT COUNT(DISTINCT symbol) FROM prices_clean;
SELECT COUNT(*) FROM prices_with_securities;
SELECT COUNT(*) FROM prices_clean p LEFT JOIN securities s ON p.symbol = s.symbol WHERE s.symbol IS NULL;
SELECT COUNT(*) FROM sector_monthly_avg;
""".strip()
    escaped = queries.replace("'", "'\\''")
    output = _master_exec(f"hive -S -e '{escaped}'", print_output=False)
    counts = parse_integer_lines(output)
    expected = [
        EXPECTED_PRICE_ROWS,
        EXPECTED_SYMBOLS,
        EXPECTED_PRICE_ROWS,
        0,
        EXPECTED_MONTHLY_SECTOR_ROWS,
    ]
    assert_counts(counts[-5:], expected, "Hive analytics")
    return {
        "price_rows": counts[-5],
        "symbols": counts[-4],
        "joined_rows": counts[-3],
        "unmatched_rows": counts[-2],
        "monthly_sector_rows": counts[-1],
    }


def collect_hive_export() -> str:
    destination = project_root() / "output" / "nyse_for_r.csv"
    client = _docker_client()
    try:
        master = client.containers.get(
            os.environ.get("HADOOP_MASTER_CONTAINER", "infrastructure-master-1")
        )
        _exec_container(
            master,
            [
                "bash",
                "-lc",
                "rm -f /tmp/nyse_for_r.csv && hdfs dfs -getmerge /data/nyse/r_export /tmp/nyse_for_r.csv",
            ],
        )
        _copy_file_from_container(master, "/tmp/nyse_for_r.csv", destination)
    finally:
        client.close()

    row_count = sum(1 for _ in destination.open(encoding="utf-8"))
    assert_counts([row_count], [EXPECTED_PRICE_ROWS], "Hive R export")
    return str(destination)


def run_r_analysis() -> str:
    root = project_root()
    container_name = f"nyse-airflow-r-{uuid.uuid4().hex[:10]}"
    client = _docker_client()
    container = None
    try:
        container = client.containers.create(
            os.environ.get("R_IMAGE", "r-base:4.5.1"),
            command=["tail", "-f", "/dev/null"],
            name=container_name,
        )
        container.start()
        _exec_container(
            container,
            ["mkdir", "-p", "/project/r", "/project/output/r_analysis"],
            print_output=False,
        )
        _put_file(container, root / "r" / "analyze_nyse.R", "/project/r")
        _put_file(container, root / "output" / "nyse_for_r.csv", "/project/output")
        _exec_container(
            container,
            ["Rscript", "/project/r/analyze_nyse.R", "/project"],
        )
        _copy_directory_from_container(
            container,
            "/project/output/r_analysis",
            root / "output" / "r_analysis",
            "r_analysis",
        )
    finally:
        if container is not None:
            container.remove(force=True)
        client.close()
    return str(root / "output" / "r_analysis")


def validate_final_outputs() -> dict[str, int]:
    output = project_root() / "output"
    export = output / "nyse_for_r.csv"
    analysis = output / "r_analysis"
    expected_artifacts = [
        "analysis_summary.txt",
        "closing_price_matrix.csv",
        "closing_price_covariance.csv",
        "closing_price_correlation.csv",
        "daily_return_covariance.csv",
        "daily_return_correlation.csv",
        "stock_price_trends.png",
        "daily_return_distribution.png",
        "correlation_heatmap.png",
    ]
    missing = [
        name
        for name in expected_artifacts
        if not (analysis / name).is_file() or (analysis / name).stat().st_size == 0
    ]
    if missing:
        raise PipelineValidationError(f"R analysis artifacts are missing: {missing}")

    export_rows = sum(1 for _ in export.open(encoding="utf-8"))
    assert_counts([export_rows], [EXPECTED_PRICE_ROWS], "Final export")
    return {"export_rows": export_rows, "artifact_count": len(expected_artifacts)}
