"""Airflow orchestration for the complete NYSE batch analytics pipeline."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, TaskGroup, task

from pipeline_helpers import (
    collect_hive_export,
    load_postgres_staging,
    preflight,
    prepare_staging_files,
    run_hive_script,
    run_r_analysis,
    run_spark_processing,
    run_sqoop_import,
    validate_final_outputs,
    validate_hdfs_raw,
    validate_hive_quality,
    validate_spark_output,
)


@task(task_id="preflight")
def preflight_task() -> dict[str, str]:
    """Confirm required data, images, containers, and network are available."""

    return preflight()


@task(task_id="prepare_csv")
def prepare_csv_task() -> dict[str, int]:
    return prepare_staging_files()


@task(task_id="load_postgres")
def load_postgres_task() -> dict[str, int]:
    return load_postgres_staging()


@task(task_id="sqoop_prices")
def sqoop_prices_task() -> str:
    return run_sqoop_import("staging_prices", "/data/nyse/prices_raw")


@task(task_id="sqoop_securities")
def sqoop_securities_task() -> str:
    return run_sqoop_import("staging_securities", "/data/nyse/securities_raw")


@task(task_id="validate_hdfs")
def validate_hdfs_task() -> dict[str, int]:
    return validate_hdfs_raw()


@task(task_id="spark_clean_to_parquet")
def spark_processing_task() -> str:
    return run_spark_processing()


@task(task_id="validate_parquet")
def validate_parquet_task() -> str:
    return validate_spark_output()


@task(task_id="create_hive_objects")
def create_hive_objects_task() -> str:
    return run_hive_script("setup_hive.sql")


@task(task_id="validate_hive")
def validate_hive_task() -> dict[str, int]:
    return validate_hive_quality()


@task(task_id="run_hive_analytics")
def run_hive_analytics_task() -> str:
    return run_hive_script("analytics.sql")


@task(task_id="export_hive_for_r")
def export_hive_for_r_task() -> str:
    return run_hive_script("export_for_r.sql")


@task(task_id="collect_hive_export")
def collect_hive_export_task() -> str:
    return collect_hive_export()


@task(task_id="run_r_analysis")
def run_r_analysis_task() -> str:
    return run_r_analysis()


@task(task_id="validate_final_outputs")
def validate_final_outputs_task() -> dict[str, int]:
    return validate_final_outputs()


with DAG(
    dag_id="nyse_end_to_end",
    description="PostgreSQL to Sqoop, HDFS, Spark, Hive, and R analytics",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["data-engineering", "hadoop", "spark", "hive", "r"],
) as dag:
    dag.doc_md = """
    # NYSE end-to-end batch pipeline

    Orchestrates the verified local pipeline without duplicating transformation
    logic. Every stage is idempotent and followed by a data-quality boundary.
    The DAG is intentionally manual because the source is a static historical
    dataset rather than a continuously arriving feed.
    """

    start = preflight_task()

    with TaskGroup(group_id="ingestion", tooltip="CSV to PostgreSQL to HDFS") as ingestion:
        prepared = prepare_csv_task()
        loaded = load_postgres_task()
        prices_imported = sqoop_prices_task()
        securities_imported = sqoop_securities_task()
        hdfs_validated = validate_hdfs_task()

        prepared >> loaded >> [prices_imported, securities_imported] >> hdfs_validated

    with TaskGroup(group_id="processing", tooltip="Spark Parquet and Hive") as processing:
        spark_complete = spark_processing_task()
        parquet_validated = validate_parquet_task()
        hive_created = create_hive_objects_task()
        hive_validated = validate_hive_task()
        hive_analytics_complete = run_hive_analytics_task()

        (
            spark_complete
            >> parquet_validated
            >> hive_created
            >> hive_validated
            >> hive_analytics_complete
        )

    with TaskGroup(group_id="analytics", tooltip="Hive export and R analytics") as analytics:
        hive_exported = export_hive_for_r_task()
        export_collected = collect_hive_export_task()
        r_complete = run_r_analysis_task()
        final_validated = validate_final_outputs_task()

        hive_exported >> export_collected >> r_complete >> final_validated

    start >> ingestion >> processing >> analytics
