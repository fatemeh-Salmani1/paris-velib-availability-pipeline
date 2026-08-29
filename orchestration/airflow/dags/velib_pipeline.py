from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

PROJECT_ID = "paris-velib-de-fs-2026"
DATASET_ID = "velib_raw"
TABLE_NAME = "station_availability"
BIGQUERY_LOCATION = "EU"

PROJECT_ROOT = Path("/opt/airflow/project")
SOURCE_FILE = PROJECT_ROOT / "data" / "raw" / "velib_concat.parquet"

BUCKET_NAME = "paris-velib-de-fs-2026-data-lake"
OBJECT_NAME = "raw/velib/source=kaggle/velib_concat.parquet"

REQUIRED_COLUMNS = {
    "ts_utc",
    "tbin_utc",
    "station_id",
    "bikes",
    "capacity",
    "mechanical",
    "ebike",
    "status",
    "lat",
    "lon",
    "name",
    "temp_C",
    "precip_mm",
    "wind_mps",
}


@dag(
    dag_id="paris_velib_pipeline",
    description="Validate and orchestrate the Paris Vélib batch pipeline.",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 29, tz="UTC"),
    catchup=False,
    tags=["portfolio", "gcp", "velib"],
)
def paris_velib_pipeline():

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    def validate_source_file() -> dict:
        """Validate the local Parquet source before cloud ingestion."""
        import pyarrow.parquet as pq

        if not SOURCE_FILE.exists():
            raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

        parquet_file = pq.ParquetFile(SOURCE_FILE)
        columns = set(parquet_file.schema_arrow.names)
        missing_columns = REQUIRED_COLUMNS - columns
        row_count = parquet_file.metadata.num_rows

        if missing_columns:
            raise ValueError(
                f"Required columns are missing: {sorted(missing_columns)}"
            )

        if row_count == 0:
            raise ValueError("The source Parquet file contains no rows.")

        result = {
            "source_file": str(SOURCE_FILE),
            "row_count": row_count,
            "column_count": len(columns),
        }

        print(f"Validated: {SOURCE_FILE}")
        print(f"Rows: {row_count:,}")
        print(f"Columns: {len(columns)}")

        return result

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    def upload_raw_file_to_gcs(validation_result: dict) -> dict:
        """Upload the validated raw file to GCS idempotently."""
        from ingestion.upload_to_gcs import upload_to_gcs

        print(
            "Upstream validation confirmed "
            f"{validation_result['row_count']:,} rows."
        )

        upload_to_gcs(
            source=SOURCE_FILE,
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_NAME,
            overwrite=False,
        )

        gcs_uri = f"gs://{BUCKET_NAME}/{OBJECT_NAME}"
        print(f"Validated GCS destination: {gcs_uri}")

        return {
            "gcs_uri": gcs_uri,
            "row_count": validation_result["row_count"],
        }


    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    def load_raw_table_to_bigquery(upload_result: dict) -> dict:
        """Load the GCS Parquet object into the BigQuery raw table."""
        from ingestion.load_gcs_to_bigquery import load_gcs_to_bigquery

        source_uri = upload_result["gcs_uri"]

        print(f"Loading verified GCS source: {source_uri}")

        load_gcs_to_bigquery(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_name=TABLE_NAME,
            source_uri=source_uri,
            location=BIGQUERY_LOCATION,
        )

        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
        print(f"Validated BigQuery destination: {table_id}")

        return {
            "table_id": table_id,
            "row_count": upload_result["row_count"],
        }

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    def run_dbt_models(bigquery_result: dict) -> dict:
        """Build the dbt analytics models in BigQuery."""
        import subprocess

        print(
            "BigQuery source is ready: "
            f"{bigquery_result['table_id']}"
        )

        command = [
            "dbt",
            "run",
            "--project-dir",
            "/opt/airflow/project/dbt",
            "--profiles-dir",
            "/home/airflow/.dbt",
            "--target-path",
            "/tmp/velib-dbt-run-target",
            "--log-path",
            "/tmp/velib-dbt-run-logs",
        ]

        print("Running dbt models...")
        subprocess.run(command, check=True)

        return {
            "status": "dbt models completed",
            "source_table": bigquery_result["table_id"],
        }

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    def test_dbt_models(dbt_run_result: dict) -> None:
        """Run all dbt data-quality tests."""
        import subprocess

        print(dbt_run_result["status"])

        command = [
            "dbt",
            "test",
            "--project-dir",
            "/opt/airflow/project/dbt",
            "--profiles-dir",
            "/home/airflow/.dbt",
            "--target-path",
            "/tmp/velib-dbt-test-target",
            "--log-path",
            "/tmp/velib-dbt-test-logs",
        ]

        print("Running dbt tests...")
        subprocess.run(command, check=True)

        print("All dbt tests completed successfully.")

    validation_result = validate_source_file()
    upload_result = upload_raw_file_to_gcs(validation_result)
    bigquery_result = load_raw_table_to_bigquery(upload_result)
    dbt_run_result = run_dbt_models(bigquery_result)
    test_dbt_models(dbt_run_result)


paris_velib_pipeline()