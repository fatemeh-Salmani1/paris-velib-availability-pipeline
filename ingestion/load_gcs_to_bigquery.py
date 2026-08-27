import argparse

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


DEFAULT_PROJECT = "paris-velib-de-fs-2026"
DEFAULT_DATASET = "velib_raw"
DEFAULT_TABLE = "station_availability"
DEFAULT_LOCATION = "EU"
DEFAULT_SOURCE_URI = (
    "gs://paris-velib-de-fs-2026-data-lake/"
    "raw/velib/source=kaggle/velib_concat.parquet"
)


def load_gcs_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_name: str,
    source_uri: str,
    location: str,
) -> None:
    """Load a Parquet object from GCS into a new BigQuery table."""
    client = bigquery.Client(project=project_id, location=location)
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    try:
        existing_table = client.get_table(table_id)
    except NotFound:
        existing_table = None

    if existing_table is not None:
        print(f"Table already exists: {table_id}")
        print(f"Existing rows: {existing_table.num_rows:,}")
        print("Load skipped to prevent accidental replacement.")
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
    )

    print(f"Loading source: {source_uri}")
    print(f"Destination: {table_id}")

    load_job = client.load_table_from_uri(
        source_uris=source_uri,
        destination=table_id,
        job_config=job_config,
        location=location,
    )

    print(f"BigQuery job started: {load_job.job_id}")

    load_job.result()

    destination_table = client.get_table(table_id)

    print("BigQuery load completed successfully.")
    print(f"Rows loaded: {destination_table.num_rows:,}")
    print(f"Columns: {len(destination_table.schema)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load a GCS Parquet object into BigQuery."
    )

    parser.add_argument(
        "--project-id",
        default=DEFAULT_PROJECT,
        help="Google Cloud project ID.",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help="Destination BigQuery dataset.",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help="Destination BigQuery table.",
    )
    parser.add_argument(
        "--source-uri",
        default=DEFAULT_SOURCE_URI,
        help="Source Parquet URI in GCS.",
    )
    parser.add_argument(
        "--location",
        default=DEFAULT_LOCATION,
        help="BigQuery job location.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    load_gcs_to_bigquery(
        project_id=args.project_id,
        dataset_id=args.dataset,
        table_name=args.table,
        source_uri=args.source_uri,
        location=args.location,
    )