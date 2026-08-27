import argparse
import hashlib
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = PROJECT_ROOT / "data" / "raw" / "velib_concat.parquet"
DEFAULT_OBJECT = "raw/velib/source=kaggle/velib_concat.parquet"

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


def calculate_sha256(file_path: Path) -> str:
    """Calculate a checksum used to identify the exact source file."""
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def validate_parquet(file_path: Path) -> int:
    """Validate the source file and return its row count."""
    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    parquet_file = pq.ParquetFile(file_path)
    columns = set(parquet_file.schema_arrow.names)
    missing_columns = REQUIRED_COLUMNS - columns

    if missing_columns:
        raise ValueError(
            f"Required columns are missing: {sorted(missing_columns)}"
        )

    row_count = parquet_file.metadata.num_rows

    if row_count == 0:
        raise ValueError("The source Parquet file contains no rows.")

    print(f"Validated source file: {file_path}")
    print(f"Rows: {row_count:,}")
    print(f"Columns: {len(columns)}")

    return row_count


def upload_to_gcs(
    source: Path,
    bucket_name: str,
    object_name: str,
    overwrite: bool,
) -> None:
    """Upload the validated source file to Google Cloud Storage."""
    row_count = validate_parquet(source)
    source_sha256 = calculate_sha256(source)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(
    object_name,
    chunk_size=8 * 1024 * 1024,
)

    if blob.exists(client):
        blob.reload()

        existing_sha256 = (blob.metadata or {}).get("source_sha256")

        if existing_sha256 == source_sha256:
            print("The same file already exists in GCS. Upload skipped.")
            return

        if not overwrite:
            raise FileExistsError(
                "A different file already exists at the destination. "
                "Use --overwrite only if replacement is intentional."
            )

    blob.metadata = {
        "source_sha256": source_sha256,
        "row_count": str(row_count),
        "source": "kaggle",
    }

    print(f"Uploading to gs://{bucket_name}/{object_name}")

    upload_retry = DEFAULT_RETRY.with_deadline(900)

    blob.upload_from_filename(
    source,
    content_type="application/vnd.apache.parquet",
    checksum="auto",
    timeout=300,
    retry=upload_retry,
)

    print("Upload completed successfully.")
    print(f"GCS generation: {blob.generation}")
    print(f"SHA-256: {source_sha256}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and upload the Vélib Parquet dataset to GCS."
    )

    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
        help="Path to the source Parquet file.",
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Destination GCS bucket name.",
    )
    parser.add_argument(
        "--object-name",
        default=DEFAULT_OBJECT,
        help="Object path inside the GCS bucket.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow an existing different object to be replaced.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    upload_to_gcs(
        source=args.source,
        bucket_name=args.bucket,
        object_name=args.object_name,
        overwrite=args.overwrite,
    )