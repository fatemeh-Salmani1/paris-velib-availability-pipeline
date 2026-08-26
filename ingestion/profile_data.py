from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "velib_concat.parquet"


def profile_dataset() -> None:
    """Inspect the raw Vélib dataset and report basic data-quality information."""

    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

    parquet_file = pq.ParquetFile(DATA_PATH)

    print("=== FILE INFORMATION ===")
    print(f"Path: {DATA_PATH}")
    print(f"Size: {DATA_PATH.stat().st_size / (1024**2):,.2f} MB")
    print(f"Rows: {parquet_file.metadata.num_rows:,}")
    print(f"Columns: {parquet_file.metadata.num_columns}")
    print(f"Row groups: {parquet_file.metadata.num_row_groups}")

    print("\n=== SCHEMA ===")
    print(parquet_file.schema_arrow)

    print("\nLoading data...")
    df = pd.read_parquet(DATA_PATH)

    print("\n=== TIME COVERAGE ===")
    print(f"Earliest snapshot: {df['ts_utc'].min()}")
    print(f"Latest snapshot:   {df['ts_utc'].max()}")
    print(f"Unique time bins:  {df['tbin_utc'].nunique():,}")

    print("\n=== STATION COVERAGE ===")
    print(f"Unique stations: {df['station_id'].nunique():,}")

    print("\n=== STATUS DISTRIBUTION ===")
    print(df["status"].value_counts(dropna=False))

    print("\n=== MISSING VALUES ===")
    missing_values = df.isna().sum()
    print(missing_values[missing_values > 0].sort_values(ascending=False))

    print("\n=== DUPLICATES ===")
    duplicate_keys = df.duplicated(
        subset=["station_id", "tbin_utc"],
        keep=False,
    ).sum()
    print(f"Rows with duplicated station/time keys: {duplicate_keys:,}")

    print("\n=== BASIC VALIDATION ===")
    print(f"Negative bike counts: {(df['bikes'] < 0).sum():,}")
    print(f"Negative capacities: {(df['capacity'] < 0).sum():,}")
    print(
        "Rows where mechanical + ebike != bikes: "
        f"{((df['mechanical'] + df['ebike']) != df['bikes']).sum():,}"
    )
    print(
        "Rows where available bikes exceed capacity: "
        f"{(df['bikes'] > df['capacity']).sum():,}"
    )


if __name__ == "__main__":
    profile_dataset()