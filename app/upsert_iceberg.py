
"""Load corporate datasets from S3 and upsert records into Iceberg."""

import os
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog


AWS_REGION = "us-east-1"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "datasets").strip("/")
DATA_DIR = Path(__file__).resolve().parent / "data"

FILENAMES = ["supply_chain.csv", "financial_data.csv"]

REQUIRED_COLUMNS = {
    "source_record_id",
    "corporate_name",
    "registration_id",
    "industry",
    "country",
    "employee_count",
    "annual_revenue",
    "source_system",
}

OUTPUT_COLUMNS = [
    "registration_id",
    "canonical_id",
    "corporate_name",
    "normalized_name",
    "is_resolved",
    "annual_revenue",
    "employee_count",
    "industry",
    "country",
]


def load_datasets():
    """Read CSV files directly from S3 or local development files."""

    frames = []

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        s3 = boto3.client("s3", region_name=AWS_REGION) if S3_BUCKET else None

        for filename in FILENAMES:
            if S3_BUCKET:
                local_file = temp_path / filename
                key = f"{S3_PREFIX}/{filename}" if S3_PREFIX else filename

                print(f"Downloading s3://{S3_BUCKET}/{key}", flush=True)
                s3.download_file(S3_BUCKET, key, str(local_file))
            else:
                local_file = DATA_DIR / filename

                if not local_file.is_file():
                    raise FileNotFoundError(
                        f"Required dataset not found: {local_file}"
                    )

            frame = pd.read_csv(
                local_file,
                dtype={"registration_id": "string"},
            )

            missing = REQUIRED_COLUMNS - set(frame.columns)
            if missing:
                raise ValueError(
                    f"{filename} is missing columns: {sorted(missing)}"
                )

            frames.append(frame)

    combined = pd.concat(frames, ignore_index=True)

    combined["corporate_name"] = (
        combined["corporate_name"].astype("string").str.strip()
    )
    combined["registration_id"] = (
        combined["registration_id"].astype("string").str.strip()
    )
    combined["employee_count"] = pd.to_numeric(
        combined["employee_count"], errors="coerce"
    )
    combined["annual_revenue"] = pd.to_numeric(
        combined["annual_revenue"], errors="coerce"
    )

    invalid = (
        combined["corporate_name"].isna()
        | combined["corporate_name"].eq("")
        | combined["registration_id"].isna()
        | combined["registration_id"].eq("")
        | combined["employee_count"].isna()
        | combined["annual_revenue"].isna()
    )

    invalid_count = int(invalid.sum())
    if invalid_count:
        print(f"Skipping {invalid_count} invalid records.", flush=True)

    combined = combined.loc[~invalid].reset_index(drop=True)

    print(f"Total records loaded: {len(combined)}", flush=True)
    print(
        "Supply-chain records: "
        f"{(combined['source_system'] == 'SupplyChain').sum()}",
        flush=True,
    )
    print(
        "Financial records: "
        f"{(combined['source_system'] == 'Financial').sum()}",
        flush=True,
    )

    return combined


def resolve_entities(data):
    """Generate stable canonical IDs using registration IDs."""

    resolved = data.copy()

    resolved["normalized_name"] = (
        resolved["corporate_name"]
        .astype("string")
        .str.lower()
        .str.replace(r"[^a-z0-9]+", " ", regex=True)
        .str.strip()
    )

    registration = (
        resolved["registration_id"].astype("string").str.strip().str.upper()
    )

    if registration.isna().any() or registration.eq("").any():
        raise ValueError("Every record requires a registration ID.")

    resolved["registration_id"] = registration
    resolved["canonical_id"] = "CORP-" + registration
    resolved["is_resolved"] = True

    return resolved


def main():
    if not S3_BUCKET:
        raise EnvironmentError("S3_BUCKET must be configured.")

    print("===== START: Direct S3 ingestion =====", flush=True)

    data = load_datasets()
    resolved = resolve_entities(data)

    for column in OUTPUT_COLUMNS:
        if column not in resolved.columns:
            resolved[column] = None

    records = resolved[OUTPUT_COLUMNS].copy()

    records = records.sort_values(
        by=["registration_id", "corporate_name"],
        na_position="last",
    )
    records = records.drop_duplicates(
        subset=["registration_id"],
        keep="last",
    ).reset_index(drop=True)

    if records["registration_id"].duplicated().any():
        raise ValueError("Duplicate registration IDs remain after deduplication.")

    records["registration_id"] = records["registration_id"].astype(str)
    records["canonical_id"] = records["canonical_id"].astype(str)
    records["is_resolved"] = records["is_resolved"].astype(bool)
    records["annual_revenue"] = records["annual_revenue"].astype("float64")
    records["employee_count"] = records["employee_count"].astype("int64")

    print(f"Source rows: {len(resolved)}", flush=True)
    print(
        f"Unique registration IDs for upsert: {len(records)}",
        flush=True,
    )

    arrow_schema = pa.schema([
        pa.field("registration_id", pa.string(), nullable=False),
        pa.field("canonical_id", pa.string(), nullable=False),
        pa.field("corporate_name", pa.string(), nullable=True),
        pa.field("normalized_name", pa.string(), nullable=True),
        pa.field("is_resolved", pa.bool_(), nullable=False),
        pa.field("annual_revenue", pa.float64(), nullable=True),
        pa.field("employee_count", pa.int64(), nullable=True),
        pa.field("industry", pa.string(), nullable=True),
        pa.field("country", pa.string(), nullable=True),
    ])

    arrow_table = pa.Table.from_pandas(
        records,
        schema=arrow_schema,
        preserve_index=False,
        safe=True,
    )

    print("===== START: Iceberg catalog connection =====", flush=True)

    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
            "glue.region": AWS_REGION,
            "warehouse": f"s3://{S3_BUCKET}/iceberg/",
        },
    )

    table = catalog.load_table("corporate_registry.corporate_registry")

    print("===== START: Iceberg upsert =====", flush=True)

    result = table.upsert(
        arrow_table,
        join_cols=["registration_id"],
        when_matched_update_all=True,
        when_not_matched_insert_all=True,
    )

    print("Iceberg upsert completed.", flush=True)
    print(f"Upsert result: {result}", flush=True)
    print(f"Iceberg table location: {table.location()}", flush=True)
    print(f"Iceberg table schema: {table.schema()}", flush=True)
    print("===== SUCCESS: Iceberg upsert =====", flush=True)


if __name__ == "__main__":
    main()
