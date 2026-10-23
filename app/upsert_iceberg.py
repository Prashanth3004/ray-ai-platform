
# JM Comment: Resolve S3 datasets and transactionally upsert unique registrations into Iceberg.
import os
import ray
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from ray_pipeline import ingest_data, resolve_entities


# JM Comment: Connect to the existing KubeRay cluster.
ray.init(address=os.getenv("RAY_ADDRESS", "auto"))

try:
    # JM Comment: Load both S3 datasets and resolve corporate entities.
    data = ingest_data()
    resolved = resolve_entities(data)

    columns = [
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

    # JM Comment: Add optional fields missing from the source data.
    for col in columns:
        if col not in resolved.columns:
            resolved[col] = None

    records = resolved[columns].copy()
    records["registration_id"] = records["registration_id"].astype("string").str.strip()

    # JM Comment: Reject missing registration IDs before deduplication.
    if records["registration_id"].isna().any() or records["registration_id"].eq("").any():
        raise ValueError("Source data contains missing registration IDs.")

    # JM Comment: Deduplicate by registration ID before the Iceberg upsert.
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

    # JM Comment: Normalize fields and numeric types to match the Iceberg schema.
    records["registration_id"] = records["registration_id"].astype(str)
    records["canonical_id"] = records["canonical_id"].astype(str)
    records["is_resolved"] = records["is_resolved"].fillna(False).astype(bool)
    records["annual_revenue"] = records["annual_revenue"].astype("float64")

    print("Source rows:", len(resolved))
    print("Unique registration IDs for upsert:", len(records))

    # JM Comment: Explicitly define required and optional Arrow fields.
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

    # JM Comment: Convert records using the explicit Arrow schema.
    arrow_table = pa.Table.from_pandas(
        records,
        schema=arrow_schema,
        preserve_index=False,
        safe=True,
    )

    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
            "glue.region": "us-east-1",
            "warehouse": "s3://ray-ai-platform-145023571604/iceberg/",
        },
    )
    table = catalog.load_table("corporate_registry.corporate_registry")

    # JM Comment: Atomically update matching registrations and insert new ones.
    result = table.upsert(
        arrow_table,
        join_cols=["registration_id"],
        when_matched_update_all=True,
        when_not_matched_insert_all=True,
    )

    print("Iceberg upsert completed.")
    print("Upsert result:", result)

    # JM Comment: Reload the table metadata after the transaction.
    table = catalog.load_table("corporate_registry.corporate_registry")
    print("Iceberg table location:", table.location())
    print("Iceberg table schema:", table.schema())
finally:
    # JM Comment: Shut down the Ray client after the Iceberg stage.
    ray.shutdown()