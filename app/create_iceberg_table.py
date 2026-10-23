
# Create the corporate registry Iceberg table in Glue with S3-backed storage.
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    BooleanType,
    LongType,
    DoubleType,
) 

catalog = load_catalog(
    "glue",
    **{
        "type": "glue",
        "glue.region": "us-east-1",
        "warehouse": "s3://ray-ai-platform-145023571604/iceberg/",
    },
)

table_name = "corporate_registry.corporate_registry"

schema = Schema(
    NestedField(1, "registration_id", StringType(), required=True),
    NestedField(2, "canonical_id", StringType(), required=True),
    NestedField(3, "corporate_name", StringType(), required=False),
    NestedField(4, "normalized_name", StringType(), required=False),
    NestedField(5, "is_resolved", BooleanType(), required=True),
    NestedField(6, "annual_revenue", DoubleType(), required=False),
    NestedField(7, "employee_count", LongType(), required=False),
    NestedField(8, "industry", StringType(), required=False),
    NestedField(9, "country", StringType(), required=False),
)

try:
    table = catalog.create_table(
        identifier=table_name,
        schema=schema,
    )
    print("Created Iceberg table:", table.name())
except Exception as exc:
    if "already exists" in str(exc).lower():
        table = catalog.load_table(table_name)
        print("Iceberg table already exists:", table.name())
    else:
        raise
