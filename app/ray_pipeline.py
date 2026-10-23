
# JM Comment: Ingest local or S3 corporate datasets with Ray Data and resolve entities.
import os
import tempfile
from pathlib import Path

import pandas as pd
import ray
import ray.data

DATA_DIR = Path(__file__).parent / "data"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "datasets")

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


def ingest_data(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Read both CSV datasets locally or download them from S3."""
    filenames = ["supply_chain.csv", "financial_data.csv"]

    # JM Comment: Use a temporary directory so S3 ingestion leaves no permanent local copies.
    with tempfile.TemporaryDirectory() as temp_dir:
        source_dir = Path(temp_dir) if S3_BUCKET else data_dir
        files = [source_dir / name for name in filenames]

        if S3_BUCKET:
            import boto3

            s3 = boto3.client("s3")
            for filename, path in zip(filenames, files):
                key = f"{S3_PREFIX.rstrip('/')}/{filename}"
                s3.download_file(S3_BUCKET, key, str(path))
        else:
            for path in files:
                if not path.is_file():
                    raise FileNotFoundError(
                        f"Required dataset not found: {path}"
                    )

        # JM Comment: Reuse an existing Ray runtime when running inside KubeRay.
        started_ray_here = not ray.is_initialized()
        if started_ray_here:
            ray.init(num_cpus=2, include_dashboard=False)

        try:
            datasets = []
            for path in files:
                dataset = ray.data.read_csv(str(path))
                missing = REQUIRED_COLUMNS - set(dataset.schema().names)
                if missing:
                    raise ValueError(
                        f"{path.name} is missing columns: {sorted(missing)}"
                    )
                datasets.append(dataset)

            combined = datasets[0].union(datasets[1]).to_pandas()
        finally:
            if started_ray_here:
                ray.shutdown()

    combined["employee_count"] = pd.to_numeric(
        combined["employee_count"], errors="coerce"
    )
    combined["annual_revenue"] = pd.to_numeric(
        combined["annual_revenue"], errors="coerce"
    )
    combined["corporate_name"] = combined["corporate_name"].astype(
        "string"
    ).str.strip()

    registration = combined["registration_id"].astype("string").str.strip()
    invalid = (
        combined["corporate_name"].isna()
        | combined["corporate_name"].eq("")
        | registration.isna()
        | registration.eq("")
        | combined["employee_count"].isna()
        | combined["annual_revenue"].isna()
    )

    invalid_count = int(invalid.sum())
    if invalid_count:
        print(f"Warning: {invalid_count} invalid records detected.")

    combined = combined.loc[~invalid].reset_index(drop=True)

    print(f"Total records loaded: {len(combined)}")
    print(
        "Supply-chain records: "
        f"{(combined['source_system'] == 'SupplyChain').sum()}"
    )
    print(
        "Financial records: "
        f"{(combined['source_system'] == 'Financial').sum()}"
    )
    return combined


def resolve_entities(df: pd.DataFrame) -> pd.DataFrame:
    """Assign a stable canonical ID using registration IDs."""
    result = df.copy()

    # JM Comment: Normalize names for auditability while using registration IDs as the match key.
    result["normalized_name"] = (
        result["corporate_name"]
        .astype("string")
        .str.lower()
        .str.replace(r"[^a-z0-9]+", " ", regex=True)
        .str.strip()
    )

    registration = (
        result["registration_id"].astype("string").str.strip().str.upper()
    )
    missing = registration.isna() | registration.eq("")
    if missing.any():
        raise ValueError("Every record requires a registration ID.")

    result["canonical_id"] = "CORP-" + registration
    result["is_resolved"] = True
    return result


@ray.remote
def resolve_entities_task(df: pd.DataFrame) -> pd.DataFrame:
    # JM Comment: Expose entity resolution as a Ray remote task.
    return resolve_entities(df)


if __name__ == "__main__":
    # JM Comment: Connect to KubeRay when RAY_ADDRESS is set; otherwise use local Ray.
    ray_address = os.getenv("RAY_ADDRESS")
    if ray_address:
        ray.init(address=ray_address)
    else:
        ray.init(num_cpus=2, include_dashboard=False)

    try:
        data = ingest_data()
        resolved = ray.get(resolve_entities_task.remote(data))
    finally:
        ray.shutdown()

    print("\nResolved sample:")
    print(
        resolved[
            [
                "corporate_name",
                "registration_id",
                "canonical_id",
                "is_resolved",
            ]
        ].head(5).to_string(index=False)
    )
    print(f"\nTotal records: {len(resolved)}")
    print(f"Unique companies: {resolved['canonical_id'].nunique()}")
    print(f"Resolved records: {int(resolved['is_resolved'].sum())}")
