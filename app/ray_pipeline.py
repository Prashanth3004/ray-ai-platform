
# JM Comment: Ingest corporate datasets with Ray Data and resolve entities.

import os
from pathlib import Path

import pandas as pd
import ray
import ray.data


DATA_DIR = Path(__file__).resolve().parent / "data"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "datasets").strip("/")

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


def ingest_data(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Read corporate datasets from S3 or local files using Ray Data."""

    started_ray_here = not ray.is_initialized()

    if started_ray_here:
        ray_address = os.getenv("RAY_ADDRESS")
        if ray_address:
            ray.init(address=ray_address)
        else:
            ray.init(num_cpus=2, include_dashboard=False)

    try:
        datasets = []

        for filename in FILENAMES:
            if S3_BUCKET:
                key = (
                    f"{S3_PREFIX}/{filename}"
                    if S3_PREFIX
                    else filename
                )
                path = f"s3://{S3_BUCKET}/{key}"
            else:
                path = str(data_dir / filename)

                if not Path(path).is_file():
                    raise FileNotFoundError(
                        f"Required dataset not found: {path}"
                    )

            print(f"Reading dataset: {path}", flush=True)

            dataset = ray.data.read_csv(path)

            missing = REQUIRED_COLUMNS - set(dataset.schema().names)
            if missing:
                raise ValueError(
                    f"{filename} is missing columns: {sorted(missing)}"
                )

            datasets.append(dataset)

        combined = datasets[0].union(datasets[1]).to_pandas()

    finally:
        # JM Comment: Only shut down Ray if this function initialized it.
        if started_ray_here:
            ray.shutdown()

    combined["employee_count"] = pd.to_numeric(
        combined["employee_count"], errors="coerce"
    )
    combined["annual_revenue"] = pd.to_numeric(
        combined["annual_revenue"], errors="coerce"
    )
    combined["corporate_name"] = (
        combined["corporate_name"].astype("string").str.strip()
    )

    registration = (
        combined["registration_id"].astype("string").str.strip()
    )

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
        print(
            f"Warning: {invalid_count} invalid records detected.",
            flush=True,
        )

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


def resolve_entities(df: pd.DataFrame) -> pd.DataFrame:
    """Assign stable canonical IDs using registration IDs."""

    result = df.copy()

    result["normalized_name"] = (
        result["corporate_name"]
        .astype("string")
        .str.lower()
        .str.replace(r"[^a-z0-9]+", " ", regex=True)
        .str.strip()
    )

    registration = (
        result["registration_id"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    missing = registration.isna() | registration.eq("")

    if missing.any():
        raise ValueError("Every record requires a registration ID.")

    result["registration_id"] = registration
    result["canonical_id"] = "CORP-" + registration
    result["is_resolved"] = True

    return result


@ray.remote
def resolve_entities_task(df: pd.DataFrame) -> pd.DataFrame:
    """Run entity resolution as a Ray task."""

    return resolve_entities(df)


def main():
    """Run the standalone distributed entity-resolution workflow."""

    ray_address = os.getenv("RAY_ADDRESS")

    if ray_address:
        ray.init(address=ray_address)
    else:
        ray.init(num_cpus=2, include_dashboard=False)

    try:
        data = ingest_data()
        resolved = ray.get(resolve_entities_task.remote(data))

        print("\nResolved sample:", flush=True)
        print(
            resolved[
                [
                    "corporate_name",
                    "registration_id",
                    "canonical_id",
                    "is_resolved",
                ]
            ].head(5).to_string(index=False),
            flush=True,
        )

        print(f"\nTotal records: {len(resolved)}", flush=True)
        print(
            f"Unique companies: {resolved['canonical_id'].nunique()}",
            flush=True,
        )
        print(
            f"Resolved records: {int(resolved['is_resolved'].sum())}",
            flush=True,
        )

    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
