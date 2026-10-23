# JM Comment: Orchestrate Iceberg ingestion and distributed Ray Train.
import os
import subprocess
import sys
from pathlib import Path

# JM Comment: Resolve application scripts relative to this file.
APP_DIR = Path(__file__).resolve().parent
STAGES = [
    ("Iceberg upsert", APP_DIR / "upsert_iceberg.py"),
    ("Distributed Ray Train", APP_DIR / "train_ray_model.py"),
]


def main():
    # JM Comment: Require the configured S3 bucket before starting.
    if not os.getenv("S3_BUCKET"):
        raise EnvironmentError("S3_BUCKET must be configured.")

    # JM Comment: Run each stage in order and stop on the first failure.
    for stage_name, script_path in STAGES:
        print(f"\n===== START: {stage_name} =====", flush=True)
        subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(APP_DIR),
            check=True,
            env=os.environ.copy(),
        )
        print(f"===== SUCCESS: {stage_name} =====", flush=True)

    print("\nPipeline stages completed successfully.", flush=True)


if __name__ == "__main__":
    main()
