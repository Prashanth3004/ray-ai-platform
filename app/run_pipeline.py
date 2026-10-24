
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

    # JM Comment: Pass the existing Ray connection settings to child processes.
    env = os.environ.copy()
    ray_address = env.get("RAY_ADDRESS", "auto")
    env["RAY_ADDRESS"] = ray_address

    # JM Comment: Run stages in order and stop immediately on failure.
    for stage_name, script_path in STAGES:
        if not script_path.is_file():
            raise FileNotFoundError(
                f"Required pipeline stage not found: {script_path}"
            )

        print(f"\n===== START: {stage_name} =====", flush=True)
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(APP_DIR),
            check=True,
            env=env,
            timeout=600,
        )
        print(f"===== SUCCESS: {stage_name} =====", flush=True)

    print("\nPipeline stages completed successfully.", flush=True)


if __name__ == "__main__":
    main()
