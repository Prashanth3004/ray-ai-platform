
"""Run the Iceberg upsert followed by distributed Ray training."""

import os
import subprocess
import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent

STAGES = [
    ("Iceberg upsert", APP_DIR / "upsert_iceberg.py", 600),
    ("Distributed Ray Train", APP_DIR / "train_ray_model.py", 1800),
]


def main():
    if not os.getenv("S3_BUCKET"):
        raise EnvironmentError("S3_BUCKET must be configured.")

    env = os.environ.copy()
    env.setdefault("RAY_ADDRESS", "auto")

    for stage_name, script_path, timeout_seconds in STAGES:
        if not script_path.is_file():
            raise FileNotFoundError(
                f"Required pipeline stage not found: {script_path}"
            )

        print(f"\n===== START: {stage_name} =====", flush=True)
        print(f"Timeout: {timeout_seconds} seconds", flush=True)

        try:
            subprocess.run(
                [sys.executable, "-u", str(script_path)],
                cwd=str(APP_DIR),
                check=True,
                env=env,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            print(
                f"ERROR: {stage_name} exceeded its "
                f"{timeout_seconds}-second timeout.",
                flush=True,
            )
            raise RuntimeError(
                f"Pipeline stage timed out: {stage_name}"
            ) from exc
        except subprocess.CalledProcessError as exc:
            print(
                f"ERROR: {stage_name} exited with code {exc.returncode}.",
                flush=True,
            )
            raise

        print(f"===== SUCCESS: {stage_name} =====", flush=True)

    print("\nPipeline stages completed successfully.", flush=True)


if __name__ == "__main__":
    main()
