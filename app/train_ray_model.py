
# JM Comment: Train, checkpoint, evaluate, and persist a revenue model with Ray Train and PyTorch.
import json
import os
import sys
import tempfile
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import ray
import torch
from ray import train
from ray.train import Checkpoint, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

# JM Comment: Configure the existing local data directory and optional S3 source.
DATA_DIR = Path(__file__).parent / "data"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "datasets")
ARTIFACT_PREFIX = os.getenv("ARTIFACT_PREFIX", "artifacts")

# JM Comment: Single source of truth for the Ray Train worker count.
NUM_TRAIN_WORKERS = 1


def unwrap_model(model):
    # JM Comment: Ray wraps the model in DistributedDataParallel only when more than one worker runs.
    return model.module if hasattr(model, "module") else model


class RevenueRegressor(nn.Module):
    def __init__(self, input_size: int):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_size, 32),
            nn.ReLU(),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.Linear(16, 1),
        )

    def forward(self, x):
        return self.network(x)


def train_loop_per_worker(config):
    # JM Comment: Ray Train shards the training dataset and synchronizes gradients.
    torch.manual_seed(42)

    X = torch.tensor(config["X_train"], dtype=torch.float32)
    y = torch.tensor(config["y_train"], dtype=torch.float32).reshape(-1, 1)

    dataset = TensorDataset(X, y)
    dataset = train.torch.prepare_data_loader(
        DataLoader(dataset, batch_size=64, shuffle=True)
    )

    model = RevenueRegressor(config["input_size"])
    model = train.torch.prepare_model(model)

    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    loss_fn = nn.MSELoss()

    for epoch in range(config["epochs"]):
        model.train()
        total_loss = 0.0
        batches = 0

        for features, targets in dataset:
            optimizer.zero_grad()
            predictions = model(features)
            loss = loss_fn(predictions, targets)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            batches += 1

        metrics = {
            "train_loss": total_loss / max(batches, 1),
            "epoch": epoch + 1,
        }

        # JM Comment: Only rank zero writes the checkpoint for each training run.
        if train.get_context().get_world_rank() == 0:
            with tempfile.TemporaryDirectory() as temp_dir:
                checkpoint_path = Path(temp_dir) / "model.pt"
                torch.save(
                    {
                        "model_state_dict": unwrap_model(model).state_dict(),
                        "input_size": config["input_size"],
                    },
                    checkpoint_path,
                )
                train.report(
                    metrics,
                    checkpoint=Checkpoint.from_directory(temp_dir),
                )
        else:
            train.report(metrics)


def load_datasets():
    # JM Comment: Read both datasets from S3 when configured, otherwise use local CSVs.
    filenames = ["supply_chain.csv", "financial_data.csv"]

    with tempfile.TemporaryDirectory() as temp_dir:
        if S3_BUCKET:
            s3 = boto3.client("s3")
            paths = []
            for filename in filenames:
                local_path = Path(temp_dir) / filename
                key = f"{S3_PREFIX.rstrip('/')}/{filename}"
                s3.download_file(S3_BUCKET, key, str(local_path))
                paths.append(local_path)
        else:
            paths = [DATA_DIR / filename for filename in filenames]

        for path in paths:
            if not path.is_file():
                raise FileNotFoundError(f"Dataset not found: {path}")

        frames = [pd.read_csv(path) for path in paths]

    # JM Comment: Combine datasets after their temporary local copies are closed.
    return pd.concat(frames, ignore_index=True)


def main():
    # JM Comment: Connect to the existing KubeRay cluster.
    ray.init(address="auto", include_dashboard=False)

    try:
        df = load_datasets()
        df = df.dropna(
            subset=["employee_count", "industry", "country", "annual_revenue"]
        )

        features = pd.get_dummies(
            df[["employee_count", "industry", "country"]],
            columns=["industry", "country"],
            dtype=float,
        )
        feature_columns = features.columns.tolist()

        target = pd.to_numeric(
            df["annual_revenue"], errors="raise"
        ).to_numpy(dtype=np.float32)

        X_train, X_test, y_train, y_test = train_test_split(
            features.to_numpy(dtype=np.float32),
            target,
            test_size=0.2,
            random_state=42,
        )

        # JM Comment: Scale the target to improve neural-network optimization.
        revenue_scale = float(np.mean(y_train))
        if not np.isfinite(revenue_scale) or revenue_scale == 0:
            raise ValueError("Training target mean must be finite and non-zero.")

        y_train_scaled = y_train / revenue_scale

        trainer = TorchTrainer(
            train_loop_per_worker,
            train_loop_config={
                "X_train": X_train.tolist(),
                "y_train": y_train_scaled.tolist(),
                "input_size": X_train.shape[1],
                "epochs": 10,
            },
            scaling_config=ScalingConfig(num_workers=NUM_TRAIN_WORKERS, use_gpu=False),
            # JM Comment: Ray Train v2 needs storage every pod can reach; local /root/ray_results is per-pod.
            run_config=(
                RunConfig(
                    storage_path=f"s3://{S3_BUCKET}/{ARTIFACT_PREFIX.strip('/')}/ray-train"
                )
                if S3_BUCKET
                else None
            ),
        )

        result = trainer.fit()
        if result.error:
            raise RuntimeError("Ray Train failed") from result.error
        if result.checkpoint is None:
            raise RuntimeError("Ray Train completed without a model checkpoint")

        checkpoint_dir = result.checkpoint.to_directory()
        checkpoint_file = Path(checkpoint_dir) / "model.pt"
        checkpoint = torch.load(
            checkpoint_file, map_location="cpu", weights_only=True
        )

        model = RevenueRegressor(checkpoint["input_size"])
        model.load_state_dict(checkpoint["model_state_dict"])
        model.eval()

        with torch.no_grad():
            scaled_predictions = model(
                torch.tensor(X_test, dtype=torch.float32)
            )
            predictions = (
                scaled_predictions.numpy().reshape(-1) * revenue_scale
            )

        metrics = {
            "status": "SUCCESS",
            "workers": NUM_TRAIN_WORKERS,
            "records_used": int(len(df)),
            "training_records": int(len(X_train)),
            "test_records": int(len(X_test)),
            "mae": float(mean_absolute_error(y_test, predictions)),
            "r2": float(r2_score(y_test, predictions)),
            "feature_columns": feature_columns,
            "revenue_scale": revenue_scale,
        }

        # JM Comment: Include inference metadata in the saved model artifact.
        checkpoint["feature_columns"] = feature_columns
        checkpoint["revenue_scale"] = revenue_scale

        with tempfile.TemporaryDirectory() as output_dir:
            model_path = Path(output_dir) / "model.pt"
            metrics_path = Path(output_dir) / "metrics.json"

            torch.save(checkpoint, model_path)
            metrics_path.write_text(
                json.dumps(metrics, indent=2),
                encoding="utf-8",
            )

            # JM Comment: Persist artifacts to S3 when configured; otherwise save locally.
            if S3_BUCKET:
                s3 = boto3.client("s3")
                prefix = ARTIFACT_PREFIX.strip("/")
                model_key = f"{prefix}/model.pt"
                metrics_key = f"{prefix}/metrics.json"

                s3.upload_file(str(model_path), S3_BUCKET, model_key)
                s3.upload_file(str(metrics_path), S3_BUCKET, metrics_key)

                print(f"Model artifact: s3://{S3_BUCKET}/{model_key}")
                print(f"Metrics artifact: s3://{S3_BUCKET}/{metrics_key}")
            else:
                local_output = DATA_DIR.parent / "artifacts"
                local_output.mkdir(parents=True, exist_ok=True)
                model_dest = local_output / "model.pt"
                metrics_dest = local_output / "metrics.json"
                torch.save(checkpoint, model_dest)
                metrics_dest.write_text(
                    json.dumps(metrics, indent=2),
                    encoding="utf-8",
                )
                print(f"Model artifact: {model_dest}")
                print(f"Metrics artifact: {metrics_dest}")

        print("Ray Train status: SUCCESS")
        print("Workers:", metrics["workers"])
        print("Records used:", metrics["records_used"])
        print("Training records:", metrics["training_records"])
        print("Test records:", metrics["test_records"])
        print(f"MAE: {metrics['mae']:,.2f}")
        print(f"R²: {metrics['r2']:.4f}")

    finally:
        # JM Comment: Disconnect this client without changing cluster configuration.
        ray.shutdown()


if __name__ == "__main__":
    import faulthandler
    import traceback

    # Diagnostic: print every thread's stack if this process is still alive after 15 minutes.
    faulthandler.dump_traceback_later(900, repeat=True, file=sys.stderr)

    exit_code = 0
    try:
        main()
    except BaseException:
        traceback.print_exc()
        exit_code = 1
    finally:
        # Skip interpreter teardown, which can hang after pyarrow S3 use.
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(exit_code)
