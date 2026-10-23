
# Train and evaluate a regression model for synthetic corporate revenue prediction.

from pathlib import Path

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

DATA_DIR = Path(__file__).parent / "data"

FEATURE_COLUMNS = [
    "employee_count",
    "industry",
    "country",
]
TARGET_COLUMN = "annual_revenue"


def train_revenue_model(data_dir: Path = DATA_DIR) -> dict:
    """Train a regression model and return evaluation metrics."""
    files = [
        data_dir / "supply_chain.csv",
        data_dir / "financial_data.csv",
    ]

    frames = []
    for file_path in files:
        if not file_path.is_file():
            raise FileNotFoundError(f"Dataset not found: {file_path}")
        frames.append(pd.read_csv(file_path))

    df = pd.concat(frames, ignore_index=True)

    required = set(FEATURE_COLUMNS + [TARGET_COLUMN])
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df = df.dropna(subset=FEATURE_COLUMNS + [TARGET_COLUMN])
    if len(df) < 10:
        raise ValueError("At least 10 valid records are required for training.")

    X = df[FEATURE_COLUMNS]
    y = pd.to_numeric(df[TARGET_COLUMN], errors="raise")

    numeric_features = ["employee_count"]
    categorical_features = ["industry", "country"]

    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", "passthrough", numeric_features),
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore"),
                categorical_features,
            ),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "regressor",
                RandomForestRegressor(
                    n_estimators=100,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
    )

    model.fit(X_train, y_train)
    predictions = model.predict(X_test)

    metrics = {
        "records_used": int(len(df)),
        "training_records": int(len(X_train)),
        "test_records": int(len(X_test)),
        "mae": float(mean_absolute_error(y_test, predictions)),
        "r2": float(r2_score(y_test, predictions)),
    }

    print("Revenue model evaluation")
    print(f"Records used: {metrics['records_used']}")
    print(f"Training records: {metrics['training_records']}")
    print(f"Test records: {metrics['test_records']}")
    print(f"Mean Absolute Error (MAE): {metrics['mae']:,.2f}")
    print(f"R² score: {metrics['r2']:.4f}")

    return metrics


if __name__ == "__main__":
    train_revenue_model()
