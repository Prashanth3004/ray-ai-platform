# Checks that the baseline model and error handling work.
import pandas as pd
import pytest

from app.train_model import train_revenue_model


def test_model_trains_and_returns_metrics():
    metrics = train_revenue_model()

    assert metrics["records_used"] == 2400
    assert metrics["training_records"] == 1920
    assert metrics["test_records"] == 480
    assert metrics["mae"] >= 0
    assert -1 <= metrics["r2"] <= 1


def test_missing_dataset_is_rejected(tmp_path):
    with pytest.raises(FileNotFoundError, match="Dataset not found"):
        train_revenue_model(data_dir=tmp_path)


def test_missing_required_column_is_rejected(tmp_path):
    pd.DataFrame({
        "employee_count": [100, 200],
        "industry": ["Technology", "Retail"],
        "country": ["India", "USA"],
    }).to_csv(tmp_path / "supply_chain.csv", index=False)

    pd.DataFrame({
        "employee_count": [300, 400],
        "industry": ["Energy", "Healthcare"],
        "country": ["UK", "Germany"],
    }).to_csv(tmp_path / "financial_data.csv", index=False)

    with pytest.raises(ValueError, match="Missing required columns"):
        train_revenue_model(data_dir=tmp_path)
