#Detects regressions in data processing.
import pandas as pd
import pytest

from app.ray_pipeline import ingest_data, resolve_entities


def test_ingest_loads_both_datasets():
    df = ingest_data()

    assert len(df) == 2400
    assert df["source_system"].nunique() == 2


def test_entity_resolution_assigns_canonical_ids():
    df = ingest_data()
    resolved = resolve_entities(df)

    assert len(resolved) == 2400
    assert resolved["canonical_id"].notna().all()
    assert resolved["is_resolved"].all()


def test_overlapping_companies_get_same_canonical_id():
    df = pd.DataFrame({
        "corporate_name": ["Acme Holdings 1", "ACME HOLDINGS 1 LTD."],
        "registration_id": ["REG-00001", "REG-00001"],
    })

    resolved = resolve_entities(df)

    assert resolved["canonical_id"].nunique() == 1


def test_unique_company_count():
    df = ingest_data()
    resolved = resolve_entities(df)

    assert resolved["canonical_id"].nunique() == 2200


def test_missing_registration_id_is_rejected():
    df = pd.DataFrame({
        "corporate_name": ["Example Company"],
        "registration_id": [None],
    })

    with pytest.raises(ValueError, match="registration ID"):
        resolve_entities(df)
