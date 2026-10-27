
# Generate synthetic corporate datasets for the Ray AI Platform assessment.

import csv
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INDUSTRIES = [
    "Manufacturing", "Technology", "Retail",
    "Healthcare", "Logistics", "Energy",
]
COUNTRIES = ["India", "USA", "UK", "Germany", "Singapore"]

# Industry-specific revenue multipliers create a learnable signal for regression.
INDUSTRY_REVENUE_MULTIPLIERS = {
    "Manufacturing": 1.2,
    "Technology": 2.0,
    "Retail": 0.9,
    "Healthcare": 1.5,
    "Logistics": 1.1,
    "Energy": 2.2,
}

FIELDS = [
    "source_record_id", "corporate_name", "registration_id",
    "industry", "country", "employee_count",
    "annual_revenue", "source_system",
]


def make_record(source, i, company_number, name):
    industry = INDUSTRIES[(i - 1) % len(INDUSTRIES)]
    country = COUNTRIES[(i - 1) % len(COUNTRIES)]
    employee_count = 50 + ((i * 37) % 9950)

    # Revenue depends on workforce size and industry, with reproducible variation.
    industry_multiplier = INDUSTRY_REVENUE_MULTIPLIERS[industry]
    variation = 0.85 + ((i * 17) % 31) / 100
    annual_revenue = int(
        employee_count * 25_000 * industry_multiplier * variation
    )

    return {
        "source_record_id": f"{source[:2].upper()}-{i:05d}",
        "corporate_name": name,
        "registration_id": f"REG-{company_number:05d}",
        "industry": industry,
        "country": country,
        "employee_count": employee_count,
        "annual_revenue": annual_revenue,
        "source_system": source,
    }


def generate_datasets():
    supply_chain = []
    financial = []

    for i in range(1, 1201):
        name = (
            f"Acme Holdings {i}" if i <= 200
            else f"Corporate Company {i}"
        )
        supply_chain.append(
            make_record("SupplyChain", i, i, name)
        )

    for i in range(1, 1201):
        company_number = i if i <= 200 else i + 1200
        name = (
            f"ACME HOLDINGS {i} LTD." if i <= 200
            else f"Corporate Company {company_number}"
        )
        financial.append(
            make_record("Financial", i, company_number, name)
        )

    for filename, records in [
        ("supply_chain.csv", supply_chain),
        ("financial_data.csv", financial),
    ]:
        path = DATA_DIR / filename
        with path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(records)
        print(f"Created {path}: {len(records)} records")

    print("Dataset generation completed.")


if __name__ == "__main__":
    generate_datasets()
