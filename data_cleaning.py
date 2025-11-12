

import pandas as pd
from pathlib import Path
import logging

# 1. Setup logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


# 2. Define file paths

BASE = Path(__file__).resolve().parents[1]
RAW_FILE = BASE / "data" / "raw" / "sales_data.csv"
CLEAN_CSV = BASE / "data" / "processed" / "sales_clean.csv"
CLEAN_PARQUET = BASE / "data" / "processed" / "sales_clean.parquet"


# 3. Load data

def load_data():
    logging.info(f"Loading raw data from {RAW_FILE} ...")
    df = pd.read_csv(RAW_FILE)
    logging.info(f"Loaded {len(df):,} rows and {len(df.columns)} columns.")
    return df


# 4. Cleaning function

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting data cleaning...")

    # Remove exact duplicates
    before = len(df)
    df = df.drop_duplicates()
    logging.info(f"Removed {before - len(df)} duplicate rows.")

    # Replace empty strings with NaN
    df = df.replace(r'^\s*$', pd.NA, regex=True)

    # Fill missing categorical fields
    df["customer_name"] = df["customer_name"].fillna("Unknown")
    df["product"] = df["product"].fillna("Unknown")

    # Fix data types
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce")

    # Remove rows with invalid/missing critical fields
    df = df.dropna(subset=["order_date", "unit_price", "quantity"])

    # Filter out nonsense data (like negative prices or zero sales)
    df = df[df["unit_price"] > 0]
    df = df[df["quantity"] > 0]

    # Compute revenue again for validation
    df["computed_sales"] = df["unit_price"] * df["quantity"]
    df["sales_diff"] = (df["sales"] - df["computed_sales"]).abs()
    mismatch_count = (df["sales_diff"] > 1e-2).sum()
    if mismatch_count:
        logging.warning(f"Found {mismatch_count} mismatched sales records, fixing...")
        df["sales"] = df["computed_sales"]

    # Add extra derived metrics
    df["margin_percent"] = (df["profit"] / df["sales"]) * 100
    df["discount"] = df["unit_price"].apply(lambda x: 0.1 if x > 1000 else 0.05)

    # Drop helper columns
    df = df.drop(columns=["computed_sales", "sales_diff"], errors="ignore")

    logging.info("Data cleaning complete.")
    logging.info(f"Final dataset shape: {df.shape}")
    return df


# 5. Save cleaned data

def save_data(df: pd.DataFrame):
    df.to_csv(CLEAN_CSV, index=False)
    df.to_parquet(CLEAN_PARQUET, index=False)
    logging.info(f"Cleaned data saved to:\n - {CLEAN_CSV}\n - {CLEAN_PARQUET}")


# 6. Main pipeline

def main():
    df_raw = load_data()
    df_clean = clean_data(df_raw)
    save_data(df_clean)
    logging.info("✅ Data cleaning process finished successfully!")

if __name__ == "__main__":
    main()
