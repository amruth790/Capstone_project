
import pandas as pd
from pathlib import Path

RAW = Path(__file__).resolve().parents[1] / "data" / "raw" / "sales_data.csv"
OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_SAMPLE = OUT_DIR / "sample_clean.csv"

def main():
    print("Loading data...")
    df = pd.read_csv(RAW, parse_dates=["order_date"])
    print("Rows, cols:", df.shape)
    print("Missing per column:\n", df.isna().sum())

    # Show duplicates
    dup_count = df.duplicated(subset=["order_id"]).sum()
    print("Duplicate order_id count:", dup_count)

    # Quick fixes (do NOT overwrite original)
    df_clean = df.copy()

    # Fill missing product with 'Unknown'
    df_clean["product"] = df_clean["product"].fillna("Unknown")

    # Fill missing customer_name with "Unknown"
    df_clean["customer_name"] = df_clean["customer_name"].replace("", pd.NA)
    df_clean["customer_name"] = df_clean["customer_name"].fillna("Unknown")

    # Ensure numeric types
    df_clean["unit_price"] = pd.to_numeric(df_clean["unit_price"], errors="coerce")
    df_clean["quantity"] = pd.to_numeric(df_clean["quantity"], errors="coerce").fillna(0).astype(int)
    df_clean["sales"] = pd.to_numeric(df_clean["sales"], errors="coerce").fillna(df_clean["unit_price"] * df_clean["quantity"])
    df_clean["profit"] = pd.to_numeric(df_clean["profit"], errors="coerce").fillna(df_clean["sales"] * 0.1)

    # Remove exact duplicate rows (if any)
    df_clean = df_clean.drop_duplicates()

    # Save a small sample for dashboard development
    df_clean.sample(frac=0.01, random_state=1).to_csv(OUT_SAMPLE, index=False)
    print("Saved sample to:", OUT_SAMPLE)
    print(df_clean.head().to_string())

if __name__ == "__main__":
    main()
