from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from src.logging.bq_client import get_bq_client
from src.logging.schemas import FEATURE_STORE_SCHEMA
try:
    import holidays  # type: ignore
except Exception:
    holidays = None

PROJECT = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
DATASET = "raf"
RAW_TABLE = f"{PROJECT}.{DATASET}.raw_sales"
FEATURE_TABLE = f"{PROJECT}.{DATASET}.feature_store"

def load_raw_sales(limit: int | None = None) -> pd.DataFrame:
    client = get_bq_client(PROJECT)
    sql = f"""
        SELECT date, date_block_num, shop_id, item_id, item_price, item_cnt_day
        FROM `{RAW_TABLE}`
        ORDER BY shop_id, item_id, date
        {f'LIMIT {limit}' if limit else ''}
    """
    df = client.query(sql).result().to_dataframe(create_bqstorage_client=True)
    df["date"] = pd.to_datetime(df["date"])
    # enforce dtypes
    df["date_block_num"] = df["date_block_num"].astype("int64")
    df["shop_id"] = df["shop_id"].astype("int64")
    df["item_id"] = df["item_id"].astype("int64")
    df["item_price"] = df["item_price"].astype("float64")
    df["item_cnt_day"] = df["item_cnt_day"].astype("float64")
    return df

def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    df["dow"] = df["date"].dt.weekday.astype("int64")        # 0=Mon
    df["weekofyear"] = df["date"].dt.isocalendar().week.astype("int64")
    df["month"] = df["date"].dt.month.astype("int64")

    if holidays is not None:
        # Use Russia holidays to match dataset locale
        year_min, year_max = int(df["date"].dt.year.min()), int(df["date"].dt.year.max())
        ru_holidays = holidays.country_holidays("RU", years=range(year_min, year_max + 1))
        df["is_holiday"] = df["date"].dt.date.isin(ru_holidays).astype(bool)
    else:
        df["is_holiday"] = False
    return df

def add_lags_rolls(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["shop_id","item_id","date"]).copy()
    g = df.groupby(["shop_id","item_id"], group_keys=False)

    # Lags of target
    for lag in [1,7,14,28]:
        df[f"lag_{lag}"] = g["item_cnt_day"].shift(lag)

    # Rolling means on a 1-step shifted series to avoid leakage
    for w in [7,14,28]:
        df[f"ma_{w}"] = g["item_cnt_day"].shift(1).rolling(w).mean()

    # Price last observed
    df["price_last"] = g["item_price"].shift(1)

    return df

def materialize_feature_store(df: pd.DataFrame) -> None:
    client = get_bq_client(PROJECT)

    # Keep the curated set of columns
    keep = [
        "date","shop_id","item_id","date_block_num","item_cnt_day",
        "lag_1","lag_7","lag_14","lag_28",
        "ma_7","ma_14","ma_28",
        "price_last",
        "dow","weekofyear","month","is_holiday",
    ]
    out = df[keep].copy()

    # Coerce dtypes for schema compatibility
    out["date"] = pd.to_datetime(out["date"]).dt.date
    for c in ["shop_id","item_id","date_block_num","dow","weekofyear","month"]:
        out[c] = out[c].astype("int64")
    for c in ["item_cnt_day","lag_1","lag_7","lag_14","lag_28","ma_7","ma_14","ma_28","price_last"]:
        out[c] = out[c].astype("float64")
    out["is_holiday"] = out["is_holiday"].astype(bool)

    # Create table if missing
    try:
        client.get_table(FEATURE_TABLE)
    except Exception:
        client.create_table(bigquery.Table(FEATURE_TABLE, schema=FEATURE_STORE_SCHEMA))

    # Write (replace for reproducibility)
    job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(out, FEATURE_TABLE, job_config=job_cfg).result()
    print(f"Wrote {len(out):,} rows to {FEATURE_TABLE}")

def main():
    if not PROJECT:
        raise SystemExit("Set GCP_PROJECT or GOOGLE_CLOUD_PROJECT before running.")
    print("Loading raw sales from BigQuery…")
    raw = load_raw_sales()
    print(f"Raw rows: {len(raw):,}")
    print("Engineering features…")
    raw = add_calendar_features(raw)
    feat = add_lags_rolls(raw)
    print("Materializing feature store to BigQuery…")
    materialize_feature_store(feat)

if __name__ == "__main__":
    main()
