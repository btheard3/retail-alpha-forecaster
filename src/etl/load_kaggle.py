from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from src.logging.bq_client import get_bq_client
from src.logging.schemas import RAW_SALES_SCHEMA, ITEMS_SCHEMA, ITEM_CATS_SCHEMA, SHOPS_SCHEMA

DATA_DIR = Path(__file__).resolve().parents[2] / "data"

def _read_csv(name: str, **kwargs) -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / name, **kwargs)

def load_local() -> dict[str, pd.DataFrame]:
    # sales_train.csv columns: date, date_block_num, shop_id, item_id, item_price, item_cnt_day
    sales = _read_csv(
        "sales_train.csv",
        parse_dates=["date"],
        dayfirst=True,   # dd.mm.yyyy format
        dtype={
            "date_block_num": "int16",
            "shop_id": "int16",
            "item_id": "int32",
            "item_price": "float32",
            "item_cnt_day": "float32",
        },
    )

    items = _read_csv("items.csv", dtype={"item_id": "int32", "item_category_id": "int16"})
    item_cats = _read_csv("item_categories.csv", dtype={"item_category_id": "int16"})
    shops = _read_csv("shops.csv", dtype={"shop_id": "int16"})

    # Optional: write quick parquet snapshots
    out_dir = DATA_DIR / "parquet"
    out_dir.mkdir(exist_ok=True)
    sales.to_parquet(out_dir / "sales_train.parquet", index=False)
    items.to_parquet(out_dir / "items.parquet", index=False)
    item_cats.to_parquet(out_dir / "item_categories.parquet", index=False)
    shops.to_parquet(out_dir / "shops.parquet", index=False)

    return {"sales": sales, "items": items, "item_cats": item_cats, "shops": shops}

def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    try:
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_id))

def ensure_table(client: bigquery.Client, table_id: str, schema) -> None:
    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(bigquery.Table(table_id, schema=schema))

def upload_to_bigquery(dfs: dict[str, pd.DataFrame], project_id: str, dataset: str = "raf") -> None:
    client = get_bq_client(project_id)
    dataset_id = f"{project_id}.{dataset}"
    ensure_dataset(client, dataset_id)

    # Create tables if missing
    ensure_table(client, f"{dataset_id}.raw_sales", RAW_SALES_SCHEMA)
    ensure_table(client, f"{dataset_id}.items", ITEMS_SCHEMA)
    ensure_table(client, f"{dataset_id}.item_categories", ITEM_CATS_SCHEMA)
    ensure_table(client, f"{dataset_id}.shops", SHOPS_SCHEMA)

    # Coerce dtypes to match schemas
    sales = dfs["sales"].copy()
    sales["date"] = pd.to_datetime(sales["date"]).dt.date  # DATE
    sales["date_block_num"] = sales["date_block_num"].astype("int64")
    sales["shop_id"] = sales["shop_id"].astype("int64")
    sales["item_id"] = sales["item_id"].astype("int64")
    sales["item_price"] = sales["item_price"].astype("float64")
    sales["item_cnt_day"] = sales["item_cnt_day"].astype("float64")

    items = dfs["items"].copy()
    items["item_id"] = items["item_id"].astype("int64")
    items["item_category_id"] = items["item_category_id"].astype("int64")

    item_cats = dfs["item_cats"].copy()
    item_cats["item_category_id"] = item_cats["item_category_id"].astype("int64")

    shops = dfs["shops"].copy()
    shops["shop_id"] = shops["shop_id"].astype("int64")

    job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(sales, f"{dataset_id}.raw_sales", job_config=job_cfg).result()
    client.load_table_from_dataframe(items, f"{dataset_id}.items", job_config=job_cfg).result()
    client.load_table_from_dataframe(item_cats, f"{dataset_id}.item_categories", job_config=job_cfg).result()
    client.load_table_from_dataframe(shops, f"{dataset_id}.shops", job_config=job_cfg).result()

    print(f"Uploaded to BigQuery dataset {dataset_id}")

if __name__ == "__main__":
    project = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project:
        raise SystemExit("Set env var GCP_PROJECT=<your-gcp-project-id> before running.")
    dfs = load_local()
    upload_to_bigquery(dfs, project_id=project)
