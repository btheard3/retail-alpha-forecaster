from __future__ import annotations
import os
import pandas as pd
from google.cloud import bigquery

def get_bq_client(project_id: str | None = None) -> bigquery.Client:
    project_id = project_id or os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    return bigquery.Client(project=project_id)

def load_df(df: pd.DataFrame, table_id: str, write_disposition: str = "WRITE_APPEND") -> None:
    client = get_bq_client()
    job_cfg = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=False,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_cfg)
    job.result()
