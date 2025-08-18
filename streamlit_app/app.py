import os
from pathlib import Path
import datetime as dt
import pandas as pd
import streamlit as st
import altair as alt

from google.cloud import bigquery
from google.oauth2 import service_account

# -----------------------------
# Config
# -----------------------------
PROJECT = os.getenv("GCP_PROJECT", "retail-alpha-forecaster")
DATASET = os.getenv("GCP_BIGQUERY_DATASET", "raf")
LOCATION = os.getenv("GCP_LOCATION", "US")

VIEW_CLEAN  = f"`{PROJECT}.{DATASET}.v_forecasts_clean`"
VIEW_PAIRS  = f"`{PROJECT}.{DATASET}.v_forecast_pairs`"

st.set_page_config(
    page_title="Retail Alpha Forecaster — Forecast Explorer & Model Monitor",
    layout="wide",
)

# -----------------------------
# BigQuery client (env or local key)
# -----------------------------
def make_bq_client():
    env_cred = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if env_cred and Path(env_cred).exists():
        creds = service_account.Credentials.from_service_account_file(env_cred)
        return bigquery.Client(project=PROJECT, credentials=creds, location=LOCATION)

    # fallback to ./keys for local dev
    key_path = Path(__file__).resolve().parents[1] / "keys" / "retail-alpha-forecaster-7f14a7b50e62.json"
    if key_path.exists():
        creds = service_account.Credentials.from_service_account_file(str(key_path))
        return bigquery.Client(project=PROJECT, credentials=creds, location=LOCATION)

    # last resort: default creds on machine (e.g., `gcloud auth application-default login`)
    return bigquery.Client(project=PROJECT, location=LOCATION)

client = make_bq_client()

# -----------------------------
# Small query helper + caching
# -----------------------------
@st.cache_data(show_spinner=False, ttl=600)
def q(sql: str) -> pd.DataFrame:
    return client.query(sql).result().to_dataframe()

# -----------------------------
# Sidebar: filters
# -----------------------------
st.sidebar.header("Filters")

# Load available pairs (non-null only; view is already non-null, we keep a guard WHERE anyway)
pairs = q(f"""
SELECT shop_id, item_id, n_rows, min_date, max_date
FROM {VIEW_PAIRS}
WHERE shop_id IS NOT NULL AND item_id IS NOT NULL
ORDER BY n_rows DESC, shop_id, item_id
""")

if pairs.empty:
    st.error("No forecast pairs found. Make sure the views exist and contain data.")
    st.stop()

# Shop select
shops = pairs["shop_id"].dropna().drop_duplicates().sort_values().tolist()
shop_id = st.sidebar.selectbox("Shop", shops, index=0, format_func=lambda x: f"{x}")

# Item select (constrained by selected shop)
items_for_shop = pairs.loc[pairs["shop_id"] == shop_id, "item_id"].drop_duplicates().sort_values().tolist()
item_id = st.sidebar.selectbox("Item", items_for_shop, index=0, format_func=lambda x: f"{x}")

# Pair window for date pickers
row = pairs[(pairs.shop_id == shop_id) & (pairs.item_id == item_id)].head(1).squeeze()
default_min = pd.to_datetime(row["min_date"]).date() if pd.notna(row["min_date"]) else dt.date(2015, 10, 1)
default_max = pd.to_datetime(row["max_date"]).date() if pd.notna(row["max_date"]) else dt.date(2015, 10, 31)

start_date = st.sidebar.date_input("Start", value=default_min, min_value=default_min, max_value=default_max)
end_date   = st.sidebar.date_input("End",   value=default_max, min_value=default_min, max_value=default_max)

# Safety clamp
if start_date > end_date:
    st.sidebar.warning("Start date is after end date. Adjusting to pair window.")
    start_date, end_date = default_min, default_max

# Manual cache refresh
if st.sidebar.button("🔄 Refresh cached data"):
    q.clear()
    st.experimental_rerun()

# -----------------------------
# Header + tabs
# -----------------------------
st.title("🛍️ Retail Alpha Forecaster — Forecast Explorer & Model Monitor")
tabs = st.tabs(["📈 Forecasts", "📦 Backtests & Runs", "📚 Data Dictionary"])

# -----------------------------
# Tab 1: Forecasts
# -----------------------------
with tabs[0]:
    st.subheader("Actuals vs Forecast (with prediction bands)")

    # Pull forecast slice from clean view (already non-null; add guard WHERE anyway)
    df = q(f"""
    SELECT date, shop_id, item_id, yhat, yhat_lower, yhat_upper, model, created_at
    FROM {VIEW_CLEAN}
    WHERE shop_id IS NOT NULL AND item_id IS NOT NULL
      AND shop_id = {shop_id}
      AND item_id = {item_id}
      AND DATE(date) BETWEEN DATE("{start_date}") AND DATE("{end_date}")
    ORDER BY date
    """)

    if df.empty:
        st.info("No data found for this selection.")
    else:
        # Build band area + line chart
        df["date"] = pd.to_datetime(df["date"])
        base = alt.Chart(df).encode(x=alt.X("date:T", title="Date"))

        band = base.mark_area(opacity=0.2).encode(
            y=alt.Y("yhat_lower:Q", title="Forecast"),
            y2="yhat_upper:Q",
            tooltip=[
                alt.Tooltip("date:T"),
                alt.Tooltip("yhat_lower:Q", format=".2f"),
                alt.Tooltip("yhat_upper:Q", format=".2f"),
            ],
        )

        line = base.mark_line().encode(
            y="yhat:Q",
            tooltip=[
                alt.Tooltip("date:T"),
                alt.Tooltip("yhat:Q", format=".2f"),
                alt.Tooltip("model:N"),
                alt.Tooltip("created_at:T", title="created"),
            ],
            color=alt.value("#4c78a8"),
        )

        st.altair_chart((band + line).properties(height=420), use_container_width=True)

        # Stats table
        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric("Rows", f"{len(df):,}")
        with c2: st.metric("Min date", df["date"].min().date().isoformat())
        with c3: st.metric("Max date", df["date"].max().date().isoformat())
        with c4: st.metric("Model (top row)", str(df["model"].iloc[0]))

        st.caption(
            "Data source: "
            f"{VIEW_CLEAN} — non-null shop/item only; one row per (date, shop, item); "
            "bands are guaranteed yhat_lower ≤ yhat_upper."
        )

# -----------------------------
# Tab 2: Backtests & Runs (optional placeholder)
# -----------------------------
with tabs[1]:
    st.subheader("Backtests & Runs")
    st.write(
        "Hook this tab to a backtests table or view when ready. "
        "For now, the main forecast visualization is available in the first tab."
    )

# -----------------------------
# Tab 3: Data Dictionary
# -----------------------------
with tabs[2]:
    st.subheader("Data Dictionary — v_forecasts_clean")
    st.markdown(
        """
**Columns**

- `date` — Day of forecast horizon  
- `shop_id`, `item_id` — Entity identifiers (non-null, validated)  
- `model` — Model name used for this row  
- `yhat` — Point forecast  
- `yhat_lower`, `yhat_upper` — Prediction interval bounds  
- `created_at` — Load timestamp for tie-breaking (latest chosen)

**Guarantees**

- One row per (date, shop_id, item_id) chosen by preference:
  `lightgbm` first, then latest `created_at`
- `yhat_lower ≤ yhat_upper` (band sanity fix)
- All filters additionally enforce `shop_id IS NOT NULL AND item_id IS NOT NULL`
        """
    )
