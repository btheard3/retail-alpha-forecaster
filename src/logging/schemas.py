from google.cloud import bigquery

RAW_SALES_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("date_block_num", "INT64"),
    bigquery.SchemaField("shop_id", "INT64"),
    bigquery.SchemaField("item_id", "INT64"),
    bigquery.SchemaField("item_price", "FLOAT64"),
    bigquery.SchemaField("item_cnt_day", "FLOAT64"),
]

ITEMS_SCHEMA = [
    bigquery.SchemaField("item_id", "INT64"),
    bigquery.SchemaField("item_name", "STRING"),
    bigquery.SchemaField("item_category_id", "INT64"),
]

ITEM_CATS_SCHEMA = [
    bigquery.SchemaField("item_category_id", "INT64"),
    bigquery.SchemaField("item_category_name", "STRING"),
]

SHOPS_SCHEMA = [
    bigquery.SchemaField("shop_id", "INT64"),
    bigquery.SchemaField("shop_name", "STRING"),
]

FEATURE_STORE_SCHEMA = [
    # keys
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("shop_id", "INT64"),
    bigquery.SchemaField("item_id", "INT64"),
    bigquery.SchemaField("date_block_num", "INT64"),

    # target (same-day actuals for training)
    bigquery.SchemaField("item_cnt_day", "FLOAT64"),

    # lags
    bigquery.SchemaField("lag_1", "FLOAT64"),
    bigquery.SchemaField("lag_7", "FLOAT64"),
    bigquery.SchemaField("lag_14", "FLOAT64"),
    bigquery.SchemaField("lag_28", "FLOAT64"),

    # rolling means (leakage-safe)
    bigquery.SchemaField("ma_7", "FLOAT64"),
    bigquery.SchemaField("ma_14", "FLOAT64"),
    bigquery.SchemaField("ma_28", "FLOAT64"),

    # price features
    bigquery.SchemaField("price_last", "FLOAT64"),

    # calendar
    bigquery.SchemaField("dow", "INT64"),
    bigquery.SchemaField("weekofyear", "INT64"),
    bigquery.SchemaField("month", "INT64"),
    bigquery.SchemaField("is_holiday", "BOOL"),
]