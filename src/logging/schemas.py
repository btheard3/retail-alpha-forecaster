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
