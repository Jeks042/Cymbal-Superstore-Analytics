# Load Olist datasets into PostgreSQL
# This script loads the Olist datasets from CSV files into a PostgreSQL database.
# Note: Ensure you have the necessary Python libraries installed (pandas, sqlalchemy) and update the database connection parameters before running.
from sqlalchemy import create_engine
import pandas as pd

#-----------------------------
# CONFIG
#-----------------------------
engine = create_engine(
    "postgresql+psycopg2://postgres:<yourpassword>@localhost:<yourport>/<yourdatabase>"
)

#-----------------------------
# LOAD DATASETS
#-----------------------------
datasets = {
    "olist_customers": "olist_customers_dataset.csv",
    "olist_orders": "olist_orders_dataset.csv",
    "olist_order_items": "olist_order_items_dataset.csv",
    "olist_order_payments": "olist_order_payments_dataset.csv",
    "olist_products": "olist_products_dataset.csv",
    "olist_order_reviews": "olist_order_reviews_dataset.csv"
}

# Load each dataset into the database
for table, file in datasets.items():
    print(f"Loading {table}...")
    df = pd.read_csv(f"data/raw/{file}")

    df.to_sql(
        name=table,
        schema="raw",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=50_000
    )

    print(f"{table} loaded: {len(df)} rows")

