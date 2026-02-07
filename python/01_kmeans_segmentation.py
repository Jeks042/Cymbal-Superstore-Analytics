# K-Means Clustering for Customer Segmentation
# This script performs K-Means clustering on RFM features to segment customers.
# It connects to the PostgreSQL database, loads the RFM features, cleans and scales them, determines the optimal number of clusters, fits the K-Means model, profiles the segments, and writes the results back to the database.

# Note: Ensure you have the necessary Python libraries installed (pandas, numpy, sqlalchemy, scikit-learn) and update the database connection parameters before running.
import pandas as pd
import numpy as np

from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


# -----------------------------
# CONFIG
# -----------------------------
DB_USER = "postgres"
DB_PASS = "<yourpassword>"
DB_HOST = "localhost"
DB_PORT = "<yourport>"
DB_NAME = "<yourdatabase>"

N_CLUSTERS = 5
RANDOM_STATE = 42

FEATURES = [
    "recency_days",
    "frequency",
    "monetary",
    "avg_order_value",
    "avg_items_per_order",
    "avg_category_diversity",
    "tenure_days",
    "avg_days_between_orders"
]

#-----------------------------
# FUNCTIONS
#-----------------------------

# Database connection and data loading
def connect_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

# Load RFM features from the database
def load_rfm(engine) -> pd.DataFrame:
    query = """
    SELECT
        customer_unique_id,
        recency_days,
        frequency,
        monetary,
        avg_order_value,
        avg_items_per_order,
        avg_category_diversity,
        tenure_days,
        avg_days_between_orders
    FROM analytics.customer_rfm
    """
    df = pd.read_sql(query, engine)
    return df

# Data cleaning and feature engineering
def clean_features(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure numeric
    for col in FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # avg_days_between_orders is NULL for one-time buyers; keep that signal
    # Use a large value so "no repeat" looks like long gap.
    # We use 9999 days (effectively "very long").
    df["avg_days_between_orders"] = df["avg_days_between_orders"].fillna(9999)

    # Basic missing handling for other columns (should be minimal)
    for col in FEATURES:
        if df[col].isna().any():
            df[col] = df[col].fillna(df[col].median())

    # Optional: reduce skew for spend-related variables (common in e-commerce)
    # Log transform helps KMeans.
    df["monetary_log"] = np.log1p(df["monetary"])
    df["avg_order_value_log"] = np.log1p(df["avg_order_value"])

    # Replace original with logged versions for clustering
    # Keep frequency/recency as-is.
    clustering_features = [
        "recency_days",
        "frequency",
        "monetary_log",
        "avg_order_value_log",
        "avg_items_per_order",
        "avg_category_diversity",
        "tenure_days",
        "avg_days_between_orders"
    ]

    return df, clustering_features

# K-Means clustering functions
def choose_k(X_scaled, k_min=2, k_max=10):
    scores = []
    for k in range(k_min, k_max + 1):
        model = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init="auto")
        labels = model.fit_predict(X_scaled)
        score = silhouette_score(X_scaled, labels)
        scores.append((k, score))
    return scores

# Fit final K-Means model
def fit_kmeans(X_scaled, n_clusters=5):
    model = KMeans(n_clusters=n_clusters, random_state=RANDOM_STATE, n_init="auto")
    labels = model.fit_predict(X_scaled)
    return model, labels

# Segment profiling and naming
def profile_segments(df: pd.DataFrame, label_col="segment") -> pd.DataFrame:
    # Profile using original (non-log) business metrics for readability
    profile = df.groupby(label_col).agg(
        customers=("customer_unique_id", "count"),
        recency_days=("recency_days", "mean"),
        frequency=("frequency", "mean"),
        monetary=("monetary", "mean"),
        avg_order_value=("avg_order_value", "mean"),
        avg_items_per_order=("avg_items_per_order", "mean"),
        avg_category_diversity=("avg_category_diversity", "mean"),
        tenure_days=("tenure_days", "mean"),
        avg_days_between_orders=("avg_days_between_orders", "mean"),
    ).reset_index()

    # Add share of customers
    profile["share"] = profile["customers"] / profile["customers"].sum()

    # Rank helper columns for naming
    profile["recency_rank"] = profile["recency_days"].rank(method="dense", ascending=True)     # lower is better
    profile["value_rank"] = profile["monetary"].rank(method="dense", ascending=False)          # higher is better
    profile["freq_rank"] = profile["frequency"].rank(method="dense", ascending=False)

    return profile

# Simple rule-based segment naming (can be refined after looking at profiles)
def name_segments(profile: pd.DataFrame) -> pd.DataFrame:
    """
    Assign business-friendly names using simple rules.
    You can refine later after looking at the profile table.
    """
    def segment_name(row):
        # Very high value + frequent + recent
        if row["value_rank"] <= 1 and row["freq_rank"] <= 2 and row["recency_rank"] <= 2:
            return "Champions"
        # High value but less recent (at risk)
        if row["value_rank"] <= 2 and row["recency_rank"] >= 4:
            return "At-Risk High Value"
        # Frequent but lower monetary
        if row["freq_rank"] <= 2 and row["value_rank"] >= 4:
            return "Loyal Low Spend"
        # Recent but low frequency (new)
        if row["recency_rank"] <= 2 and row["freq_rank"] >= 4:
            return "New Customers"
        # Default bucket
        return "Occasional Shoppers"

    profile["segment_name"] = profile.apply(segment_name, axis=1)
    return profile

# Write segments back to the database
def write_segments(engine, df_segments: pd.DataFrame):
    df_segments.to_sql(
        name="customer_segments",
        schema="analytics",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=50_000
    )

# Main function to orchestrate the workflow
def main():
    engine = connect_engine()

    # 1) Load
    df = load_rfm(engine)
    print("Loaded rows:", len(df))

    # 2) Clean + feature set
    df, clustering_features = clean_features(df)

    # 3) Scale
    scaler = StandardScaler()
    X = df[clustering_features].copy()
    X_scaled = scaler.fit_transform(X)

    # 4) Evaluate K (optional but recommended)
    k_scores = choose_k(X_scaled, 2, 10)
    print("\nSilhouette scores (higher is better):")
    for k, s in k_scores:
        print(f"k={k}: {s:.4f}")

    # 5) Fit final model with 5 clusters
    model, labels = fit_kmeans(X_scaled, N_CLUSTERS)
    df["segment"] = labels

    # 6) Profile
    prof = profile_segments(df, "segment")
    prof = name_segments(prof)

    print("\nSegment profile:")
    print(prof.sort_values("customers", ascending=False).to_string(index=False))

    # 7) Map segment names back to customers
    seg_map = prof[["segment", "segment_name"]]
    df_out = df.merge(seg_map, on="segment", how="left")

    # Keep output clean for SQL/Power BI
    df_out = df_out[[
        "customer_unique_id",
        "segment",
        "segment_name",
        "recency_days",
        "frequency",
        "monetary",
        "avg_order_value",
        "avg_items_per_order",
        "avg_category_diversity",
        "tenure_days",
        "avg_days_between_orders"
    ]]

    # 8) Write back
    write_segments(engine, df_out)
    print("\nWrote analytics.customer_segments successfully.")

    # Also write the segment profile table (very useful for Power BI)
    prof.to_sql(
        name="segment_profile",
        schema="analytics",
        con=engine,
        if_exists="replace",
        index=False
    )
    print("Wrote analytics.segment_profile successfully.")


if __name__ == "__main__":
    main()
