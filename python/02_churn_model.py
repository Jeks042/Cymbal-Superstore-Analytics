# Churn prediction model
# This script trains a logistic regression model to predict customer churn based on RFM features, time-windowed features, and customer segments. It connects to the PostgreSQL database, loads the training data, preprocesses it, trains the model, evaluates its performance, scores all customers with churn risk, assigns priority bands based on risk and value, and writes the results back to the database.
# Note: Ensure you have the necessary Python libraries installed (pandas, numpy, sqlalchemy, scikit-learn) and update the database connection parameters before running.
import numpy as np
import pandas as pd

from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report


# -----------------------------
# CONFIG
# -----------------------------
DB_USER = "postgres"
DB_PASS = "<yourpassword>"
DB_HOST = "localhost"
DB_PORT = "<yourport>"
DB_NAME = "<yourdatabase>"

CHURN_THRESHOLD_DAYS = 180
RANDOM_STATE = 42

# Database connection and data loading
def engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

# Load training data with features and churn label
def load_training_frame(db_engine) -> pd.DataFrame:
    q = """
    SELECT
        r.customer_unique_id,
        r.frequency,
        r.monetary,
        r.avg_order_value,
        r.avg_items_per_order,
        r.avg_category_diversity,
        r.tenure_days,
        r.avg_days_between_orders,
        r.recency_days,
        s.segment_name,

        t.spend_30d,
        t.spend_90d,
        t.spend_180d,
        t.orders_30d,
        t.orders_90d,
        t.orders_180d,
        t.recent_order_ratio,
        t.recent_spend_ratio

    FROM analytics.customer_rfm r
    LEFT JOIN analytics.customer_segments s
    ON r.customer_unique_id = s.customer_unique_id
    LEFT JOIN analytics.customer_time_features t
    ON r.customer_unique_id = t.customer_unique_id
    """

    df = pd.read_sql(q, db_engine)

    # ensure numeric types
    lifetime_cols = [
    "frequency",
    "monetary",
    "avg_order_value",
    "avg_items_per_order",
    "avg_category_diversity",
    "tenure_days",
    "avg_days_between_orders"
    ]

    time_cols = [
        "spend_30d",
        "spend_90d",
        "orders_30d",
        "orders_90d"
    ]

    # numeric coercion
    for c in lifetime_cols + time_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # lifetime → median
    for c in lifetime_cols:
        if df[c].isna().any():
            df[c] = df[c].fillna(df[c].median())

    # time-windowed → zero
    for c in time_cols:
        if df[c].isna().any():
            df[c] = df[c].fillna(0)

    # one-time buyers were set to 9999 earlier; keep that signal
    df["avg_days_between_orders"] = df["avg_days_between_orders"].fillna(9999)

    # label (baseline churn definition)
    df["churned"] = (df["recency_days"] >= CHURN_THRESHOLD_DAYS).astype(int)

    # replace missing segment with "Unknown"
    df["segment_name"] = df["segment_name"].fillna("Unknown")

    return df

# Feature engineering, model training, and evaluation
def train_and_score(df: pd.DataFrame):
    feature_cols_num = [
    "frequency",
    "monetary",
    "avg_order_value",
    "avg_items_per_order",
    "avg_category_diversity",
    "tenure_days",
    "avg_days_between_orders",

    # time-windowed
    "spend_30d",
    "spend_90d",
    "orders_30d",
    "orders_90d"
    ]

    feature_cols_cat = ["segment_name"]

    X = df[feature_cols_num + feature_cols_cat]
    y = df["churned"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE, stratify=y
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), feature_cols_num),
            ("cat", OneHotEncoder(handle_unknown="ignore"), feature_cols_cat),
        ]
    )

    model = LogisticRegression(max_iter=2000, class_weight="balanced")

    clf = Pipeline(steps=[
        ("prep", preprocessor),
        ("model", model)
    ])

    clf.fit(X_train, y_train)

    # Evaluate
    proba = clf.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, proba)

    preds = (proba >= 0.5).astype(int)
    print(f"\nROC-AUC: {auc:.4f}")
    print("\nClassification report (threshold=0.5):")
    print(classification_report(y_test, preds))

    # Score everyone
    df_out = df.copy()
    df_out["churn_risk"] = clf.predict_proba(X)[:, 1]

    return df_out, auc

# Write results back to database
def write_back(db_engine, scored: pd.DataFrame):
    out_cols = [
        # identifiers + outputs
        "customer_unique_id",
        "segment_name",
        "churned",
        "churn_risk",
        "value_at_risk",

        # QA / definition field (OK to store, but not used in X)
        "recency_days",

        # lifetime behaviour features
        "frequency",
        "monetary",
        "avg_order_value",
        "avg_items_per_order",
        "avg_category_diversity",
        "tenure_days",
        "avg_days_between_orders",

        # time-windowed features USED (leakage-safe: < 180 days)
        "orders_30d",
        "orders_90d",
        "spend_30d",
        "spend_90d",
    ]

    # Optional: if created later
    for optional in ["risk_decile", "priority_band"]:
        if optional in scored.columns and optional not in out_cols:
            out_cols.append(optional)

    out = scored[out_cols].copy()

    out.to_sql(
        name="customer_churn_scores",
        schema="analytics",
        con=db_engine,
        if_exists="replace",
        index=False,
        chunksize=50_000
    )
    print("\nWrote analytics.customer_churn_scores successfully.")

# Assign priority bands based on churn risk and customer value
def assign_priority_band(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign priority bands using churn risk x customer value.
    """

    df = df.copy()

    # Rank into tertiles (1 = highest)
    df["risk_band"] = pd.qcut(
        df["churn_risk"],
        q=3,
        labels=[3, 2, 1]  # 1 = highest risk
    ).astype(int)

    df["value_band"] = pd.qcut(
        df["monetary"],
        q=3,
        labels=[3, 2, 1]  # 1 = highest value
    ).astype(int)

    # Priority logic
    def priority(row):
        if row["risk_band"] == 1 and row["value_band"] == 1:
            return "HIGH"
        if row["risk_band"] == 1 and row["value_band"] == 2:
            return "MEDIUM"
        if row["risk_band"] == 2 and row["value_band"] == 1:
            return "MEDIUM"
        return "LOW"

    df["priority_band"] = df.apply(priority, axis=1)

    return df

# Main function to orchestrate the workflow
def main():
    db_engine = engine()
    df = load_training_frame(db_engine)
    print("Loaded customers:", len(df))

    # confirm class balance
    print(df["churned"].value_counts(dropna=False))
    print("Churn rate (by definition):", df["churned"].mean().round(4))

    scored, auc = train_and_score(df)

    # Validation checks (ranking / lift)
    scored = scored.copy()
    scored["risk_decile"] = pd.qcut(scored["churn_risk"], 10, labels=False)

    lift = (
        scored
        .groupby("risk_decile")["churned"]
        .mean()
        .sort_index(ascending=False)
    )

    print("\nChurn rate by risk decile (9 = highest risk):")
    print(lift)

    # Priority band (risk × value)
    scored = assign_priority_band(scored)

    print("\nPriority band distribution:")
    print(scored["priority_band"].value_counts())

    # Expected value at risk
    scored["value_at_risk"] = scored["monetary"] * scored["churn_risk"]

    # Write back AFTER validation (now includes risk_decile)
    write_back(db_engine, scored)

if __name__ == "__main__":
    main()