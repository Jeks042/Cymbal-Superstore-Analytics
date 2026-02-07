-- This SQL script creates the analytics layer of our data warehouse, transforming raw e-commerce data into structured tables and views for customer segmentation, RFM analysis, cohort retention, and more.
-- It also includes dimension tables for customers and dates to facilitate easier analysis in BI tools.

-- Note: Run this script after running create_schemas.sql and dim_date.sql to ensure the necessary schemas and dimension tables are in place.
CREATE OR REPLACE VIEW analytics.v_delivered_orders AS
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_delivered_customer_date
FROM raw.olist_orders
WHERE order_status = 'delivered'
  AND order_purchase_timestamp IS NOT NULL;

-- customer_master table
DROP TABLE IF EXISTS analytics.customer_master;

CREATE TABLE analytics.customer_master AS
WITH delivered AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp
    FROM analytics.v_delivered_orders o
),
customer_map AS (
    -- map customer_id -> customer_unique_id
    SELECT DISTINCT
        customer_id,
        customer_unique_id
    FROM raw.olist_customers
),
geo_counts AS (
    -- count location occurrences per unique customer
    SELECT
        customer_unique_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix,
        COUNT(*) AS cnt
    FROM raw.olist_customers
    GROUP BY
        customer_unique_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix
),
primary_geo AS (
    -- pick the most frequent location (mode) per unique customer
    SELECT DISTINCT ON (customer_unique_id)
        customer_unique_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix
    FROM geo_counts
    ORDER BY customer_unique_id, cnt DESC, customer_city, customer_state
)
SELECT
    m.customer_unique_id,
    g.customer_city,
    g.customer_state,
    g.customer_zip_code_prefix,

    MIN(d.order_purchase_timestamp) AS first_order_ts,
    MAX(d.order_purchase_timestamp) AS last_order_ts,
    COUNT(DISTINCT d.order_id)      AS total_delivered_orders,
    DATE_PART('day', MAX(d.order_purchase_timestamp) - MIN(d.order_purchase_timestamp))::int AS tenure_days
FROM delivered d
JOIN customer_map m
  ON d.customer_id = m.customer_id
JOIN primary_geo g
  ON m.customer_unique_id = g.customer_unique_id
GROUP BY
    m.customer_unique_id,
    g.customer_city,
    g.customer_state,
    g.customer_zip_code_prefix;

-- check if mode-geo makes sense
SELECT COUNT(*) AS customers_with_multiple_locations
FROM (
    SELECT customer_unique_id
    FROM raw.olist_customers
    GROUP BY customer_unique_id
    HAVING COUNT(DISTINCT customer_city || '|' || customer_state) > 1
) t;

-- quick check
SELECT COUNT(*) FROM analytics.customer_master;


-- customer_orders table
DROP TABLE IF EXISTS analytics.customer_orders;

CREATE TABLE analytics.customer_orders AS
WITH delivered AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date
    FROM analytics.v_delivered_orders o
),
cust_map AS (
    SELECT DISTINCT
        customer_id,
        customer_unique_id
    FROM raw.olist_customers
),
items AS (
    SELECT
        oi.order_id,
        SUM(oi.price)::numeric(12,2)        AS items_revenue,
        SUM(oi.freight_value)::numeric(12,2) AS freight_value,
        COUNT(*)                            AS items_count,
        COUNT(DISTINCT oi.product_id)        AS distinct_products
    FROM raw.olist_order_items oi
    GROUP BY oi.order_id
),
category_div AS (
    SELECT
        oi.order_id,
        COUNT(DISTINCT p.product_category_name) AS category_diversity
    FROM raw.olist_order_items oi
    JOIN raw.olist_products p
      ON p.product_id = oi.product_id
    GROUP BY oi.order_id
),
payments AS (
    SELECT
        op.order_id,
        SUM(op.payment_value)::numeric(12,2) AS payment_value,
        COUNT(DISTINCT op.payment_type)      AS payment_type_count,
        MAX(op.payment_installments)         AS max_installments
    FROM raw.olist_order_payments op
    GROUP BY op.order_id
),
reviews AS (
    SELECT
        r.order_id,
        AVG(r.review_score)::numeric(5,2) AS avg_review_score
    FROM raw.olist_order_reviews r
    GROUP BY r.order_id
)
SELECT
    cm.customer_unique_id,
    d.order_id,
    d.order_purchase_timestamp,
    d.order_delivered_customer_date,
    -- Financials
    COALESCE(i.items_revenue, 0)::numeric(12,2)   AS items_revenue,
    COALESCE(i.freight_value, 0)::numeric(12,2)   AS freight_value,
    (COALESCE(i.items_revenue, 0) + COALESCE(i.freight_value, 0))::numeric(12,2) AS gross_order_value,
    -- Basket composition
    COALESCE(i.items_count, 0)                    AS items_count,
    COALESCE(i.distinct_products, 0)              AS distinct_products,
    COALESCE(cd.category_diversity, 0)            AS category_diversity,
    -- Payment behaviour
    COALESCE(p.payment_value, 0)::numeric(12,2)   AS payment_value,
    COALESCE(p.payment_type_count, 0)             AS payment_type_count,
    COALESCE(p.max_installments, 0)               AS max_installments,
    -- Satisfaction proxy
    rv.avg_review_score

FROM delivered d
JOIN cust_map cm
  ON d.customer_id = cm.customer_id
LEFT JOIN items i
  ON d.order_id = i.order_id
LEFT JOIN category_div cd
  ON d.order_id = cd.order_id
LEFT JOIN payments p
  ON d.order_id = p.order_id
LEFT JOIN reviews rv
  ON d.order_id = rv.order_id;

  -- Add indexes for faster querying
  CREATE INDEX IF NOT EXISTS idx_customer_orders_customer
ON analytics.customer_orders(customer_unique_id);

CREATE INDEX IF NOT EXISTS idx_customer_orders_order
ON analytics.customer_orders(order_id);

CREATE INDEX IF NOT EXISTS idx_customer_orders_purchase_ts
ON analytics.customer_orders(order_purchase_timestamp);

-- quick check
SELECT COUNT(*) FROM analytics.customer_orders;

-- check for duplicate orders
SELECT order_id, COUNT(*) AS n
FROM analytics.customer_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Null timestamp check
SELECT COUNT(*) AS null_purchase_ts
FROM analytics.customer_orders
WHERE order_purchase_timestamp IS NULL;

-- customer_rfm table
DROP TABLE IF EXISTS analytics.customer_rfm;

CREATE TABLE analytics.customer_rfm AS
WITH dataset_end AS (
  SELECT MAX(order_purchase_timestamp) AS end_ts
  FROM analytics.customer_orders
),
base AS (
  SELECT
    customer_unique_id,
    order_id,
    order_purchase_timestamp,
    gross_order_value,
    items_count,
    category_diversity
  FROM analytics.customer_orders
  WHERE order_purchase_timestamp IS NOT NULL
),
cust_agg AS (
  SELECT
    customer_unique_id,
    MIN(order_purchase_timestamp) AS first_order_ts,
    MAX(order_purchase_timestamp) AS last_order_ts,
    COUNT(DISTINCT order_id)      AS frequency,
    SUM(gross_order_value)::numeric(14,2) AS monetary,
    AVG(gross_order_value)::numeric(14,2) AS avg_order_value,
    AVG(items_count)::numeric(10,2)       AS avg_items_per_order,
    AVG(category_diversity)::numeric(10,2) AS avg_category_diversity
  FROM base
  GROUP BY customer_unique_id
),
gaps AS (
  SELECT
    customer_unique_id,
    AVG(gap_days)::numeric(10,2) AS avg_days_between_orders
  FROM (
    SELECT
      customer_unique_id,
      EXTRACT(EPOCH FROM (
        order_purchase_timestamp
        - LAG(order_purchase_timestamp) OVER (
            PARTITION BY customer_unique_id
            ORDER BY order_purchase_timestamp
        )
      )) / 86400.0 AS gap_days
    FROM base
  ) t
  WHERE gap_days IS NOT NULL
  GROUP BY customer_unique_id
)
SELECT
  c.customer_unique_id,
  -- dataset-relative recency
  DATE_PART('day', (de.end_ts - c.last_order_ts))::int AS recency_days,

  c.frequency,
  c.monetary,
  c.avg_order_value,
  c.avg_items_per_order,
  c.avg_category_diversity,
  DATE_PART('day', c.last_order_ts - c.first_order_ts)::int AS tenure_days,
  g.avg_days_between_orders,
  c.first_order_ts,
  c.last_order_ts
FROM cust_agg c
CROSS JOIN dataset_end de
LEFT JOIN gaps g
  ON c.customer_unique_id = g.customer_unique_id;


-- add indexes for faster querying
CREATE INDEX IF NOT EXISTS idx_customer_rfm_customer
ON analytics.customer_rfm(customer_unique_id);

-- sanity checks
SELECT COUNT(*) FROM analytics.customer_rfm;   -- should be close to 93,358

SELECT
  MIN(recency_days) AS min_recency,
  MAX(recency_days) AS max_recency
FROM analytics.customer_rfm;

SELECT
  COUNT(*) AS repeat_customers
FROM analytics.customer_rfm
WHERE frequency > 1;

-- customer_orders_enriched table - joins each order to its customer segment
DROP TABLE IF EXISTS analytics.customer_orders_enriched;

CREATE TABLE analytics.customer_orders_enriched AS
SELECT
    co.customer_unique_id,
    cs.segment,
    cs.segment_name,

    co.order_id,
    co.order_purchase_timestamp,
    co.order_delivered_customer_date,

    co.items_revenue,
    co.freight_value,
    co.gross_order_value,

    co.items_count,
    co.distinct_products,
    co.category_diversity,

    co.payment_value,
    co.payment_type_count,
    co.max_installments,

    co.avg_review_score
FROM analytics.customer_orders co
LEFT JOIN analytics.customer_segments cs
  ON co.customer_unique_id = cs.customer_unique_id;

  -- add indexes for faster querying
CREATE INDEX IF NOT EXISTS idx_orders_enriched_segment
ON analytics.customer_orders_enriched(segment_name);

CREATE INDEX IF NOT EXISTS idx_orders_enriched_purchase_ts
ON analytics.customer_orders_enriched(order_purchase_timestamp);

-- segment_kpis table
DROP TABLE IF EXISTS analytics.segment_kpis;

CREATE TABLE analytics.segment_kpis AS
WITH base AS (
    SELECT
        segment_name,
        customer_unique_id,
        order_id,
        order_purchase_timestamp,
        gross_order_value,
        items_count,
        category_diversity,
        avg_review_score
    FROM analytics.customer_orders_enriched
    WHERE segment_name IS NOT NULL
),
cust_level AS (
    SELECT
        segment_name,
        customer_unique_id,
        COUNT(DISTINCT order_id) AS orders_per_customer,
        SUM(gross_order_value)   AS spend_per_customer,
        MAX(order_purchase_timestamp) AS last_purchase_ts
    FROM base
    GROUP BY segment_name, customer_unique_id
)
SELECT
    segment_name,

    COUNT(DISTINCT customer_unique_id) AS customers,
    COUNT(DISTINCT order_id)           AS orders,

    ROUND(SUM(gross_order_value)::numeric, 2) AS revenue,
    ROUND(AVG(gross_order_value)::numeric, 2) AS avg_order_value,
    ROUND(AVG(items_count)::numeric, 2)       AS avg_items_per_order,
    ROUND(AVG(category_diversity)::numeric, 2) AS avg_category_diversity,

    ROUND(AVG(avg_review_score)::numeric, 2)  AS avg_review_score,
    -- customer-level
    ROUND(AVG(orders_per_customer)::numeric, 2) AS avg_orders_per_customer,
    ROUND(AVG(spend_per_customer)::numeric, 2)  AS avg_spend_per_customer,
    -- segment recency (median helps with outliers)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY DATE_PART('day', CURRENT_DATE::timestamp - last_purchase_ts)
    )::numeric(10,2) AS median_recency_days

FROM cust_level
JOIN base USING (segment_name, customer_unique_id)
GROUP BY segment_name;

-- Verify segment_kpis
SELECT *
FROM analytics.segment_kpis
ORDER BY revenue DESC;

-- segment_actions table
DROP TABLE IF EXISTS analytics.segment_actions;

CREATE TABLE analytics.segment_actions AS
SELECT *
FROM (VALUES
  ('Champions', 'Reward & retain', 'VIP perks, early access, premium bundles, referral incentives.'),
  ('At-Risk High Value', 'Win-back', 'Triggered email + time-limited offer, personalised recommendations, free shipping.'),
  ('Loyal Low Spend', 'Increase basket size', 'Cross-sell bundles, threshold-based discounts, subscribe-and-save style offers.'),
  ('New Customers', 'Onboard & convert', 'Welcome journey, product education, second-purchase incentive within 7-14 days.'),
  ('Occasional Shoppers', 'Reactivate gently', 'Seasonal campaigns, reminders, category-based promos, social proof/reviews.')
) AS t(segment_name, objective, recommended_actions);

-- customer_cohort table
DROP TABLE IF EXISTS analytics.customer_cohort;

CREATE TABLE analytics.customer_cohort AS
WITH first_purchase AS (
  SELECT
    customer_unique_id,
    DATE_TRUNC('month', MIN(order_purchase_timestamp))::date AS cohort_month
  FROM analytics.customer_orders
  GROUP BY customer_unique_id
),
orders_by_month AS (
  SELECT
    customer_unique_id,
    DATE_TRUNC('month', order_purchase_timestamp)::date AS order_month
  FROM analytics.customer_orders
  GROUP BY customer_unique_id, DATE_TRUNC('month', order_purchase_timestamp)::date
)
SELECT
  o.customer_unique_id,
  f.cohort_month,
  o.order_month,
  (
    (DATE_PART('year', o.order_month::timestamp) - DATE_PART('year', f.cohort_month::timestamp)) * 12
    + (DATE_PART('month', o.order_month::timestamp) - DATE_PART('month', f.cohort_month::timestamp))
  )::int AS month_index
FROM orders_by_month o
JOIN first_purchase f
  ON o.customer_unique_id = f.customer_unique_id;

-- cohort_retention table
DROP TABLE IF EXISTS analytics.cohort_retention;

CREATE TABLE analytics.cohort_retention AS
WITH cohort_size AS (
  SELECT
    cohort_month,
    COUNT(DISTINCT customer_unique_id) AS cohort_size
  FROM analytics.customer_cohort
  GROUP BY cohort_month
),
retained AS (
  SELECT
    cohort_month,
    month_index,
    COUNT(DISTINCT customer_unique_id) AS retained_customers
  FROM analytics.customer_cohort
  GROUP BY cohort_month, month_index
)
SELECT
  r.cohort_month,
  r.month_index,
  cs.cohort_size,
  r.retained_customers,
  ROUND((r.retained_customers::numeric / NULLIF(cs.cohort_size, 0)), 4) AS retention_rate
FROM retained r
JOIN cohort_size cs
  ON r.cohort_month = cs.cohort_month
ORDER BY r.cohort_month, r.month_index;

-- Quick checks
-- sanity: month_index should start at 0 for every cohort
SELECT cohort_month, MIN(month_index) AS min_month_index
FROM analytics.cohort_retention
GROUP BY cohort_month
ORDER BY cohort_month;

-- peek at a few cohorts
SELECT *
FROM analytics.cohort_retention
ORDER BY cohort_month, month_index
LIMIT 50;

-- segment-level retention
DROP TABLE IF EXISTS analytics.cohort_retention_by_segment;

CREATE TABLE analytics.cohort_retention_by_segment AS
WITH cc AS (
  SELECT
    c.cohort_month,
    c.month_index,
    s.segment_name,
    c.customer_unique_id
  FROM analytics.customer_cohort c
  JOIN analytics.customer_segments s
    ON c.customer_unique_id = s.customer_unique_id
),
cohort_size AS (
  SELECT
    cohort_month,
    segment_name,
    COUNT(DISTINCT customer_unique_id) AS cohort_size
  FROM cc
  WHERE month_index = 0
  GROUP BY cohort_month, segment_name
),
retained AS (
  SELECT
    cohort_month,
    segment_name,
    month_index,
    COUNT(DISTINCT customer_unique_id) AS retained_customers
  FROM cc
  GROUP BY cohort_month, segment_name, month_index
)
SELECT
  r.cohort_month,
  r.segment_name,
  r.month_index,
  cs.cohort_size,
  r.retained_customers,
  ROUND((r.retained_customers::numeric / NULLIF(cs.cohort_size, 0)), 4) AS retention_rate
FROM retained r
JOIN cohort_size cs
  ON r.cohort_month = cs.cohort_month
 AND r.segment_name = cs.segment_name
ORDER BY r.cohort_month, r.segment_name, r.month_index;

-- marketing_target_list table
DROP TABLE IF EXISTS analytics.marketing_target_list;

CREATE TABLE analytics.marketing_target_list AS
WITH scored AS (
    SELECT
        c.customer_unique_id,
        c.segment_name,
        c.churn_risk,
        r.monetary,
        r.frequency,
        r.avg_order_value,
        r.tenure_days
    FROM analytics.customer_churn_scores c
    JOIN analytics.customer_rfm r
      ON c.customer_unique_id = r.customer_unique_id
),
ranked AS (
    SELECT
        *,
        NTILE(3) OVER (ORDER BY churn_risk DESC)    AS churn_band,
        NTILE(3) OVER (ORDER BY monetary DESC)      AS value_band
    FROM scored
)
SELECT
    customer_unique_id,
    segment_name,
    churn_risk,
    monetary,
    frequency,
    avg_order_value,
    tenure_days,

    CASE
        WHEN churn_band = 1 AND value_band = 1 THEN 'HIGH PRIORITY'
        WHEN churn_band = 1 AND value_band = 2 THEN 'MEDIUM PRIORITY'
        WHEN churn_band = 2 AND value_band = 1 THEN 'MEDIUM PRIORITY'
        ELSE 'LOW PRIORITY'
    END AS priority_band,

    CASE
        WHEN churn_band = 1 AND value_band = 1 THEN
            'Immediate retention offer: personalised incentive, free shipping, or VIP outreach.'
        WHEN churn_band = 1 AND value_band = 2 THEN
            'Triggered email + reminder + category-based recommendations.'
        WHEN churn_band = 2 AND value_band = 1 THEN
            'Soft engagement: loyalty points, content-based nudges.'
        ELSE
            'Low-cost, broad campaigns only.'
    END AS recommended_action
FROM ranked;


-- Verify marketing_target_list
SELECT priority_band, COUNT(*) AS customers
FROM analytics.marketing_target_list
GROUP BY priority_band
ORDER BY customers DESC;

-- inspect a few high-priority targets
SELECT *
FROM analytics.marketing_target_list
WHERE priority_band = 'HIGH PRIORITY'
ORDER BY churn_risk DESC, monetary DESC
LIMIT 10;

-- customer_time_features table
DROP TABLE IF EXISTS analytics.customer_time_features;

CREATE TABLE analytics.customer_time_features AS
WITH dataset_end AS (
    SELECT MAX(order_purchase_timestamp) AS end_ts
    FROM analytics.customer_orders
),
orders AS (
    SELECT
        customer_unique_id,
        order_purchase_timestamp,
        gross_order_value
    FROM analytics.customer_orders
),
windows AS (
    SELECT
        o.customer_unique_id,
        -- spend
        SUM(CASE WHEN o.order_purchase_timestamp >= de.end_ts - INTERVAL '30 days'
                 THEN o.gross_order_value ELSE 0 END) AS spend_30d,

        SUM(CASE WHEN o.order_purchase_timestamp >= de.end_ts - INTERVAL '90 days'
                 THEN o.gross_order_value ELSE 0 END) AS spend_90d,

        SUM(CASE WHEN o.order_purchase_timestamp >= de.end_ts - INTERVAL '180 days'
                 THEN o.gross_order_value ELSE 0 END) AS spend_180d,
        -- orders
        COUNT(CASE WHEN o.order_purchase_timestamp >= de.end_ts - INTERVAL '30 days'
                   THEN 1 END) AS orders_30d,

        COUNT(CASE WHEN o.order_purchase_timestamp >= de.end_ts - INTERVAL '90 days'
                   THEN 1 END) AS orders_90d,

        COUNT(CASE WHEN o.order_purchase_timestamp >= de.end_ts - INTERVAL '180 days'
                   THEN 1 END) AS orders_180d

    FROM orders o
    CROSS JOIN dataset_end de
    GROUP BY o.customer_unique_id
),
lifetime AS (
    SELECT
        customer_unique_id,
        COUNT(*) AS lifetime_orders,
        SUM(gross_order_value) AS lifetime_spend
    FROM analytics.customer_orders
    GROUP BY customer_unique_id
)
SELECT
    w.customer_unique_id,

    w.spend_30d,
    w.spend_90d,
    w.spend_180d,

    w.orders_30d,
    w.orders_90d,
    w.orders_180d,

    l.lifetime_orders,
    l.lifetime_spend,
    -- velocity / intensity
    CASE WHEN w.orders_180d > 0
         THEN w.spend_180d / w.orders_180d
         ELSE 0 END AS avg_order_value_180d,

    CASE WHEN l.lifetime_orders > 0
         THEN w.orders_180d::numeric / l.lifetime_orders
         ELSE 0 END AS recent_order_ratio,

    CASE WHEN l.lifetime_spend > 0
         THEN w.spend_180d / l.lifetime_spend
         ELSE 0 END AS recent_spend_ratio

FROM windows w
JOIN lifetime l
  ON w.customer_unique_id = l.customer_unique_id;

-- sanity check
SELECT
  COUNT(*) AS customers,
  AVG(spend_30d) AS avg_spend_30d,
  AVG(spend_90d) AS avg_spend_90d,
  AVG(spend_180d) AS avg_spend_180d
FROM analytics.customer_time_features;

-- sparsity check - should be mostly zeros
SELECT
  COUNT(*) FILTER (WHERE orders_30d > 0) AS active_30d,
  COUNT(*) FILTER (WHERE orders_90d > 0) AS active_90d,
  COUNT(*) FILTER (WHERE orders_180d > 0) AS active_180d
FROM analytics.customer_time_features;

# value-at-risk by priority band
SELECT
    priority_band,
    COUNT(*) AS customers,
    ROUND(SUM(value_at_risk)::numeric, 2) AS total_value_at_risk,
    ROUND(AVG(value_at_risk)::numeric, 2) AS avg_value_at_risk
FROM analytics.customer_churn_scores
GROUP BY priority_band
ORDER BY total_value_at_risk DESC;

-- New marketing_target_list with value_at_risk
SELECT *
FROM analytics.customer_churn_scores
WHERE priority_band = 'HIGH'
ORDER BY value_at_risk DESC
LIMIT 500;

-- dim_customer table for easier joins in BI tools
DROP TABLE IF EXISTS analytics.dim_customer;

CREATE TABLE analytics.dim_customer AS
SELECT
  cm.customer_unique_id,
  cm.customer_city,
  cm.customer_state,
  cm.customer_zip_code_prefix,
  cm.first_order_ts,
  cm.last_order_ts,
  cm.total_delivered_orders,
  cm.tenure_days,

  cs.segment,
  cs.segment_name,

  ccs.churned,
  ccs.churn_risk,
  ccs.value_at_risk,
  ccs.risk_decile,
  ccs.priority_band,

  ccs.frequency,
  ccs.monetary,
  ccs.avg_order_value,
  ccs.avg_items_per_order,
  ccs.avg_category_diversity,
  ccs.avg_days_between_orders,
  ccs.orders_30d,
  ccs.orders_90d,
  ccs.spend_30d,
  ccs.spend_90d
FROM analytics.customer_master cm
LEFT JOIN analytics.customer_segments cs
  ON cm.customer_unique_id = cs.customer_unique_id
LEFT JOIN analytics.customer_churn_scores ccs
  ON cm.customer_unique_id = ccs.customer_unique_id;
