-- dim_date table for easier time-based analysis in BI tools
DROP TABLE IF EXISTS analytics.dim_date;

CREATE TABLE analytics.dim_date AS
WITH bounds AS (
    SELECT
        MIN(order_purchase_timestamp)::date AS min_date,
        MAX(order_purchase_timestamp)::date AS max_date
    FROM analytics.customer_orders_enriched
),
dates AS (
    SELECT
        generate_series(min_date, max_date, interval '1 day')::date AS date
    FROM bounds
)
SELECT
    d.date,
     -- ISO calendar attributes
    -- Calendar attributes
    EXTRACT(YEAR FROM d.date)::int  AS year,
    EXTRACT(MONTH FROM d.date)::int AS month,
    TO_CHAR(d.date, 'Mon')          AS month_name_short,
    TO_CHAR(d.date, 'Month')        AS month_name,
    DATE_TRUNC('month', d.date)::date AS month_start,
    (DATE_TRUNC('month', d.date) + INTERVAL '1 month - 1 day')::date AS month_end,

    EXTRACT(QUARTER FROM d.date)::int AS quarter,
    DATE_TRUNC('quarter', d.date)::date AS quarter_start,
    (DATE_TRUNC('quarter', d.date) + INTERVAL '3 months - 1 day')::date AS quarter_end,

    EXTRACT(ISODOW FROM d.date)::int AS iso_day_of_week,   -- 1=Mon .. 7=Sun
    TO_CHAR(d.date, 'Dy')            AS day_name_short,
    TO_CHAR(d.date, 'Day')           AS day_name,
    EXTRACT(DAY FROM d.date)::int    AS day_of_month,

    EXTRACT(WEEK FROM d.date)::int   AS week_of_year,
    EXTRACT(ISOYEAR FROM d.date)::int AS iso_year,
    -- Fiscal attributes (FY starts 1 April)
    CASE
        WHEN EXTRACT(MONTH FROM d.date)::int >= 4 THEN EXTRACT(YEAR FROM d.date)::int
        ELSE EXTRACT(YEAR FROM d.date)::int - 1
    END AS fiscal_year_start,
    -- Label like FY2017/18 (optional but nice)
    CASE
        WHEN EXTRACT(MONTH FROM d.date)::int >= 4
            THEN 'FY' || EXTRACT(YEAR FROM d.date)::int || '/' || RIGHT((EXTRACT(YEAR FROM d.date)::int + 1)::text, 2)
        ELSE
            'FY' || (EXTRACT(YEAR FROM d.date)::int - 1) || '/' || RIGHT(EXTRACT(YEAR FROM d.date)::int::text, 2)
    END AS fiscal_year_label,
    -- Fiscal month: Apr=1 ... Mar=12
    CASE
        WHEN EXTRACT(MONTH FROM d.date)::int >= 4 THEN EXTRACT(MONTH FROM d.date)::int - 3
        ELSE EXTRACT(MONTH FROM d.date)::int + 9
    END AS fiscal_month,
    -- Fiscal quarter: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
    CASE
        WHEN EXTRACT(MONTH FROM d.date)::int BETWEEN 4 AND 6  THEN 1
        WHEN EXTRACT(MONTH FROM d.date)::int BETWEEN 7 AND 9  THEN 2
        WHEN EXTRACT(MONTH FROM d.date)::int BETWEEN 10 AND 12 THEN 3
        ELSE 4
    END AS fiscal_quarter,
    -- Convenience flags
    (d.date = DATE_TRUNC('month', d.date)::date) AS is_month_start,
    (d.date = (DATE_TRUNC('month', d.date) + INTERVAL '1 month - 1 day')::date) AS is_month_end

FROM dates d;

-- index for faster date joins
CREATE INDEX IF NOT EXISTS idx_dim_date_date 
ON analytics.dim_date(date);

-- quick check
SELECT COUNT(*) AS total_dates, MIN(date) AS min_date, MAX(date) AS max_date
FROM analytics.dim_date;