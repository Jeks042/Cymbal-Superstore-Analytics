# Customer Churn, Retention, and Value Prioritization
**Cymbal Superstore - End-to-End Analytics Case Study**

## Project Overview
This project simulates a real-world analytics engagement for an ecommerce retailer, Cymbal Superstore. The objective is to move beyond descriptive reporting and deliver actionable customer intelligence through customer segmentation, churn risk modeling, value-based prioritization, cohort retention analysis, and executive-level Power BI storytelling.

The outcome is a decision-ready analytics system that helps stakeholders identify where churn risk is concentrated, quantify exposed revenue, and understand how retention evolves over time.

## Business Questions
- Which customers are most likely to churn?
- How much revenue is at risk due to churn?
- Which customer segments should be prioritized for retention?
- How does retention behavior change over time and across segments?

## Tech Stack
- PostgreSQL for analytics engineering and feature creation
- Python (pandas, scikit-learn) for churn modeling and scoring
- Power BI for executive dashboards and business storytelling

Heavy transformations are performed in SQL and Python, while Power BI is used as the presentation layer.

## Data Model
The analytics layer follows a clean, star-like structure:
- `dim_customer`: one row per customer
- `customer_orders_enriched`: order-level fact table
- `dim_date`: dynamically generated date dimension (fiscal year starts April 1)
- Cohort tables for retention and segment-level cohort analysis

This design keeps Power BI performant and minimizes complex DAX logic.

## Customer Segmentation
Customers are grouped into four behavioral segments:
- New Customers
- Occasional Shoppers
- Loyal Low Spend
- Champions

These segments are used consistently across churn modeling, prioritization, and retention analysis.

## Churn Modeling
### Definition
A customer is labeled as churned if they have not purchased within 180 days of their last order.

### Model
- Logistic Regression (selected for interpretability)
- Stratified train/test split
- Class imbalance handled with `class_weight="balanced"`

### Features
- Lifetime behavior (frequency, monetary value, tenure)
- Time-windowed behavior (30/90-day spend and order activity)
- Leakage prevention applied during feature design

### Performance
- ROC-AUC approximately 0.76
- Model is designed for ranking and prioritization, not strict binary classification

## Value-Based Prioritization
To translate churn risk into business impact:

`value_at_risk = churn_probability x customer_lifetime_value`

Customers are grouped into priority bands based on churn risk and customer value:
- HIGH: high churn risk and high customer value
- MEDIUM: mixed risk/value profile
- LOW: low expected revenue exposure

This ensures retention actions are focused where they produce the greatest impact.

## Retention and Cohort Analysis
Retention is analyzed with cohort methods based on each customer's first purchase month.

### Key Principles
- Retention is measured relative to cohort size
- Retained customers are not additive across months
- Cohort size defines the baseline population

### Outputs
- Retention curves over time
- Cohort heatmaps
- Segment-level weighted retention rates

These outputs reveal strong early-life drop-off and longer-term stabilization patterns.

## Power BI Dashboards
### Page 1: Executive Overview
- Total Customers
- Total Revenue
- Churn Rate
- Total Value at Risk
- Priority-based risk distribution

### Page 2: Churn and Risk Drivers
- Churn rate and risk by segment
- Value at risk by segment and priority
- Average order value by segment

### Page 3: Retention and Cohorts
- Retention trends over time
- Cohort heatmaps
- Segment-level weighted retention

## Key Insights
- Churn risk is concentrated in specific segments
- New Customers drive volume-based exposure
- Loyal Low Spend customers drive probability-based risk
- Retention is front-loaded, requiring early intervention

## Next Steps
Potential extensions include:
- Survival analysis (time-to-churn modeling)
- Uplift modeling for campaign impact
- Cost-aware retention optimization
- Acquisition versus retention trade-off analysis
