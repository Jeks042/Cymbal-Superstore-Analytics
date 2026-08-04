# Customer Churn, Retention and Value Prioritisation

**Cymbal Superstore — simulated end-to-end analytics case study**

> **Portfolio disclosure:** Cymbal Superstore is a simulated business scenario. The project is intended to demonstrate analytical reasoning, data modelling and decision support. Any commercial impact discussed is scenario-based rather than a realised business result.

## Executive summary

This project develops a decision-support workflow for an e-commerce retention team. It combines PostgreSQL, Python and Power BI to identify customers showing elevated churn risk, examine how retention changes across cohorts and prioritise customer groups by both risk and potential value.

The project is designed to answer four business questions:

1. Which customers appear most at risk of becoming inactive?
2. Where is the greatest potential value exposure?
3. Which customer groups should receive retention attention first?
4. How does repeat purchasing change over time and across cohorts?

## Business context

The analysis assumes that the retention team has limited capacity. The goal is therefore not to classify every customer perfectly. It is to produce a ranked, explainable view of risk that can support prioritisation and further testing.

## Analytical workflow

### 1. Data preparation and modelling

- PostgreSQL for transformation, feature creation and reusable analytical tables
- A customer dimension with one record per customer
- An enriched order-level fact table
- A dynamic date dimension using an April-to-March fiscal year
- Cohort tables for retention and segment-level analysis

### 2. Customer segmentation

Customers are grouped into four behavioural segments:

- New Customers
- Occasional Shoppers
- Loyal Low Spend
- Champions

The same segment definitions are used across churn, value and cohort reporting to keep the analysis consistent.

### 3. Churn-risk model

A customer is provisionally labelled inactive when no purchase has occurred within 180 days of the last order. Logistic regression is used because its outputs are interpretable and suitable for ranking customers.

Current features include:

- Purchase frequency
- Historical monetary value
- Customer tenure
- Recent 30-day and 90-day activity
- Recent spend and order behaviour

The current model records ROC-AUC of approximately 0.76. This is treated as an initial ranking result rather than proof that the model is ready for operational deployment.

### 4. Value prioritisation

The current prioritisation measure is:

```text
value_at_risk = estimated_churn_probability × estimated_customer_value
```

This is a screening measure, not a complete estimate of recoverable value. A production decision should also consider contribution margin, contact cost, incentive cost and the probability that an intervention would change the outcome.

### 5. Cohort retention

Retention is measured from each customer's first-purchase month. The outputs include:

- Retention curves
- Cohort heatmaps
- Weighted segment-level retention
- Early-life drop-off and later stabilisation patterns

## Power BI report

### Page 1 — Executive overview

- Total customers
- Total revenue
- Inactivity rate
- Estimated value at risk
- Priority-band distribution

### Page 2 — Risk and customer value

- Risk by customer segment
- Value exposure by segment and priority
- Average order value by segment
- Customer-level prioritisation

### Page 3 — Retention and cohorts

- Retention over time
- Cohort heatmap
- Weighted segment retention
- New-customer drop-off

## Current findings

The initial analysis suggests that:

- Risk and value exposure are not concentrated in exactly the same customer groups.
- New customers create substantial volume-based exposure because many have not yet established repeat behaviour.
- Some lower-spend loyal customers show higher modelled risk but lower individual value exposure.
- The strongest retention loss appears early in the customer lifecycle, indicating that onboarding and early repeat-purchase behaviour deserve separate investigation.

These findings are directional until the sensitivity and validation work below is completed.

## Limitations

- The scenario is simulated and should not be interpreted as a live commercial deployment.
- The 180-day inactivity rule is a working assumption and requires sensitivity testing against alternative windows.
- The current stratified random train/test split does not fully represent future-period performance.
- ROC-AUC alone is insufficient for selecting an intervention threshold.
- Customer lifetime value is simplified and does not yet use contribution margin or uncertainty ranges.
- Transactional data without treatment assignment cannot establish that a retention campaign would cause customers to stay.

## Validation and improvement plan

The next iteration will add:

1. A clearly defined observation date and prediction window
2. Time-based training and validation periods
3. Sensitivity tests for 90-day, 180-day and 270-day inactivity definitions
4. A simple behavioural baseline for comparison
5. Precision-recall, PR-AUC, lift and calibration analysis
6. Precision at realistic contact-capacity thresholds
7. Cost and contribution-margin scenarios
8. Transparent segment and priority thresholds
9. Dashboard screenshots and a one-page executive decision memo

Uplift modelling will not be added to this dataset unless valid treatment and control data become available. That question is better addressed in a dedicated experimentation case study.

## Repository structure

- `sql/` — data preparation, analytical tables and feature queries
- `python/` — modelling, scoring and validation
- `data/` — project data and supporting outputs
- `data visualisation/power_bi/` — Power BI report materials
- `docs/` — project documentation

## Technology

`PostgreSQL` · `Python` · `pandas` · `scikit-learn` · `Power BI`

## Reproducibility

Install the Python dependencies from `requirements.txt`, review the SQL scripts in execution order and use the generated analytical outputs as the Power BI presentation layer. No result should be treated as production-ready without completing the validation plan above.
