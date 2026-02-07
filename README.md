# 📊 Customer Churn, Retention & Value Prioritisation  
**Cymbal Superstore — End-to-End Analytics Case Study**

---

## 🔍 Project Overview

This project simulates a real-world analytics engagement for an ecommerce retailer, **Cymbal Superstore**.  
The goal was to move beyond descriptive reporting and deliver **actionable customer intelligence** by combining:

- Customer segmentation  
- Churn risk modelling  
- Value-based prioritisation  
- Cohort retention analysis  
- Executive-level Power BI storytelling  

The final output is a **decision-ready analytics system** that helps stakeholders understand:
- where churn risk is concentrated  
- how much revenue is exposed  
- how retention evolves over time  

---

## 🎯 Business Questions

- Which customers are most likely to churn?  
- How much revenue is at risk due to churn?  
- Which customer segments should be prioritised for retention?  
- How does retention behaviour change over time and across segments?  

---

## 🧰 Tech Stack

- **PostgreSQL** – analytics engineering & feature creation  
- **Python (pandas, scikit-learn)** – churn modelling & scoring  
- **Power BI** – executive dashboards & business storytelling  

All heavy transformations were performed in SQL and Python, with Power BI used strictly as the **presentation layer**.

---

## 🗄️ Data Model

The analytics layer follows a clean, star-like structure:

- **dim_customer** – one row per customer  
- **customer_orders_enriched** – order-level fact table  
- **dim_date** – dynamically generated date dimension  
  - fiscal year starts **1 April**  
- **Cohort tables** – retention and segment-level cohort analysis  

This design keeps Power BI performant and avoids complex DAX.

---

## 👥 Customer Segmentation

Customers are grouped into four behavioural segments:

- **New Customers**  
- **Occasional Shoppers**  
- **Loyal Low Spend**  
- **Champions**  

Segments are used consistently across churn modelling, prioritisation, and retention analysis.

---

## 🤖 Churn Modelling

### Definition
A customer is labelled as **churned** if they have not made a purchase within **180 days** of their last order.

### Model
- Logistic Regression (chosen for interpretability)  
- Stratified train/test split  
- Class imbalance handled using `class_weight="balanced"`  

### Features
- Lifetime behaviour (frequency, monetary value, tenure)  
- Time-windowed behaviour (30 / 90-day spend and order activity)  
- No label leakage  

### Performance
- **ROC-AUC ≈ 0.76**  
- Designed for **ranking and prioritisation**, not binary prediction  

---

## 💰 Value-Based Prioritisation

To translate churn risk into business impact:

value_at_risk = churn_probability × customer_lifetime_value

Customers are grouped into **priority bands** based on churn risk × customer value:

- **HIGH** – high churn risk × high customer value  
- **MEDIUM** – mixed risk/value  
- **LOW** – low expected revenue exposure  

This ensures retention goals are focused where they matter most.

---

## 🔁 Retention & Cohort Analysis

Retention is analysed using **cohort methods** based on customers’ first purchase month.

### Key principles
- Retention is relative to cohort size  
- Retained customers are **not additive across months**  
- Cohort size defines the baseline population  

### Outputs
- Retention curves over time  
- Cohort heatmaps  
- Segment-level weighted retention rates  

This highlights strong early-life drop-off and long-term stabilisation patterns.

---

## 📈 Power BI Dashboards

### Page 1 – Executive Overview
- Total Customers  
- Total Revenue  
- Churn Rate  
- Total Value at Risk  
- Priority-based risk distribution  

### Page 2 – Churn & Risk Drivers
- Churn rate and risk by segment  
- Value at risk by segment and priority  
- Average order value by segment  

### Page 3 – Retention & Cohorts
- Retention trends over time  
- Cohort heatmaps  
- Segment-level weighted retention  

---

## 💡 Key Insights

- Churn risk is highly concentrated in specific segments  
- New Customers drive **volume-based exposure**  
- Loyal Low Spend customers drive **probability-based risk**  
- Retention is a **front-loaded problem**, requiring early intervention  

---

## 🚀 Next Steps

Potential extensions include:

- Survival analysis (time-to-churn modelling)  
- Uplift modelling for campaign impact  
- Cost-aware retention optimisation  
- Acquisition vs retention trade-off analysis  

