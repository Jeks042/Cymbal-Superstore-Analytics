# Modelling Notes — Churn Prediction & Prioritisation

This document outlines the modelling approach, assumptions, limitations, and future improvements for the churn prediction component of the project.

---

## 1. Modelling Objective

The objective of the churn model is **customer prioritisation**, not binary churn prediction.

The model is designed to:
- rank customers by churn risk
- support value-based retention decisions
- remain interpretable for business stakeholders

---

## 2. Churn Definition

A customer is labelled as churned if:
- they have not placed an order within **180 days** of their last purchase

This threshold balances:
- typical ecommerce purchase cycles
- data coverage limits
- interpretability

---

## 3. Model Choice

**Algorithm:** Logistic Regression  
**Rationale:**
- high interpretability
- stable probability outputs
- easy calibration
- strong baseline for business decisioning

Class imbalance is handled using `class_weight="balanced"`.

---

## 4. Feature Design Principles

- No label leakage: all features precede the churn window
- Combination of lifetime and recent behaviour
- Time-windowed features capture behavioural acceleration / decay
- Heavy-tailed spend features log-transformed where appropriate

---

## 5. Model Evaluation

Primary metric:
- **ROC-AUC ≈ 0.75**

Secondary diagnostics:
- risk decile lift analysis
- churn rate monotonicity across deciles

The model shows strong ranking performance, particularly in the highest-risk deciles, which aligns with its intended use.

---

## 6. Value-Based Prioritisation

Churn probabilities are combined with customer lifetime value to compute:

value_at_risk = churn_probability × monetary_value

This enables:
- prioritisation of retention resources
- alignment of analytics with financial impact

Customers are grouped into priority bands (LOW / MEDIUM / HIGH) based on risk × value thresholds.

---

## 7. Known Limitations

- Static churn definition (does not model time-to-event explicitly)
- No treatment/control data for causal uplift modelling
- Assumes retention cost is uniform across customers
- Does not model acquisition vs retention trade-offs

These limitations are acceptable for a first production-grade implementation.

---

## 8. Recommended Next Steps

- Survival analysis (Cox or discrete-time models)
- Uplift modelling to estimate campaign impact
- Cost-aware optimisation of retention spend
- Continuous retraining and calibration monitoring
