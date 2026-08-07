# Customer Churn, Retention and Value Prioritisation

**Cymbal Superstore — simulated customer-retention analytics case study**

> **Disclosure:** Cymbal Superstore is a simulated business scenario. The analysis demonstrates customer analytics, decision support and model governance; commercial impact is illustrative rather than a realised business result.

## Executive decision

**Use churn risk and customer value together to prioritise retention review, with particular attention to early-life customers and high-value accounts. Do not treat the current churn score as evidence that an intervention will prevent churn.**

The analysis combines PostgreSQL, Python and Power BI to identify customers showing elevated inactivity risk, estimate value exposure and examine how repeat purchasing changes across cohorts.

The central business implication is that **risk and value are not concentrated in the same customer groups**. A retention team with limited capacity should therefore avoid prioritising customers on churn probability alone.

## Decision evidence

| Question | Finding | Decision implication |
|---|---|---|
| Which customers appear most at risk? | A logistic-regression model using purchase frequency, historical spend, tenure and recent activity achieved ROC-AUC of approximately **0.76** on the current validation design. | Use the score for relative risk ranking, not as a production deployment claim. |
| Where is value exposure concentrated? | High churn probability does not always coincide with high customer value. | Prioritisation should combine risk and value rather than using either metric independently. |
| Where does retention weaken most? | The strongest loss in repeat purchasing appears early in the customer lifecycle. | Onboarding and early repeat-purchase behaviour deserve focused retention analysis. |
| Should the model determine who receives an intervention? | No treatment/control data are available to estimate preventable churn or campaign lift. | The model supports screening and prioritisation only; intervention effectiveness requires a separate experiment. |

## Business context

The scenario assumes a retention team with limited contact capacity. The decision problem is therefore not to classify every customer perfectly, but to identify where analytical attention and retention resources are most likely to matter.

The workflow answers four questions:

1. Which customers show the strongest signs of becoming inactive?
2. Which customers represent the greatest potential value exposure?
3. Which customer groups should be reviewed first when retention capacity is limited?
4. How does repeat purchasing change over time and across acquisition cohorts?

## Customer-risk framework

A customer is labelled inactive when no purchase has occurred within **180 days** of the last order. That threshold is a modelling assumption rather than a universal definition of churn.

The logistic-regression model uses interpretable pre-outcome behavioural features including:

- purchase frequency;
- historical monetary value;
- customer tenure;
- recent 30-day and 90-day activity; and
- recent spend and order behaviour.

The model's ROC-AUC of approximately **0.76** indicates useful ranking ability under the current validation setup. It does **not** establish future-period performance, treatment responsiveness or operational profitability.

## Value prioritisation

Customer priority is informed by the screening measure:

```text
value_at_risk = estimated_churn_probability × estimated_customer_value
```

This measure helps distinguish customers who are high risk from customers whose potential value exposure is materially larger.

It is intentionally treated as a **prioritisation proxy**, not an estimate of recoverable value. A live retention decision would also require contribution margin, contact cost, incentive cost and evidence that an intervention changes customer behaviour.

## Retention and cohort findings

Retention is measured from each customer's first-purchase month and reviewed through cohort curves, heatmaps and segment-level summaries.

The main patterns are:

- **Early-life retention is the principal weakness.** The largest loss in repeat purchasing occurs soon after acquisition, suggesting that first-to-second purchase behaviour is a distinct problem from later-life churn.
- **New customers create material volume exposure.** Their individual value may vary, but their scale makes weak early retention commercially relevant.
- **Risk and value require separate interpretation.** Some lower-spend loyal customers show elevated modelled risk without carrying the highest individual value exposure.
- **Customer segments behave differently over time.** Cohort analysis adds context that a single churn probability cannot provide.

These findings support prioritisation and further testing; they do not establish the causal effect of a retention intervention.

## Customer segmentation

Customers are grouped into four behavioural segments used consistently across churn, value and retention reporting:

- New Customers
- Occasional Shoppers
- Loyal Low Spend
- Champions

Using one segment definition across the analytical workflow keeps risk, value and cohort views comparable for stakeholders.

## Power BI decision report

The Power BI report is organised around the retention decision rather than the modelling workflow.

### Executive overview

- customer population and revenue context;
- inactivity rate;
- estimated value at risk; and
- distribution of customer priority bands.

### Risk and customer value

- churn risk by customer segment;
- value exposure by segment and priority;
- average order value by segment; and
- customer-level prioritisation.

### Retention and cohorts

- retention curves over customer age;
- cohort heatmap;
- weighted segment retention; and
- early-life customer drop-off.

## Analytical controls and decision boundaries

Several controls prevent the model from being presented as more actionable than the evidence supports:

- The 180-day inactivity definition is explicitly treated as an assumption.
- The current train/test design is a stratified random split, so results are not presented as proof of future-period stability.
- ROC-AUC is used as a ranking diagnostic rather than the sole basis for operational threshold selection.
- `value_at_risk` is a screening proxy rather than recoverable customer value.
- No claim is made that high-risk customers are necessarily persuadable to stay.
- No uplift or incremental-retention claim is made because the dataset does not contain valid treatment and control assignment.

## What would be required before operational deployment

A production retention policy would require stronger prospective validation, including:

- a fixed observation date and prediction window;
- time-based model validation;
- sensitivity testing of the inactivity definition;
- calibration, precision-recall and lift at realistic contact capacities;
- contribution-margin and intervention-cost assumptions; and
- direct experimental evidence that the selected retention action creates incremental value.

The appropriate next step for intervention effectiveness is a controlled experiment, not a more complex churn model on the same observational data.

## Repository structure

- `sql/` — analytical tables, customer features and cohort preparation
- `python/` — modelling, scoring and validation
- `data/` — project data and analytical outputs
- `data visualisation/power_bi/` — Power BI reporting materials
- `docs/` — supporting project documentation

## Technology

`PostgreSQL` · `Python` · `pandas` · `scikit-learn` · `Power BI`

## Analytical position

The project demonstrates a practical distinction that matters in retention analytics:

**A churn model estimates who is more likely to become inactive. It does not establish who can be persuaded to stay, which intervention will work, or whether that intervention will be profitable.**

The completed analysis therefore uses the model for customer-risk prioritisation while keeping causal intervention decisions outside the claims supported by the data.
