# 🛒 VitaShop — Customer Lifecycle Analytics

> **End-to-end BigQuery pipeline for multi-dimensional customer lifecycle classification using a 24-month rolling window.**

---

## Overview

This project implements a production-ready customer analytics pipeline for a health e-commerce platform. The goal is to track *how customers evolve over time* across five lifecycle states — **new, recurring, inactive, churned, and recovered** — computed independently for multiple business dimensions simultaneously.

The pipeline processes data from January 2022 onwards and is designed to backfill historical months automatically via a procedural SQL loop, producing a fully partitioned analytical table ready for dashboarding.

---

## Business Problem

A common failure in customer analytics is treating lifecycle classification as a static label. A customer who buys once and disappears looks identical to one who bought regularly and then stopped — but they require completely different retention strategies.

Additionally, a customer can be *recurring* at the brand level but *new* to a specific product category, or *churned* from home delivery but still active in-store. Flattening these dimensions into a single label destroys actionable signal.

This pipeline solves both problems:

1. **Temporal classification** — state is recomputed every month, reflecting actual behavior evolution.
2. **Multi-dimensional classification** — five independent lifecycle labels per customer per month (overall, brand, payment method, product type, fulfillment modality).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                            │
│  fct_orders_vitashop  │  fct_order_lines  │  GA4 Attribution    │
│  CRM Interactions     │                                         │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  01_tb_dimensions_       │
                    │  customer_orders         │
                    │                          │
                    │  • GA4 channel join       │
                    │  • Order origin class.   │
                    │  • Age bracket           │
                    │  • Partitioned by date   │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  02_tb_temporal_         │
                    │  customer_classification │
                    │                          │
                    │  • WHILE loop (monthly)  │
                    │  • 24-month rolling win. │
                    │  • 5-state lifecycle     │
                    │  • 5 dimensions          │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  BI / Dashboard Layer    │
                    │  (Power BI)              │
                    └─────────────────────────┘
```

---

## Lifecycle Classification Logic

The same 5-state logic is applied independently across all dimensions:

| State | Condition |
|---|---|
| **new** | No purchase history at all; first order this month |
| **recurring** | 2+ orders across the rolling 23 months + current month |
| **inactive** | Exactly 1 order in the last 23 months; nothing this month |
| **churned** | Had prior history (>24 months ago); zero orders in 23m + current month |
| **recovered** | Had prior history (>24 months ago); skipped 23 months; purchased this month |

### Rolling Window Definition

For each analysis month *M*:

```
|← older history →|←────── 23 rolling months ──────→|← M →|
[before window_start]     [window_start .. M-1]       [M_start .. M_end]
    total_hist                 total_roll               total_curr
```

The 24-month window ensures churn is detected with enough granularity without requiring infinite history scans on every run.

---

## Dimensions

| Dimension | Field | Description |
|---|---|---|
| Overall | `lifecycle_overall` | Cross-brand, all purchases |
| Brand | `lifecycle_brand` | Per retail brand |
| Payment | `lifecycle_payment` | Per payment method (card, wallet, health plan) |
| Product | `lifecycle_product` | Per product type (supplement, device, service) |
| Modality | `lifecycle_modality` | Per fulfillment type (in-store vs. home delivery) |

**Example insight:** A customer classified as `recurring` overall may be `new` in the Device category — a cross-sell opportunity that would be invisible with a single lifecycle label.

---

## SQL Files

```
sql/
├── 01_tb_dimensions_customer_orders.sql
│       Enriched order-line fact table. Joins GA4 attribution,
│       classifies booking origin, computes age brackets.
│       Partitioned by order_date.
│
└── 02_tb_temporal_customer_classification.sql
        Procedural loop (WHILE) that iterates month by month,
        computes 24-month rolling window metrics with BigQuery
        window functions, applies lifecycle classification,
        and inserts one month's worth of records per iteration.
```

---

## Synthetic Data

The `data/` folder contains a Python script that generates realistic fake order data matching the pipeline's schema — useful for demos, unit testing, and local development.

```bash
# Install dependency (standard library only, no pip needed)
python data/generate_synthetic_data.py
# → Creates data/sample_orders.csv  (~2 700 rows, 800 customers)
```

The generated dataset includes:
- 800 unique customers with randomized demographics
- Orders spanning 2022–2025 with realistic frequency distributions
- 3 brands, 5 regions, 30 stores, 6 product categories
- Digital + call-center + in-store booking origins with GA4 channel attribution

---

## Key Technical Highlights

**BigQuery procedural SQL (WHILE loop for backfill)**
Rather than running a single query over all history (expensive and hard to debug), the pipeline processes one month at a time. This pattern enables incremental loads in production: just run the loop for the current month instead of rebuilding the entire table.

**Window functions with conditional COUNT DISTINCT**
Each rolling metric is computed as a `COUNT(DISTINCT CASE WHEN ... END) OVER (PARTITION BY ...)` — avoiding self-joins and keeping the logic declarative and readable.

**Multi-dimensional classification without duplication**
Five lifecycle labels are computed in the same pass over the data by partitioning the window functions along different keys. No repeated scans, no temp tables per dimension.

**GA4 last-touch attribution join**
A `QUALIFY ROW_NUMBER() = 1` deduplication ensures exactly one GA4 session is attributed per customer per day before joining to orders, preventing fan-out on the order table.

**Partition pruning**
Both source and output tables are partitioned by date, ensuring each monthly loop iteration only scans the relevant data slice.

---

## Example Queries

### Monthly lifecycle distribution

```sql
SELECT
  analysis_month,
  lifecycle_overall,
  COUNT(DISTINCT customer_cpf) AS customers,
  ROUND(SUM(revenue), 2)       AS total_revenue
FROM `prj-analytics.vitashop.tb_temporal_customer_classification`
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Cross-sell signal: recurring overall but new in product category

```sql
SELECT
  analysis_month,
  COUNT(DISTINCT customer_cpf) AS cross_sell_candidates
FROM `prj-analytics.vitashop.tb_temporal_customer_classification`
WHERE
  lifecycle_overall = 'recurring'
  AND lifecycle_product = 'new'
GROUP BY 1
ORDER BY 1;
```

### Churn rate by brand

```sql
SELECT
  analysis_month,
  brand,
  COUNTIF(lifecycle_brand = 'churned') AS churned_customers,
  COUNT(DISTINCT customer_cpf)          AS total_active,
  ROUND(
    COUNTIF(lifecycle_brand = 'churned') /
    NULLIF(COUNT(DISTINCT customer_cpf), 0) * 100, 2
  )                                     AS churn_rate_pct
FROM `prj-analytics.vitashop.tb_temporal_customer_classification`
GROUP BY 1, 2
ORDER BY 1, 2;
```

---

## Stack

| Layer | Technology |
|---|---|
| Data Warehouse | Google BigQuery |
| Transformation | BigQuery Procedural SQL (WHILE loop) |
| Web Attribution | GA4 → BigQuery Export |
| Orchestration | Scheduled Query / dbt / Airflow (pluggable) |
| Visualization | Looker Studio / Power BI / Tableau |
| Synthetic Data | Python (stdlib only) |

---

## Repository Structure

```
vitashop-customer-lifecycle/
├── sql/
│   ├── 01_tb_dimensions_customer_orders.sql
│   └── 02_tb_temporal_customer_classification.sql
├── data/
│   ├── generate_synthetic_data.py
│   └── sample_orders.csv
├── docs/
│   └── lifecycle_states_diagram.png  (coming soon)
└── README.md
```

---

## About

Built as part of a data engineering & analytics portfolio. This project demonstrates design patterns for customer lifecycle modeling applicable to any subscription or repeat-purchase business — e-commerce, health services, SaaS, and beyond.

The domain (VitaShop, a fictional health e-commerce) and all data are synthetic. The analytical logic and architecture patterns are original.

---

*Questions or feedback? Open an issue or reach out on [LinkedIn](https://www.linkedin.com/in/pedro-augusto-camargo-de-oliveira/).*
