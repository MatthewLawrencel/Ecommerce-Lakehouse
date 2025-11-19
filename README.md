#  Ecommerce_Lakehouse
A complete end-to-end ecommerce data lakehouse built with BigQuery, dbt, and Looker Studio. Includes raw ingestion, staging, fact/dimension modeling, analytics layer, and a live dashboard with revenue trends, product performance, LTV, and order funnel insights.

##  Live Dashboard

   **Looker Studio Dashboard:**
https://lookerstudio.google.com/reporting/27d909cb-c17e-493d-bc1f-3d5aa367255b
##  Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Storage | BigQuery | Raw & analytics warehouse |
| Transformation | dbt Core | Modeling, tests, schema management |
| Cloud Auth | Google Cloud SDK | OAuth authentication for dbt |
| Visualization | Looker Studio | Interactive dashboards |
| Development | Python + Virtualenv | Isolated environment |
| File Storage | Local CSVs → BigQuery | Initial ingestion |

## Medium-Level Architecture Diagram
```bash
               ┌─────────────────────────────┐
               │ Local Raw CSV Files         │
               │ (customers, orders, items)  │
               └───────────────┬─────────────┘
                               │ Upload
                               ▼
                  ┌─────────────────────────┐
                  │     BigQuery RAW Layer  │
                  │  raw_ecommerce dataset  │
                  │  - raw_customers        │
                  │  - raw_orders           │
                  │  - raw_order_items      │
                  └───────────────┬─────────┘
                                  │ dbt sources
                                  ▼
                  ┌─────────────────────────┐
                  │   dbt: STAGING Layer    │
                  │   (Cleaned Views)       │
                  │   - stg_customers       │
                  │   - stg_orders          │
                  │   - stg_order_items     │
                  └───────────────┬─────────┘
                                  │ dbt refs
                                  ▼
                  ┌─────────────────────────┐
                  │   dbt: MARTS Layer      │
                  │   (Fact & Dimensions)   │
                  │   - dim_customers       │
                  │   - dim_products        │
                  │   - fact_orders         │
                  │   - fact_order_items    │
                  └───────────────┬─────────┘
                                  │ dbt refs
                                  ▼
                  ┌─────────────────────────┐
                  │ Analytics Layer         │
                  │ - daily_revenue         │
                  │ - product_performance   │
                  │ - customer_LTV          │
                  │ - order_funnel          │
                  └───────────────┬─────────┘
                                  │
                                  ▼
                 ┌──────────────────────────────────┐
                 │     Looker Studio Dashboard      │
                 │ Revenue • Products • LTV • Funnel│
                 └──────────────────────────────────┘


##  Project Structure
```bash
ecommerce_lakehouse/
│
├── models/
│   ├── staging/
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   └── stg_order_items.sql
│   │
│   ├── marts/
│   │   ├── dim_customers.sql
│   │   ├── dim_products.sql
│   │   ├── fact_orders.sql
│   │   └── fact_order_items.sql
│   │
│   └── analytics/
│       ├── daily_revenue.sql
│       ├── product_performance.sql
│       ├── customer_lifetime_value.sql
│       └── order_funnel.sql
│
├── seeds/  (Optional)
├── snapshots/ (Optional)
├── macros/
├── dbt_project.yml
└── README.md



##  dbt Models Overview

### 🔹 Staging Layer (Clean Raw Data)

- `stg_customers`
- `stg_orders` 
- `stg_order_items`

✔ Casts data types  
✔ Cleans columns  
✔ Standardizes naming

### 🔹 Marts Layer (Business Entities)

**Dimensions**
- `dim_customers` — unique customer master table
- `dim_products` — product catalog derived from order items

**Facts**
- `fact_orders` — order-level metrics
- `fact_order_items` — granular product-level metrics

### 🔹 Analytics Layer (Business Metrics)

- `daily_revenue` — revenue + orders per day
- `product_performance` — units sold & revenue by product
- `customer_lifetime_value` — total spend & order count
- `order_funnel` — order status distribution

## Dashboard Pages (Looker Studio)

###   Revenue Overview
- Daily Revenue (line chart)
- Total Orders (scorecard)
- Total Revenue (scorecard)

###  Product Performance
- Top Products by Revenue (table)
- Revenue by Category (bar chart)

###  Customer Insights
- Customer LTV (table)
- Avg LTV (scorecard)
- Total LTV (scorecard)

###  Order Funnel
- Delivered / Cancelled / Returned distribution (donut/pie)
- Status breakdown (table)

## ⚙️ Setup Instructions

### Clone repo & create venv
```bash
python3 -m venv venv
source venv/bin/activate

### Install dbt BigQuery
```bash
pip install dbt-bigquery

### Configure your profiles.yml
Located at:
~/.dbt/profiles.yml

Example:
```bash
ecommerce_lakehouse:
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: ecommerce-lakehouse
      dataset: analytics_ecommerce
      threads: 4
      location: asia-south1
  target: dev

### Run dbt
```bash
dbt debug
dbt run
dbt test

## Author
   **Matthew Lawrence L**
     Data Engineer
     Bengaluru,Karnataka,India

