# eCommerce Lakehouse Data Pipeline

## Overview

This project implements a **Data Lakehouse** for an eCommerce platform. It demonstrates a complete data engineering workflow from ingestion to transformation and aggregation using **Python, Pandas, and Airflow**.

The pipeline is designed to handle raw order data, process it through **bronze**, **silver**, and **gold** layers, and generate analytics-ready datasets.

---

## Project Structure

```
ecom-lakehouse/
├── airflow/           # Airflow configuration and DAGs
├── bronze/            # Raw ingested data (Bronze layer)
├── silver/            # Cleaned & transformed data (Silver layer)
├── gold/              # Aggregated analytics data (Gold layer)
├── data/              # Processed datasets
├── data_raw/          # Source raw data
├── dags/              # Airflow DAG definitions
├── scripts/           # Python scripts for ETL
├── docs/              # Documentation, diagrams
├── requirements.txt   # Python dependencies
└── README.md          # Project documentation
```

---

## ETL Pipeline Layers

1. **Bronze Layer** – Raw ingestion of order data.
2. **Silver Layer** – Cleaned and transformed datasets.
3. **Gold Layer** – Aggregated and analytics-ready datasets.

---

## Airflow DAGs

* `bronze_ingest` – Ingest raw data into the Bronze layer.
* `silver_transform` – Transform and clean Bronze data into Silver layer.
* `gold_aggregate` – Aggregate Silver data to Gold layer for analytics.

---

## How to Run

1. Install Python dependencies:

```bash
pip install -r requirements.txt
```

2. Initialize Airflow:

```bash
export AIRFLOW_HOME=~/ecom-lakehouse/airflow
airflow db init
```

3. Start Airflow scheduler:

```bash
airflow scheduler
```

4. Trigger the DAG:

```bash
airflow dags trigger ecom_orders_pipeline
```

---

## Pipeline Diagram

![eCommerce Lakehouse Pipeline](docs/pipeline_diagram.png)

---

## Author

**Matthew Lawrence L**
Email: [lawrence82773824@gmail.com](mailto:lawrence82773824@gmail.com)

---

## Notes

* This project is designed for learning and demonstrating **data engineering best practices**.
* Use this project as a reference for building end-to-end ETL pipelines with **Airflow and Python**.
# ecom-lakehouse
