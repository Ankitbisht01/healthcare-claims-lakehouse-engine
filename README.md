# Healthcare Claims Data Lakehouse 

## Project Objective
This project implements an end-to-end Medallion Architecture (Bronze, Silver, Gold) using Azure Databricks and PySpark. The pipeline is designed to ingest disparate healthcare claims data (Clinic, Hospital, and EDI-837 formats), harmonize them into a unified schema, and aggregate the data into business-ready reporting tables. The workflow concludes with an automated email alert system to deliver daily financial metrics to stakeholders.

## Technology Stack
* **Compute & Orchestration:** Azure Databricks, Databricks Workflows
* **Storage Framework:** Azure Data Lake Storage (ADLS Gen2), Delta Lake
* **Data Processing:** Python, PySpark, Spark SQL
* **Security:** Databricks Secret Scopes
* **Version Control:** Git, GitHub (via Databricks Git Folders)

## Architecture Overview


### 1. Bronze Layer (Raw Ingestion)
* **Objective:** Extract raw, unstructured, and semi-structured data from the Azure landing zone and persist it as Delta tables.
* **Sources:** 
  * Clinic Claims (CSV)
  * Hospital Claims (Parquet)
  * EDI-837 Claims (Nested JSON)
* **Script:** `01_ingest_bronze.py`

### 2. Silver Layer (Harmonization & Deduplication)
* **Objective:** Cleanse, flatten, and unify the disparate data sources into a single, query-ready table.
* **Transformations:**
  * Flattens deeply nested JSON structures using dot-notation.
  * Standardizes column names (e.g., `hospital_claim_id` and `claim_id` mapped to `unified_claim_id`).
  * Enforces data type casting (String to DateType/DoubleType).
  * Executes idempotent Upserts (`MERGE`) to prevent duplicate records on pipeline reruns.
* **Script:** `02_transform_silver.py`

## Architecture diagram

```mermaid
flowchart LR
    subgraph ADLS [Azure Data Lake Storage]
        direction TB
        C[Clinic CSV]
        H[Hospital Parquet]
        E[EDI-837 JSON]
    end

    subgraph Databricks [Databricks Workflows]
        subgraph Bronze [Bronze: Raw]
            direction TB
            BC[(Bronze Clinic)]
            BH[(Bronze Hospital)]
            BE[(Bronze EDI)]
        end

        subgraph Silver [Silver: Harmonized]
            SU[(Unified Claims Table)]
        end

        subgraph Gold [Gold: Aggregated]
            direction TB
            GF[(Daily Financial)]
            GP[(Patient Summary)]
        end
    end

    subgraph Output [Downstream]
        direction TB
        SMTP[Automated Email]
        BI[Power BI / Tableau]
    end

    %% Ingestion
    C -->|01_ingest| BC
    H -->|01_ingest| BH
    E -->|01_ingest| BE

    %% Transformation & Upsert
    BC -->|02_transform| SU
    BH -->|02_transform| SU
    BE -->|02_transform| SU

    %% Aggregation
    SU -->|03_aggregate| GF
    SU -->|03_aggregate| GP

    %% Consumption
    GF -->|04_email_alerts| SMTP
    GF -.-> BI
    GP -.-> BI
    
    style SU fill:#e1f5fe,stroke:#0288d1
    style GF fill:#fff8e1,stroke:#ffa000
    style GP fill:#fff8e1,stroke:#ffa000
    style BC fill:#efebe9,stroke:#5d4037
    style BH fill:#efebe9,stroke:#5d4037
    style BE fill:#efebe9,stroke:#5d4037

### 3. Gold Layer (Business Aggregations)
* **Objective:** Generate analytics-ready dimensional and fact tables for downstream BI consumption.
* **Outputs:**
  * **Daily Financial Summary:** Aggregates total claim volumes and revenue by source system and date.
  * **Patient Lifetime Summary:** Calculates total visits, lifetime spend, and retention dates per patient.
* **Script:** `03_aggregate_gold.py`

### 4. Automated Alerting
* **Objective:** Proactively push insights to stakeholders upon successful pipeline execution.
* **Mechanism:** Queries the Gold layer for the latest daily metrics and dispatches an HTML-formatted email via Gmail SMTP. Credentials are authenticated securely via Databricks Secrets.
* **Script:** `04_email_alerts.py`

## Orchestration
The entire pipeline is orchestrated via **Databricks Workflows**. The jobs are chained sequentially (`01` -> `02` -> `03` -> `04`) establishing a strict dependency graph. If a downstream task fails (e.g., a corrupted file in the Bronze layer), the pipeline halts to prevent bad data from polluting the Gold reporting tables.

## How to Run
1. Clone this repository into your Databricks workspace using Git Folders.
2. Ensure your Azure Storage configurations are passed via `argparse` or set as default parameters in the scripts.
3. Configure your SMTP credentials in the Databricks CLI:
   `databricks secrets put-secret smtp_creds email_address`
   `databricks secrets put-secret smtp_creds app_password`
4. Execute the workflow via the Databricks Jobs UI.