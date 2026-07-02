# Banking Transaction Monitoring Pipeline (AML)

## Overview

This project demonstrates an end-to-end Anti-Money Laundering (AML) transaction monitoring solution built using Azure Data Factory, Azure Storage, Databricks (PySpark), and Power BI.

The solution simulates how financial institutions identify suspicious activities, high-risk customers, unusual transaction patterns, and high-risk business relationships.

---

## Business Problem

Financial institutions must monitor customer transactions and identify suspicious activities that may indicate:

* Money Laundering
* Terrorist Financing
* Structuring / Smurfing
* High-Risk International Transfers
* Unusual Customer Behaviour

The objective of this project is to automate AML monitoring and generate alerts for investigation teams.

---

## Technology Stack

### Data Integration

* Azure Data Factory (ADF)

### Data Processing

* Azure Databricks
* PySpark
* Python

### Storage

* Azure Blob Storage (ADLS Gen2 compatible structure)

### Visualization

* Power BI

### Source Control

* GitHub

---

## Architecture

```text
Raw Data
    ↓
Azure Storage (Raw Layer)
    ↓
ADF Orchestration
    ↓
Databricks Transformations
    ↓
Silver Layer
    ↓
Risk Scoring
    ↓
Gold Layer
    ↓
AML Alerts
    ↓
Power BI Dashboard
```

---

## Project Structure

```text
banking-transaction-monitoring-pipeline

├── azure-v2
│   ├── adf
│   ├── databricks
│   ├── powerbi
│   └── screenshot
│
├── scripts
├── README.md
└── .gitignore
```

---

## AML Monitoring Components

### 1. Customer Risk Assessment

Validates and evaluates customer information:

* Email validation
* Domain validation
* High-risk country identification
* Incomplete KYC detection

Output:

```text
customer_risk_assessment.csv
```

---

### 2. Customer Risk Scoring

Generates customer risk scores.

Risk Levels:

* LOW
* MEDIUM
* HIGH

Output:

```text
customer_risk_score.csv
```

---

### 3. Transaction Risk Monitoring

Identifies:

* High-value transactions
* High-risk country transfers
* Suspicious wire transfers

Output:

```text
flagged_data.csv
```

---

### 4. Business Relationship Risk Analysis

Analyzes:

* Counterparty concentration
* Wire transfer frequency
* High-value transaction volume

Output:

```text
high_risk_relationship_alert.csv
```

---

### 5. Rolling 24-Hour Monitoring

Detects customers exceeding AML thresholds within a rolling 24-hour period.

Calculated Metrics:

* Rolling Transaction Count
* Rolling Transaction Amount
* AML Alert Flag

Output:

```text
rolling_24h_transactions.csv
```

---

## Databricks Notebooks

* 01_kyc_customer_risk_scoring
* 02_account_profile_transform
* 03_transaction_risk_scoring
* 04_relationship_risk_scoring
* 99_error_logging
* NB_98_PIPELINE_RUN_LOGGING

---

## ADF Pipelines

* PL_01_KYC_CUSTOMER_RISK_SCORING
* PL_02_ACCOUNT_PROFILE_TRANSFORM
* PL_03_TRANSACTION_RISK_SCORING
* PL_04_RELATIONSHIP_RISK_SCORING
* 05_rolling_24h_transaction_monitoring.ipynb
* PL_98_PIPELINE_RUN_LOGGING
* PL_99_ERROR_LOGGING
* PL_AML_MASTER_PIPELINE

---

## Power BI Dashboards

### Customer Investigation

Customer risk review and AML alert analysis.

### Transaction Monitoring

Suspicious transaction monitoring and threshold tracking.

### Relationship Analysis

High-risk business relationship identification.

### AML Investigation Dashboard

Centralized AML alert investigation dashboard.

---

## Security

Sensitive information is stored using Databricks Secret Scope.

No credentials, access keys, or connection strings are stored in GitHub.

---

## Future Enhancements

* Real-time streaming detection
* Machine Learning anomaly detection
* FINTRAC reporting automation
* Customer behavioural profiling
* Sanctions screening integration

---

## Author

Azam Haddadi

Senior Business Intelligence / Data Engineering Portfolio Project

2026

## Git Branch Practice

Created feature branch for learning Git workflow.
Created feature branch for learning Git workflow - A.
