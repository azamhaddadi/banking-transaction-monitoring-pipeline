# AML Pipeline Monitoring Solution

## Overview

This project demonstrates an end-to-end AML monitoring solution built using Azure Data Factory, Azure Databricks, Azure Storage and Power BI.
Credentials and secrets are managed through Databricks Secret Scope.
No secrets or access keys are stored in source code.

## Architecture

ADF
→ Databricks
→ Azure Storage
→ Power BI

## Components

### Azure Data Factory

- Master Pipeline Orchestration
- Pipeline Run Logging
- Error Logging
- Dependency Management

### Azure Databricks

- AML Data Processing
- Pipeline Run Logging Notebook
- Error Logging Notebook

### Azure Storage

- pipeline_run_logs
- error_logs

### Power BI Dashboards

- AML Monitoring Dashboard
- KYC Dashboard
- Transaction Monitoring Dashboard
- Relationship Analysis Dashboard

## Screenshots

### ADF Master Pipeline

![ADF](screenshot/Master_pipeline.png)

### Azure Storage Logs

![Storage](screenshot/Azure%20Storage%20Logs.png)

### Databricks Logging Notebook

![Databricks](screenshot/NB_98_PIPELINE_RUN_LOGGING.png)

### AML Monitoring Dashboard

![AML](screenshot/powerbi%20dashboard_aml_monitoring.png)

### KYC Dashboard

![KYC](screenshot/powerbi%20dashboard_kyc.png)

### Transaction Monitoring Dashboard

![Transaction](screenshot/powerbi_transaction_monitoring.png)

### Relationship Analysis Dashboard

![Relationship](screenshot/powerbidashboard_relationship%20analysis.png)

## Technology Stack

- Azure Data Factory
- Azure Databricks
- PySpark
- Azure Blob Storage
- Power BI
- GitHub

## Key Features

- Pipeline Monitoring
- Error Tracking
- AML Data Processing
- KYC Risk Assessment
- Transaction Monitoring
- Relationship Risk Analysis
- Operational Reporting
