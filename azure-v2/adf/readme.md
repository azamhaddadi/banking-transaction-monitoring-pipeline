
# Azure Data Factory Pipelines

This folder contains Azure Data Factory (ADF) pipelines used to orchestrate the AML Transaction Monitoring solution.

## Architecture Overview

ADF is responsible for orchestrating Databricks notebooks and managing the end-to-end AML processing workflow.

Pipeline execution flow:

01_KYC_CUSTOMER_RISK_SCORING
↓
02_ACCOUNT_PROFILE_TRANSFORM
↓
03_TRANSACTION_RISK_SCORING
↓
05_ROLLING_24H_TRANSACTION_MONITORING
↓
04_RELATIONSHIP_RISK_SCORING
↓
98_PIPELINE_RUN_LOGGING

Error handling is implemented through dedicated logging and failure pipelines.

---

## Pipeline Details

### PL_01_KYC_CUSTOMER_RISK_SCORING

Purpose:

* Customer KYC validation
* Email verification
* High-risk country assessment
* Customer risk scoring

Output:

* Customer risk assessment datasets

---

### PL_02_ACCOUNT_PROFILE_TRANSFORM

Purpose:

* Customer account profiling
* Active account analysis
* Account summary generation

Output:

* Account profile datasets

---

### PL_03_TRANSACTION_RISK_SCORING

Purpose:

* Transaction monitoring
* High-value transaction detection
* Suspicious wire transfer detection
* AML alert generation

Output:

* Transaction risk datasets
* AML alerts

---

### PL_05_ROLLING_24H_TRANSACTION_MONITORING

Purpose:

* Rolling 24-hour transaction analysis
* Transaction accumulation monitoring
* AML threshold detection

Rules:

* Generate alert when cumulative transaction amount exceeds $10,000 within a rolling 24-hour window.

Output:

* Rolling 24-hour monitoring dataset
* AML alert indicators

---

### PL_04_RELATIONSHIP_RISK_SCORING

Purpose:

* Relationship risk analysis
* Counterparty monitoring
* Cross-border transaction analysis

Output:

* Relationship risk alerts

---

### PL_98_PIPELINE_RUN_LOGGING

Purpose:

* Pipeline execution tracking
* Run history logging
* Monitoring and audit support

Output:

* Pipeline run logs

---

### PL_99_ERROR_LOGGING

Purpose:

* Error capture and tracking
* Failure auditing
* Operational troubleshooting

Output:

* Error logs

---

### PL_AML_MASTER_PIPELINE

Purpose:

* Main orchestration pipeline
* Controls execution order
* Coordinates AML processing workflow
* Central monitoring entry point
