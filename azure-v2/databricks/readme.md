# Databricks Notebooks

This folder contains the Databricks notebooks used in the AML (Anti-Money Laundering) Transaction Monitoring Pipeline.

## Notebook Overview

### 01_KYC_CUSTOMER_RISK_SCORING

Performs KYC data cleansing, validation, and customer risk assessment.

Key functions:

* Email validation
* Domain extraction
* High-risk country detection
* KYC completeness checks
* Customer risk scoring

---

### 02_ACCOUNT_PROFILE_TRANSFORM

Processes customer account information and generates account profile metrics.

Key functions:

* Account aggregation
* Active account analysis
* Customer account profiling

---

### 03_TRANSACTION_RISK_SCORING

Analyzes transaction activity and identifies suspicious transactions.

Key functions:

* High-value transaction detection
* High-risk country monitoring
* Suspicious wire transfer identification
* AML alert generation

---

###
