
# AML Pipeline Monitoring

## Architecture

Azure Data Factory
        ↓
Azure Databricks
        ↓
Azure Blob Storage
        ↓
Power BI Dashboard

## Components

### Azure Data Factory
- Master Pipeline
- Pipeline Run Logging
- Error Logging
- Dependency Management

### Databricks
- AML Data Processing
- Run Logging Notebook
- Error Logging Notebook

### Azure Storage
- pipeline_run_logs
- error_logs

### Power BI
- Total Runs
- Successful Runs
- Failed Runs
- Total Errors
- Error Analysis
