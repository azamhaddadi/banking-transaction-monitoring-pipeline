# Power BI Dashboards

## 1. KYC Dashboard

### KPI Cards

* Total Customers
* High Risk Customers
* Medium Risk Customers
* Low Risk Customers
* Average KYC Score

### DAX Measures

* Total Customers
* High Risk Customers
* Medium Risk Customers
* Low Risk Customers
* Avg KYC Score

### Filters

* KYC Risk Level
* Residency Country
* Country Risk Level

### Visualizations

#### Pie Chart

* KYC Risk Level Distribution

#### Bar Chart

* Country Risk Level Distribution

#### Investigation Table

Columns:

* customer_id
* residency_country
* country_risk_level
* kyc_risk_level
* kyc_risk_score
* account_count
* active_account_count
* risk_reason

---

## 2. Customer Investigation Dashboard

### KPI Cards

* Selected Customer
* Customer Risk Level
* Maximum Risk Score
* Country Risk Level

### Visualizations

#### Customer Risk Profile

* Customer KYC Details
* Risk Factors
* Customer Investigation Table

#### Drill Through Capability

Users can drill through from the KYC Dashboard directly to the selected customer investigation page.

---

## 3. Transaction Monitoring Dashboard

### KPI Cards

* Total Transactions
* Total Amount
* Average Transaction Amount
* High Value Transactions
* High Value Customers
* High Risk Countries
* High Risk Channels
* Unique Accounts
* Unique Customers
* Counterparties

### Filters

* Transaction Type
* Channel
* Counterparty Country
* Transaction Date

### Visualizations

#### Bar Charts

* Total Amount by Counterparty Country
* Transactions by Transaction Type
* Transactions by Transaction Category
* Transactions by Transaction Subcategory

#### AML Monitoring

* Transactions by AML Alert Status
* Rolling 24-Hour Alert Monitoring

#### Transaction Detail Table

Columns:

* customer_id
* account_id
* counterparty_name
* counterparty_country
* transaction_amount
* transaction_type
* transaction_category
* transaction_timestamp

---

## 4. Transaction Investigation Dashboard

### KPI Cards

* Total Transactions
* High Risk Transactions
* High Risk Countries
* Wire Transactions

### Filters

* Transaction Type
* Channel
* Counterparty Country
* Transaction Date

### Visualizations

#### Pie Chart

* Transaction Type Distribution

#### Trend Analysis

* Transactions Over Time

#### Country Analysis

* Transactions by Counterparty Country

#### Investigation Table

Columns:

* transaction_id
* customer_id
* counterparty_country
* transaction_amount
* channel
* transaction_type
* rolling_24h_amount
* alert_reason

---

## 5. Relationship Analysis Dashboard

### KPI Cards

* Total Relationships
* High Risk Relationships
* Medium Risk Relationships
* Average Relationship Score
* Offshore Banking Relationships
* High Risk Counterparties
* Dormant Account Relationships
* Cross Border Relationships

### Filters

* Customer ID
* Counterparty Country
* KYC Risk Rating
* Customer Status
* Relationship Risk Level

### Visualizations

#### Stacked Bar Chart

* Relationships by Counterparty Country and Relationship Risk Level

#### Relationship Detail Table

Columns:

* customer_id
* counterparty_account_id
* total_transaction_amount
* transaction_count
* counterparty_name
* counterparty_country
* relationship_risk_level

### Drill Through Capability

Users can investigate customer relationships and counterparties from relationship monitoring results.
