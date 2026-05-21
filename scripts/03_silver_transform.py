from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Banking_silver_transform").getOrCreate()

raw_path = "data/raw/"
bronze_path = "data/bronze/"
silver_path = "data/silver/"
lookup_path = "data/lookup/"


df_transactions_clean = spark.read.option("header","true").option("inferSchema","true").csv(f"{bronze_path}clean_transactions.csv")


df_customers = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}customers.csv") 
df_customers_trans = df_customers.withColumnRenamed("customer_id", "cust_customer_id")


df_accounts = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}accounts.csv")
df_accounts_trans = df_accounts \
.withColumnRenamed("account_id", "acc_account_id") \
.withColumnRenamed("customer_id", "acc_customer_id")


df_risk_country_lookup = spark.read.option("header","true").option("inferSchema","true").csv(f"{lookup_path}risk_country_lookup.csv")
df_risk_country_lookup_trans = df_risk_country_lookup.withColumnRenamed("country_code", "risk_country_code")



df_join_trans_cust  = df_transactions_clean.join(df_customers_trans , df_customers_trans.cust_customer_id  == df_transactions_clean.customer_id , "inner")
df_join_trans_cust_acc = df_join_trans_cust.join(df_accounts_trans, df_accounts_trans.acc_account_id == df_join_trans_cust.account_id , "inner")
df_join_trans_cust_acc_lookup = df_join_trans_cust_acc.join(df_risk_country_lookup_trans, df_risk_country_lookup_trans.risk_country_code == df_join_trans_cust_acc.country_code , "left") \
.select ("transaction_id","transaction_date","transaction_amount","transaction_type",
        "customer_id","customer_name","risk_level",
        "account_id","account_type","account_status",
        "country_code","country_risk_level","branch_id"
        )

print(f"count of df_join_trans_cust_acc_lookup:",df_join_trans_cust_acc_lookup.count())
df_join_trans_cust_acc_lookup.show()
df_join_trans_cust_acc_lookup.toPandas().to_csv(f"{silver_path}silver_transactions.csv", index = False)

#df_silver = df_join_trans_cust_acc_lookup \



#print('count of transformed transactions:', df_silver.count())
#df_silver.show()

spark.stop()
