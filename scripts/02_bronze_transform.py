from pyspark.sql import SparkSession 
import pandas as pd



spark = SparkSession.builder.appName("Banking_bronze_transform").getOrCreate()

raw_path = "data/raw/"
bronze_path = "data/bronze/"

df_transactions = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}transactions.csv")
print(f"Number of transaction: {df_transactions.count()}")

df_clean_transaction = df_transactions.filter("transaction_amount > 0 and customer_id is not null")
df_clean_transaction = df_clean_transaction.filter("transaction_amount >= 10000")

print(f"Number of clean transaction: {df_clean_transaction.count()}")


df_clean_transaction.show()

df_clean_transaction.toPandas().to_csv(f"{bronze_path}clean_transactions.csv", index = False)
spark.stop()