from  pyspark.sql import SparkSession

spark = SparkSession.builder.appName("banking_bronze_ingestion").getOrCreate()

raw_path = "data/raw/"

df_transactions = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}transactions.csv")
df_customers = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}customers.csv")
df_accounts = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}accounts.csv")

print("transactions count :", df_transactions.count())
print("customers count :", df_customers.count())
print("accounts count :", df_accounts.count())
df_transactions.show()
df_customers.show()
df_accounts.show()


spark.stop()