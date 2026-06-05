from pyspark.sql import SparkSession
from pyspark.sql.functions import col , trim , date_format , to_number


spark = SparkSession.builder.appName("fact creation").getOrCreate()

raw_path = "data/raw/"
df_transactions = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}rolling_24h_transactions_v2.csv")

df_fact_transaction = df_transactions \
.select(
    "transaction_id",
    "transaction_timestamp",
    "customer_id",
    "account_id",
    "counterparty_account_id",
    "counterparty_name",
    "counterparty_country",
    "transaction_amount",
    "transaction_type",
    "channel",
    "transaction_category",
    "transaction_subcategory"
) \
.dropDuplicates()

df_fact_transaction.printSchema()
df_fact_transaction.show(10, truncate=False)

gold_path = "data/gold/"

df_fact_transaction.toPandas().to_csv(f"{gold_path}fact_transaction.csv", index = False)