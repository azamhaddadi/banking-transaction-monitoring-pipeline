from pyspark.sql import SparkSession
from pyspark.sql.functions import col , date_format , dayofweek , date_format
from datetime import datetime

spark = SparkSession.builder.appName("Dimention creation").getOrCreate()


raw_path= "data/raw/"
gold_path= "data/gold/"
df_transaction_date = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}rolling_24h_transactions_v2.csv")

df_date = df_transaction_date \
.withColumn("transaction_date", date_format(col("transaction_timestamp"),"yyyy-MM-dd")) \
.withColumn("transaction_year", date_format(col("transaction_timestamp"),"yyyy")) \
.withColumn("transaction_month", date_format(col("transaction_timestamp"),"MM")) \
.withColumn("transaction_day", date_format(col("transaction_timestamp"),"dd")) \
.withColumn("Day_of_week", dayofweek(col("transaction_timestamp"))) \
.select("transaction_date","transaction_year","transaction_month","transaction_day","Day_of_week") \
.dropDuplicates()

df_date.printSchema()
df_date.show(10)
df_date.toPandas().to_csv(f"{gold_path}dim_date.csv", index=False)

df_counterparty = df_transaction_date \
.select(
    "counterparty_account_id",
    "counterparty_name",
    "counterparty_country"
) \
.dropDuplicates()

df_counterparty.show(10)

df_counterparty.toPandas().to_csv(
    f"{gold_path}dim_counterparty.csv",
    index=False
)


df_time = df_transaction_date \
.withColumn(
    "time_key",
    date_format(col("transaction_timestamp"), "HH:mm")
) \
.withColumn(
    "Hour",
    date_format(col("transaction_timestamp"), "HH")
) \
.withColumn(
    "Minute",
    date_format(col("transaction_timestamp"), "mm")
) \
.select("time_key", "Hour", "Minute") \
.dropDuplicates()

df_time.toPandas().to_csv(f"{gold_path}dim_time.csv", index=False)



df_channel =  df_transaction_date \
.withColumn("channel", col("channel"))\
.select("channel") \
.dropDuplicates()
df_channel.toPandas().to_csv(f"{gold_path}dim_channel.csv", index=False)


df_category =  df_transaction_date \
.withColumn("transaction_category", col("transaction_category"))\
.withColumn("transaction_subcategory", col("transaction_subcategory")) \
.select("transaction_category","transaction_subcategory") \
.dropDuplicates()
df_category.toPandas().to_csv(f"{gold_path}dim_category.csv", index=False)

df_transaction_type  =  df_transaction_date \
.withColumn("transaction_type", col("transaction_type"))\
.select("transaction_type") \
.dropDuplicates()
df_transaction_type.toPandas().to_csv(f"{gold_path}dim_transaction_type.csv", index=False)


df_customer = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}customers.csv")
df_customer.toPandas().to_csv(f"{gold_path}dim_customer.csv", index=False)
df_accounts = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}accounts.csv")
df_accounts.toPandas().to_csv(f"{gold_path}dim_accounts.csv", index=False)

spark.stop()