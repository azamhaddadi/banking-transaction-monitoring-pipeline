from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number ,to_timestamp , count , sum, unix_timestamp ,format_number, when
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder.appName("rolling_24th_transaction_monitoring").getOrCreate()

raw_path = "data/raw/"
df_transactions = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}rolling_24h_transactions_v2.csv")

df_transactions_clean = df_transactions \
.withColumn("clean_transaction_timestamp", to_timestamp(col("transaction_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
.withColumn("transaction_unix_time",unix_timestamp(col("clean_transaction_timestamp")))

window_spec = Window.partitionBy("customer_id").orderBy(col("transaction_unix_time")).rangeBetween(-86400, 0)


rolling_24h_amount = sum(col("transaction_amount")).over(window_spec)


df_transactions_clean_rolling = df_transactions_clean \
.withColumn("rolling_24h_amount", rolling_24h_amount) \
.withColumn("rolling_24h_alert_flag",when(col("rolling_24h_amount") > 10000, 1).otherwise(0))

df_transactions_clean_rolling.printSchema()
df_transactions_clean_rolling.orderBy("customer_id" , "clean_transaction_timestamp").show(10, truncate=False)

