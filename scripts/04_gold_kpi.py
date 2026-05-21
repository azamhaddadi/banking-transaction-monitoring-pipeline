from pyspark.sql import SparkSession 
from pyspark.sql.functions  import col, count, max, sum , rank , when

from pyspark.sql.window  import Window

spark = SparkSession.builder.appName("Banking_gold_kpi").getOrCreate()


silver_path = "data/silver/"
gold_path = "data/gold/"

df_silver = spark.read.option("header","true").option("inferSchema", "true").csv(f"{silver_path}silver_transactions.csv")

high_amount_flag = when(col("transaction_amount") >= 10000, 1).otherwise(0)
high_risk_country_flag  = when(col("country_risk_level") == "HIGH", 1).otherwise(0)
suspicious_wire_flag = when(
    (col("transaction_type") == "WIRE") &
    (col("transaction_amount") >= 10000) &
    (col("country_risk_level") == "HIGH"),
    1
).otherwise(0)


df_flagged = df_silver.withColumn("high_amount_flag", high_amount_flag) \
.withColumn("high_risk_country_flag", high_risk_country_flag) \
.withColumn("suspicious_wire_flag", suspicious_wire_flag) 


df_flagged_data = df_flagged \
.select("transaction_id",
        "transaction_amount",
        "transaction_type",
        "country_risk_level",
        "high_amount_flag",
        "high_risk_country_flag",
        "suspicious_wire_flag").show()

df_flagged.toPandas().to_csv(f"{gold_path}flagged_data.csv" , index=False)





df_flagged_agg = df_flagged \
.filter(
    (col("high_amount_flag") == 1) |
    (col("high_risk_country_flag") == 1) |
    (col("suspicious_wire_flag") == 1)
) \
.groupBy("country_risk_level","country_code") \
.agg(
    sum("transaction_amount").alias("total_suspicious_amount"),
    count("transaction_id").alias("suspicious_transaction_count"),
    sum(col("suspicious_wire_flag")).alias("sum_suspicious_wire_flag")
) \
.orderBy("total_suspicious_amount", ascending=False)


df_flagged_agg.printSchema()
df_flagged_agg.show()


window_spec = Window.partitionBy("country_risk_level").orderBy(col("total_suspicious_amount").desc())

df_gold_kpi = df_flagged_agg.withColumn("rank", rank().over(window_spec)) \
.filter("rank = 1")

df_gold_kpi.show()

df_gold_kpi.toPandas().to_csv(f"data/gold/gold_kpi.csv", index = False)
#print(f"count of df_silver_changed:" , df_silver_changed.count())
spark.stop()



#df_silver_changed.show()



