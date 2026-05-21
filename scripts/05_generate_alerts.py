from pyspark.sql import SparkSession
from pyspark.sql.functions import col , when

spark = SparkSession.builder.appName("generate_alert").getOrCreate()

gold_path = "data/gold/"

df_gold = spark.read.option("header","true").option("inferSchema", "true").csv(f"{gold_path}flagged_data.csv")
df_gold.printSchema()

df_gold_sus = df_gold.filter(
    (col("high_amount_flag") == 1) |
    (col("high_risk_country_flag") == 1) |
    (col("suspicious_wire_flag") == 1)
)


df_gold_alert = df_gold_sus\
.withColumn("alert_reason",when(col("suspicious_wire_flag") == 1 , "WIRE")
                           .when(col("high_amount_flag")==1 ,"HIGH_AMOUNT") 
                           .when(col("high_risk_country_flag") == 1 ,"HIGH_RISK_COUNTRY" \
                           "")
)

gold_path = "data/gold/"

df_gold_alert.show()

df_gold_alert.toPandas().to_csv(f"{gold_path}gold_alert.csv" , index=False)

spark.stop()