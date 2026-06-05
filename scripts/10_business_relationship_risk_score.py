from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws , when , col 
from datetime import datetime

run_date = datetime.now().strftime("%Y%m%d")

spark = SparkSession.builder.appName("relationship_risk_score").getOrCreate()

gold_path = "data/gold/"

df_high_risk = spark.read.option("header","true").option("inferSchema","true").csv(f"{gold_path}business_relationship_risk.csv")

df_high_risk_trans = df_high_risk \
.filter(col("relationship_risk_flag") == 1) \
.withColumn("risk_reason" , concat_ws("|",
                                     when(col("relationship_risk_level") == "HIGH" ,"RELATIONSHIP_ISSUE"),
                                     when(col("wire_transaction_count") >= 3 , "HIGH_WIRE_RATE"),
                                     when(col("distinct_counterparty_count") >= 3 , "HIGH_COUNTERPARTY"),
                                     when(col("high_amount_transaction_count") >= 1 , "HIGH_VOLUME_AMOUNT" )
                                    )
            )
print("High Risk Relationship Alert Generated Successfully")
print(f"Total Alert Count : {df_high_risk_trans.count()}")

if df_high_risk_trans.count() == 0:
    print("No High Risk Alerts Found")
else:
    print("Writing Gold Alert Output")
            
gold_path = "data/gold/"            
df_high_risk_trans.toPandas().to_csv(f"{gold_path}high_risk_relationship_alert_{run_date}.csv", index = False)
