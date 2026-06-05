from pyspark.sql import SparkSession

from pyspark.sql.functions import when , col ,concat_ws

spark = SparkSession.builder.appName("high_risk_customer_alerts").getOrCreate()

gold_path = "data/gold/"

df_high_risk = spark.read.option("header","true").option("inferSchema","true").csv(f"{gold_path}high_risk_customers.csv") 

df_gen_alert = df_high_risk \
.withColumn("alert_reason", concat_ws(  "|",
                            when(col("domain_valid_flag") == 0 , "INVALID_DOMAIN"),
                            when(col("email_valid_flag") == 0, "INVALID_EMAIL"),
                            when(col("unusual_domain_flag") == 1 , "UNUSUAL_DOMAIN"),
                            when(col("kyc_incomplete_flag") == 1 , "KYC_INCOMPLETE"),
                            when(col("high_risk_country_flag") == 1 , "HIGH_RISK_COUNTRY")                         
            )
)
df_gen_alert.printSchema()
df_gen_alert.show(5)

df_gen_alert.toPandas().to_csv(f"{gold_path}kyc_customer_risk_alert.csv" , index = False)
spark.stop()