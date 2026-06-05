from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, sum as spark_sum, when , concat_ws ,date_format
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Transaction Monitoring").getOrCreate()

gold_path = "data/gold/"
df_transactions = spark.read.option("header","true").option("inferSchema","true").csv(f"{gold_path}fact_transaction.csv")
df_transactions = df_transactions \
.withColumn("transaction_unix_time",unix_timestamp(col("transaction_timestamp"))) \
.withColumn(
    "transaction_date",
    date_format(col("transaction_timestamp"), "yyyy-MM-dd")
) \
.withColumn("transaction_time_key", date_format(col("transaction_timestamp"), "HH:mm")) 


window_spec = Window.partitionBy("customer_id").orderBy("transaction_unix_time").rangeBetween(-86400 , 0)

df_transactions.printSchema()
df_transactions.show(10, truncate=False)



df_transactions_clean = df_transactions \
.withColumn("rolling_24h_amount", spark_sum(col("transaction_amount")).over(window_spec)) \
.withColumn("rolling_24h_alert_flag", when(col("rolling_24h_amount") >= 10000,1 ).otherwise(0)) \
.withColumn("high_risk_country_flag", when(col("counterparty_country").isin("IR", "RU", "CN", "SY", "HK", "AE"),1).otherwise(0)) \
.withColumn("channel_risk_flag", when(col("channel").isin("CRYPTO", "CASH"),1).otherwise(0) ) \
.withColumn("transaction_type_risk_flag",when(col("transaction_type") == "WIRE" , 1 ).otherwise(0)) \
.withColumn("alert_reason", concat_ws( "|",
                            when(col("high_risk_country_flag") == 1 , "HIGH_RISK_COUNTRY")    ,      
                            when(col("channel_risk_flag") == 1 ,"HIGH_RISK_CHANNEL"),
                            when(col("transaction_type_risk_flag") == 1 , "WIRE_TYPE_TRANSACTION"),
                            when (
                                   (col("rolling_24h_amount") >= 10000) & 
                                   (col("transaction_type") == "WIRE") ,
                                     "HIGH_24H_WIRE_ACTIVITY" 
                                  ),
                            when(
                                   col("rolling_24h_amount") >= 10000, 
                                     "24H_AMOUNT_OVER_10000"
                                  )
                            
            )
            ) \
.withColumn(
    "alert_reason",
    when(col("alert_reason") == "", "NORMAL")
    .otherwise(col("alert_reason"))
)




df_transactions_clean.show(10 , truncate= False)



df_transactions_clean.toPandas().to_csv(
    f"{gold_path}fact_transaction_monitoring.csv",
    index=False
)