from pyspark.sql import SparkSession

from pyspark.sql.functions import lower,trim, col , regexp_replace , countDistinct, when 

from datetime import datetime

spark = SparkSession.builder.appName("customer_identity_risk").getOrCreate()

raw_path = "data/raw/"

df_identity_risk = spark.read.option("header","true").option("inferSchema","true").csv(f"{raw_path}customer_identity_dirty_dataset.csv")

df_identity_risk.printSchema()
df_identity_risk.show(10, truncate=False)

df_identity_risk_clean = df_identity_risk \
.withColumn("clean_customer_name",trim(lower(col("customer_name")))) \
.withColumn("clean_home_address",trim(lower(col("home_address")))) \
.withColumn("clean_mailing_address", trim(lower(col("mailing_address")))) \
.withColumn("clean_phone_number",trim(lower(regexp_replace(col("phone_number"),  r"[^0-9]", "")))) \
.withColumn("clean_email",trim(lower(col("email")))) 

df_identity_risk_device_agg = df_identity_risk_clean \
.groupBy("device_id") \
.agg(
    countDistinct(col("customer_id")).alias("distinct_customer_device_count")
) \
.select("device_id", "distinct_customer_device_count")

df_identity_risk_device_agg.printSchema()
df_identity_risk_device_agg.show(10, truncate=False)



df_risk_clean_device = df_identity_risk_clean.join(df_identity_risk_device_agg,
        df_identity_risk_clean["device_id"] == df_identity_risk_device_agg["device_id"], "left")


df_identity_risk_ip_agg = df_identity_risk_clean \
.groupBy("ip_address") \
.agg(
    countDistinct(col("customer_id")).alias("distinct_customer_ip_count")
) \
.filter(col("distinct_customer_ip_count") >= 2) \
.select("ip_address", "distinct_customer_ip_count")

df_identity_risk_ip_agg.printSchema()
df_identity_risk_ip_agg.show(10, truncate=False)

df_risk_clean_device_ip = df_risk_clean_device.join(df_identity_risk_ip_agg, df_risk_clean_device["ip_address"] == df_identity_risk_ip_agg["ip_address"], "left")

#shared phone number analysis
df_identity_risk_phone_agg = df_identity_risk_clean \
.groupBy("clean_phone_number") \
.agg(
    countDistinct(col("customer_id")).alias("distinct_customer_phone_count")
) \
.filter(col("distinct_customer_phone_count") >= 2) \
.select("clean_phone_number", "distinct_customer_phone_count")

df_identity_risk_phone_agg.printSchema()
df_identity_risk_phone_agg.show(10, truncate=False)


df_risk_clean_device_ip_phone = df_risk_clean_device_ip.join(df_identity_risk_phone_agg, df_risk_clean_device_ip["clean_phone_number"] == df_identity_risk_phone_agg["clean_phone_number"], "left")

#shared email analysis 
df_identity_risk_email_agg = df_identity_risk_clean \
.groupBy("clean_email") \
.agg(
    countDistinct(col("customer_id")).alias("distinct_customer_email_count")
) \
.filter(col("distinct_customer_email_count") >= 2) \
.select("clean_email", "distinct_customer_email_count")

df_identity_risk_email_agg.printSchema()
df_identity_risk_email_agg.show(10, truncate=False)

df_risk_clean_device_ip_phone_email = df_risk_clean_device_ip_phone.join(df_identity_risk_email_agg, df_risk_clean_device_ip_phone["clean_email"] == df_identity_risk_email_agg["clean_email"], "left")


identity_risk_flag = when(
                       (col("distinct_customer_device_count") >= 2 )  | 
                       (col("distinct_customer_ip_count") >= 2  )  |
                       (col("distinct_customer_phone_count") >= 2) |
                       (col("distinct_customer_email_count") >= 2 ) ,
                       1).otherwise(0)

df_identity_risk_flag = df_risk_clean_device_ip_phone_email \
.withColumn("identity_risk_flag", identity_risk_flag)


df_identity_risk_flag.printSchema()
df_identity_risk_flag.show(10, truncate=False)

gold_path = "data/gold/"

df_identity_risk_flag.toPandas().to_csv(f"{gold_path}high_risk_customers.csv")