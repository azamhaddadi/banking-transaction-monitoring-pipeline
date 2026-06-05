from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when , trim , lower , regexp_extract, rlike
 
spark = SparkSession.builder.appName("KYC Customer Risk Assessment").getOrCreate()

raw_path = "data/raw/"

df_customer_risk = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{raw_path}/dirty_aml_customers.csv")


df_customer_risk_cleaned_s1 = df_customer_risk \
    .withColumn("clean_email", trim(lower(col("email")))) \
     .withColumn("clean_kyc_status", trim(lower(col("kyc_status")))) \
    .select("customer_id","email","clean_email","clean_kyc_status","country_code")


df_customer_risk_cleaned_s2 = df_customer_risk_cleaned_s1 \
     .withColumn("domain" , regexp_extract(col("clean_email"),"@(.+)",1)) \
     .withColumn("domain_valid_flag",when (col("domain").rlike(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),0).otherwise(1)) \
     .withColumn("email_valid_flag", when(col("clean_email").rlike(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"),0).otherwise(1)) \
     .withColumn("unusual_domain_flag", when(col("domain").isin(["mail.ru", "tempmail.com", "unknown.xyz", "company.ir", "protonmail.com"]),1).otherwise(0)) \
     .withColumn("kyc_incomplete_flag",when((col("clean_kyc_status") == "complete"),0).otherwise(1)) \
     .withColumn("high_risk_country_flag", when(col("country_code").isin("IR","RU","KP","SY"),1).otherwise(0))
     


df_customer_risk.show(5)
df_customer_risk_cleaned_s1.show(5)
df_customer_risk_cleaned_s2.show(5)

silver_path = "data/silver/"

df_customer_risk_cleaned_s2.toPandas().to_csv(f"{silver_path}customer_risk_assessment.csv", index = False)

spark.stop()