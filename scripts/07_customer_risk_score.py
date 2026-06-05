from pyspark.sql import SparkSession 
from pyspark.sql.functions import when, col  

spark = SparkSession.builder.appName("Customer Risk Scoring").getOrCreate()

silver_path = "data/silver/"
gold_path = "data/gold/"

df_cust_risk_ass = spark.read.option("header","true").option("inferSchema","true").csv(f"{silver_path}customer_risk_assessment.csv")

df_cust_risk_ass_trans = df_cust_risk_ass \
.withColumn("customer_risk_score", when(col("email_valid_flag") == 0 ,1).otherwise(1) +
                                   when(col("domain_valid_flag") == 0,1).otherwise(1) +
                                   when(col("unusual_domain_flag") == 1, 1).otherwise(0) +
                                           when (col("kyc_incomplete_flag") == 1 ,1).otherwise(0) +
                                           when(col("high_risk_country_flag") == 1 , 1).otherwise(0)
                                   ) \
.withColumn("customer_risk_level", when(col("customer_risk_score") == 0,"LOW")
                                   .when(col("customer_risk_score").isin(1,2) ,"MEDIUM")
                                   .when(col("customer_risk_score") >= 3 ,"HIGH")
                                   .otherwise("NONE")
            )
 
#df_cust_risk_ass_trans.printSchema()
#df_cust_risk_ass_trans.show(5)

df_high_risk_customers = df_cust_risk_ass_trans \
.filter(col("customer_risk_level") == "HIGH" )



df_high_risk_customers.printSchema()
df_high_risk_customers.show(5)
df_high_risk_customers.toPandas().to_csv(f"{gold_path}high_risk_customers.csv" , index = False)
