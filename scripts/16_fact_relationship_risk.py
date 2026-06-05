from pyspark.sql import SparkSession

from pyspark.sql.functions import col, when, concat_ws, sum as spark_sum, count, max as spark_max, min as spark_min , regexp_replace, expr

spark = SparkSession.builder \
    .appName("Relationship Risk") \
    .getOrCreate()

# =====================================
# Read Source Files
# =====================================

raw_path = "data/raw/"
look_path = "data/lookup/"
gold_path = "data/gold/"

df_transactions = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{raw_path}transactions.csv")

df_counterparty = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{raw_path}counterparty_relationship_history.csv") \
    .withColumnRenamed("customer_id", "rel_customer_id")

df_customer = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{raw_path}customers.csv") \
    .withColumnRenamed("customer_id", "cust_customer_id")

df_country = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{look_path}risk_country_lookup.csv")

# =====================================
# Join Data
# =====================================

df_relationship = df_transactions \
    .join(
        df_counterparty,
        (
            (df_transactions["customer_id"] ==
             df_counterparty["rel_customer_id"])
            &
            (df_transactions["counterparty_account_id"] ==
             df_counterparty["counterparty_account_id"])
        ),
        "left"
    ) \
    .join(
        df_country,
        df_transactions["counterparty_country"] ==
        df_country["country_code"],
        "left"
    ) \
    .join(
        df_customer,
        df_transactions["customer_id"] ==
        df_customer["cust_customer_id"],
        "left"
    )

# =====================================
# Relationship Risk Score
# =====================================

df_relationship = df_relationship \
    .withColumn(
        "country_score",
        when(col("country_risk_level") == "HIGH", 40)
        .otherwise(0)
    ) \
    .withColumn(
        "kyc_score",
        when(col("kyc_risk_rating") == "HIGH", 30)
        .otherwise(0)
    ) \
    .withColumn(
        "customer_status_score",
        when(col("customer_status") == "DORMANT", 20)
        .otherwise(0)
    ) \
    .withColumn(
        "relationship_score",
        when(
            col("relationship_status").isNull()
            |
            (col("relationship_status") != "KNOWN"),
            10
        ).otherwise(0)
    ) \
    .withColumn(
        "relationship_risk_score",
        col("country_score")
        + col("kyc_score")
        + col("customer_status_score")
        + col("relationship_score")
    )

# =====================================
# Risk Level
# =====================================

df_relationship = df_relationship \
    .withColumn(
        "relationship_risk_level",
        when(col("relationship_risk_score") >= 70, "HIGH")
        .when(col("relationship_risk_score") >= 30, "MEDIUM")
        .otherwise("LOW")
    )

# =====================================
# Risk Reason
# =====================================

df_relationship = df_relationship \
    .withColumn(
        "relationship_reason",
        concat_ws(
            "|",
            when(
                col("country_risk_level") == "HIGH",
                "HIGH_RISK_COUNTRY"
            ),
            when(
                col("kyc_risk_rating") == "HIGH",
                "HIGH_KYC_RATING"
            ),
            when(
                col("customer_status") == "DORMANT",
                "DORMANT_CUSTOMER"
            ),
            when(
                col("relationship_status").isNull()
                ,
                "UNKNOWN_RELATIONSHIP"
            )
        )
    )

# =====================================
# Final Fact Table
# =====================================

df_fact_relationship_risk = df_relationship.select(
    df_transactions["customer_id"],
    df_transactions["account_id"],
    df_transactions["counterparty_account_id"],
    df_transactions["counterparty_name"],
    df_transactions["counterparty_country"],
    df_country["country_risk_level"],
    df_counterparty["relationship_status"],
    df_counterparty["first_seen_date"],
    df_customer["kyc_risk_rating"],
    df_customer["customer_status"],
    df_transactions["transaction_amount"],
    df_transactions["transaction_type"],
    df_transactions["transaction_channel"],
    "relationship_risk_score",
    "relationship_risk_level",
    "relationship_reason"
)

df_fact_relationship_risk = df_fact_relationship_risk \
.withColumn(
    "transaction_amount_clean",
    expr("try_cast(transaction_amount as double)")
     ) \
    .groupBy(
        "customer_id",
        "account_id",
        "counterparty_account_id",
        "counterparty_name",
        "counterparty_country",
        "country_risk_level",
        "relationship_status",
        "first_seen_date",
        "kyc_risk_rating",
        "customer_status"
    ) \
    .agg(
        spark_sum("transaction_amount_clean").alias("total_transaction_amount"),
        count("*").alias("transaction_count"),
        spark_max("relationship_risk_score").alias("relationship_risk_score"),
        spark_max("relationship_risk_level").alias("relationship_risk_level"),
        spark_max("relationship_reason").alias("relationship_reason")
    )

# =====================================
# Validation
# =====================================

df_fact_relationship_risk.printSchema()

df_fact_relationship_risk.show(
    50,
    truncate=False
)

# =====================================
# Save Output
# =====================================

df_fact_relationship_risk.toPandas().to_csv(
    f"{gold_path}fact_relationship_risk.csv",
    index=False
)

spark.stop()