from pyspark.sql import SparkSession, functions as F, types as T
import re

def load_data(spark, path):
    df = spark.read.option("header", "true").csv(path)
    return df

def apply_schema(df):
    return df.withColumn("id", F.col("id").cast(T.IntegerType())) \
             .withColumn("date_inscription", F.to_date("date_inscription", "yyyy-MM-dd"))

def clean_data(df):
    # Valider email via regex
    return df.withColumn(
        "email_valide",
        F.col("email").rlike(r"[^@]+@[^@]+\.[^@]+")
    )

def compute_kpi(df):
    total = df.count()
    emails_valides = df.filter(F.col("email_valide") == True).count()
    taux_validite = emails_valides / total if total > 0 else 0
    return {"total":total,"Mv": emails_valides,"Mf": taux_validite}

def run(input_path, output_path, min_valid_rate=0.8):
    spark = SparkSession.builder.appName("tp_spark_job").getOrCreate()

    df = load_data(spark, input_path)
    df = apply_schema(df)
    df = clean_data(df)

    total, valid, rate = compute_kpi(df)
    print(f"TOTAL={total}, VALID={valid}, RATE={rate}")

    if rate < min_valid_rate:
        raise ValueError(f"Taux de validité trop faible: {rate:.2f}")

    df.write.mode("overwrite").parquet(output_path)
    spark.stop()
