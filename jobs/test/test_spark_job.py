from pyspark.sql import SparkSession
from jobs.spark_job import load_data, apply_schema, clean_data, compute_kpi

def test_apply_schema():
    spark = SparkSession.builder.master("local[*]").appName("tests").getOrCreate()
    df = spark.createDataFrame(
        [(1, "Alice", "alice@mail.com", "2025-01-01")],
        ["id", "nom", "email", "date_inscription"]
    )
    df2 = apply_schema(df)
    assert str(df2.schema["id"].dataType) == "IntegerType"
    spark.stop()

def test_clean_data():
    spark = SparkSession.builder.master("local[*]").appName("tests").getOrCreate()
    df = spark.createDataFrame(
        [(1, "Alice", "alice@mail.com", "2025-01-01"),
         (2, "Bob", "invalidmail", "2025-01-01")],
        ["id", "nom", "email", "date_inscription"]
    )
    df2 = clean_data(df)
    rows = df2.collect()
    assert rows[0]["email_valide"] == True
    assert rows[1]["email_valide"] == False
    spark.stop()

def test_kpi():
    spark = SparkSession.builder.master("local[*]").appName("tests").getOrCreate()
    df = spark.createDataFrame(
        [(1, "Alice", "alice@mail.com", True),
         (2, "Bob", "invalidmail", False)],
        ["id", "nom", "email", "email_valide"]
    )
    total, valid, rate = compute_kpi(df)
    assert total == 2
    assert valid == 1
    assert rate == 0.5
    spark.stop()
