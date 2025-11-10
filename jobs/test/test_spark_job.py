from pyspark.sql import SparkSession
from jobs.spark_job import load_data, apply_schema, clean_data, compute_kpi
from pyspark.sql.types import IntegerType

def test_apply_schema():
    spark = SparkSession.builder.master("local[*]").appName("tests").getOrCreate()
    df = spark.createDataFrame(
        [(1, "Alice", "alice@mail.com", "2025-01-01")],
        ["id", "nom", "email", "date_inscription"]
    )
    df2 = apply_schema(df)

    assert isinstance(df2.schema["id"].dataType, IntegerType)
   # assert str(df2.schema["date_inscription"].dataType) == "DateType"
    assert df2.collect()[0]["date_inscription"].strftime("%Y-%m-%d") == "2025-01-01"
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
    kpi = compute_kpi(df)
    assert kpi['total' ]== 2
    assert kpi['Mv'] == 1
    assert kpi['Mf'] == 0.5
    spark.stop()
