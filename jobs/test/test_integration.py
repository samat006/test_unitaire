from pyspark.sql import SparkSession
from jobs.spark_job import load_data, apply_schema, clean_data, compute_kpi

def test_pipeline_integration():
    # 1️⃣ SparkSession
    spark = SparkSession.builder.master("local[*]").appName("integration_tests").getOrCreate()

    # 2️⃣ Données de test
    data = [(1, "Alice", "alice@mail.com", "2025-01-01"),
            (2, "Bob", None, "2025-01-02"),
            (3, "Charlie", "charlie@mail.com", "2025-01-03")]
    df = spark.createDataFrame(data, ["id", "nom", "email", "date_inscription"])

    # 3️⃣ Appliquer le pipeline complet
    df2 = apply_schema(df)
    df3 = clean_data(df2)
    kpis = compute_kpi(df3)

    # 4️⃣ Vérifications
    # Aucune valeur null dans 'email'
    assert df3.filter(df3.email.isNull()).count() == 1
    # Types corrects
    from pyspark.sql.types import IntegerType
    assert isinstance(df3.schema["id"].dataType, IntegerType)
    # KPI corrects
    assert kpis["total"] == df3.count()

    spark.stop()
