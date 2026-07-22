from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("W4M2-Spark-Test")
        .getOrCreate()
    )