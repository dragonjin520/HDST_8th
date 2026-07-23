from pyspark.sql import DataFrame, SparkSession


def load_parquet_data(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """Parquet 형식의 원본 데이터를 Spark DataFrame으로 읽는다."""

    return spark.read.parquet(input_path)