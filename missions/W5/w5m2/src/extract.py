from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def create_spark_session(
    app_name: str,
    master_url: str,
) -> SparkSession:
    """SparkSession을 생성한다."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )


def load_trip_data(
    spark: SparkSession,
    input_path: Path,
) -> DataFrame:
    """NYC TLC Parquet 데이터를 Spark DataFrame으로 읽는다."""
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file does not exist: {input_path}"
        )

    return spark.read.parquet(str(input_path))