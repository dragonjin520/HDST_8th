from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """W4M2 분석용 SparkSession을 생성한다."""

    return (
        SparkSession.builder
        .appName("W4M2-NYC-Taxi-Analysis")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )