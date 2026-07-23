from pyspark.sql import DataFrame
from pyspark.sql import functions as F


ANALYSIS_START = "2026-02-01 00:00:00"
ANALYSIS_END = "2026-03-01 00:00:00"
MIN_TRIP_DURATION_MINUTES = 1.0
MAX_TRIP_DURATION_MINUTES = 300.0
MIN_TRIP_DISTANCE_MILES = 0.0
MAX_TRIP_DISTANCE_MILES = 100.0


def add_trip_duration(df: DataFrame) -> DataFrame:
    """승차·하차 시각의 차이로 운행시간(분)을 계산한다."""

    return df.withColumn(
        "trip_duration_minutes",
        F.timestamp_diff(
        "SECOND",
        F.col("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime"),
        ) / F.lit(60.0),
    )


def clean_taxi_data(df: DataFrame) -> DataFrame:
    """W4M2 분석 기준에 맞게 NYC Yellow Taxi 데이터를 정제한다."""

    required_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
    ]

    cleaned_df = df.dropna(subset=required_columns)
    cleaned_df = add_trip_duration(cleaned_df)

    return cleaned_df.filter(
        (F.col("tpep_pickup_datetime") >= F.to_timestamp(F.lit(ANALYSIS_START)))
        & (F.col("tpep_pickup_datetime") < F.to_timestamp(F.lit(ANALYSIS_END)))
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (
            F.col("trip_duration_minutes")
            >= F.lit(MIN_TRIP_DURATION_MINUTES)
        )
        & (
            F.col("trip_duration_minutes")
            <= F.lit(MAX_TRIP_DURATION_MINUTES)
        )
        & (F.col("trip_distance") > F.lit(MIN_TRIP_DISTANCE_MILES))
        & (F.col("trip_distance") <= F.lit(MAX_TRIP_DISTANCE_MILES))
    )


def build_quality_summary(raw_df: DataFrame) -> DataFrame:
    """정제 규칙별 비정상 데이터 건수를 한 행으로 집계한다."""

    duration_minutes = (
        F.timestamp_diff(
        "SECOND",
        F.col("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime"),
        ) / F.lit(60.0)
    )

    return raw_df.agg(
        F.count("*").alias("input_count"),
        F.sum(
            F.when(
                F.col("tpep_pickup_datetime").isNull()
                | F.col("tpep_dropoff_datetime").isNull()
                | F.col("trip_distance").isNull(),
                1,
            ).otherwise(0)
        ).alias("missing_required_value_count"),
        F.sum(
            F.when(
                (F.col("tpep_pickup_datetime") < F.to_timestamp(F.lit(ANALYSIS_START)))
                | (F.col("tpep_pickup_datetime") >= F.to_timestamp(F.lit(ANALYSIS_END))),
                1,
            ).otherwise(0)
        ).alias("outside_analysis_period_count"),
        F.sum(
            F.when(
                F.col("tpep_dropoff_datetime")
                <= F.col("tpep_pickup_datetime"),
                1,
            ).otherwise(0)
        ).alias("invalid_datetime_order_count"),
        F.sum(
            F.when(
                (duration_minutes < F.lit(MIN_TRIP_DURATION_MINUTES))
                | (duration_minutes > F.lit(MAX_TRIP_DURATION_MINUTES)),
                1,
            ).otherwise(0)
        ).alias("invalid_trip_duration_count"),
        F.sum(
            F.when(
                (F.col("trip_distance") <= F.lit(MIN_TRIP_DISTANCE_MILES))
                | (F.col("trip_distance") > F.lit(MAX_TRIP_DISTANCE_MILES)),
                1,
            ).otherwise(0)
        ).alias("invalid_trip_distance_count"),
    )