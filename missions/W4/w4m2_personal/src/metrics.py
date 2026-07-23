from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_trip_metrics(df: DataFrame) -> DataFrame:
    """정제된 택시 데이터의 핵심 운행 지표를 계산한다."""

    return df.agg(
        F.count("*").alias("valid_trip_count"),
        F.round(
            F.avg("trip_duration_minutes"),
            2,
        ).alias("average_trip_duration_minutes"),
        F.round(
            F.avg("trip_distance"),
            2,
        ).alias("average_trip_distance_miles"),
    )

def calculate_hourly_trip_counts(df: DataFrame) -> DataFrame:
    """승차 시각 기준으로 0~23시 운행 건수를 집계한다."""

    return (
        df
        .withColumn(
            "pickup_hour",
            F.hour("tpep_pickup_datetime"),
        )
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
        )
        .orderBy("pickup_hour")
    )


def calculate_peak_hours(
    hourly_trip_counts_df: DataFrame,
    top_n: int = 3,
) -> DataFrame:
    """운행 건수가 많은 상위 시간대를 반환한다."""

    return (
        hourly_trip_counts_df
        .orderBy(
            F.desc("trip_count"),
            F.asc("pickup_hour"),
        )
        .limit(top_n)
    )

def calculate_hourly_taxi_demand(df: DataFrame) -> DataFrame:
    """택시 승차 건수를 날짜·시간 단위로 집계한다."""

    return (
        df
        .withColumn(
            "pickup_hour",
            F.date_trunc(
                "hour",
                F.col("tpep_pickup_datetime"),
            ),
        )
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
        )
        .orderBy("pickup_hour")
    )

def calculate_weather_correlations(df: DataFrame) -> DataFrame:
    """기온·강수량과 택시 수요 간 피어슨 상관계수를 계산한다."""

    return df.agg(
        F.round(
            F.corr("trip_count", "temperature_2m"),
            4,
        ).alias("temperature_trip_correlation"),

        F.round(
            F.corr("trip_count", "precipitation"),
            4,
        ).alias("precipitation_trip_correlation"),
    )

def calculate_demand_by_rain_condition(df: DataFrame) -> DataFrame:
    """강수 여부에 따른 평균 택시 수요를 비교한다."""

    return (
        df
        .withColumn(
            "rain_condition",
            F.when(
                F.col("precipitation") > 0,
                F.lit("rain"),
            ).otherwise(F.lit("no_rain")),
        )
        .groupBy("rain_condition")
        .agg(
            F.count("*").alias("hour_count"),
            F.round(
                F.avg("trip_count"),
                2,
            ).alias("average_trip_count"),
            F.round(
                F.avg("precipitation"),
                2,
            ).alias("average_precipitation"),
        )
        .orderBy("rain_condition")
    )