from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def select_required_columns(df: DataFrame) -> DataFrame:
    """분석과 데이터 품질 검증에 필요한 컬럼만 선택한다."""
    return df.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    """운행 시간과 집계용 날짜·시간 컬럼을 생성한다."""
    return (
        df.withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            )
            / 60.0,
        )
        .withColumn(
            "pickup_date",
            F.to_date("tpep_pickup_datetime"),
        )
        .withColumn(
            "pickup_hour",
            F.hour("tpep_pickup_datetime"),
        )
    )

def filter_analysis_period(
    df: DataFrame,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """승차 시각을 기준으로 분석 기간 안의 데이터만 남긴다."""
    return df.filter(
        (F.col("tpep_pickup_datetime") >= F.lit(start_date))
        & (F.col("tpep_pickup_datetime") < F.lit(end_date))
    )

def add_validation_reason(
    df: DataFrame,
    max_trip_distance_miles: float,
    max_trip_duration_minutes: float,
) -> DataFrame:
    """각 행의 첫 번째 무효 사유를 validation_reason 컬럼에 기록한다."""
    return df.withColumn(
        "validation_reason",
        F.when(
            F.col("tpep_pickup_datetime").isNull()
            | F.col("tpep_dropoff_datetime").isNull()
            | F.col("trip_distance").isNull()
            | F.col("fare_amount").isNull()
            | F.col("total_amount").isNull(),
            F.lit("missing_required_value"),
        )
        .when(
            F.col("tpep_dropoff_datetime")
            <= F.col("tpep_pickup_datetime"),
            F.lit("invalid_datetime_order"),
        )
        .when(
            F.col("trip_distance") <= 0,
            F.lit("non_positive_distance"),
        )
        .when(
            F.col("trip_distance") > max_trip_distance_miles,
            F.lit("excessive_distance"),
        )
        .when(
            F.col("trip_duration_minutes") > max_trip_duration_minutes,
            F.lit("excessive_duration"),
        )
        .when(
            F.col("fare_amount") < 0,
            F.lit("negative_fare"),
        )
        .when(
            F.col("total_amount") < 0,
            F.lit("negative_total_amount"),
        ),
    )


def split_valid_and_invalid_trips(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """검증 사유 존재 여부를 기준으로 유효·무효 데이터를 분리한다."""
    valid_df = df.filter(
        F.col("validation_reason").isNull()
    ).drop("validation_reason")

    invalid_df = df.filter(
        F.col("validation_reason").isNotNull()
    )

    return valid_df, invalid_df


def aggregate_hourly_trips(df: DataFrame) -> DataFrame:
    """승차 시간대별 운행 통계를 계산한다."""
    return (
        df.groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(
                F.avg("trip_distance"),
                2,
            ).alias("average_trip_distance"),
            F.round(
                F.avg("trip_duration_minutes"),
                2,
            ).alias("average_trip_duration_minutes"),
            F.round(
                F.sum("total_amount"),
                2,
            ).alias("total_revenue"),
            F.round(
                F.avg("total_amount"),
                2,
            ).alias("average_revenue_per_trip"),
        )
        .orderBy("pickup_hour")
    )


def aggregate_daily_trips(df: DataFrame) -> DataFrame:
    """승차 일자별 운행 통계를 계산한다."""
    return (
        df.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(
                F.avg("trip_distance"),
                2,
            ).alias("average_trip_distance"),
            F.round(
                F.avg("trip_duration_minutes"),
                2,
            ).alias("average_trip_duration_minutes"),
            F.round(
                F.sum("total_amount"),
                2,
            ).alias("total_revenue"),
        )
        .orderBy("pickup_date")
    )