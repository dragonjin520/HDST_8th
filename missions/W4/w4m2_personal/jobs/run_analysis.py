from pathlib import Path

from pyspark.sql import functions as F

from src.extract import load_parquet_data
from src.spark_session import create_spark_session
from src.transform import build_quality_summary, clean_taxi_data


PROJECT_ROOT = Path(__file__).resolve().parents[1]

TAXI_INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "taxi"
    / "yellow_tripdata_2026-02.parquet"
)


def main() -> None:
    spark = create_spark_session()

    try:
        taxi_df = load_parquet_data(
            spark=spark,
            input_path=str(TAXI_INPUT_PATH),
        )

        print("=" * 60)
        print("Input path :", TAXI_INPUT_PATH)
        print("Spark master:", spark.sparkContext.master)
        print("=" * 60)

        print("\n[Schema]")
        taxi_df.printSchema()

        print("\n[Sample rows]")
        taxi_df.show(5, truncate=False)

        print("\n[Basic information]")
        print("Row count  :", taxi_df.count())
        print("Columns    :", len(taxi_df.columns))
        print("Partitions :", taxi_df.rdd.getNumPartitions())

        print("\n[Selected columns summary]")
        taxi_df.select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ).describe().show(truncate=False)

        required_columns = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
        ]

        print("\n[Required column null counts]")
        taxi_df.select(
            [
                F.sum(
                    F.col(column).isNull().cast("int")
                ).alias(column)
                for column in required_columns
            ]
        ).show(truncate=False)

        invalid_datetime_count = taxi_df.filter(
            F.col("tpep_dropoff_datetime")
            <= F.col("tpep_pickup_datetime")
        ).count()

        non_positive_distance_count = taxi_df.filter(
            F.col("trip_distance") <= 0
        ).count()

        print("\n[Invalid value counts]")
        print("Dropoff <= pickup :", invalid_datetime_count)
        print("Distance <= 0     :", non_positive_distance_count)

        print("\n[Datetime and distance range]")
        taxi_df.agg(
            F.min("tpep_pickup_datetime").alias("min_pickup"),
            F.max("tpep_pickup_datetime").alias("max_pickup"),
            F.min("tpep_dropoff_datetime").alias("min_dropoff"),
            F.max("tpep_dropoff_datetime").alias("max_dropoff"),
            F.min("trip_distance").alias("min_distance"),
            F.max("trip_distance").alias("max_distance"),
            F.avg("trip_distance").alias("avg_distance"),
        ).show(truncate=False)

        print("\n[Quality summary]")
        quality_summary_df = build_quality_summary(taxi_df)
        quality_summary_df.show(truncate=False)

        print("\n[Cleaning result]")
        cleaned_df = clean_taxi_data(taxi_df).cache()

        input_count = taxi_df.count()
        cleaned_count = cleaned_df.count()
        rejected_count = input_count - cleaned_count
        rejected_rate = (
            rejected_count / input_count * 100
            if input_count > 0
            else 0.0
        )

        print("Input rows    :", input_count)
        print("Cleaned rows  :", cleaned_count)
        print("Rejected rows :", rejected_count)
        print(f"Rejected rate : {rejected_rate:.2f}%")

        print("\n[Cleaned sample rows]")
        cleaned_df.select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_duration_minutes",
            "trip_distance",
            "passenger_count",
        ).show(5, truncate=False)

        print("\n[Cleaned data range]")
        cleaned_df.agg(
            F.min("tpep_pickup_datetime").alias("min_pickup"),
            F.max("tpep_pickup_datetime").alias("max_pickup"),
            F.min("trip_duration_minutes").alias("min_duration_minutes"),
            F.max("trip_duration_minutes").alias("max_duration_minutes"),
            F.min("trip_distance").alias("min_distance"),
            F.max("trip_distance").alias("max_distance"),
        ).show(truncate=False)

        cleaned_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()