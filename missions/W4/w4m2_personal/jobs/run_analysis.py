from pathlib import Path

from pyspark.sql import functions as F

from src.extract import load_parquet_data
from src.spark_session import create_spark_session
from src.transform import build_quality_summary, clean_taxi_data
from src.metrics import (
    calculate_hourly_trip_counts,
    calculate_peak_hours,
    calculate_trip_metrics,
)


from src.output import save_dataframe
from src.weather import load_hourly_weather
from src.metrics import calculate_hourly_taxi_demand
from src.metrics import (
    calculate_demand_by_rain_condition,
    calculate_weather_correlations,
)

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


        print("\n[Trip metrics]")
        trip_metrics_df = calculate_trip_metrics(cleaned_df)
        trip_metrics_df.show(truncate=False)
        
        print("\n[Hourly trip counts]")

        hourly_trip_counts_df = calculate_hourly_trip_counts(cleaned_df)
        hourly_trip_counts_df.show(24, truncate=False)

        print("\n[Peak hours]")

        peak_hours_df = calculate_peak_hours(
            hourly_trip_counts_df,
            top_n=3,
        )
        peak_hours_df.show(truncate=False)

        print("\n[Save hourly trip counts]")

        save_dataframe(
            hourly_trip_counts_df,
            "/opt/w4m2/output/hourly_trip_counts_csv",
            "csv",
        )

        save_dataframe(
            hourly_trip_counts_df,
            "/opt/w4m2/output/hourly_trip_counts_parquet",
            "parquet",
        )

        save_dataframe(
            peak_hours_df,
            "/opt/w4m2/output/peak_hours_csv",
            "csv",
        )

        print("Hourly trip count results saved.")



        print("\n[Hourly weather data]")

        weather_df = load_hourly_weather(
            spark,
            "/opt/w4m2/data/raw/weather/nyc_weather_2026-02.json",
        )

        weather_df.show(10, truncate=False)
        weather_df.printSchema()
        print(f"Weather row count: {weather_df.count()}")



        print("\n[Hourly taxi demand]")

        hourly_taxi_demand_df = calculate_hourly_taxi_demand(cleaned_df)

        hourly_taxi_demand_df.show(10, truncate=False)
        print(f"Hourly taxi demand row count: {hourly_taxi_demand_df.count()}")



        print("\n[Taxi demand with weather]")

        taxi_weather_df = (
            hourly_taxi_demand_df
            .join(
                weather_df,
                hourly_taxi_demand_df["pickup_hour"]
                == weather_df["weather_hour"],
                "inner",
            )
            .select(
                "pickup_hour",
                "trip_count",
                "temperature_2m",
                "precipitation",
            )
            .orderBy("pickup_hour")
        )

        taxi_weather_df.show(10, truncate=False)
        print(f"Joined row count: {taxi_weather_df.count()}")




        print("\n[Weather correlations]")

        weather_correlations_df = calculate_weather_correlations(
            taxi_weather_df
        )

        weather_correlations_df.show(truncate=False)


        print("\n[Demand by rain condition]")

        rain_demand_df = calculate_demand_by_rain_condition(
            taxi_weather_df
        )

        rain_demand_df.show(truncate=False)


        print("\n[Save weather analysis results]")

        save_dataframe(
            taxi_weather_df,
            "/opt/w4m2/output/taxi_weather_hourly_parquet",
            "parquet",
        )

        save_dataframe(
            taxi_weather_df,
            "/opt/w4m2/output/taxi_weather_hourly_csv",
            "csv",
        )

        save_dataframe(
            weather_correlations_df,
            "/opt/w4m2/output/weather_correlations_csv",
            "csv",
        )

        save_dataframe(
            rain_demand_df,
            "/opt/w4m2/output/rain_demand_csv",
            "csv",
        )

        print("Weather analysis results saved.")

        


        
        cleaned_df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()