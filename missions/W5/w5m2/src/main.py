from pyspark import StorageLevel
from pyspark.sql import functions as F
from src.load import save_dataframe
from config.analysis_config import (
    ANALYSIS_END_DATE,
    ANALYSIS_START_DATE,
    APP_NAME,
    CLEANED_OUTPUT_PATH,
    DAILY_OUTPUT_PATH,
    HOURLY_OUTPUT_PATH,
    INPUT_PATH,
    MASTER_URL,
    MAX_TRIP_DISTANCE_MILES,
    MAX_TRIP_DURATION_MINUTES,
    OUTPUT_FORMAT,
    OUTPUT_MODE,
    QUALITY_OUTPUT_PATH,
)


from src.extract import create_spark_session, load_trip_data
from src.transform import (
    add_derived_columns,
    add_validation_reason,
    aggregate_daily_trips,
    aggregate_hourly_trips,
    filter_analysis_period,
    select_required_columns,
    split_valid_and_invalid_trips,
)



def main() -> None:
    spark = create_spark_session(
        app_name=APP_NAME,
        master_url=MASTER_URL,
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = load_trip_data(
            spark=spark,
            input_path=INPUT_PATH,
        )

        selected_df = select_required_columns(raw_df)

        period_filtered_df = filter_analysis_period(
            df=selected_df,
            start_date=ANALYSIS_START_DATE,
            end_date=ANALYSIS_END_DATE,
        )

        derived_df = add_derived_columns(period_filtered_df)


        validated_df = add_validation_reason(
            df=derived_df,
            max_trip_distance_miles=MAX_TRIP_DISTANCE_MILES,
            max_trip_duration_minutes=MAX_TRIP_DURATION_MINUTES,
        )

        valid_df, invalid_df = split_valid_and_invalid_trips(
            validated_df
        )

        valid_df = valid_df.persist(
            StorageLevel.MEMORY_AND_DISK
        )

        invalid_df = invalid_df.persist(
            StorageLevel.MEMORY_AND_DISK
        )

        print("=" * 60)
        print("NYC Taxi DataFrame Cleaning")
        print("=" * 60)
        print(f"Input path        : {INPUT_PATH}")
        print(f"Partition count   : {raw_df.rdd.getNumPartitions()}")

        print("\nTransformations have been defined.")
        print("No Action has been executed yet.")

        raw_row_count = raw_df.count()
        analysis_period_row_count = period_filtered_df.count()
        out_of_period_count = raw_row_count - analysis_period_row_count

        valid_trip_count = valid_df.count()
        invalid_trip_count = invalid_df.count()

        print("\nRow counts:")
        print(f"Raw row count          : {raw_row_count:,}")
        print(f"Analysis period count  : {analysis_period_row_count:,}")
        print(f"Out-of-period count    : {out_of_period_count:,}")
        print(f"Valid trip count       : {valid_trip_count:,}")
        print(f"Invalid trip count     : {invalid_trip_count:,}")

        print("\nInvalid reason summary:")

        invalid_reason_df = (
            invalid_df
            .groupBy("validation_reason")
            .agg(
                F.count("*").alias("invalid_count")
            )
            .orderBy(
                F.desc("invalid_count")
            )
        )

        invalid_reason_df.show(
            truncate=False
        )

        hourly_summary_df = aggregate_hourly_trips(valid_df)
        daily_summary_df = aggregate_daily_trips(valid_df)

        print("\nHourly summary execution plan:")
        hourly_summary_df.explain(mode="formatted")

        print("\nDaily summary execution plan:")
        daily_summary_df.explain(mode="formatted")

        



        print("\nSaving results...")

        save_dataframe(
            df=valid_df,
            output_path=CLEANED_OUTPUT_PATH,
            output_format=OUTPUT_FORMAT,
            output_mode=OUTPUT_MODE,
        )

        save_dataframe(
            df=hourly_summary_df,
            output_path=HOURLY_OUTPUT_PATH,
            output_format=OUTPUT_FORMAT,
            output_mode=OUTPUT_MODE,
            coalesce_to_one=True,
        )

        save_dataframe(
            df=daily_summary_df,
            output_path=DAILY_OUTPUT_PATH,
            output_format=OUTPUT_FORMAT,
            output_mode=OUTPUT_MODE,
            coalesce_to_one=True,
        )

        save_dataframe(
            df=invalid_reason_df,
            output_path=QUALITY_OUTPUT_PATH,
            output_format=OUTPUT_FORMAT,
            output_mode=OUTPUT_MODE,
            coalesce_to_one=True,
        )
        
        print("Results saved successfully.")

        print("\nHourly trip summary:")

        hourly_summary_df.show(
            24,
            truncate=False,
        )

        print("\nDaily trip summary:")

        daily_summary_df.show(
            31,
            truncate=False,
        )


        print("Valid trip sample:")

        valid_df.select(
            "pickup_date",
            "pickup_hour",
            "trip_duration_minutes",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ).show(
            5,
            truncate=False,
        )

        print("=" * 60)

    finally:
        if "valid_df" in locals():
            valid_df.unpersist()

        if "invalid_df" in locals():
            invalid_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()