from pyspark import StorageLevel
from pyspark.sql import functions as F

from config.analysis_config import (
    ANALYSIS_END_DATE,
    ANALYSIS_START_DATE,
    APP_NAME,
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
from src.load import save_dataframe
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

    validated_df = None

    try:
        raw_df = load_trip_data(
            spark=spark,
            input_path=INPUT_PATH,
        )

        selected_df = select_required_columns(raw_df)

        period_marked_df = selected_df.withColumn(
            "is_in_analysis_period",
            (
                F.col("tpep_pickup_datetime")
                >= F.lit(ANALYSIS_START_DATE)
            )
            & (
                F.col("tpep_pickup_datetime")
                < F.lit(ANALYSIS_END_DATE)
            ),
        )

        derived_df = add_derived_columns(period_marked_df)

        validated_df = add_validation_reason(
            df=derived_df,
            max_trip_distance_miles=MAX_TRIP_DISTANCE_MILES,
            max_trip_duration_minutes=MAX_TRIP_DURATION_MINUTES,
        ).persist(
            StorageLevel.MEMORY_AND_DISK
        )

        valid_df = (
            validated_df
            .filter(
                F.col("is_in_analysis_period")
                & F.col("validation_reason").isNull()
            )
            .drop(
                "validation_reason",
                "is_in_analysis_period",
            )
        )

        quality_summary_df = (

                    validated_df

                    .withColumn(

                        "quality_group",

                        F.when(

                            ~F.col("is_in_analysis_period"),

                            F.lit("out_of_period"),

                        )

                        .when(

                            F.col("validation_reason").isNull(),

                            F.lit("valid"),

                        )

                        .otherwise(

                            F.col("validation_reason")

                        ),

                    )

                    .groupBy("quality_group")

                    .agg(

                        F.count("*").alias("row_count")

                    )

                )

        hourly_summary_df = aggregate_hourly_trips(valid_df)
        daily_summary_df = aggregate_daily_trips(valid_df)

        print("=" * 60)
        print("NYC Taxi DataFrame DAG Analysis")
        print("=" * 60)
        print(f"Input path      : {INPUT_PATH}")
        print(f"Partition count : {raw_df.rdd.getNumPartitions()}")
        print("\nTransformations have been defined.")
        print("No Action has been executed yet.")

        print("\nHourly summary execution plan:")

        print("\nDaily summary execution plan:")

        print("\nSaving results...")
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

            df=quality_summary_df,

            output_path=QUALITY_OUTPUT_PATH,

            output_format=OUTPUT_FORMAT,

            output_mode=OUTPUT_MODE,

            coalesce_to_one=True,

        )

        print("Results saved successfully.")
        print("=" * 60)

    finally:
        if validated_df is not None:
            validated_df.unpersist()
        input()
        spark.stop()


if __name__ == "__main__":
    main()