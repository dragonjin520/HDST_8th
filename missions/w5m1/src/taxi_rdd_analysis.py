import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession


def delete_output_path_if_exists(spark, output_path):
    hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(output_path)
    file_system = path.getFileSystem(hadoop_config)

    if file_system.exists(path):
        file_system.delete(path, True)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="NYC Taxi 데이터를 RDD API로 분석합니다."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="애플리케이션 설정 JSON 파일 경로",
    )
    return parser.parse_args()


def load_config(config_path):
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {config_path}")

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def create_spark_session(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def load_parquet_as_rdd(spark, input_path):
    dataframe = spark.read.parquet(input_path)

    selected_dataframe = dataframe.select(
        "tpep_pickup_datetime",
        "fare_amount",
        "trip_distance",
    )

    return selected_dataframe.rdd

def parse_trip(row):
    try:
        pickup_datetime = row["tpep_pickup_datetime"]
        fare_amount = row["fare_amount"]
        trip_distance = row["trip_distance"]

        if (
            pickup_datetime is None
            or fare_amount is None
            or trip_distance is None
        ):
            return None

        return (
            pickup_datetime.date().isoformat(),
            float(fare_amount),
            float(trip_distance),
        )

    except (TypeError, ValueError, AttributeError):
        return None


def main():
    args = parse_arguments()
    config = load_config(args.config)

    spark = create_spark_session(config["app_name"])
    spark.sparkContext.setLogLevel("WARN")

    try:
        input_format = config["input_format"].lower()
        input_path = config["input_path"]

        if input_format != "parquet":
            raise ValueError(
                f"현재 단계에서는 parquet 형식만 지원합니다: {input_format}"
            )

        raw_rdd = load_parquet_as_rdd(spark, input_path)

        parsed_rdd = raw_rdd.map(parse_trip)

        valid_trip_rdd = (
            parsed_rdd
            .filter(lambda trip: trip is not None)
            .filter(
                lambda trip: (
                    trip[1] > config["min_fare_amount"]
                    and trip[2] > config["min_trip_distance"]
                )
            )
            .cache()
        )

        summary = (
            valid_trip_rdd
            .map(
                lambda trip: (
                    1,
                    trip[1],
                    trip[2],
                )
            )
            .reduce(
                lambda left, right: (
                    left[0] + right[0],
                    left[1] + right[1],
                    left[2] + right[2],
                )
            )
        )
        daily_metrics_rdd = (
            valid_trip_rdd
            .map(
                lambda trip: (
                    trip[0],
                    (1, trip[1]),
                )
            )
            .reduceByKey(
                lambda left, right: (
                    left[0] + right[0],
                    left[1] + right[1],
                )
            )
            .sortByKey()
        )

        daily_output_rdd = (
            daily_metrics_rdd
            .map(
                lambda item: (
                    f"{item[0]},"
                    f"{item[1][0]},"
                    f"{item[1][1]:.2f}"
                )
            )
        )

        total_trip_count = summary[0]
        total_revenue = summary[1]
        total_distance = summary[2]

        average_trip_distance = (
            total_distance / total_trip_count
            if total_trip_count > 0
            else 0
        )

        raw_row_count = raw_rdd.count()
        valid_trip_count = total_trip_count
        invalid_trip_count = raw_row_count - valid_trip_count
        daily_metrics_sample = daily_metrics_rdd.take(10)
        summary_output_rdd = spark.sparkContext.parallelize(
            [
                "total_trip_count,total_revenue,average_trip_distance",
                (
                    f"{total_trip_count},"
                    f"{total_revenue:.2f},"
                    f"{average_trip_distance:.2f}"
                ),
            ],
            1,
        )
        daily_output_with_header_rdd = (
            spark.sparkContext.parallelize(
                ["pickup_date,trip_count,total_revenue"],
                1,
            )
            .union(daily_output_rdd)
        )

        summary_output_path = config["summary_output_path"]
        daily_output_path = config["daily_output_path"]

        delete_output_path_if_exists(spark, summary_output_path)
        delete_output_path_if_exists(spark, daily_output_path)

        summary_output_rdd.saveAsTextFile(summary_output_path)

        daily_output_with_header_rdd \
            .coalesce(1) \
            .saveAsTextFile(daily_output_path)


        print("=" * 50)
        print(config["app_name"])
        print("=" * 50)
        print(f"Input path        : {input_path}")
        print(f"Input format      : {input_format}")
        print(f"Partition count   : {raw_rdd.getNumPartitions()}")
        print(f"Raw row count     : {raw_row_count:,}")
        print(f"Valid trip count  : {valid_trip_count:,}")
        print(f"Invalid trip count: {invalid_trip_count:,}")
        print(f"Total revenue     : ${total_revenue:,.2f}")
        print(f"Average distance  : {average_trip_distance:,.2f} miles")

        print("Cleaned sample rows:")

        for trip in valid_trip_rdd.take(5):
            print(trip)

        print("Daily metrics sample:")

        for pickup_date, metrics in daily_metrics_sample:
            trip_count, revenue = metrics

            print(
                f"{pickup_date} | "
                f"Trips: {trip_count:,} | "
                f"Revenue: ${revenue:,.2f}"
            )
        print(f"Summary output    : {summary_output_path}")
        print(f"Daily output      : {daily_output_path}")
        print("=" * 50)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()