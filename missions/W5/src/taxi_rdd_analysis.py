import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession


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

        print("=" * 50)
        print(config["app_name"])
        print("=" * 50)
        print(f"Input path      : {input_path}")
        print(f"Input format    : {input_format}")
        print(f"Partition count : {raw_rdd.getNumPartitions()}")
        print(f"Raw row count   : {raw_rdd.count():,}")
        print("Sample rows:")

        for row in raw_rdd.take(5):
            print(row)

        print("=" * 50)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()