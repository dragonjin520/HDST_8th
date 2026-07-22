from w4m2_personal.src.spark_session import create_spark_session


def main() -> None:
    spark = create_spark_session()

    try:
        test_df = spark.range(1_000_000)

        result_df = (
            test_df
            .groupBy((test_df.id % 10).alias("group_id"))
            .count()
            .orderBy("group_id")
        )

        print("=" * 50)
        print("Spark version :", spark.version)
        print("Spark master  :", spark.sparkContext.master)
        print("Partitions    :", test_df.rdd.getNumPartitions())
        print("=" * 50)

        result_df.show()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()