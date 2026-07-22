import random

from pyspark.sql import SparkSession


def is_inside_circle(_: int) -> int:
    x = random.random()
    y = random.random()
    return 1 if x * x + y * y <= 1 else 0


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("PiEstimation")
        .getOrCreate()
    )

    sample_count = 1_000_000

    inside_count = (
        spark.sparkContext
        .parallelize(range(sample_count), numSlices=8)
        .map(is_inside_circle)
        .sum()
    )

    pi_estimate = 4.0 * inside_count / sample_count

    print("=" * 50)
    print(f"Sample count : {sample_count}")
    print(f"Estimated Pi : {pi_estimate}")
    print("=" * 50)

    spark.stop()


if __name__ == "__main__":
    main()