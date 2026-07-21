import platform

import sys

from pyspark.sql import SparkSession

def main() -> None:

    spark = (

        SparkSession.builder

        .appName("SparkVersionCheck")

        .getOrCreate()

    )

    print("=" * 50)

    print(f"Spark version : {spark.version}")

    print(f"Python version: {sys.version}")

    print(f"Platform      : {platform.platform()}")

    print("=" * 50)

    spark.stop()

if __name__ == "__main__":

    main()