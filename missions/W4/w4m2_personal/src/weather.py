import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060


def download_hourly_weather(
    start_date: str,
    end_date: str,
    output_path: str,
) -> None:
    """뉴욕의 시간별 기온·강수량 데이터를 JSON으로 저장한다."""

    params = {
        "latitude": NYC_LATITUDE,
        "longitude": NYC_LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation",
        "timezone": "America/New_York",
    }

    request_url = f"{OPEN_METEO_ARCHIVE_URL}?{urlencode(params)}"

    with urlopen(request_url, timeout=30) as response:
        weather_data = json.load(response)

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(
            weather_data,
            file,
            ensure_ascii=False,
            indent=2,
        )

    print(f"Weather data saved: {output_file}")

def load_hourly_weather(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """Open-Meteo JSON을 시간별 날씨 DataFrame으로 변환한다."""

    raw_df = spark.read.option("multiline", True).json(input_path)

    weather_df = (
        raw_df
        .select(
            F.arrays_zip(
                F.col("hourly.time"),
                F.col("hourly.temperature_2m"),
                F.col("hourly.precipitation"),
            ).alias("weather_rows")
        )
        .select(F.explode("weather_rows").alias("weather"))
        .select(
            F.col("weather.time").alias("weather_time"),
            F.col("weather.temperature_2m").alias("temperature_2m"),
            F.col("weather.precipitation").alias("precipitation"),
        )
        .withColumn(
            "weather_hour",
            F.to_timestamp("weather_time"),
        )
        .drop("weather_time")
        .orderBy("weather_hour")
    )

    return weather_df