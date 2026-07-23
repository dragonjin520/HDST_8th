from src.weather import download_hourly_weather


def main() -> None:
    download_hourly_weather(
        start_date="2026-02-01",
        end_date="2026-02-28",
        output_path="data/raw/weather/nyc_weather_2026-02.json",
    )


if __name__ == "__main__":
    main()