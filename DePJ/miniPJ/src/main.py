from src.collector import collect_all_areas


def main() -> None:
    collect_all_areas(
        csv_path="data/city_areas.csv",
        request_interval=0.2,
    )


if __name__ == "__main__":
    main()