from __future__ import annotations

import logging
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.load import load_raw_file_to_mysql


RAW_DIRECTORY = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "manual_full_collection_test"
)


def main() -> None:
    """두 날짜의 Raw JSON 파일을 MySQL에 적재한다."""
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    target_dates = [
        "20260627",
        "20260628",
    ]

    results: list[dict[str, object]] = []

    for rent_date in target_dates:
        path = RAW_DIRECTORY / f"{rent_date}.json"

        result = load_raw_file_to_mysql(
            path=path,
            mysql_conn_id="bike_mysql",
            batch_size=1000,
        )

        results.append(result)

    print("=" * 60)
    print("Raw load results")
    print("=" * 60)

    for result in results:
        print(result)

    print("=" * 60)


if __name__ == "__main__":
    main()