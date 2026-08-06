from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.aggregate import aggregate_station_period_usage


def main() -> None:
    """수동 Silver 데이터를 기간별 대여소 집계로 변환한다."""
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    config_path = PROJECT_ROOT / "config" / "config.json"

    with config_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        config = json.load(file)

    target_dates = config["pipeline"]["target_dates"]

    period_start_date = datetime.strptime(
        min(target_dates),
        "%Y%m%d",
    ).date()

    period_end_date = datetime.strptime(
        max(target_dates),
        "%Y%m%d",
    ).date()

    result = aggregate_station_period_usage(
        dag_run_id="manual_full_collection_test",
        period_start_date=period_start_date,
        period_end_date=period_end_date,
        mysql_conn_id=config["database"]["connection_id"],
    )

    print("=" * 60)
    print("Station aggregation result")
    print("=" * 60)

    for key, value in result.items():
        print(f"{key:<24}: {value}")

    print("=" * 60)


if __name__ == "__main__":
    main()