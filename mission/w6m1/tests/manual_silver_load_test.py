from __future__ import annotations

import logging
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.transform import transform_raw_to_silver


def main() -> None:
    """수동 수집 데이터를 Silver와 Reject 테이블에 적재한다."""
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    result = transform_raw_to_silver(
        dag_run_id="manual_full_collection_test",
        mysql_conn_id="bike_mysql",
        batch_size=1000,
    )

    print("=" * 60)
    print("Silver load result")
    print("=" * 60)

    for key, value in result.items():
        print(f"{key:<20}: {value}")

    print("=" * 60)


if __name__ == "__main__":
    main()