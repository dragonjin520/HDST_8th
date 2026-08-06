from __future__ import annotations

import json
import logging
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.quality import validate_required_schemas


def main() -> None:
    """정제 및 집계 테이블 스키마 검사를 실행한다."""
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

    result = validate_required_schemas(
        mysql_conn_id=config["database"]["connection_id"],
        fail_on_schema_mismatch=(
            config["quality"]["fail_on_schema_mismatch"]
        ),
    )

    print("=" * 60)
    print("Schema quality result")
    print("=" * 60)

    for key, value in result.items():
        if key == "details":
            continue

        print(f"{key:<24}: {value}")

    print("details:")

    for detail in result["details"]:
        print(detail)

    print("=" * 60)


if __name__ == "__main__":
    main()