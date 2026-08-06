from __future__ import annotations

import json
import logging
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.quality import validate_silver_sample


def main() -> None:
    """Silver 데이터의 5% 샘플 품질 검사를 실행한다."""
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    config_path = (
        PROJECT_ROOT
        / "config"
        / "config.json"
    )

    with config_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        config = json.load(file)

    quality_config = config["quality"]

    result = validate_silver_sample(
        dag_run_id="manual_full_collection_test",
        mysql_conn_id=config["database"]["connection_id"],
        sample_ratio=quality_config["sample_ratio"],
        random_seed=quality_config["sample_random_seed"],
        max_violation_count=(
            quality_config[
                "max_sample_violation_count"
            ]
        ),
    )

    print("=" * 60)
    print("Silver sample quality result")
    print("=" * 60)

    for key, value in result.items():
        if key == "violations":
            continue

        print(f"{key:<24}: {value}")

    print("violations:")
    for violation in result["violations"]:
        print(violation)

    print("=" * 60)


if __name__ == "__main__":
    main()