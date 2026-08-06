from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.extract import collect_date


CONFIG_PATH = PROJECT_ROOT / "config" / "config.json"
RAW_OUTPUT_ROOT = PROJECT_ROOT / "data" / "raw"


def load_config() -> dict[str, Any]:
    """프로젝트 JSON 설정 파일을 읽는다."""
    with CONFIG_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def main() -> None:
    """2026-06-27의 전체 API 데이터를 수집한다."""
    load_dotenv(PROJECT_ROOT / ".env")

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    api_key = os.getenv("SEOUL_API_KEY")

    if not api_key:
        raise RuntimeError(
            "SEOUL_API_KEY 환경 변수가 설정되지 않았습니다."
        )

    config = load_config()
    api_config = config["api"]
    target_dates = config["pipeline"]["target_dates"]
    results: list[dict[str, Any]] = []
    for rent_date in target_dates:
        result = collect_date(
            rent_date=rent_date,
            dag_run_id="manual_full_collection_test",
            output_root=RAW_OUTPUT_ROOT,
            base_url=api_config["base_url"],
            api_key=api_key,
            response_type=api_config["response_type"],
            request_service=api_config["request_service"],
            response_key=api_config["response_key"],
            page_size=api_config["page_size"],
            timeout_seconds=api_config["timeout_seconds"],
            max_retries=api_config["max_retries"],
            retry_delay_seconds=api_config[
                "retry_delay_seconds"
            ],
        )
        results.append(result)

    print("=" * 60)
    print("Full collection results")
    print("=" * 60)

    for result in results:
        print(result)
        
    print("=" * 60)

    for key, value in result.items():
        print(f"{key:<20}: {value}")

    print("=" * 60)


if __name__ == "__main__":
    main()