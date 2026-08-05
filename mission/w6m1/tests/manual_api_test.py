from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from src.api_client import request_page


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.json"
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / "manual_page_test.json"


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


def main() -> None:
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

    service_payload = request_page(
        base_url=api_config["base_url"],
        api_key=api_key,
        response_type=api_config["response_type"],
        request_service=api_config["request_service"],
        response_key=api_config["response_key"],
        start_index=1,
        end_index=1000,
        rent_date="20260627",
        timeout_seconds=api_config["timeout_seconds"],
        max_retries=api_config["max_retries"],
        retry_delay_seconds=api_config["retry_delay_seconds"],
    )

    rows = service_payload.get("row", [])
    total_count = service_payload.get("list_total_count")
    result = service_payload.get("RESULT", {})

    print("=" * 60)
    print("Seoul Bike API Manual Test")
    print("=" * 60)
    print(f"RESULT.CODE       : {result.get('CODE')}")
    print(f"RESULT.MESSAGE    : {result.get('MESSAGE')}")
    print(f"list_total_count  : {total_count}")
    print(f"received rows     : {len(rows)}")

    if rows:
        first_row = rows[0]

        print(f"first row keys    : {list(first_row.keys())}")
        print("first row values:")
        print(
            json.dumps(
                first_row,
                ensure_ascii=False,
                indent=2,
            )
        )

        print("first row value types:")

        for key, value in first_row.items():
            print(
                f"  {key:<15} "
                f"value={value!r:<20} "
                f"type={type(value).__name__}"
            )

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with OUTPUT_PATH.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            service_payload,
            file,
            ensure_ascii=False,
            indent=2,
        )

    print(f"saved output      : {OUTPUT_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()