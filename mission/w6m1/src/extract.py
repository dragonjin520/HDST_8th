from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from src.api_client import NonRetryableSeoulApiError
from src.api_client import request_page


logger = logging.getLogger(__name__)


REQUIRED_COLUMNS = {
    "RENT_DT",
    "RENT_ID",
    "RENT_NM",
    "RENT_TYPE",
    "GENDER_CD",
    "AGE_TYPE",
    "USE_CNT",
    "MOVE_METER",
    "MOVE_TIME",
}

OPTIONAL_COLUMNS = {
    "EXER_AMT",
    "CARBON_AMT",
}


class CollectionValidationError(RuntimeError):
    """수집 데이터의 건수 또는 구조 검증 실패."""


def parse_total_count(value: Any) -> int:
    """API의 list_total_count 값을 양의 정수로 변환한다."""
    try:
        total_count = int(value)
    except (TypeError, ValueError) as exc:
        raise CollectionValidationError(
            f"list_total_count를 정수로 변환할 수 없습니다: {value!r}"
        ) from exc

    if total_count < 0:
        raise CollectionValidationError(
            f"list_total_count가 음수입니다: {total_count}"
        )

    return total_count


def validate_row_schema(
    rows: list[dict[str, Any]],
    *,
    rent_date: str,
    start_index: int,
    end_index: int,
) -> None:
    """페이지 내 모든 행의 필수 컬럼 존재 여부를 검사한다."""
    for row_offset, row in enumerate(rows):
        if not isinstance(row, dict):
            raise CollectionValidationError(
                "row 데이터가 객체가 아닙니다: "
                f"rent_date={rent_date}, "
                f"start_index={start_index}, "
                f"row_offset={row_offset}"
            )

        missing_columns = REQUIRED_COLUMNS - set(row.keys())

        if missing_columns:
            raise CollectionValidationError(
                "필수 컬럼이 누락되었습니다: "
                f"rent_date={rent_date}, "
                f"start_index={start_index}, "
                f"end_index={end_index}, "
                f"row_offset={row_offset}, "
                f"missing_columns={sorted(missing_columns)}"
            )


def add_collection_metadata(
    rows: list[dict[str, Any]],
    *,
    requested_date: str,
    page_start_index: int,
    page_end_index: int,
) -> list[dict[str, Any]]:
    """
    API 원본 행에 수집 추적용 메타데이터를 추가한다.

    누락된 선택 컬럼은 None으로 정규화한다.
    """
    enriched_rows: list[dict[str, Any]] = []

    for row_number, row in enumerate(rows, start=1):
        normalized_row = {
            **row,
            "EXER_AMT": row.get("EXER_AMT"),
            "CARBON_AMT": row.get("CARBON_AMT"),
        }

        enriched_row = {
            **normalized_row,
            "_requested_date": requested_date,
            "_page_start_index": page_start_index,
            "_page_end_index": page_end_index,
            "_row_number_in_page": row_number,
        }

        enriched_rows.append(enriched_row)

    return enriched_rows


def collect_date(
    *,
    rent_date: str,
    dag_run_id: str,
    output_root: Path,
    base_url: str,
    api_key: str,
    response_type: str,
    request_service: str,
    response_key: str,
    page_size: int,
    timeout_seconds: int,
    max_retries: int,
    retry_delay_seconds: int,
) -> dict[str, Any]:
    """특정 날짜의 모든 API 페이지를 수집하고 Raw JSON으로 저장한다."""
    if page_size <= 0:
        raise ValueError("page_size는 1 이상이어야 합니다.")

    collection_started_at = time.monotonic()

    first_start_index = 1
    first_end_index = page_size

    logger.info(
        "Date collection started: "
        "rent_date=%s dag_run_id=%s page_size=%s",
        rent_date,
        dag_run_id,
        page_size,
    )

    first_payload = request_page(
        base_url=base_url,
        api_key=api_key,
        response_type=response_type,
        request_service=request_service,
        response_key=response_key,
        start_index=first_start_index,
        end_index=first_end_index,
        rent_date=rent_date,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
    )

    total_count = parse_total_count(
        first_payload.get("list_total_count")
    )
    first_rows = first_payload.get("row", [])

    validate_row_schema(
        first_rows,
        rent_date=rent_date,
        start_index=first_start_index,
        end_index=first_end_index,
    )

    expected_first_page_count = min(
        page_size,
        total_count,
    )

    if len(first_rows) != expected_first_page_count:
        raise CollectionValidationError(
            "첫 페이지 예상 건수와 실제 건수가 다릅니다: "
            f"rent_date={rent_date}, "
            f"expected_page_count={expected_first_page_count}, "
            f"received_count={len(first_rows)}"
        )

    collected_rows = add_collection_metadata(
        first_rows,
        requested_date=rent_date,
        page_start_index=first_start_index,
        page_end_index=min(first_end_index, total_count),
    )

    logger.info(
        "Page collection completed: "
        "rent_date=%s start_index=%s end_index=%s "
        "received_count=%s expected_total_count=%s",
        rent_date,
        first_start_index,
        min(first_end_index, total_count),
        len(first_rows),
        total_count,
    )

    next_start_index = first_end_index + 1

    while next_start_index <= total_count:
        next_end_index = min(
            next_start_index + page_size - 1,
            total_count,
        )

        page_payload = request_page(
            base_url=base_url,
            api_key=api_key,
            response_type=response_type,
            request_service=request_service,
            response_key=response_key,
            start_index=next_start_index,
            end_index=next_end_index,
            rent_date=rent_date,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
        )

        page_total_count = parse_total_count(
            page_payload.get("list_total_count")
        )

        if page_total_count != total_count:
            raise CollectionValidationError(
                "페이지 수집 중 list_total_count가 변경되었습니다: "
                f"rent_date={rent_date}, "
                f"initial_total_count={total_count}, "
                f"page_total_count={page_total_count}, "
                f"start_index={next_start_index}, "
                f"end_index={next_end_index}"
            )

        page_rows = page_payload.get("row", [])

        validate_row_schema(
            page_rows,
            rent_date=rent_date,
            start_index=next_start_index,
            end_index=next_end_index,
        )

        expected_page_count = (
            next_end_index - next_start_index + 1
        )

        if len(page_rows) != expected_page_count:
            raise CollectionValidationError(
                "페이지 예상 건수와 실제 건수가 다릅니다: "
                f"rent_date={rent_date}, "
                f"start_index={next_start_index}, "
                f"end_index={next_end_index}, "
                f"expected_page_count={expected_page_count}, "
                f"received_count={len(page_rows)}"
            )

        collected_rows.extend(
            add_collection_metadata(
                page_rows,
                requested_date=rent_date,
                page_start_index=next_start_index,
                page_end_index=next_end_index,
            )
        )

        logger.info(
            "Page collection completed: "
            "rent_date=%s start_index=%s end_index=%s "
            "received_count=%s accumulated_count=%s "
            "expected_total_count=%s",
            rent_date,
            next_start_index,
            next_end_index,
            len(page_rows),
            len(collected_rows),
            total_count,
        )

        next_start_index = next_end_index + 1

    actual_count = len(collected_rows)

    if actual_count != total_count:
        raise CollectionValidationError(
            "날짜별 전체 수집 건수가 일치하지 않습니다: "
            f"rent_date={rent_date}, "
            f"expected_count={total_count}, "
            f"actual_count={actual_count}"
        )

    expected_response_date = datetime.strptime(
        rent_date,
        "%Y%m%d",
    ).strftime("%Y-%m-%d")

    mismatched_date_count = sum(
        row.get("RENT_DT") != expected_response_date
        for row in collected_rows
    )

    if mismatched_date_count:
        raise CollectionValidationError(
            "요청 날짜와 다른 데이터가 포함되어 있습니다: "
            f"rent_date={rent_date}, "
            f"expected_response_date={expected_response_date}, "
            f"mismatched_date_count={mismatched_date_count}"
        )

    output_directory = output_root / dag_run_id
    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = output_directory / f"{rent_date}.json"

    output_payload = {
        "metadata": {
            "dag_run_id": dag_run_id,
            "requested_date": rent_date,
            "response_date": expected_response_date,
            "expected_count": total_count,
            "actual_count": actual_count,
            "page_size": page_size,
            "collected_at": datetime.now().astimezone().isoformat(),
        },
        "rows": collected_rows,
    }

    temporary_path = output_path.with_suffix(".json.tmp")

    with temporary_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            output_payload,
            file,
            ensure_ascii=False,
        )

    temporary_path.replace(output_path)

    elapsed_seconds = time.monotonic() - collection_started_at

    logger.info(
        "Date collection succeeded: "
        "rent_date=%s expected_count=%s actual_count=%s "
        "output_path=%s elapsed_seconds=%.3f",
        rent_date,
        total_count,
        actual_count,
        output_path,
        elapsed_seconds,
    )

    return {
        "rent_date": rent_date,
        "expected_count": total_count,
        "actual_count": actual_count,
        "output_path": str(output_path),
        "elapsed_seconds": elapsed_seconds,
    }