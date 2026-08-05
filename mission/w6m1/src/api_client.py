from __future__ import annotations

import logging
import time
from typing import Any

import requests
from requests import Response
from requests.exceptions import ConnectionError, Timeout


logger = logging.getLogger(__name__)


RETRYABLE_STATUS_CODES = {
    429,
    500,
    502,
    503,
    504,
}


class SeoulApiError(RuntimeError):
    """서울시 OpenAPI 요청 또는 응답 검증 실패."""


def build_api_url(
    *,
    base_url: str,
    api_key: str,
    response_type: str,
    request_service: str,
    start_index: int,
    end_index: int,
    rent_date: str,
) -> str:
    if start_index < 1:
        raise ValueError("start_index는 1 이상이어야 합니다.")

    if end_index < start_index:
        raise ValueError("end_index는 start_index 이상이어야 합니다.")

    if len(rent_date) != 8 or not rent_date.isdigit():
        raise ValueError("rent_date는 YYYYMMDD 형식이어야 합니다.")

    return (
        f"{base_url.rstrip('/')}/"
        f"{api_key}/"
        f"{response_type}/"
        f"{request_service}/"
        f"{start_index}/"
        f"{end_index}/"
        f"{rent_date}"
    )

def _validate_http_response(response: Response) -> None:
    if response.status_code in RETRYABLE_STATUS_CODES:
        raise SeoulApiError(
            f"재시도 가능한 HTTP 오류: status={response.status_code}"
        )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise SeoulApiError(
            f"재시도하지 않는 HTTP 오류: status={response.status_code}"
        ) from exc


def _validate_api_result(
    payload: dict[str, Any],
    response_key: str,
) -> dict[str, Any]:
    service_payload = payload.get(response_key)

    if not isinstance(service_payload, dict):
        top_level_result = payload.get("RESULT")

        raise SeoulApiError(
            f"응답에 서비스 키 '{response_key}'가 없습니다. "
            f"top_level_keys={list(payload.keys())}, "
            f"error={top_level_result}"
        )

    result = service_payload.get("RESULT", {})
    result_code = result.get("CODE")
    result_message = result.get("MESSAGE")

    if result_code != "INFO-000":
        raise SeoulApiError(
            "서울시 API 오류: "
            f"code={result_code}, message={result_message}"
        )

    return service_payload

def request_page(
    *,
    base_url: str,
    api_key: str,
    response_type: str,
    request_service: str,
    response_key: str,
    start_index: int,
    end_index: int,
    rent_date: str,
    timeout_seconds: int = 15,
    max_retries: int = 2,
    retry_delay_seconds: int = 5,
) -> dict[str, Any]:
    """
    서울시 API 한 페이지를 요청한다.

    max_retries=2이면 최초 요청 1회와 추가 재시도 2회로,
    최대 3번 호출한다.
    """
    url = build_api_url(
        base_url=base_url,
        api_key=api_key,
        response_type=response_type,
        request_service=request_service,
        start_index=start_index,
        end_index=end_index,
        rent_date=rent_date,
    )

    total_attempts = max_retries + 1

    for attempt in range(1, total_attempts + 1):
        started_at = time.monotonic()

        try:
            logger.info(
                "API request started: "
                "rent_date=%s start_index=%s end_index=%s attempt=%s",
                rent_date,
                start_index,
                end_index,
                attempt,
            )

            response = requests.get(
                url,
                timeout=timeout_seconds,
            )

            _validate_http_response(response)

            try:
                payload = response.json()
            except ValueError as exc:
                raise SeoulApiError(
                    "API 응답을 JSON으로 변환할 수 없습니다."
                ) from exc

            service_payload = _validate_api_result(
                payload=payload,
                response_key=response_key,
            )

            rows = service_payload.get("row", [])
            total_count = service_payload.get("list_total_count")

            elapsed_seconds = time.monotonic() - started_at

            logger.info(
                "API request succeeded: "
                "rent_date=%s start_index=%s end_index=%s "
                "attempt=%s received_count=%s total_count=%s "
                "response_key=%s elapsed_seconds=%.3f",
                rent_date,
                start_index,
                end_index,
                attempt,
                len(rows),
                total_count,
                response_key,
                elapsed_seconds,
            )

            return service_payload

        except (ConnectionError, Timeout, SeoulApiError) as exc:
            elapsed_seconds = time.monotonic() - started_at

            logger.warning(
                "API request failed: "
                "rent_date=%s start_index=%s end_index=%s "
                "attempt=%s/%s elapsed_seconds=%.3f error=%s",
                rent_date,
                start_index,
                end_index,
                attempt,
                total_attempts,
                elapsed_seconds,
                exc,
            )

            if attempt >= total_attempts:
                raise SeoulApiError(
                    "API 요청이 최종 실패했습니다: "
                    f"rent_date={rent_date}, "
                    f"start_index={start_index}, "
                    f"end_index={end_index}"
                ) from exc

            time.sleep(retry_delay_seconds)

    raise SeoulApiError("도달할 수 없는 코드입니다.")