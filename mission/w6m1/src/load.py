from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from airflow.providers.mysql.hooks.mysql import MySqlHook


logger = logging.getLogger(__name__)


class RawLoadError(RuntimeError):
    """Raw 데이터 적재 또는 검증 실패."""


RAW_INSERT_SQL = """
INSERT INTO bike_usage_raw (
    dag_run_id,
    source_date,
    page_start_index,
    page_end_index,
    row_number_in_page,
    rent_dt,
    rent_id,
    rent_nm,
    rent_type,
    gender_cd,
    age_type,
    use_cnt,
    exer_amt,
    carbon_amt,
    move_meter,
    move_time,
    raw_record
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s
)
ON DUPLICATE KEY UPDATE
    rent_dt = VALUES(rent_dt),
    rent_id = VALUES(rent_id),
    rent_nm = VALUES(rent_nm),
    rent_type = VALUES(rent_type),
    gender_cd = VALUES(gender_cd),
    age_type = VALUES(age_type),
    use_cnt = VALUES(use_cnt),
    exer_amt = VALUES(exer_amt),
    carbon_amt = VALUES(carbon_amt),
    move_meter = VALUES(move_meter),
    move_time = VALUES(move_time),
    raw_record = VALUES(raw_record),
    collected_at = CURRENT_TIMESTAMP(6)
"""


def chunked(
    values: list[tuple[Any, ...]],
    batch_size: int,
) -> Iterable[list[tuple[Any, ...]]]:
    """리스트를 지정한 크기의 배치로 나눈다."""
    if batch_size <= 0:
        raise ValueError("batch_size는 1 이상이어야 합니다.")

    for start in range(0, len(values), batch_size):
        yield values[start : start + batch_size]


def load_raw_json(path: Path) -> dict[str, Any]:
    """Raw JSON 파일을 읽고 기본 구조를 검증한다."""
    if not path.exists():
        raise RawLoadError(
            f"Raw JSON 파일이 존재하지 않습니다: {path}"
        )

    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    metadata = payload.get("metadata")
    rows = payload.get("rows")

    if not isinstance(metadata, dict):
        raise RawLoadError(
            f"metadata가 없거나 객체가 아닙니다: {path}"
        )

    if not isinstance(rows, list):
        raise RawLoadError(
            f"rows가 없거나 배열이 아닙니다: {path}"
        )

    expected_count = metadata.get("expected_count")
    actual_count = metadata.get("actual_count")

    if expected_count != len(rows):
        raise RawLoadError(
            "Raw 파일 expected_count와 실제 rows 건수가 다릅니다: "
            f"path={path}, "
            f"expected_count={expected_count}, "
            f"rows_count={len(rows)}"
        )

    if actual_count != len(rows):
        raise RawLoadError(
            "Raw 파일 actual_count와 실제 rows 건수가 다릅니다: "
            f"path={path}, "
            f"actual_count={actual_count}, "
            f"rows_count={len(rows)}"
        )

    return payload


def convert_raw_row(
    row: dict[str, Any],
    *,
    dag_run_id: str,
    source_date: str,
) -> tuple[Any, ...]:
    """API Raw 행을 MySQL INSERT 파라미터로 변환한다."""
    required_metadata = {
        "_page_start_index",
        "_page_end_index",
        "_row_number_in_page",
    }

    missing_metadata = required_metadata - set(row.keys())

    if missing_metadata:
        raise RawLoadError(
            "Raw 행에 수집 메타데이터가 없습니다: "
            f"missing={sorted(missing_metadata)}"
        )

    return (
        dag_run_id,
        source_date,
        int(row["_page_start_index"]),
        int(row["_page_end_index"]),
        int(row["_row_number_in_page"]),
        row.get("RENT_DT"),
        row.get("RENT_ID"),
        row.get("RENT_NM"),
        row.get("RENT_TYPE"),
        row.get("GENDER_CD"),
        row.get("AGE_TYPE"),
        row.get("USE_CNT"),
        row.get("EXER_AMT"),
        row.get("CARBON_AMT"),
        row.get("MOVE_METER"),
        row.get("MOVE_TIME"),
        json.dumps(
            row,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    )


def count_raw_rows(
    *,
    hook: MySqlHook,
    dag_run_id: str,
    source_date: str,
) -> int:
    """DAG Run과 날짜 기준 Raw 적재 건수를 조회한다."""
    query = """
    SELECT COUNT(*)
    FROM bike_usage_raw
    WHERE dag_run_id = %s
      AND source_date = %s
    """

    result = hook.get_first(
        query,
        parameters=(
            dag_run_id,
            source_date,
        ),
    )

    if result is None:
        return 0

    return int(result[0])


def load_raw_file_to_mysql(
    *,
    path: Path,
    mysql_conn_id: str = "bike_mysql",
    batch_size: int = 1000,
) -> dict[str, Any]:
    """날짜별 Raw JSON을 MySQL에 배치 적재하고 건수를 검증한다."""
    started_at = datetime.now()

    payload = load_raw_json(path)
    metadata = payload["metadata"]
    rows = payload["rows"]

    dag_run_id = str(metadata["dag_run_id"])
    requested_date = str(metadata["requested_date"])

    try:
        source_date = datetime.strptime(
            requested_date,
            "%Y%m%d",
        ).date()
    except ValueError as exc:
        raise RawLoadError(
            "requested_date 형식이 YYYYMMDD가 아닙니다: "
            f"{requested_date!r}"
        ) from exc

    insert_values = [
        convert_raw_row(
            row,
            dag_run_id=dag_run_id,
            source_date=source_date.isoformat(),
        )
        for row in rows
    ]

    hook = MySqlHook(
        mysql_conn_id=mysql_conn_id,
    )

    connection = hook.get_conn()

    try:
        with connection.cursor() as cursor:
            for batch_number, batch in enumerate(
                chunked(insert_values, batch_size),
                start=1,
            ):
                cursor.executemany(
                    RAW_INSERT_SQL,
                    batch,
                )

                logger.info(
                    "Raw batch inserted: "
                    "dag_run_id=%s source_date=%s "
                    "batch_number=%s batch_count=%s",
                    dag_run_id,
                    source_date,
                    batch_number,
                    len(batch),
                )

        connection.commit()

    except Exception:
        connection.rollback()

        logger.exception(
            "Raw load failed and rolled back: "
            "dag_run_id=%s source_date=%s",
            dag_run_id,
            source_date,
        )

        raise

    finally:
        connection.close()

    loaded_count = count_raw_rows(
        hook=hook,
        dag_run_id=dag_run_id,
        source_date=source_date.isoformat(),
    )

    expected_count = len(rows)

    if loaded_count != expected_count:
        raise RawLoadError(
            "Raw 적재 건수가 일치하지 않습니다: "
            f"dag_run_id={dag_run_id}, "
            f"source_date={source_date}, "
            f"expected_count={expected_count}, "
            f"loaded_count={loaded_count}"
        )

    elapsed_seconds = (
        datetime.now() - started_at
    ).total_seconds()

    logger.info(
        "Raw load succeeded: "
        "dag_run_id=%s source_date=%s "
        "expected_count=%s loaded_count=%s "
        "elapsed_seconds=%.3f",
        dag_run_id,
        source_date,
        expected_count,
        loaded_count,
        elapsed_seconds,
    )

    return {
        "dag_run_id": dag_run_id,
        "source_date": source_date.isoformat(),
        "expected_count": expected_count,
        "loaded_count": loaded_count,
        "elapsed_seconds": elapsed_seconds,
    }