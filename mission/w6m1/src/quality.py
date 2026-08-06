from __future__ import annotations

import logging
import math
import random
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from airflow.providers.mysql.hooks.mysql import MySqlHook


logger = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    """데이터 품질 검사 기준을 충족하지 못한 경우 발생하는 오류."""


@dataclass(frozen=True)
class QualityViolation:
    """샘플 데이터에서 발견한 품질 위반 정보를 표현한다."""

    silver_id: int
    raw_id: int
    column_name: str
    violation_reason: str
    value: str | None


SILVER_SELECT_SQL = """
SELECT
    silver_id,
    raw_id,
    dag_run_id,
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
    move_time
FROM bike_usage_silver
WHERE dag_run_id = %s
ORDER BY silver_id
"""

COUNT_COMPARISON_SQL = """
SELECT
    r.source_date,
    COUNT(DISTINCT r.raw_id) AS raw_count,
    COUNT(DISTINCT s.raw_id) AS silver_count,
    COUNT(DISTINCT CASE
        WHEN s.raw_id IS NULL THEN r.raw_id
    END) AS excluded_count
FROM bike_usage_raw AS r
LEFT JOIN bike_usage_silver AS s
    ON r.dag_run_id = s.dag_run_id
   AND r.raw_id = s.raw_id
WHERE r.dag_run_id = %s
GROUP BY r.source_date
ORDER BY r.source_date
"""

EXPECTED_TABLE_SCHEMAS: dict[str, dict[str, str]] = {
    "bike_usage_silver": {
        "silver_id": "bigint",
        "dag_run_id": "varchar",
        "raw_id": "bigint",
        "record_hash": "char",
        "rent_dt": "date",
        "rent_id": "varchar",
        "rent_nm": "varchar",
        "rent_type": "varchar",
        "gender_cd": "varchar",
        "age_type": "varchar",
        "use_cnt": "bigint",
        "exer_amt": "decimal",
        "carbon_amt": "decimal",
        "move_meter": "decimal",
        "move_time": "decimal",
        "cleaned_at": "datetime",
    },
    "station_period_usage": {
        "period_start_date": "date",
        "period_end_date": "date",
        "station_id": "varchar",
        "station_name": "varchar",
        "total_usage_count": "bigint",
        "total_distance_m": "decimal",
        "total_duration_min": "decimal",
        "usage_valid_row_count": "bigint",
        "distance_valid_row_count": "bigint",
        "duration_valid_row_count": "bigint",
        "source_dag_run_id": "varchar",
        "created_at": "datetime",
        "updated_at": "datetime",
    },
}

SCHEMA_SELECT_SQL = """
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_KEY
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = %s
ORDER BY ORDINAL_POSITION
"""


def normalize_text(value: Any) -> str | None:
    """값을 공백이 제거된 문자열로 바꾸고 빈 값은 None으로 만든다."""
    if value is None:
        return None

    normalized = str(value).strip()

    if normalized == "":
        return None

    return normalized


def fetch_silver_rows(
    *,
    cursor: Any,
    dag_run_id: str,
) -> list[dict[str, Any]]:
    """지정한 DAG Run의 Silver 데이터를 조회한다."""
    cursor.execute(
        SILVER_SELECT_SQL,
        (dag_run_id,),
    )

    column_names = [
        description[0]
        for description in cursor.description
    ]

    return [
        dict(zip(column_names, row))
        for row in cursor.fetchall()
    ]


def sample_rows(
    rows: list[dict[str, Any]],
    *,
    sample_ratio: float,
    random_seed: int,
) -> list[dict[str, Any]]:
    """고정 시드를 사용해 지정 비율의 행을 무작위 추출한다."""
    if not 0 < sample_ratio <= 1:
        raise ValueError(
            "sample_ratio는 0보다 크고 1 이하여야 합니다."
        )

    if not rows:
        return []

    sample_count = max(
        1,
        math.ceil(len(rows) * sample_ratio),
    )

    random_generator = random.Random(random_seed)

    return random_generator.sample(
        rows,
        sample_count,
    )


def create_violation(
    *,
    row: dict[str, Any],
    column_name: str,
    violation_reason: str,
    value: Any,
) -> QualityViolation:
    """품질 위반 레코드를 생성한다."""
    return QualityViolation(
        silver_id=int(row["silver_id"]),
        raw_id=int(row["raw_id"]),
        column_name=column_name,
        violation_reason=violation_reason,
        value=None if value is None else str(value),
    )


def validate_silver_row(
    row: dict[str, Any],
) -> list[QualityViolation]:
    """Silver 행 하나에 정제 규칙을 다시 적용한다."""
    violations: list[QualityViolation] = []

    rent_dt = row.get("rent_dt")
    rent_id = normalize_text(row.get("rent_id"))
    rent_type = normalize_text(row.get("rent_type"))
    gender_cd = normalize_text(row.get("gender_cd"))
    age_type = normalize_text(row.get("age_type"))

    if not isinstance(rent_dt, (date, datetime)):
        violations.append(
            create_violation(
                row=row,
                column_name="RENT_DT",
                violation_reason="INVALID_CORE_ATTRIBUTE",
                value=rent_dt,
            )
        )

    if rent_id is None or rent_id == "0":
        violations.append(
            create_violation(
                row=row,
                column_name="RENT_ID",
                violation_reason="INVALID_CORE_ATTRIBUTE",
                value=row.get("rent_id"),
            )
        )

    if rent_type is None:
        violations.append(
            create_violation(
                row=row,
                column_name="RENT_TYPE",
                violation_reason="INVALID_CORE_ATTRIBUTE",
                value=row.get("rent_type"),
            )
        )

    if gender_cd is None:
        violations.append(
            create_violation(
                row=row,
                column_name="GENDER_CD",
                violation_reason="INVALID_CORE_ATTRIBUTE",
                value=row.get("gender_cd"),
            )
        )
    elif gender_cd != gender_cd.upper():
        violations.append(
            create_violation(
                row=row,
                column_name="GENDER_CD",
                violation_reason="NOT_UPPERCASE",
                value=row.get("gender_cd"),
            )
        )

    if age_type is None:
        violations.append(
            create_violation(
                row=row,
                column_name="AGE_TYPE",
                violation_reason="INVALID_CORE_ATTRIBUTE",
                value=row.get("age_type"),
            )
        )

    measurement_columns = (
        "use_cnt",
        "exer_amt",
        "carbon_amt",
        "move_meter",
        "move_time",
    )

    for column_name in measurement_columns:
        value = row.get(column_name)

        if value is None:
            continue

        try:
            is_negative = Decimal(str(value)) < 0
        except Exception:
            violations.append(
                create_violation(
                    row=row,
                    column_name=column_name.upper(),
                    violation_reason="INVALID_NUMERIC_VALUE",
                    value=value,
                )
            )
            continue

        if is_negative:
            violations.append(
                create_violation(
                    row=row,
                    column_name=column_name.upper(),
                    violation_reason="NEGATIVE_NUMERIC_VALUE",
                    value=value,
                )
            )

    return violations

def validate_silver_sample(
    *,
    dag_run_id: str,
    mysql_conn_id: str = "bike_mysql",
    sample_ratio: float = 0.05,
    random_seed: int = 42,
    max_violation_count: int = 0,
) -> dict[str, Any]:
    """Silver 데이터 일부를 추출해 정제 규칙을 재검증한다."""
    hook = MySqlHook(
        mysql_conn_id=mysql_conn_id,
    )
    connection = hook.get_conn()

    try:
        with connection.cursor() as cursor:
            silver_rows = fetch_silver_rows(
                cursor=cursor,
                dag_run_id=dag_run_id,
            )
    finally:
        connection.close()

    if not silver_rows:
        raise DataQualityError(
            "품질 검사 대상 Silver 데이터가 없습니다: "
            f"dag_run_id={dag_run_id}"
        )

    sampled_rows = sample_rows(
        silver_rows,
        sample_ratio=sample_ratio,
        random_seed=random_seed,
    )

    violations: list[QualityViolation] = []

    for row in sampled_rows:
        violations.extend(
            validate_silver_row(row)
        )

    result = {
        "dag_run_id": dag_run_id,
        "silver_count": len(silver_rows),
        "sample_ratio": sample_ratio,
        "sample_count": len(sampled_rows),
        "violation_count": len(violations),
        "max_violation_count": max_violation_count,
        "passed": len(violations) <= max_violation_count,
        "violations": [
            {
                "silver_id": violation.silver_id,
                "raw_id": violation.raw_id,
                "column_name": violation.column_name,
                "violation_reason": violation.violation_reason,
                "value": violation.value,
            }
            for violation in violations[:20]
        ],
    }

    logger.info(
        "Silver sample quality result: %s",
        {
            key: value
            for key, value in result.items()
            if key != "violations"
        },
    )

    if not result["passed"]:
        raise DataQualityError(
            "Silver 샘플 품질 검사에 실패했습니다: "
            f"sample_count={result['sample_count']}, "
            f"violation_count={result['violation_count']}, "
            f"allowed={max_violation_count}, "
            f"examples={result['violations']}"
        )

    return result


def validate_raw_silver_counts(
    *,
    dag_run_id: str,
    mysql_conn_id: str = "bike_mysql",
) -> dict[str, Any]:
    """날짜별 Raw, Silver, 제외 건수 관계를 검증한다."""
    hook = MySqlHook(
        mysql_conn_id=mysql_conn_id,
    )
    connection = hook.get_conn()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                COUNT_COMPARISON_SQL,
                (dag_run_id,),
            )

            rows = cursor.fetchall()

            column_names = [
                description[0]
                for description in cursor.description
            ]
    finally:
        connection.close()

    if not rows:
        raise DataQualityError(
            "Raw-Silver 건수 비교 대상이 없습니다: "
            f"dag_run_id={dag_run_id}"
        )

    details: list[dict[str, Any]] = []
    violation_dates: list[str] = []

    total_raw_count = 0
    total_silver_count = 0
    total_excluded_count = 0

    for row in rows:
        result_row = dict(
            zip(column_names, row)
        )

        source_date = result_row["source_date"]
        raw_count = int(result_row["raw_count"])
        silver_count = int(result_row["silver_count"])
        excluded_count = int(
            result_row["excluded_count"]
        )

        expected_raw_count = (
            silver_count + excluded_count
        )

        passed = (
            raw_count == expected_raw_count
        )

        if not passed:
            violation_dates.append(
                str(source_date)
            )

        details.append(
            {
                "source_date": str(source_date),
                "raw_count": raw_count,
                "silver_count": silver_count,
                "excluded_count": excluded_count,
                "expected_raw_count": (
                    expected_raw_count
                ),
                "passed": passed,
            }
        )

        total_raw_count += raw_count
        total_silver_count += silver_count
        total_excluded_count += excluded_count

    total_passed = (
        total_raw_count
        == total_silver_count
        + total_excluded_count
    )

    result = {
        "dag_run_id": dag_run_id,
        "date_count": len(details),
        "total_raw_count": total_raw_count,
        "total_silver_count": total_silver_count,
        "total_excluded_count": (
            total_excluded_count
        ),
        "violation_date_count": len(
            violation_dates
        ),
        "violation_dates": violation_dates,
        "passed": (
            total_passed
            and len(violation_dates) == 0
        ),
        "details": details,
    }

    logger.info(
        "Raw-Silver count quality result: %s",
        {
            key: value
            for key, value in result.items()
            if key != "details"
        },
    )

    if not result["passed"]:
        raise DataQualityError(
            "Raw-Silver 건수 비교에 실패했습니다: "
            f"violation_dates={violation_dates}, "
            f"total_raw_count={total_raw_count}, "
            f"total_silver_count={total_silver_count}, "
            f"total_excluded_count={total_excluded_count}"
        )

    return result

def validate_table_schema(
    *,
    cursor: Any,
    table_name: str,
    expected_schema: dict[str, str],
) -> dict[str, Any]:
    """테이블의 필수 컬럼과 데이터 타입을 검증한다."""
    cursor.execute(
        SCHEMA_SELECT_SQL,
        (table_name,),
    )

    rows = cursor.fetchall()

    if not rows:
        return {
            "table_name": table_name,
            "table_exists": False,
            "missing_columns": list(expected_schema.keys()),
            "type_mismatches": [],
            "passed": False,
        }

    actual_columns = {
        str(row[0]): {
            "data_type": str(row[1]).lower(),
            "is_nullable": str(row[2]),
            "column_key": str(row[3]),
        }
        for row in rows
    }

    missing_columns = [
        column_name
        for column_name in expected_schema
        if column_name not in actual_columns
    ]

    type_mismatches: list[dict[str, str]] = []

    for column_name, expected_type in expected_schema.items():
        if column_name not in actual_columns:
            continue

        actual_type = actual_columns[column_name]["data_type"]

        if actual_type != expected_type.lower():
            type_mismatches.append(
                {
                    "column_name": column_name,
                    "expected_type": expected_type.lower(),
                    "actual_type": actual_type,
                }
            )

    return {
        "table_name": table_name,
        "table_exists": True,
        "missing_columns": missing_columns,
        "type_mismatches": type_mismatches,
        "passed": (
            len(missing_columns) == 0
            and len(type_mismatches) == 0
        ),
    }

def validate_required_schemas(
    *,
    mysql_conn_id: str = "bike_mysql",
    fail_on_schema_mismatch: bool = True,
) -> dict[str, Any]:
    """정제 및 집계 테이블의 필수 스키마를 검증한다."""
    hook = MySqlHook(
        mysql_conn_id=mysql_conn_id,
    )
    connection = hook.get_conn()

    try:
        with connection.cursor() as cursor:
            table_results = [
                validate_table_schema(
                    cursor=cursor,
                    table_name=table_name,
                    expected_schema=expected_schema,
                )
                for table_name, expected_schema
                in EXPECTED_TABLE_SCHEMAS.items()
            ]
    finally:
        connection.close()

    failed_tables = [
        result["table_name"]
        for result in table_results
        if not result["passed"]
    ]

    result = {
        "checked_table_count": len(table_results),
        "failed_table_count": len(failed_tables),
        "failed_tables": failed_tables,
        "passed": len(failed_tables) == 0,
        "details": table_results,
    }

    logger.info(
        "Schema quality result: %s",
        {
            key: value
            for key, value in result.items()
            if key != "details"
        },
    )

    if fail_on_schema_mismatch and not result["passed"]:
        raise DataQualityError(
            "필수 테이블 스키마 검증에 실패했습니다: "
            f"failed_tables={failed_tables}, "
            f"details={table_results}"
        )

    return result