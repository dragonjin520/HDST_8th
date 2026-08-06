from __future__ import annotations

import hashlib
import logging

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from datetime import date, datetime
from typing import Any, Iterable

from airflow.providers.mysql.hooks.mysql import MySqlHook

logger = logging.getLogger(__name__)

class SilverTransformError(RuntimeError):
    """Raw 데이터를 Silver 형태로 변환하는 중 발생한 오류."""


@dataclass(frozen=True)
class RejectRecord:
    """정제 과정에서 발견한 오류 정보를 표현한다."""

    raw_id: int
    dag_run_id: str
    source_date: date
    error_column: str
    error_value: str | None
    error_reason: str
    error_message: str


@dataclass(frozen=True)
class SilverRecord:
    """정제를 통과한 Silver 데이터를 표현한다."""

    raw_id: int
    dag_run_id: str
    record_hash: str

    rent_dt: date
    rent_id: str
    rent_nm: str | None
    rent_type: str
    gender_cd: str
    age_type: str

    use_cnt: int | None
    exer_amt: Decimal | None
    carbon_amt: Decimal | None
    move_meter: Decimal | None
    move_time: Decimal | None

RAW_SELECT_SQL = """
SELECT
    raw_id,
    dag_run_id,
    source_date,
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
FROM bike_usage_raw
WHERE dag_run_id = %s
ORDER BY source_date, raw_id
"""


SILVER_INSERT_SQL = """
INSERT INTO bike_usage_silver (
    dag_run_id,
    raw_id,
    record_hash,
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
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    record_hash = VALUES(record_hash),
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
    cleaned_at = CURRENT_TIMESTAMP(6)
"""


REJECT_INSERT_SQL = """
INSERT INTO bike_usage_reject (
    dag_run_id,
    raw_id,
    source_date,
    error_column,
    error_value,
    error_reason,
    error_message
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    error_value = VALUES(error_value),
    error_message = VALUES(error_message),
    rejected_at = CURRENT_TIMESTAMP(6)
"""



def normalize_text(value: Any) -> str | None:
    """값을 공백이 제거된 문자열로 변환하고 빈 값은 None으로 만든다."""
    if value is None:
        return None

    normalized = str(value).strip()

    if normalized == "":
        return None

    return normalized

def normalize_gender(value: Any) -> str | None:
    """성별 코드를 공백 제거 후 대문자로 정규화한다."""
    normalized = normalize_text(value)

    if normalized is None:
        return None

    return normalized.upper()


def parse_date_value(value: Any) -> date | None:
    """YYYY-MM-DD 또는 YYYYMMDD 문자열을 date로 변환한다."""
    normalized = normalize_text(value)

    if normalized is None:
        return None

    for date_format in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(
                normalized,
                date_format,
            ).date()
        except ValueError:
            continue

    return None


def parse_nonnegative_decimal(
    value: Any,
) -> tuple[Decimal | None, str | None]:
    """
    값을 0 이상의 Decimal로 변환한다.

    변환 실패 또는 음수이면 값은 None으로 반환하고
    오류 사유를 함께 반환한다.
    """
    normalized = normalize_text(value)

    if normalized is None:
        return None, None

    try:
        parsed = Decimal(normalized)
    except InvalidOperation:
        return None, "INVALID_NUMERIC_VALUE"

    if not parsed.is_finite():
        return None, "INVALID_NUMERIC_VALUE"

    if parsed < 0:
        return None, "NEGATIVE_NUMERIC_VALUE"

    return parsed, None


def parse_nonnegative_integer(
    value: Any,
) -> tuple[int | None, str | None]:
    """값을 0 이상의 정수로 변환한다."""
    parsed, error_reason = parse_nonnegative_decimal(value)

    if error_reason is not None:
        return None, error_reason

    if parsed is None:
        return None, None

    if parsed != parsed.to_integral_value():
        return None, "INVALID_NUMERIC_VALUE"

    return int(parsed), None



def make_reject(
    *,
    raw_id: int,
    dag_run_id: str,
    source_date: date,
    error_column: str,
    error_value: Any,
    error_reason: str,
    error_message: str,
) -> RejectRecord:
    """정제 오류 정보를 RejectRecord로 생성한다."""
    return RejectRecord(
        raw_id=raw_id,
        dag_run_id=dag_run_id,
        source_date=source_date,
        error_column=error_column,
        error_value=(
            None
            if error_value is None
            else str(error_value)
        ),
        error_reason=error_reason,
        error_message=error_message,
    )



def build_record_hash(
    *,
    raw_id: int,
    rent_dt: date,
    rent_id: str,
    rent_type: str,
    gender_cd: str,
    age_type: str,
) -> str:
    """Raw 행과 핵심 정제값을 기반으로 SHA-256 해시를 생성한다."""
    hash_source = "|".join(
        [
            str(raw_id),
            rent_dt.isoformat(),
            rent_id,
            rent_type,
            gender_cd,
            age_type,
        ]
    )

    return hashlib.sha256(
        hash_source.encode("utf-8")
    ).hexdigest()



def transform_raw_row(
    raw_row: dict[str, Any],
) -> tuple[SilverRecord | None, list[RejectRecord]]:
    """
    Raw 행 하나를 Silver 레코드와 Reject 목록으로 변환한다.

    핵심 속성 오류가 있으면 Silver 행을 생성하지 않는다.
    측정값 오류가 있으면 해당 값만 None으로 처리한다.
    """
    rejects: list[RejectRecord] = []

    raw_id = int(raw_row["raw_id"])
    dag_run_id = str(raw_row["dag_run_id"])

    source_date = raw_row["source_date"]

    if isinstance(source_date, datetime):
        source_date = source_date.date()

    if not isinstance(source_date, date):
        raise SilverTransformError(
            "source_date 타입이 올바르지 않습니다: "
            f"raw_id={raw_id}, value={source_date!r}"
        )

    rent_dt = parse_date_value(
        raw_row.get("rent_dt")
    )
    rent_id = normalize_text(
        raw_row.get("rent_id")
    )
    rent_nm = normalize_text(
        raw_row.get("rent_nm")
    )
    rent_type = normalize_text(
        raw_row.get("rent_type")
    )
    gender_cd = normalize_gender(
        raw_row.get("gender_cd")
    )
    age_type = normalize_text(
        raw_row.get("age_type")
    )

    if rent_dt is None:
        rejects.append(
            make_reject(
                raw_id=raw_id,
                dag_run_id=dag_run_id,
                source_date=source_date,
                error_column="RENT_DT",
                error_value=raw_row.get("rent_dt"),
                error_reason="INVALID_CORE_ATTRIBUTE",
                error_message=(
                    "RENT_DT가 없거나 날짜 형식이 올바르지 않습니다."
                ),
            )
        )

    if rent_id is None or rent_id == "0":
        rejects.append(
            make_reject(
                raw_id=raw_id,
                dag_run_id=dag_run_id,
                source_date=source_date,
                error_column="RENT_ID",
                error_value=raw_row.get("rent_id"),
                error_reason="INVALID_CORE_ATTRIBUTE",
                error_message=(
                    "RENT_ID가 비어 있거나 0입니다."
                ),
            )
        )

    if rent_type is None:
        rejects.append(
            make_reject(
                raw_id=raw_id,
                dag_run_id=dag_run_id,
                source_date=source_date,
                error_column="RENT_TYPE",
                error_value=raw_row.get("rent_type"),
                error_reason="INVALID_CORE_ATTRIBUTE",
                error_message="RENT_TYPE이 비어 있습니다.",
            )
        )

    if gender_cd is None:
        rejects.append(
            make_reject(
                raw_id=raw_id,
                dag_run_id=dag_run_id,
                source_date=source_date,
                error_column="GENDER_CD",
                error_value=raw_row.get("gender_cd"),
                error_reason="INVALID_CORE_ATTRIBUTE",
                error_message="GENDER_CD가 비어 있습니다.",
            )
        )

    if age_type is None:
        rejects.append(
            make_reject(
                raw_id=raw_id,
                dag_run_id=dag_run_id,
                source_date=source_date,
                error_column="AGE_TYPE",
                error_value=raw_row.get("age_type"),
                error_reason="INVALID_CORE_ATTRIBUTE",
                error_message="AGE_TYPE이 비어 있습니다.",
            )
        )

    core_attribute_invalid = (
        rent_dt is None
        or rent_id is None
        or rent_id == "0"
        or rent_type is None
        or gender_cd is None
        or age_type is None
    )

    if core_attribute_invalid:
        return None, rejects

    use_cnt, use_cnt_error = parse_nonnegative_integer(
        raw_row.get("use_cnt")
    )

    exer_amt, exer_amt_error = parse_nonnegative_decimal(
        raw_row.get("exer_amt")
    )
    carbon_amt, carbon_amt_error = parse_nonnegative_decimal(
        raw_row.get("carbon_amt")
    )
    move_meter, move_meter_error = parse_nonnegative_decimal(
        raw_row.get("move_meter")
    )
    move_time, move_time_error = parse_nonnegative_decimal(
        raw_row.get("move_time")
    )

    measurement_results = {
        "USE_CNT": (
            raw_row.get("use_cnt"),
            use_cnt_error,
        ),
        "EXER_AMT": (
            raw_row.get("exer_amt"),
            exer_amt_error,
        ),
        "CARBON_AMT": (
            raw_row.get("carbon_amt"),
            carbon_amt_error,
        ),
        "MOVE_METER": (
            raw_row.get("move_meter"),
            move_meter_error,
        ),
        "MOVE_TIME": (
            raw_row.get("move_time"),
            move_time_error,
        ),
    }

    for column_name, (
        original_value,
        error_reason,
    ) in measurement_results.items():
        if error_reason is None:
            continue

        rejects.append(
            make_reject(
                raw_id=raw_id,
                dag_run_id=dag_run_id,
                source_date=source_date,
                error_column=column_name,
                error_value=original_value,
                error_reason=error_reason,
                error_message=(
                    f"{column_name} 값을 0 이상의 숫자로 "
                    "변환할 수 없습니다."
                ),
            )
        )

    record_hash = build_record_hash(
        raw_id=raw_id,
        rent_dt=rent_dt,
        rent_id=rent_id,
        rent_type=rent_type,
        gender_cd=gender_cd,
        age_type=age_type,
    )

    silver_record = SilverRecord(
        raw_id=raw_id,
        dag_run_id=dag_run_id,
        record_hash=record_hash,
        rent_dt=rent_dt,
        rent_id=rent_id,
        rent_nm=rent_nm,
        rent_type=rent_type,
        gender_cd=gender_cd,
        age_type=age_type,
        use_cnt=use_cnt,
        exer_amt=exer_amt,
        carbon_amt=carbon_amt,
        move_meter=move_meter,
        move_time=move_time,
    )

    return silver_record, rejects



def chunked(
    values: list[Any],
    batch_size: int,
) -> Iterable[list[Any]]:
    """리스트를 지정된 크기의 배치로 나눈다."""
    if batch_size <= 0:
        raise ValueError("batch_size는 1 이상이어야 합니다.")

    for start_index in range(0, len(values), batch_size):
        yield values[
            start_index : start_index + batch_size
        ]

def fetch_raw_rows(
    *,
    cursor: Any,
    dag_run_id: str,
) -> list[dict[str, Any]]:
    """지정된 DAG Run의 Raw 데이터를 사전 형태로 조회한다."""
    cursor.execute(
        RAW_SELECT_SQL,
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


def silver_to_tuple(
    record: SilverRecord,
) -> tuple[Any, ...]:
    """SilverRecord를 SQL INSERT 파라미터로 변환한다."""
    return (
        record.dag_run_id,
        record.raw_id,
        record.record_hash,
        record.rent_dt,
        record.rent_id,
        record.rent_nm,
        record.rent_type,
        record.gender_cd,
        record.age_type,
        record.use_cnt,
        record.exer_amt,
        record.carbon_amt,
        record.move_meter,
        record.move_time,
    )

def reject_to_tuple(
    record: RejectRecord,
) -> tuple[Any, ...]:
    """RejectRecord를 SQL INSERT 파라미터로 변환한다."""
    return (
        record.dag_run_id,
        record.raw_id,
        record.source_date,
        record.error_column,
        record.error_value,
        record.error_reason,
        record.error_message,
    )


def transform_raw_to_silver(
    *,
    dag_run_id: str,
    mysql_conn_id: str = "bike_mysql",
    batch_size: int = 1000,
) -> dict[str, Any]:
    """Raw 데이터를 정제해 Silver와 Reject 테이블에 적재한다."""
    started_at = datetime.now()

    hook = MySqlHook(
        mysql_conn_id=mysql_conn_id,
    )
    connection = hook.get_conn()

    raw_count = 0
    silver_records: list[SilverRecord] = []
    reject_records: list[RejectRecord] = []
    excluded_raw_ids: set[int] = set()

    try:
        with connection.cursor() as cursor:
            raw_rows = fetch_raw_rows(
                cursor=cursor,
                dag_run_id=dag_run_id,
            )

            raw_count = len(raw_rows)

            if raw_count == 0:
                raise SilverTransformError(
                    "정제할 Raw 데이터가 없습니다: "
                    f"dag_run_id={dag_run_id}"
                )

            for raw_row in raw_rows:
                silver_record, row_rejects = transform_raw_row(
                    raw_row
                )

                reject_records.extend(row_rejects)

                if silver_record is None:
                    excluded_raw_ids.add(
                        int(raw_row["raw_id"])
                    )
                    continue

                silver_records.append(silver_record)

            for batch_number, batch in enumerate(
                chunked(silver_records, batch_size),
                start=1,
            ):
                cursor.executemany(
                    SILVER_INSERT_SQL,
                    [
                        silver_to_tuple(record)
                        for record in batch
                    ],
                )

                logger.info(
                    "Silver batch inserted: "
                    "dag_run_id=%s batch_number=%s "
                    "batch_count=%s",
                    dag_run_id,
                    batch_number,
                    len(batch),
                )

            for batch_number, batch in enumerate(
                chunked(reject_records, batch_size),
                start=1,
            ):
                cursor.executemany(
                    REJECT_INSERT_SQL,
                    [
                        reject_to_tuple(record)
                        for record in batch
                    ],
                )

                logger.info(
                    "Reject batch inserted: "
                    "dag_run_id=%s batch_number=%s "
                    "batch_count=%s",
                    dag_run_id,
                    batch_number,
                    len(batch),
                )

        connection.commit()

    except Exception:
        connection.rollback()

        logger.exception(
            "Raw-to-Silver transform failed and rolled back: "
            "dag_run_id=%s",
            dag_run_id,
        )

        raise

    finally:
        connection.close()

    silver_count_result = hook.get_first(
        """
        SELECT COUNT(*)
        FROM bike_usage_silver
        WHERE dag_run_id = %s
        """,
        parameters=(dag_run_id,),
    )

    reject_count_result = hook.get_first(
        """
        SELECT COUNT(*)
        FROM bike_usage_reject
        WHERE dag_run_id = %s
        """,
        parameters=(dag_run_id,),
    )

    silver_count = int(silver_count_result[0])
    reject_count = int(reject_count_result[0])

    excluded_count = len(excluded_raw_ids)

    if raw_count != silver_count + excluded_count:
        raise SilverTransformError(
            "Raw와 Silver 정제 건수 관계가 일치하지 않습니다: "
            f"raw_count={raw_count}, "
            f"silver_count={silver_count}, "
            f"excluded_count={excluded_count}"
        )

    elapsed_seconds = (
        datetime.now() - started_at
    ).total_seconds()

    result = {
        "dag_run_id": dag_run_id,
        "raw_count": raw_count,
        "silver_count": silver_count,
        "excluded_count": excluded_count,
        "reject_count": reject_count,
        "elapsed_seconds": elapsed_seconds,
    }

    logger.info(
        "Raw-to-Silver transform succeeded: %s",
        result,
    )

    return result