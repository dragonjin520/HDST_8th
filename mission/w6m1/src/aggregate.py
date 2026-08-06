from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from airflow.providers.mysql.hooks.mysql import MySqlHook


logger = logging.getLogger(__name__)


class AggregationError(RuntimeError):
    """대여소 단위 집계 또는 Gold 반영 실패."""


STAGING_INSERT_SQL = """
INSERT INTO station_period_usage_staging (
    dag_run_id,
    period_start_date,
    period_end_date,
    station_id,
    station_name,
    total_usage_count,
    total_distance_m,
    total_duration_min,
    usage_valid_row_count,
    distance_valid_row_count,
    duration_valid_row_count
)
SELECT
    %s AS dag_run_id,
    %s AS period_start_date,
    %s AS period_end_date,
    rent_id AS station_id,
    MAX(rent_nm) AS station_name,
    COALESCE(SUM(use_cnt), 0) AS total_usage_count,
    COALESCE(SUM(move_meter), 0) AS total_distance_m,
    COALESCE(SUM(move_time), 0) AS total_duration_min,
    COUNT(use_cnt) AS usage_valid_row_count,
    COUNT(move_meter) AS distance_valid_row_count,
    COUNT(move_time) AS duration_valid_row_count
FROM bike_usage_silver
WHERE dag_run_id = %s
  AND rent_dt BETWEEN %s AND %s
GROUP BY rent_id
ON DUPLICATE KEY UPDATE
    station_name = VALUES(station_name),
    total_usage_count = VALUES(total_usage_count),
    total_distance_m = VALUES(total_distance_m),
    total_duration_min = VALUES(total_duration_min),
    usage_valid_row_count = VALUES(usage_valid_row_count),
    distance_valid_row_count = VALUES(distance_valid_row_count),
    duration_valid_row_count = VALUES(duration_valid_row_count)
"""


STAGING_COUNT_SQL = """
SELECT COUNT(*)
FROM station_period_usage_staging
WHERE dag_run_id = %s
  AND period_start_date = %s
  AND period_end_date = %s
"""


GOLD_DELETE_SQL = """
DELETE FROM station_period_usage
WHERE period_start_date = %s
  AND period_end_date = %s
"""


GOLD_INSERT_SQL = """
INSERT INTO station_period_usage (
    period_start_date,
    period_end_date,
    station_id,
    station_name,
    total_usage_count,
    total_distance_m,
    total_duration_min,
    usage_valid_row_count,
    distance_valid_row_count,
    duration_valid_row_count,
    source_dag_run_id
)
SELECT
    period_start_date,
    period_end_date,
    station_id,
    station_name,
    total_usage_count,
    total_distance_m,
    total_duration_min,
    usage_valid_row_count,
    distance_valid_row_count,
    duration_valid_row_count,
    dag_run_id
FROM station_period_usage_staging
WHERE dag_run_id = %s
  AND period_start_date = %s
  AND period_end_date = %s
"""


def aggregate_station_period_usage(
    *,
    dag_run_id: str,
    period_start_date: date,
    period_end_date: date,
    mysql_conn_id: str = "bike_mysql",
) -> dict[str, Any]:
    """Silver 데이터를 대여소 단위로 집계해 Staging과 Gold에 반영한다."""
    if period_start_date > period_end_date:
        raise ValueError(
            "period_start_date는 period_end_date보다 늦을 수 없습니다."
        )

    started_at = datetime.now()

    hook = MySqlHook(
        mysql_conn_id=mysql_conn_id,
    )
    connection = hook.get_conn()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                STAGING_INSERT_SQL,
                (
                    dag_run_id,
                    period_start_date,
                    period_end_date,
                    dag_run_id,
                    period_start_date,
                    period_end_date,
                ),
            )

            cursor.execute(
                STAGING_COUNT_SQL,
                (
                    dag_run_id,
                    period_start_date,
                    period_end_date,
                ),
            )

            staging_count = int(cursor.fetchone()[0])

            if staging_count == 0:
                raise AggregationError(
                    "집계 결과가 0건입니다: "
                    f"dag_run_id={dag_run_id}, "
                    f"period={period_start_date}~{period_end_date}"
                )

            cursor.execute(
                GOLD_DELETE_SQL,
                (
                    period_start_date,
                    period_end_date,
                ),
            )

            deleted_gold_count = cursor.rowcount

            cursor.execute(
                GOLD_INSERT_SQL,
                (
                    dag_run_id,
                    period_start_date,
                    period_end_date,
                ),
            )

            inserted_gold_count = cursor.rowcount

            if inserted_gold_count != staging_count:
                raise AggregationError(
                    "Staging과 Gold 적재 건수가 일치하지 않습니다: "
                    f"staging_count={staging_count}, "
                    f"inserted_gold_count={inserted_gold_count}"
                )

        connection.commit()

    except Exception:
        connection.rollback()

        logger.exception(
            "Station aggregation failed and rolled back: "
            "dag_run_id=%s period=%s~%s",
            dag_run_id,
            period_start_date,
            period_end_date,
        )
        raise

    finally:
        connection.close()

    elapsed_seconds = (
        datetime.now() - started_at
    ).total_seconds()

    result = {
        "dag_run_id": dag_run_id,
        "period_start_date": period_start_date.isoformat(),
        "period_end_date": period_end_date.isoformat(),
        "staging_count": staging_count,
        "deleted_gold_count": deleted_gold_count,
        "inserted_gold_count": inserted_gold_count,
        "elapsed_seconds": elapsed_seconds,
    }

    logger.info(
        "Station aggregation succeeded: %s",
        result,
    )

    return result