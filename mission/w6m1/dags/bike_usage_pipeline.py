from __future__ import annotations
# Airflow DAG: 서울 따릉이 대여 데이터 파이프라인
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pendulum
import os
from airflow.sdk import dag, get_current_context, task

from src.aggregate import aggregate_station_period_usage
from src.extract import collect_date
from src.load import load_raw_file_to_mysql
from src.quality import (
    validate_raw_silver_counts,
    validate_required_schemas,
    validate_silver_sample,
)
from src.transform import transform_raw_to_silver


PROJECT_ROOT = Path("/opt/airflow")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.json"



def load_config() -> dict[str, Any]:
    """파이프라인 설정 파일을 읽는다."""
    with CONFIG_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


CONFIG = load_config()
API_CONFIG = CONFIG["api"]
DATABASE_CONFIG = CONFIG["database"]
PIPELINE_CONFIG = CONFIG["pipeline"]
QUALITY_CONFIG = CONFIG["quality"]
AIRFLOW_CONFIG = CONFIG["airflow"]


DEFAULT_TASK_ARGS = {
    "retries": AIRFLOW_CONFIG["task_retries"],
    "retry_delay": timedelta(
        minutes=AIRFLOW_CONFIG["retry_delay_minutes"],
    ),
    "retry_exponential_backoff": (
        AIRFLOW_CONFIG["retry_exponential_backoff"]
    ),
    "max_retry_delay": timedelta(
        minutes=AIRFLOW_CONFIG["max_retry_delay_minutes"],
    ),
}

@dag(
    dag_id="bike_usage_pipeline",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="Asia/Seoul",
    ),
    catchup=False,
    max_active_runs=AIRFLOW_CONFIG["max_active_runs"],
    default_args=DEFAULT_TASK_ARGS,
    tags=[
        "w6m1",
        "seoul-bike",
        "team-2",
    ],

 )

def bike_usage_pipeline() -> None:
    """서울 따릉이 대여 데이터 수집·정제·집계 파이프라인."""



    @task(
        execution_timeout=timedelta(
            minutes=AIRFLOW_CONFIG[
                "collection_timeout_minutes"
            ],
        ),
    )
    def collect_for_date(
        rent_date: str,
    ) -> dict[str, Any]:
        """지정 날짜의 API 전체 페이지를 수집한다."""
        context = get_current_context()
        dag_run_id = context["run_id"]

        return collect_date(
            rent_date=rent_date,
            dag_run_id=dag_run_id,
            output_root=PROJECT_ROOT / "data" / "raw",
            base_url=API_CONFIG["base_url"],
            api_key=os.environ["SEOUL_API_KEY"],
            response_type=API_CONFIG["response_type"],
            request_service=API_CONFIG["request_service"],
            response_key=API_CONFIG["response_key"],
            page_size=API_CONFIG["page_size"],
            timeout_seconds=API_CONFIG["timeout_seconds"],
            max_retries=API_CONFIG["max_retries"],
            retry_delay_seconds=API_CONFIG[
                "retry_delay_seconds"
            ],
        )

    @task(
        execution_timeout=timedelta(
            minutes=AIRFLOW_CONFIG[
                "default_timeout_minutes"
            ],
        ),
    )
    def load_raw_for_date(
        collection_result: dict[str, Any],
    ) -> dict[str, Any]:
        """수집된 날짜별 Raw JSON을 MySQL에 저장한다."""
        output_path = Path(
            collection_result["output_path"]
        )

        return load_raw_file_to_mysql(
            path=output_path,
            mysql_conn_id=DATABASE_CONFIG[
                "connection_id"
            ],
            batch_size=1000,
        )



    @task(
        execution_timeout=timedelta(
            minutes=AIRFLOW_CONFIG[
                "collection_timeout_minutes"
            ],
        ),
    )
    def transform_all_dates(
        raw_load_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """모든 날짜의 Raw 저장 완료 후 Silver로 정제한다."""
        del raw_load_results

        context = get_current_context()
        dag_run_id = context["run_id"]

        return transform_raw_to_silver(
            dag_run_id=dag_run_id,
            mysql_conn_id=DATABASE_CONFIG[
                "connection_id"
            ],
            batch_size=1000,
        )


    @task(
        execution_timeout=timedelta(
            minutes=AIRFLOW_CONFIG[
                "default_timeout_minutes"
            ],
        ),
    )
    def aggregate_all_dates(
        transform_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Silver 데이터를 기간별·대여소별로 집계한다."""
        dag_run_id = str(
            transform_result["dag_run_id"]
        )

        target_dates = PIPELINE_CONFIG["target_dates"]

        period_start_date = datetime.strptime(
            min(target_dates),
            "%Y%m%d",
        ).date()

        period_end_date = datetime.strptime(
            max(target_dates),
            "%Y%m%d",
        ).date()

        return aggregate_station_period_usage(
            dag_run_id=dag_run_id,
            period_start_date=period_start_date,
            period_end_date=period_end_date,
            mysql_conn_id=DATABASE_CONFIG[
                "connection_id"
            ],
        )

    @task(
        execution_timeout=timedelta(
            minutes=AIRFLOW_CONFIG[
                "default_timeout_minutes"
            ],
        ),
    )
    def quality_sample(
        aggregate_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Silver 데이터의 5% 샘플을 재검증한다."""
        dag_run_id = str(
            aggregate_result["dag_run_id"]
        )

        return validate_silver_sample(
            dag_run_id=dag_run_id,
            mysql_conn_id=DATABASE_CONFIG[
                "connection_id"
            ],
            sample_ratio=QUALITY_CONFIG["sample_ratio"],
            random_seed=QUALITY_CONFIG[
                "sample_random_seed"
            ],
            max_violation_count=QUALITY_CONFIG[
                "max_sample_violation_count"
            ],
        )


    @task(
        execution_timeout=timedelta(
            minutes=AIRFLOW_CONFIG[
                "default_timeout_minutes"
            ],
        ),
    )
    def quality_count(
        aggregate_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Raw와 Silver 날짜별 건수를 비교한다."""
        dag_run_id = str(
            aggregate_result["dag_run_id"]
        )

        return validate_raw_silver_counts(
            dag_run_id=dag_run_id,
            mysql_conn_id=DATABASE_CONFIG[
                "connection_id"
            ],
        )


    @task(
        execution_timeout=timedelta(
            minutes=AIRFLOW_CONFIG[
                "default_timeout_minutes"
            ],
        ),
    )
    def quality_schema(
        aggregate_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Silver와 Gold 테이블 스키마를 검증한다."""
        del aggregate_result

        return validate_required_schemas(
            mysql_conn_id=DATABASE_CONFIG[
                "connection_id"
            ],
            fail_on_schema_mismatch=QUALITY_CONFIG[
                "fail_on_schema_mismatch"
            ],
        )


    collection_results = [
        collect_for_date.override(
            task_id=f"collect_{rent_date}",
        )(rent_date)
        for rent_date in PIPELINE_CONFIG["target_dates"]
    ]

    raw_load_results = [
        load_raw_for_date.override(
            task_id=f"load_raw_{rent_date}",
        )(collection_result)
        for rent_date, collection_result
        in zip(
            PIPELINE_CONFIG["target_dates"],
            collection_results,
        )
    ]

    transform_result = transform_all_dates(
        raw_load_results
    )

    aggregate_result = aggregate_all_dates(
        transform_result
    )

    quality_sample(aggregate_result)
    quality_count(aggregate_result)
    quality_schema(aggregate_result)

bike_usage_pipeline_dag = bike_usage_pipeline()