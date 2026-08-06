from __future__ import annotations

import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.transform import transform_raw_row


def print_result(
    title: str,
    raw_row: dict[str, object],
) -> None:
    """테스트 행의 정제 결과를 출력한다."""
    silver, rejects = transform_raw_row(raw_row)

    print("=" * 60)
    print(title)
    print("silver:", silver)
    print("rejects:")

    for reject in rejects:
        print(
            reject.error_column,
            reject.error_reason,
            reject.error_value,
        )


def main() -> None:
    """핵심 속성 및 측정값 정제 규칙을 확인한다."""
    base_row: dict[str, object] = {
        "raw_id": 1,
        "dag_run_id": "manual_transform_test",
        "source_date": date(2026, 6, 27),
        "rent_dt": "2026-06-27",
        "rent_id": "00746",
        "rent_nm": "746. 목동2단지 상가",
        "rent_type": "정기권",
        "gender_cd": "m",
        "age_type": "~10대",
        "use_cnt": "1",
        "exer_amt": "21.77",
        "carbon_amt": "0.20",
        "move_meter": "845.81",
        "move_time": "5",
    }

    print_result(
        "정상 행",
        base_row,
    )

    missing_gender_row = {
        **base_row,
        "raw_id": 2,
        "gender_cd": "",
    }

    print_result(
        "성별 결측 행",
        missing_gender_row,
    )

    invalid_rent_id_row = {
        **base_row,
        "raw_id": 3,
        "rent_id": "0",
    }

    print_result(
        "대여소번호 0 행",
        invalid_rent_id_row,
    )

    invalid_measurement_row = {
        **base_row,
        "raw_id": 4,
        "use_cnt": "-1",
        "move_meter": "invalid",
    }

    print_result(
        "측정값 오류 행",
        invalid_measurement_row,
    )


if __name__ == "__main__":
    main()