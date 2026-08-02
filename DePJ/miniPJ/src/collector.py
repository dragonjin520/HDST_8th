from __future__ import annotations

import csv
import time
from dataclasses import dataclass
from pathlib import Path

from src.api_client import fetch_citydata_xml
from src.parser import parse_city_population_xml
from src.repository import save_city_population


@dataclass(frozen=True)
class CityArea:
    area_code: str
    area_name: str


def load_city_areas(csv_path: str) -> list[CityArea]:
    path = Path(csv_path)

    if not path.exists():
        raise FileNotFoundError(
            f"장소 목록 파일을 찾을 수 없습니다: {csv_path}"
        )

    areas: list[CityArea] = []

    with path.open(
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        reader = csv.DictReader(file)

        required_columns = {"area_code", "area_name"}
        actual_columns = set(reader.fieldnames or [])

        missing_columns = required_columns - actual_columns

        if missing_columns:
            raise ValueError(
                "CSV 필수 컬럼이 없습니다: "
                + ", ".join(sorted(missing_columns))
            )

        for row_number, row in enumerate(reader, start=2):
            area_code = (row.get("area_code") or "").strip()
            area_name = (row.get("area_name") or "").strip()

            if not area_code or not area_name:
                raise ValueError(
                    f"CSV {row_number}번째 행의 장소 정보가 비어 있습니다."
                )

            areas.append(
                CityArea(
                    area_code=area_code,
                    area_name=area_name,
                )
            )

    if not areas:
        raise ValueError("수집할 장소가 없습니다.")

    return areas


def collect_area(area: CityArea) -> None:
    xml_text = fetch_citydata_xml(area.area_name)
    parsed_data = parse_city_population_xml(xml_text)

    if parsed_data.realtime.area_code != area.area_code:
        raise ValueError(
            "CSV 장소 코드와 API 응답 장소 코드가 다릅니다. "
            f"CSV={area.area_code}, "
            f"API={parsed_data.realtime.area_code}"
        )

    save_city_population(parsed_data)

    print(
        f"[SUCCESS] {area.area_code} / {area.area_name} | "
        f"기준 시각={parsed_data.realtime.population_time} | "
        f"현재 1건 + 예측 {len(parsed_data.forecasts)}건"
    )


def collect_all_areas(
    csv_path: str = "data/city_areas.csv",
    request_interval: float = 0.2,
) -> None:
    areas = load_city_areas(csv_path)

    success_count = 0
    failure_count = 0

    print(f"수집 대상 장소 수: {len(areas)}")

    for index, area in enumerate(areas, start=1):
        print(
            f"[{index}/{len(areas)}] "
            f"{area.area_code} / {area.area_name} 수집 시작"
        )

        try:
            collect_area(area)
            success_count += 1

        except Exception as exc:
            failure_count += 1

            print(
                f"[FAILED] {area.area_code} / "
                f"{area.area_name} | {exc}"
            )

        if index < len(areas):
            time.sleep(request_interval)

    print("=" * 60)
    print("전체 수집 완료")
    print(f"성공: {success_count}")
    print(f"실패: {failure_count}")
    print("=" * 60)


if __name__ == "__main__":
    collect_all_areas()