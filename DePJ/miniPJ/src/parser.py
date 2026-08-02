from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree as ET


DATETIME_FORMAT = "%Y-%m-%d %H:%M"


@dataclass(frozen=True)
class RealtimePopulation:
    area_code: str
    area_name: str
    population_time: datetime

    congestion_level: str | None
    congestion_message: str | None

    population_min: int | None
    population_max: int | None
    population_avg: float | None

    male_population_rate: float | None
    female_population_rate: float | None

    population_rate_0: float | None
    population_rate_10: float | None
    population_rate_20: float | None
    population_rate_30: float | None
    population_rate_40: float | None
    population_rate_50: float | None
    population_rate_60: float | None
    population_rate_70: float | None

    resident_rate: float | None
    nonresident_rate: float | None

    replace_yn: str | None
    forecast_yn: str | None


@dataclass(frozen=True)
class ForecastPopulation:
    area_code: str
    area_name: str

    forecast_hour: int
    base_time: datetime
    target_time: datetime

    congestion_level: str | None

    population_min: int | None
    population_max: int | None
    population_avg: float | None


@dataclass(frozen=True)
class CityPopulationData:
    realtime: RealtimePopulation
    forecasts: list[ForecastPopulation]


def _text(
    element: ET.Element,
    tag: str,
) -> str | None:
    value = element.findtext(tag)

    if value is None:
        return None

    value = value.strip()
    return value or None


def _to_int(value: str | None) -> int | None:
    if value is None:
        return None

    return int(value.replace(",", ""))


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None

    return float(value)


def _to_datetime(value: str | None) -> datetime:
    if value is None:
        raise ValueError("필수 날짜·시간 값이 없습니다.")

    return datetime.strptime(value, DATETIME_FORMAT)


def _calculate_average(
    minimum: int | None,
    maximum: int | None,
) -> float | None:
    if minimum is None or maximum is None:
        return None

    return (minimum + maximum) / 2


def parse_city_population_xml(xml_text: str) -> CityPopulationData:
    root = ET.fromstring(xml_text)

    result_code = root.findtext("./RESULT/RESULT.CODE")
    result_message = root.findtext("./RESULT/RESULT.MESSAGE")

    if result_code != "INFO-000":
        raise ValueError(
            f"서울시 API 오류: code={result_code}, "
            f"message={result_message}"
        )

    citydata = root.find("./CITYDATA")
    if citydata is None:
        raise ValueError("CITYDATA 요소가 없습니다.")

    live = citydata.find(
        "./LIVE_PPLTN_STTS/LIVE_PPLTN_STTS"
    )
    if live is None:
        raise ValueError("실시간 인구 요소가 없습니다.")

    area_code = _text(live, "AREA_CD")
    area_name = _text(live, "AREA_NM")

    if area_code is None or area_name is None:
        raise ValueError("장소 코드 또는 장소명이 없습니다.")

    base_time = _to_datetime(
        _text(live, "PPLTN_TIME")
    )

    realtime_min = _to_int(
        _text(live, "AREA_PPLTN_MIN")
    )
    realtime_max = _to_int(
        _text(live, "AREA_PPLTN_MAX")
    )

    realtime = RealtimePopulation(
        area_code=area_code,
        area_name=area_name,
        population_time=base_time,
        congestion_level=_text(
            live,
            "AREA_CONGEST_LVL",
        ),
        congestion_message=_text(
            live,
            "AREA_CONGEST_MSG",
        ),
        population_min=realtime_min,
        population_max=realtime_max,
        population_avg=_calculate_average(
            realtime_min,
            realtime_max,
        ),
        male_population_rate=_to_float(
            _text(live, "MALE_PPLTN_RATE")
        ),
        female_population_rate=_to_float(
            _text(live, "FEMALE_PPLTN_RATE")
        ),
        population_rate_0=_to_float(
            _text(live, "PPLTN_RATE_0")
        ),
        population_rate_10=_to_float(
            _text(live, "PPLTN_RATE_10")
        ),
        population_rate_20=_to_float(
            _text(live, "PPLTN_RATE_20")
        ),
        population_rate_30=_to_float(
            _text(live, "PPLTN_RATE_30")
        ),
        population_rate_40=_to_float(
            _text(live, "PPLTN_RATE_40")
        ),
        population_rate_50=_to_float(
            _text(live, "PPLTN_RATE_50")
        ),
        population_rate_60=_to_float(
            _text(live, "PPLTN_RATE_60")
        ),
        population_rate_70=_to_float(
            _text(live, "PPLTN_RATE_70")
        ),
        resident_rate=_to_float(
            _text(live, "RESNT_PPLTN_RATE")
        ),
        nonresident_rate=_to_float(
            _text(live, "NON_RESNT_PPLTN_RATE")
        ),
        replace_yn=_text(live, "REPLACE_YN"),
        forecast_yn=_text(live, "FCST_YN"),
    )

    forecast_elements = live.findall(
        "./FCST_PPLTN/FCST_PPLTN"
    )

    forecasts: list[ForecastPopulation] = []

    for forecast_hour, element in enumerate(
        forecast_elements,
        start=1,
    ):
        target_time = _to_datetime(
            _text(element, "FCST_TIME")
        )

        population_min = _to_int(
            _text(element, "FCST_PPLTN_MIN")
        )
        population_max = _to_int(
            _text(element, "FCST_PPLTN_MAX")
        )

        forecasts.append(
            ForecastPopulation(
                area_code=area_code,
                area_name=area_name,
                forecast_hour=forecast_hour,
                base_time=base_time,
                target_time=target_time,
                congestion_level=_text(
                    element,
                    "FCST_CONGEST_LVL",
                ),
                population_min=population_min,
                population_max=population_max,
                population_avg=_calculate_average(
                    population_min,
                    population_max,
                ),
            )
        )

    if len(forecasts) != 12:
        raise ValueError(
            f"예측 데이터가 12건이 아닙니다: "
            f"{len(forecasts)}건"
        )

    return CityPopulationData(
        realtime=realtime,
        forecasts=forecasts,
    )


def parse_city_population_file(
    file_path: str,
) -> CityPopulationData:
    xml_text = Path(file_path).read_text(
        encoding="utf-8"
    )

    return parse_city_population_xml(xml_text)


if __name__ == "__main__":
    data = parse_city_population_file(
        "data/raw/citydata_poi009.xml"
    )

    print("XML 파싱 성공")
    print(
        f"장소: {data.realtime.area_code} / "
        f"{data.realtime.area_name}"
    )
    print(
        f"현재 기준 시각: "
        f"{data.realtime.population_time}"
    )
    print(
        f"현재 인구: "
        f"{data.realtime.population_min:,} ~ "
        f"{data.realtime.population_max:,}"
    )
    print(
        f"현재 평균 인구: "
        f"{data.realtime.population_avg:,.0f}"
    )
    print(f"미래 예측 건수: {len(data.forecasts)}")

    for forecast in data.forecasts:
        print(
            f"+{forecast.forecast_hour:02d}시간 | "
            f"{forecast.target_time} | "
            f"{forecast.population_min:,} ~ "
            f"{forecast.population_max:,} | "
            f"{forecast.congestion_level}"
        )