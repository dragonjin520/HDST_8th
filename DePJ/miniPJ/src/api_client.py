from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE_NAME = "citydata"


def fetch_citydata_xml(area_name: str) -> str:
    api_key = os.getenv("SEOUL_API_KEY")

    if not api_key:
        raise RuntimeError("SEOUL_API_KEY 환경변수가 없습니다.")

    encoded_area_name = quote(area_name, safe="")

    url = (
        f"{BASE_URL}/"
        f"{api_key}/xml/{SERVICE_NAME}/1/5/"
        f"{encoded_area_name}"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    if not response.encoding:
        response.encoding = "utf-8"

    return response.text


def save_xml(xml_text: str, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(xml_text, encoding="utf-8")


if __name__ == "__main__":
    area_name = "광화문·덕수궁"

    xml_text = fetch_citydata_xml(area_name)
    save_xml(
        xml_text,
        "data/raw/citydata_poi009.xml",
    )

    print("API 호출 및 XML 저장 성공")
    print("장소:", area_name)
    print("저장 경로: data/raw/citydata_poi009.xml")
    print("응답 길이:", len(xml_text))