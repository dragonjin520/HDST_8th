import re
import sqlite3
from datetime import datetime
from urllib.request import Request, urlopen

import pandas as pd
from bs4 import BeautifulSoup

import country_converter as coco
# country_converter는 국가명을 표준화하거나,
# 국가명을 기준으로 대륙(continent), ISO 코드 등을 변환해주는 외부 라이브러리이다.
# 이번 과제의 GDP 원본 테이블에는 Region 컬럼이 없기 때문에,
# 국가명(Country)을 기준으로 Region 정보를 추가하기 위해 사용한다.

# =========================
# 상수 설정
# =========================

URL = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"

JSON_PATH = "Countries_by_GDP.json"
DB_NAME = "World_Economies.db"
TABLE_NAME = "Countries_by_GDP"
LOG_FILE = "etl_project_log.txt"


# =========================
# Log
# =========================

def log_progress(message):
    """
    ETL 작업 진행 상황을 로그 파일에 기록한다.
    로그 파일은 기존 내용을 지우지 않고 append 모드로 기록한다.

    log format:
    Year-Monthname-Day-Hour-Minute-Second, log
    """
    timestamp = datetime.now().strftime("%Y-%B-%d-%H-%M-%S")

    with open(LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(f"{timestamp}, {message}\n")


# =========================
# Extract
# =========================

def get_soup(url):
    """
    URL에서 HTML을 가져와 BeautifulSoup 객체로 변환한다.
    """
    request = Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"}
    )

    html = urlopen(request).read()
    soup = BeautifulSoup(html, "html.parser")

    return soup


def extract(url):
    """
    Wikipedia 페이지에서 IMF 기준 국가별 GDP 데이터를 추출한다.

    Extract 결과:
    - Country
    - GDP_USD_million

    추출한 정보는 Countries_by_GDP.json 파일로 저장한다.
    """
    log_progress("Extract phase Started")

    soup = get_soup(url)

    # Wikipedia 페이지 안의 wikitable 표들을 모두 찾는다.
    tables = soup.find_all("table", {"class": "wikitable"})

    target_table = None

    # Country/Territory와 IMF가 포함된 GDP 메인 표를 찾는다.
    for table in tables:
        table_text = table.get_text(" ", strip=True)

        if "Country/Territory" in table_text and "IMF" in table_text:
            target_table = table
            break

    if target_table is None:
        raise Exception("GDP table not found")

    data = []

    # GDP 메인 표에서 국가명과 IMF GDP 값만 추출한다.
    for row in target_table.find_all("tr"):
        cells = row.find_all(["td", "th"])

        # 국가명과 GDP 값을 가져오려면 최소 2개의 셀이 필요하다.
        if len(cells) < 2:
            continue

        country = cells[0].get_text(" ", strip=True)
        imf_gdp = cells[1].get_text(" ", strip=True)

        # 국가명 뒤 각주 제거: China [ n 1 ] -> China
        country = re.sub(r"\s*\[.*?\]", "", country).strip()

        # GDP 값 뒤 연도 제거: 98,964 (2024) -> 98,964
        imf_gdp = re.sub(r"\s*\(.*?\)", "", imf_gdp).strip()

        # 헤더, World 전체 합계, 빈 값 제거
        if country in ["Country/Territory", "World", ""] or imf_gdp == "":
            continue

        data.append({
            "Country": country,
            "GDP_USD_million": imf_gdp
        })

    raw_df = pd.DataFrame(data)


    log_progress("Extract phase Ended")

    return raw_df


# =========================
# Transform
# =========================

def transform(raw_df):
    """
    추출한 GDP 데이터를 요구사항에 맞게 가공한다.

    처리 내용:
    - GDP 값에서 쉼표, 각주, 문자 제거
    - GDP 값을 숫자형으로 변환
    - million USD 단위를 billion USD 단위로 변환
    - 소수점 둘째 자리까지 반올림
    - Region 컬럼 추가
    - GDP 내림차순 정렬
    """
    log_progress("Transform phase Started")

    gdp_df = raw_df.copy()

    # GDP 값에서 쉼표, 각주, 문자 등을 제거한다.
    gdp_df["GDP_USD_million"] = (
        gdp_df["GDP_USD_million"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(r"\[.*?\]", "", regex=True)
        .str.replace(r"\(.*?\)", "", regex=True)
        .str.replace(r"[^0-9.]", "", regex=True)
    )

    # GDP 값을 숫자형으로 변환한다.
    gdp_df["GDP_USD_million"] = pd.to_numeric(
        gdp_df["GDP_USD_million"],
        errors="coerce"
    )

    # GDP 값이 없는 행은 제거한다.
    gdp_df = gdp_df.dropna(subset=["GDP_USD_million"])

    # million USD를 billion USD로 변환하고 소수점 둘째 자리까지 반올림한다.
    gdp_df["GDP_USD_billion"] = (
        gdp_df["GDP_USD_million"] / 1000
    ).round(2)

    # country_converter를 사용해서 국가명을 Region으로 변환한다.
    converter = coco.CountryConverter()

    gdp_df["Region"] = converter.convert(
        names=gdp_df["Country"],
        to="continent",
        not_found="Other"
    )

    # 필요한 컬럼만 선택한다.
    gdp_df = gdp_df[["Country", "Region", "GDP_USD_billion"]]

    # GDP가 높은 국가부터 나오도록 정렬한다.
    gdp_df = gdp_df.sort_values(
        by="GDP_USD_billion",
        ascending=False
    ).reset_index(drop=True)

    log_progress("Transform phase Ended")

    return gdp_df

def save_to_json(df, json_path):
    """
    Transform까지 완료된 최종 데이터를 JSON 파일로 저장한다.
    """
    log_progress("Save to JSON Started")

    df.to_json(
        json_path,
        orient="records",
        indent=4,
        force_ascii=False
    )

    log_progress("Save to JSON Ended")

# =========================
# Load
# =========================

def load_to_database(df, db_name, table_name):
    """
    가공된 DataFrame을 SQLite 데이터베이스에 저장한다.
    """
    log_progress("Load phase Started")

    conn = sqlite3.connect(db_name)

    df.to_sql(
        table_name,
        conn,
        if_exists="replace",
        index=False
    )

    conn.close()

    log_progress("Load phase Ended")


# =========================
# Output
# =========================

def show_gdp_over_100b_by_sql(db_name):
    """
    SQL Query를 사용해서 GDP가 100B USD 이상인 국가를 화면에 출력한다.
    """
    query = f"""
    SELECT Country, Region, GDP_USD_billion
    FROM {TABLE_NAME}
    WHERE GDP_USD_billion >= 100
    ORDER BY GDP_USD_billion DESC;
    """

    result = run_query(db_name, query)

    print("\nGDP가 100B USD 이상인 국가")
    print(result.to_string(index=False))

    return result


def show_region_top5_average_by_sql(db_name):
    """
    SQL Query를 사용해서 각 Region별 GDP Top5 국가의 GDP 평균을 화면에 출력한다.
    """
    query = f"""
    SELECT
        Region,
        ROUND(AVG(GDP_USD_billion), 2) AS Top5_Avg_GDP_USD_billion
    FROM (
        SELECT
            Country,
            Region,
            GDP_USD_billion,
            ROW_NUMBER() OVER (
                PARTITION BY Region
                ORDER BY GDP_USD_billion DESC
            ) AS ranking
        FROM {TABLE_NAME}
    )
    WHERE ranking <= 5
    GROUP BY Region
    ORDER BY Region;
    """

    result = run_query(db_name, query)

    print("\n각 Region별 GDP Top5 국가의 GDP 평균")
    print(result.to_string(index=False))

    return result


# =========================
# SQL Query
# =========================

def run_query(db_name, query):
    """
    SQLite 데이터베이스에 SQL 쿼리를 실행하고 결과를 DataFrame으로 반환한다.
    """
    conn = sqlite3.connect(db_name)

    result = pd.read_sql(query, conn)

    conn.close()

    return result


# =========================
# ETL Pipeline
# =========================

def run_etl_pipeline():
    """
    전체 ETL 파이프라인을 실행한다.
    """
    log_progress("ETL Job Started")

    raw_df = extract(URL)
    transformed_df = transform(raw_df)

    save_to_json(transformed_df, JSON_PATH)

    load_to_database(
        transformed_df,
        DB_NAME,
        TABLE_NAME
    
    )
    # 화면 출력은 SQL Query를 사용한다.
    show_gdp_over_100b_by_sql(DB_NAME)
    show_region_top5_average_by_sql(DB_NAME)

    log_progress("ETL Job Ended")

    return transformed_df


if __name__ == "__main__":
    run_etl_pipeline()