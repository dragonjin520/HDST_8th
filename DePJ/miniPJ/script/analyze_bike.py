from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

BIKE_CSV_PATH = Path(
    "/Users/admin/Documents/GitHub/HDST_8th/DePJ/miniPJ/data/raw/서울특별시 공공자전거 대여이력 정보_2605.csv"
)
OUTPUT_DIR = Path("data/processed")

CHART_DIR = OUTPUT_DIR / "charts"


def read_csv_safely(file_path: Path) -> pd.DataFrame:
    """서울시 CSV를 CP949 기준으로 읽는다."""
    df = pd.read_csv(
        file_path,
        encoding="cp949",
        encoding_errors="replace",
        low_memory=False,
    )

    print(f"[성공] 파일: {file_path}")
    print("[성공] 인코딩: cp949")

    return df


def find_column(
    columns: pd.Index,
    keywords: list[str],
) -> str | None:
    """키워드가 포함된 첫 번째 컬럼을 찾는다."""
    for column in columns:
        normalized = str(column).replace(" ", "").lower()

        if all(
            keyword.replace(" ", "").lower() in normalized
            for keyword in keywords
        ):
            return str(column)

    return None


def print_basic_info(df: pd.DataFrame) -> None:
    print("\n" + "=" * 80)
    print("1. 데이터 기본 정보")
    print("=" * 80)

    print(f"행 개수: {len(df):,}")
    print(f"컬럼 개수: {len(df.columns):,}")

    print("\n컬럼 목록")
    for index, column in enumerate(df.columns, start=1):
        print(f"{index:2}. {column}")

    print("\n상위 5개 행")
    print(df.head().to_string())

    print("\n데이터 타입")
    print(df.dtypes.to_string())


def print_missing_values(df: pd.DataFrame) -> None:
    print("\n" + "=" * 80)
    print("2. 결측치 현황")
    print("=" * 80)

    missing = pd.DataFrame(
        {
            "결측치_개수": df.isna().sum(),
            "결측치_비율": (df.isna().mean() * 100).round(2),
        }
    )

    print(missing.to_string())


def detect_columns(df: pd.DataFrame) -> dict[str, str | None]:
    columns = df.columns

    detected = {
        "대여일시": (
            find_column(columns, ["대여", "일시"])
            or find_column(columns, ["대여", "일자"])
            or find_column(columns, ["대여일"])
        ),
        "반납일시": (
            find_column(columns, ["반납", "일시"])
            or find_column(columns, ["반납", "일자"])
            or find_column(columns, ["반납일"])
        ),
        "대여소명": (
            find_column(columns, ["대여", "대여소", "명"])
            or find_column(columns, ["대여소명"])
        ),
        "반납대여소명": (
            find_column(columns, ["반납", "대여소", "명"])
            or find_column(columns, ["반납대여소"])
        ),
        "대여소번호": (
            find_column(columns, ["대여", "대여소", "번호"])
            or find_column(columns, ["대여소번호"])
        ),
        "반납대여소번호": (
            find_column(columns, ["반납", "대여소", "번호"])
        ),
        "이용시간": (
            find_column(columns, ["이용", "시간"])
        ),
        "이용거리": (
            find_column(columns, ["이용", "거리"])
        ),
    }

    print("\n" + "=" * 80)
    print("3. 자동 탐지한 주요 컬럼")
    print("=" * 80)

    for name, column in detected.items():
        print(f"{name:12}: {column}")

    return detected


def analyze_datetime(
    df: pd.DataFrame,
    rental_datetime_column: str | None,
) -> pd.DataFrame:
    if rental_datetime_column is None:
        print("\n[건너뜀] 대여 일시 컬럼을 찾지 못했습니다.")
        return df

    df = df.copy()

    df["대여일시_변환"] = pd.to_datetime(
        df[rental_datetime_column],
        errors="coerce",
    )

    valid_datetime_count = df["대여일시_변환"].notna().sum()

    print("\n" + "=" * 80)
    print("4. 대여 일시 분석")
    print("=" * 80)

    print(f"날짜 변환 성공: {valid_datetime_count:,}")
    print(f"날짜 변환 실패: {len(df) - valid_datetime_count:,}")

    if valid_datetime_count == 0:
        return df

    print(f"최초 대여 시각: {df['대여일시_변환'].min()}")
    print(f"최종 대여 시각: {df['대여일시_변환'].max()}")

    df["대여일자"] = df["대여일시_변환"].dt.date
    df["대여시간"] = df["대여일시_변환"].dt.hour
    df["요일번호"] = df["대여일시_변환"].dt.dayofweek

    day_names = {
        0: "월요일",
        1: "화요일",
        2: "수요일",
        3: "목요일",
        4: "금요일",
        5: "토요일",
        6: "일요일",
    }

    df["요일"] = df["요일번호"].map(day_names)
    df["주말여부"] = df["요일번호"].isin([5, 6])

    hourly = (
        df.groupby("대여시간")
        .size()
        .rename("대여건수")
        .reset_index()
        .sort_values("대여시간")
    )

    print("\n시간대별 대여 건수")
    print(hourly.to_string(index=False))

    weekday = (
        df.groupby(["요일번호", "요일"])
        .size()
        .rename("대여건수")
        .reset_index()
        .sort_values("요일번호")
    )

    print("\n요일별 대여 건수")
    print(
        weekday[
            ["요일", "대여건수"]
        ].to_string(index=False)
    )

    weekend = (
        df.groupby("주말여부")
        .size()
        .rename("대여건수")
        .reset_index()
    )

    weekend["구분"] = weekend["주말여부"].map(
        {
            False: "평일",
            True: "주말",
        }
    )

    print("\n평일·주말 대여 건수")
    print(
        weekend[
            ["구분", "대여건수"]
        ].to_string(index=False)
    )

    daily = (
        df.groupby("대여일자")
        .size()
        .rename("대여건수")
        .reset_index()
        .sort_values("대여일자")
    )

    print("\n일별 대여 건수 상위 10일")
    print(
        daily.sort_values(
            "대여건수",
            ascending=False,
        )
        .head(10)
        .to_string(index=False)
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    hourly.to_csv(
        OUTPUT_DIR / "bike_hourly_demand.csv",
        index=False,
        encoding="utf-8-sig",
    )

    weekday.to_csv(
        OUTPUT_DIR / "bike_weekday_demand.csv",
        index=False,
        encoding="utf-8-sig",
    )

    daily.to_csv(
        OUTPUT_DIR / "bike_daily_demand.csv",
        index=False,
        encoding="utf-8-sig",
    )

    return df


def analyze_stations(
    df: pd.DataFrame,
    rental_station_column: str | None,
    return_station_column: str | None,
) -> None:
    print("\n" + "=" * 80)
    print("5. 대여소 분석")
    print("=" * 80)

    if rental_station_column:
        rental_station = (
            df[rental_station_column]
            .astype("string")
            .str.strip()
            .value_counts()
            .rename_axis("대여소")
            .reset_index(name="대여건수")
        )

        print("\n대여 건수 상위 20개 대여소")
        print(
            rental_station
            .head(20)
            .to_string(index=False)
        )

        rental_station.to_csv(
            OUTPUT_DIR / "bike_rental_station_ranking.csv",
            index=False,
            encoding="utf-8-sig",
        )
    else:
        print("[건너뜀] 대여 대여소명 컬럼을 찾지 못했습니다.")

    if return_station_column:
        return_station = (
            df[return_station_column]
            .astype("string")
            .str.strip()
            .value_counts()
            .rename_axis("반납대여소")
            .reset_index(name="반납건수")
        )

        print("\n반납 건수 상위 20개 대여소")
        print(
            return_station
            .head(20)
            .to_string(index=False)
        )

        return_station.to_csv(
            OUTPUT_DIR / "bike_return_station_ranking.csv",
            index=False,
            encoding="utf-8-sig",
        )
    else:
        print("[건너뜀] 반납 대여소명 컬럼을 찾지 못했습니다.")


def analyze_numeric_column(
    df: pd.DataFrame,
    column: str | None,
    label: str,
) -> None:
    if column is None:
        print(f"\n[건너뜀] {label} 컬럼을 찾지 못했습니다.")
        return

    values = pd.to_numeric(
        df[column],
        errors="coerce",
    )

    print(f"\n{label} 기초 통계")
    print(values.describe().to_string())

    print(f"{label} 0 이하 건수: {(values <= 0).sum():,}")
    print(f"{label} 변환 실패·결측 건수: {values.isna().sum():,}")

def configure_korean_font() -> None:
    """macOS에서 한글이 깨지지 않도록 폰트를 설정한다."""
    plt.rcParams["font.family"] = "AppleGothic"
    plt.rcParams["axes.unicode_minus"] = False


def save_demand_charts(df: pd.DataFrame) -> None:
    """시간대별·요일별·일자별 대여 건수 그래프를 저장한다."""
    if "대여일시_변환" not in df.columns:
        print("[건너뜀] 대여일시 변환 컬럼이 없어 그래프를 만들 수 없습니다.")
        return

    chart_data = df[df["대여일시_변환"].notna()].copy()

    if chart_data.empty:
        print("[건너뜀] 유효한 대여 일시 데이터가 없습니다.")
        return

    configure_korean_font()
    CHART_DIR.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------
    # 1. 24시간대별 대여 건수
    # --------------------------------------------------
    hourly = (
        chart_data.groupby("대여시간")
        .size()
        .reindex(range(24), fill_value=0)
    )

    plt.figure(figsize=(12, 6))
    plt.plot(
        hourly.index,
        hourly.values,
        marker="o",
    )

    plt.title("24시간대별 따릉이 이용 건수")
    plt.xlabel("대여 시간")
    plt.ylabel("대여 건수")
    plt.xticks(range(24))
    plt.grid(alpha=0.3)
    plt.tight_layout()

    hourly_chart_path = CHART_DIR / "bike_hourly_demand.png"
    plt.savefig(hourly_chart_path, dpi=150)
    plt.close()

    print(f"[저장] {hourly_chart_path}")

    # --------------------------------------------------
    # 2. 요일별 대여 건수
    # --------------------------------------------------
    weekday_order = [
        "월요일",
        "화요일",
        "수요일",
        "목요일",
        "금요일",
        "토요일",
        "일요일",
    ]

    weekday = (
        chart_data.groupby("요일")
        .size()
        .reindex(weekday_order, fill_value=0)
    )

    plt.figure(figsize=(10, 6))
    plt.bar(
        weekday.index,
        weekday.values,
    )

    plt.title("요일별 따릉이 이용 건수")
    plt.xlabel("요일")
    plt.ylabel("대여 건수")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    weekday_chart_path = CHART_DIR / "bike_weekday_demand.png"
    plt.savefig(weekday_chart_path, dpi=150)
    plt.close()

    print(f"[저장] {weekday_chart_path}")

    # --------------------------------------------------
    # 3. 일자별 대여 건수
    # --------------------------------------------------
    daily = (
        chart_data.groupby(
            chart_data["대여일시_변환"].dt.normalize()
        )
        .size()
        .sort_index()
    )

    plt.figure(figsize=(14, 6))
    plt.plot(
        daily.index,
        daily.values,
        marker="o",
    )

    plt.title("일자별 따릉이 이용 건수")
    plt.xlabel("일자")
    plt.ylabel("대여 건수")
    plt.xticks(rotation=45)
    plt.grid(alpha=0.3)
    plt.tight_layout()

    daily_chart_path = CHART_DIR / "bike_daily_demand.png"
    plt.savefig(daily_chart_path, dpi=150)
    plt.close()

    print(f"[저장] {daily_chart_path}")


def main() -> None:
    if not BIKE_CSV_PATH.exists():
        raise FileNotFoundError(
            f"따릉이 CSV를 찾지 못했습니다: {BIKE_CSV_PATH}"
        )

    df = read_csv_safely(BIKE_CSV_PATH)

    print_basic_info(df)
    print_missing_values(df)

    columns = detect_columns(df)

    df = analyze_datetime(
        df,
        columns["대여일시"],
    )
    save_demand_charts(df)
    analyze_stations(
        df,
        columns["대여소명"],
        columns["반납대여소명"],
    )

    print("\n" + "=" * 80)
    print("6. 이용 시간·거리 분석")
    print("=" * 80)

    analyze_numeric_column(
        df,
        columns["이용시간"],
        "이용시간",
    )

    analyze_numeric_column(
        df,
        columns["이용거리"],
        "이용거리",
    )

    print("\n" + "=" * 80)
    print("분석 완료")
    print("=" * 80)
    print(f"결과 저장 위치: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()