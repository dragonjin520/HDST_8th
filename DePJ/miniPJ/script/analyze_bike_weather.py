from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import requests


BIKE_CSV_PATH = Path(
    "/Users/admin/Documents/GitHub/HDST_8th/DePJ/miniPJ/"
    "data/raw/서울특별시 공공자전거 대여이력 정보_2605.csv"
)

OUTPUT_DIR = Path("data/processed")
CHART_DIR = OUTPUT_DIR / "charts"

# 서울시청 기준 좌표
SEOUL_LATITUDE = 37.5665
SEOUL_LONGITUDE = 126.9780

START_DATE = "2026-05-01"
END_DATE = "2026-05-31"


def configure_korean_font() -> None:
    plt.rcParams["font.family"] = "AppleGothic"
    plt.rcParams["axes.unicode_minus"] = False


def read_bike_data() -> pd.DataFrame:
    if not BIKE_CSV_PATH.exists():
        raise FileNotFoundError(
            f"따릉이 CSV를 찾지 못했습니다: {BIKE_CSV_PATH}"
        )

    return pd.read_csv(
        BIKE_CSV_PATH,
        encoding="cp949",
        encoding_errors="replace",
        low_memory=False,
    )


def find_rental_datetime_column(df: pd.DataFrame) -> str:
    candidates = [
        "대여일시",
        "대여 일시",
        "대여일자",
        "대여 일자",
    ]

    for candidate in candidates:
        if candidate in df.columns:
            return candidate

    for column in df.columns:
        normalized = str(column).replace(" ", "")

        if "대여" in normalized and (
            "일시" in normalized or "일자" in normalized
        ):
            return str(column)

    raise ValueError("대여 일시 컬럼을 찾지 못했습니다.")


def build_daily_bike_demand(df: pd.DataFrame) -> pd.DataFrame:
    rental_column = find_rental_datetime_column(df)

    df = df.copy()

    df["대여일시"] = pd.to_datetime(
        df[rental_column],
        errors="coerce",
    )

    df = df[df["대여일시"].notna()].copy()
    df["일자"] = df["대여일시"].dt.normalize()

    daily_bike = (
        df.groupby("일자")
        .size()
        .rename("대여건수")
        .reset_index()
        .sort_values("일자")
    )

    return daily_bike


def fetch_hourly_weather() -> pd.DataFrame:
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": SEOUL_LATITUDE,
        "longitude": SEOUL_LONGITUDE,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ",".join(
            [
                "temperature_2m",
                "precipitation",
                "rain",
                "relative_humidity_2m",
                "wind_speed_10m",
            ]
        ),
        "timezone": "Asia/Seoul",
    }

    response = requests.get(
        url,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()

    if "hourly" not in payload:
        raise ValueError(
            f"날씨 API 응답에 hourly 데이터가 없습니다: {payload}"
        )

    hourly = payload["hourly"]

    weather = pd.DataFrame(
        {
            "시각": pd.to_datetime(hourly["time"]),
            "기온": hourly["temperature_2m"],
            "강수량": hourly["precipitation"],
            "강우량": hourly["rain"],
            "습도": hourly["relative_humidity_2m"],
            "풍속": hourly["wind_speed_10m"],
        }
    )

    return weather


def build_daily_weather(
    hourly_weather: pd.DataFrame,
) -> pd.DataFrame:
    weather = hourly_weather.copy()
    weather["일자"] = weather["시각"].dt.normalize()

    daily_weather = (
        weather.groupby("일자")
        .agg(
            평균기온=("기온", "mean"),
            최저기온=("기온", "min"),
            최고기온=("기온", "max"),
            일강수량=("강수량", "sum"),
            평균습도=("습도", "mean"),
            평균풍속=("풍속", "mean"),
            최대풍속=("풍속", "max"),
        )
        .reset_index()
        .sort_values("일자")
    )

    daily_weather["비여부"] = daily_weather["일강수량"] > 0
    daily_weather["평균기온"] = daily_weather["평균기온"].round(1)
    daily_weather["평균습도"] = daily_weather["평균습도"].round(1)
    daily_weather["평균풍속"] = daily_weather["평균풍속"].round(1)

    return daily_weather


def merge_bike_weather(
    daily_bike: pd.DataFrame,
    daily_weather: pd.DataFrame,
) -> pd.DataFrame:
    merged = daily_bike.merge(
        daily_weather,
        on="일자",
        how="left",
        validate="one_to_one",
    )

    return merged


def print_summary(merged: pd.DataFrame) -> None:
    print("\n" + "=" * 80)
    print("따릉이 + 날씨 결합 결과")
    print("=" * 80)

    print(merged.to_string(index=False))

    print("\n상관계수")
    correlation_columns = [
        "대여건수",
        "평균기온",
        "일강수량",
        "평균습도",
        "평균풍속",
        "최대풍속",
    ]

    print(
        merged[correlation_columns]
        .corr(numeric_only=True)["대여건수"]
        .sort_values(ascending=False)
        .to_string()
    )

    rain_summary = (
        merged.groupby("비여부")
        .agg(
            날짜수=("일자", "count"),
            평균대여건수=("대여건수", "mean"),
            중앙대여건수=("대여건수", "median"),
            평균강수량=("일강수량", "mean"),
        )
        .reset_index()
    )

    rain_summary["구분"] = rain_summary["비여부"].map(
        {
            False: "비 안 온 날",
            True: "비 온 날",
        }
    )

    print("\n비 여부별 대여 건수")
    print(
        rain_summary[
            [
                "구분",
                "날짜수",
                "평균대여건수",
                "중앙대여건수",
                "평균강수량",
            ]
        ].to_string(index=False)
    )


def save_charts(merged: pd.DataFrame) -> None:
    configure_korean_font()
    CHART_DIR.mkdir(parents=True, exist_ok=True)

    # 일별 대여량과 강수량
    fig, first_axis = plt.subplots(figsize=(14, 6))

    first_axis.plot(
        merged["일자"],
        merged["대여건수"],
        marker="o",
        label="대여 건수",
    )
    first_axis.set_xlabel("일자")
    first_axis.set_ylabel("대여 건수")
    first_axis.tick_params(axis="x", rotation=45)
    first_axis.grid(alpha=0.3)

    second_axis = first_axis.twinx()
    second_axis.bar(
        merged["일자"],
        merged["일강수량"],
        alpha=0.3,
        label="강수량",
    )
    second_axis.set_ylabel("일 강수량(mm)")

    plt.title("일별 따릉이 대여 건수와 강수량")
    fig.tight_layout()

    plt.savefig(
        CHART_DIR / "bike_demand_precipitation.png",
        dpi=150,
    )
    plt.close()

    # 평균기온과 대여 건수 산점도
    plt.figure(figsize=(9, 6))
    plt.scatter(
        merged["평균기온"],
        merged["대여건수"],
    )

    for _, row in merged.iterrows():
        plt.annotate(
            row["일자"].strftime("%m-%d"),
            (row["평균기온"], row["대여건수"]),
            fontsize=8,
            alpha=0.7,
        )

    plt.title("평균기온과 따릉이 대여 건수")
    plt.xlabel("평균기온(°C)")
    plt.ylabel("대여 건수")
    plt.grid(alpha=0.3)
    plt.tight_layout()

    plt.savefig(
        CHART_DIR / "bike_demand_temperature_scatter.png",
        dpi=150,
    )
    plt.close()

    # 강수량과 대여 건수 산점도
    plt.figure(figsize=(9, 6))
    plt.scatter(
        merged["일강수량"],
        merged["대여건수"],
    )

    for _, row in merged.iterrows():
        plt.annotate(
            row["일자"].strftime("%m-%d"),
            (row["일강수량"], row["대여건수"]),
            fontsize=8,
            alpha=0.7,
        )

    plt.title("강수량과 따릉이 대여 건수")
    plt.xlabel("일 강수량(mm)")
    plt.ylabel("대여 건수")
    plt.grid(alpha=0.3)
    plt.tight_layout()

    plt.savefig(
        CHART_DIR / "bike_demand_precipitation_scatter.png",
        dpi=150,
    )
    plt.close()


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    bike_df = read_bike_data()
    daily_bike = build_daily_bike_demand(bike_df)

    hourly_weather = fetch_hourly_weather()
    daily_weather = build_daily_weather(hourly_weather)

    merged = merge_bike_weather(
        daily_bike,
        daily_weather,
    )

    hourly_weather.to_csv(
        OUTPUT_DIR / "weather_hourly_2026_05.csv",
        index=False,
        encoding="utf-8-sig",
    )

    daily_weather.to_csv(
        OUTPUT_DIR / "weather_daily_2026_05.csv",
        index=False,
        encoding="utf-8-sig",
    )

    merged.to_csv(
        OUTPUT_DIR / "bike_weather_daily_2026_05.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print_summary(merged)
    save_charts(merged)

    print("\n저장 완료")
    print(
        OUTPUT_DIR / "bike_weather_daily_2026_05.csv"
    )
    print(CHART_DIR)


if __name__ == "__main__":
    main()