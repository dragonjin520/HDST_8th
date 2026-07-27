from pathlib import Path

import pandas as pd


CSV_PATH = Path("data/raw/서울시 문화행사 정보.csv")
OUTPUT_PATH = Path("data/processed/venue_coordinate_groups.csv")


def read_event_csv(file_path: Path) -> pd.DataFrame:
    return pd.read_csv(
        file_path,
        encoding="cp949",
        encoding_errors="replace",
    )


def get_mode(series: pd.Series):
    """최빈값을 반환한다."""
    mode = series.dropna().mode()

    if mode.empty:
        return None

    return mode.iloc[0]


def join_unique_names(series: pd.Series, limit: int = 15) -> str:
    """같은 좌표에 포함된 고유 장소명을 문자열로 합친다."""
    names = (
        series.dropna()
        .astype(str)
        .str.strip()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    displayed_names = names[:limit]

    result = " | ".join(displayed_names)

    if len(names) > limit:
        result += f" | ... 외 {len(names) - limit}개"

    return result


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"파일이 없습니다: {CSV_PATH}")

    df = read_event_csv(CSV_PATH)

    required_columns = [
        "분류",
        "자치구",
        "공연/행사명",
        "장소",
        "시작일",
        "종료일",
        "경도(Y좌표)",
        "위도(X좌표)",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"필수 컬럼이 없습니다: {missing_columns}"
        )

    events = df[required_columns].copy()

    # 문자열 정제
    for column in ["분류", "자치구", "공연/행사명", "장소"]:
        events[column] = (
            events[column]
            .astype("string")
            .str.strip()
        )

    # 날짜 변환
    events["시작일"] = pd.to_datetime(
        events["시작일"],
        errors="coerce",
    )

    events["종료일"] = pd.to_datetime(
        events["종료일"],
        errors="coerce",
    )

    # 좌표 변환
    events["경도"] = pd.to_numeric(
        events["경도(Y좌표)"],
        errors="coerce",
    )

    events["위도"] = pd.to_numeric(
        events["위도(X좌표)"],
        errors="coerce",
    )

    # 행사 기간
    events["행사일수"] = (
        events["종료일"] - events["시작일"]
    ).dt.days + 1

    # 분석 가능한 단기 행사
    short_events = events[
        events["행사일수"].between(1, 7)
        & events["경도"].notna()
        & events["위도"].notna()
        & events["장소"].notna()
    ].copy()

    # 좌표 소수점 4자리로 묶는다.
    # 위·경도 소수점 4자리는 약 10m 정도 범위이므로
    # 같은 시설의 세부 공연장을 묶는 데 활용할 수 있다.
    short_events["경도그룹"] = short_events["경도"].round(4)
    short_events["위도그룹"] = short_events["위도"].round(4)

    coordinate_groups = (
        short_events.groupby(
            ["경도그룹", "위도그룹"],
            dropna=False,
        )
        .agg(
            단기행사수=("공연/행사명", "count"),
            고유행사수=("공연/행사명", "nunique"),
            장소명수=("장소", "nunique"),
            행사종류수=("분류", "nunique"),
            대표자치구=("자치구", get_mode),
            대표장소명=("장소", get_mode),
            포함장소명=("장소", join_unique_names),
            실제평균경도=("경도", "mean"),
            실제평균위도=("위도", "mean"),
        )
        .reset_index()
        .sort_values(
            ["단기행사수", "고유행사수"],
            ascending=False,
        )
    )

    coordinate_groups["실제평균경도"] = (
        coordinate_groups["실제평균경도"].round(6)
    )

    coordinate_groups["실제평균위도"] = (
        coordinate_groups["실제평균위도"].round(6)
    )

    print("=" * 100)
    print("좌표 기준 행사장 군집 상위 30개")
    print("=" * 100)

    print(
        coordinate_groups[
            [
                "대표장소명",
                "대표자치구",
                "단기행사수",
                "고유행사수",
                "장소명수",
                "행사종류수",
                "실제평균경도",
                "실제평균위도",
                "포함장소명",
            ]
        ]
        .head(30)
        .to_string(index=False)
    )

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    coordinate_groups.to_csv(
        OUTPUT_PATH,
        index=False,
        encoding="utf-8-sig",
    )

    print(f"\n저장 완료: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()