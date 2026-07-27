from pathlib import Path

import pandas as pd


CSV_PATH = Path("data/raw/서울시 문화행사 정보.csv")
OUTPUT_PATH = Path("data/processed/sejong_event_candidates.csv")

SEJONG_LONGITUDE = 126.976005
SEJONG_LATITUDE = 37.572624


def read_event_csv(file_path: Path) -> pd.DataFrame:
    return pd.read_csv(
        file_path,
        encoding="cp949",
        encoding_errors="replace",
    )


def main() -> None:
    df = read_event_csv(CSV_PATH)

    events = df[
        [
            "분류",
            "자치구",
            "공연/행사명",
            "장소",
            "기관명",
            "시작일",
            "종료일",
            "경도(Y좌표)",
            "위도(X좌표)",
            "행사시간",
        ]
    ].copy()

    events["장소"] = events["장소"].astype("string").str.strip()
    events["기관명"] = events["기관명"].astype("string").str.strip()

    events["경도"] = pd.to_numeric(
        events["경도(Y좌표)"],
        errors="coerce",
    )

    events["위도"] = pd.to_numeric(
        events["위도(X좌표)"],
        errors="coerce",
    )

    events["시작일"] = pd.to_datetime(
        events["시작일"],
        errors="coerce",
    )

    events["종료일"] = pd.to_datetime(
        events["종료일"],
        errors="coerce",
    )

    events["행사일수"] = (
        events["종료일"] - events["시작일"]
    ).dt.days + 1

    # 세종문화회관 좌표와 거의 같은 행
    coordinate_candidates = events[
        events["경도"].round(4).eq(round(SEJONG_LONGITUDE, 4))
        & events["위도"].round(4).eq(round(SEJONG_LATITUDE, 4))
        & events["행사일수"].between(1, 7)
    ].copy()

    print("=" * 100)
    print("1. 세종문화회관 좌표에 등록된 단기 행사")
    print("=" * 100)
    print(f"행사 수: {len(coordinate_candidates):,}")
    print(f"고유 장소명 수: {coordinate_candidates['장소'].nunique():,}")

    print("\n장소명별 행사 수")
    print(
        coordinate_candidates["장소"]
        .value_counts()
        .head(100)
        .to_string()
    )

    # 세종문화회관 관련 장소명만 추출
    sejong_keywords = [
        "세종문화회관",
        "세종대극장",
        "세종체임버홀",
        "세종 체임버홀",
        "세종S씨어터",
        "세종 S씨어터",
        "세종M씨어터",
        "세종 M씨어터",
        "세종미술관",
        "세종 미술관",
        "세종예술아카데미",
        "세종 예술아카데미",
        "뜨락",
    ]

    keyword_pattern = "|".join(sejong_keywords)

    sejong_events = coordinate_candidates[
        coordinate_candidates["장소"]
        .fillna("")
        .str.contains(
            keyword_pattern,
            case=False,
            regex=True,
        )
    ].copy()

    print("\n" + "=" * 100)
    print("2. 장소명이 실제 세종문화회관과 관련된 행사")
    print("=" * 100)
    print(f"행사 수: {len(sejong_events):,}")
    print(f"고유 행사 수: {sejong_events['공연/행사명'].nunique():,}")
    print(f"고유 장소명 수: {sejong_events['장소'].nunique():,}")

    print("\n장소명별 행사 수")
    print(
        sejong_events["장소"]
        .value_counts()
        .head(50)
        .to_string()
    )

    print("\n" + "=" * 100)
    print("3. 좌표는 같지만 세종문화회관이 아닌 의심 장소")
    print("=" * 100)

    suspicious_events = coordinate_candidates[
        ~coordinate_candidates.index.isin(sejong_events.index)
    ]

    print(
        suspicious_events["장소"]
        .value_counts()
        .head(50)
        .to_string()
    )

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    sejong_events.to_csv(
        OUTPUT_PATH,
        index=False,
        encoding="utf-8-sig",
    )

    print(f"\n저장 완료: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()