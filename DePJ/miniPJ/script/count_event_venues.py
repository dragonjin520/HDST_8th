from pathlib import Path

import pandas as pd


CSV_PATH = Path("data/raw/서울시 문화행사 정보.csv")


def read_event_csv(file_path: Path) -> pd.DataFrame:
    return pd.read_csv(
        file_path,
        encoding="cp949",
        encoding_errors="replace",
    )


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"파일이 없습니다: {CSV_PATH}")

    df = read_event_csv(CSV_PATH)

    # 필요한 컬럼만 선택
    events = df[
        [
            "분류",
            "자치구",
            "공연/행사명",
            "장소",
            "시작일",
            "종료일",
            "경도(Y좌표)",
            "위도(X좌표)",
            "행사시간",
        ]
    ].copy()

    # 문자열 앞뒤 공백 제거
    events["장소"] = events["장소"].astype("string").str.strip()
    events["자치구"] = events["자치구"].astype("string").str.strip()

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

    # 행사 기간 계산
    events["행사일수"] = (
        events["종료일"] - events["시작일"]
    ).dt.days + 1

    # 유효한 장소만 사용
    valid_events = events[
        events["장소"].notna()
        & events["시작일"].notna()
        & events["종료일"].notna()
    ].copy()

    print("=" * 80)
    print("1. 정제 결과")
    print("=" * 80)
    print(f"전체 행사 수: {len(events):,}")
    print(f"날짜와 장소가 유효한 행사 수: {len(valid_events):,}")
    print(f"고유 장소 수: {valid_events['장소'].nunique():,}")

    venue_summary = (
        valid_events.groupby("장소")
        .agg(
            행사수=("공연/행사명", "count"),
            고유행사수=("공연/행사명", "nunique"),
            행사종류수=("분류", "nunique"),
            평균행사일수=("행사일수", "mean"),
            최대행사일수=("행사일수", "max"),
            좌표보유행사수=("경도", "count"),
            대표자치구=(
                "자치구",
                lambda x: x.mode().iloc[0]
                if not x.mode().empty
                else None
            ),
            대표경도=("경도", "median"),
            대표위도=("위도", "median"),
        )
        .reset_index()
    )

    venue_summary["평균행사일수"] = (
        venue_summary["평균행사일수"].round(1)
    )

    venue_summary["좌표보유율"] = (
        venue_summary["좌표보유행사수"]
        / venue_summary["행사수"]
        * 100
    ).round(1)

    venue_summary = venue_summary.sort_values(
        ["행사수", "고유행사수"],
        ascending=False,
    )

    print("\n" + "=" * 80)
    print("2. 행사 개최 건수 상위 30개 장소")
    print("=" * 80)

    print(
        venue_summary[
            [
                "장소",
                "대표자치구",
                "행사수",
                "고유행사수",
                "행사종류수",
                "평균행사일수",
                "최대행사일수",
                "좌표보유율",
                "대표경도",
                "대표위도",
            ]
        ]
        .head(30)
        .to_string(index=False)
    )

    # 7일 이하 단기 행사만 별도 집계
    short_events = valid_events[
        valid_events["행사일수"].between(1, 7)
    ].copy()

    short_venue_summary = (
        short_events.groupby("장소")
        .agg(
            단기행사수=("공연/행사명", "count"),
            고유행사수=("공연/행사명", "nunique"),
            행사종류수=("분류", "nunique"),
            대표자치구=(
                "자치구",
                lambda x: x.mode().iloc[0]
                if not x.mode().empty
                else None
            ),
            대표경도=("경도", "median"),
            대표위도=("위도", "median"),
        )
        .reset_index()
        .sort_values(
            ["단기행사수", "고유행사수"],
            ascending=False,
        )
    )

    print("\n" + "=" * 80)
    print("3. 7일 이하 단기 행사 개최 건수 상위 30개 장소")
    print("=" * 80)

    print(
        short_venue_summary.head(30).to_string(index=False)
    )

    output_path = Path("data/processed/venue_summary.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    venue_summary.to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig",
    )

    print(f"\n저장 완료: {output_path}")


if __name__ == "__main__":
    main()