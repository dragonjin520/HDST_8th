from pathlib import Path

import pandas as pd


# CSV 파일 경로
CSV_PATH = Path("data/raw/서울특별시 공공자전거 대여이력 정보_2605.csv")


def read_csv_safely(file_path: Path) -> pd.DataFrame:
    """주요 한글 CSV 인코딩을 순서대로 시도해 읽는다."""
    encodings = ["utf-8-sig", "utf-8", "cp949", "euc-kr"]

    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            print(f"파일 인코딩: {encoding}")
            return df
        except UnicodeDecodeError:
            continue

    raise UnicodeDecodeError(
        "unknown",
        b"",
        0,
        1,
        "지원하는 인코딩으로 CSV 파일을 읽지 못했습니다.",
    )


def inspect_csv(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"파일이 존재하지 않습니다: {file_path}")

    df = read_csv_safely(file_path)

    print("\n" + "=" * 60)
    print("1. 데이터 크기")
    print("=" * 60)
    print(f"행 개수: {len(df):,}")
    print(f"컬럼 개수: {len(df.columns):,}")

    print("\n" + "=" * 60)
    print("2. 컬럼 목록")
    print("=" * 60)
    for index, column in enumerate(df.columns, start=1):
        print(f"{index:2}. {column}")

    print("\n" + "=" * 60)
    print("3. 상위 5개 행")
    print("=" * 60)
    print(df.head().to_string())

    print("\n" + "=" * 60)
    print("4. 컬럼별 데이터 타입")
    print("=" * 60)
    print(df.dtypes.to_string())

    print("\n" + "=" * 60)
    print("5. 결측치 현황")
    print("=" * 60)

    missing_count = df.isna().sum()
    missing_ratio = (missing_count / len(df) * 100).round(2)

    missing_summary = pd.DataFrame(
        {
            "결측치_개수": missing_count,
            "결측치_비율(%)": missing_ratio,
        }
    )

    print(missing_summary.to_string())

    print("\n" + "=" * 60)
    print("6. 컬럼별 고유값 개수")
    print("=" * 60)
    print(df.nunique(dropna=False).to_string())

    print("\n" + "=" * 60)
    print("7. 전체 데이터 정보")
    print("=" * 60)
    df.info()


if __name__ == "__main__":
    inspect_csv(CSV_PATH)