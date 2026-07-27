from pathlib import Path

import pandas as pd


CSV_PATH = Path("data/raw/서울시 문화행사 정보.csv")


def read_csv_safely(file_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        file_path,
        encoding="cp949",
        encoding_errors="replace",
    )

    print("[성공] 인코딩: cp949")
    print("[주의] 해석할 수 없는 문자는 �로 치환했습니다.")

    return df

def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"파일이 없습니다: {CSV_PATH}")

    df = read_csv_safely(CSV_PATH)

    print("행·열 개수:", df.shape)

    print("\n컬럼 목록")
    for index, column in enumerate(df.columns, start=1):
        print(f"{index}. {column}")

    print("\n상위 10개 행")
    print(df.head(10).to_string())

    print("\n데이터 타입")
    print(df.dtypes.to_string())

    print("\n결측치")
    print(df.isna().sum().to_string())


if __name__ == "__main__":
    main()