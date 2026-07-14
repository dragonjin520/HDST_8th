#!/usr/bin/env python3

import csv
import sys


def main() -> None:
    reader = csv.reader(sys.stdin)

    for row in reader:
        # 헤더 제외
        if row and row[0] == "userId":
            continue

        # 정상 데이터는 4개 컬럼으로 구성됨
        if len(row) != 4:
            print(f"[WARN] invalid row: {row}", file=sys.stderr)
            continue

        try:
            movie_id = int(row[1])
            rating = float(row[2])
        except ValueError:
            print(f"[WARN] invalid values: {row}", file=sys.stderr)
            continue

        print(f"{movie_id}\t{rating}")


if __name__ == "__main__":
    main()