#!/usr/bin/env python3

import re
import sys


def extract_words(line: str) -> list[str]:
    """
    입력 문장을 소문자로 변환하고 영문 단어만 추출한다.
    apostrophe가 포함된 단어도 하나의 단어로 처리한다.
    """
    return re.findall(r"[a-z]+(?:'[a-z]+)?", line.lower())


def main() -> None:
    for line in sys.stdin:
        for word in extract_words(line):
            print(f"{word}\t1")


if __name__ == "__main__":
    main()