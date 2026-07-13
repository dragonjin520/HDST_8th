#!/usr/bin/env python3

import sys
from typing import Optional


def emit_result(word: Optional[str], count: int) -> None:
    if word is not None:
        print(f"{word}\t{count}")


def main() -> None:
    current_word: Optional[str] = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        try:
            word, count_text = line.split("\t", 1)
            count = int(count_text)
        except ValueError:
            print(f"[WARN] 잘못된 입력 형식: {line}", file=sys.stderr)
            continue

        if word == current_word:
            current_count += count
        else:
            emit_result(current_word, current_count)
            current_word = word
            current_count = count

    emit_result(current_word, current_count)


if __name__ == "__main__":
    main()