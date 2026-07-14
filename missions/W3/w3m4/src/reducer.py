#!/usr/bin/env python3

import sys


def emit_result(sentiment, count):
    if sentiment is not None:
        print(f"{sentiment}\t{count}")


def main():
    current_sentiment = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        try:
            sentiment, count = line.split("\t", 1)
            count = int(count)
        except (ValueError, TypeError):
            continue

        if sentiment == current_sentiment:
            current_count += count
        else:
            emit_result(current_sentiment, current_count)
            current_sentiment = sentiment
            current_count = count

    emit_result(current_sentiment, current_count)


if __name__ == "__main__":
    main()