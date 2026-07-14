#!/usr/bin/env python3

import csv
import sys


LABEL_TO_SENTIMENT = {
    "0": "negative",
    "2": "neutral",
    "4": "positive",
}


def main():
    reader = csv.reader(sys.stdin)

    for row in reader:
        if not row:
            continue

        label = row[0].strip()
        sentiment = LABEL_TO_SENTIMENT.get(label)

        if sentiment is None:
            continue

        print(f"{sentiment}\t1")


if __name__ == "__main__":
    main()