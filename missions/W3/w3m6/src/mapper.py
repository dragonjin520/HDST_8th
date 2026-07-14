#!/usr/bin/env python3

import json
import sys


def main():
    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        try:
            review = json.loads(line)

            product_id = review.get("parent_asin")
            rating = review.get("rating")

            if not product_id or rating is None:
                continue

            rating = float(rating)

            if not 0.0 <= rating <= 5.0:
                continue

            print(f"{product_id}\t{rating}")

        except (json.JSONDecodeError, TypeError, ValueError):
            continue


if __name__ == "__main__":
    main()
