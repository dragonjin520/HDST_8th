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

            product_id = review.get("parent_asin") or review.get("asin")
            rating = review.get("rating")

            if not product_id or rating is None:
                print(
                    "[WARN] missing product_id or rating",
                    file=sys.stderr,
                )
                continue

            rating = float(rating)

            if not 0.0 <= rating <= 5.0:
                print(
                    f"[WARN] invalid rating: {rating}",
                    file=sys.stderr,
                )
                continue

            print(f"{product_id}\t{rating}")

        except json.JSONDecodeError as error:
            print(
                f"[WARN] invalid JSON: {error}",
                file=sys.stderr,
            )
        except (TypeError, ValueError) as error:
            print(
                f"[WARN] invalid value: {error}",
                file=sys.stderr,
            )


if __name__ == "__main__":
    main()