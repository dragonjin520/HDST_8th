#!/usr/bin/env python3

import sys


def emit_result(product_id, rating_sum, review_count):
    if product_id is None or review_count == 0:
        return

    average_rating = rating_sum / review_count
    print(f"{product_id}\t{review_count}\t{average_rating:.2f}")


def main():
    current_product_id = None
    rating_sum = 0.0
    review_count = 0

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        try:
            product_id, rating_text = line.split("\t", 1)
            rating = float(rating_text)
        except (ValueError, TypeError):
            continue

        if product_id == current_product_id:
            rating_sum += rating
            review_count += 1
            continue

        emit_result(current_product_id, rating_sum, review_count)

        current_product_id = product_id
        rating_sum = rating
        review_count = 1

    emit_result(current_product_id, rating_sum, review_count)


if __name__ == "__main__":
    main()