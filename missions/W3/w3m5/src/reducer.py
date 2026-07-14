#!/usr/bin/env python3

import sys


def output_average(movie_id: str, total: float, count: int) -> None:
    average = total / count
    print(f"{movie_id}\t{average:.6f}")


def main() -> None:
    current_movie_id: str | None = None
    rating_sum = 0.0
    rating_count = 0

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        try:
            movie_id, rating_text = line.split("\t", 1)
            rating = float(rating_text)
        except ValueError:
            print(f"[WARN] invalid input: {line}", file=sys.stderr)
            continue

        if current_movie_id == movie_id:
            rating_sum += rating
            rating_count += 1
            continue

        if current_movie_id is not None:
            output_average(current_movie_id, rating_sum, rating_count)

        current_movie_id = movie_id
        rating_sum = rating
        rating_count = 1

    if current_movie_id is not None:
        output_average(current_movie_id, rating_sum, rating_count)


if __name__ == "__main__":
    main()