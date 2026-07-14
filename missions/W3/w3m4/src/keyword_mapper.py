#!/usr/bin/env python3

import csv
import json
import os
import re
import sys


def load_keywords():
    config_path = os.environ.get(
        "SENTIMENT_CONFIG",
        "sentiment_keywords.json"
    )

    try:
        with open(config_path, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)

        positive_keywords = set(config["positive"])
        negative_keywords = set(config["negative"])

        return positive_keywords, negative_keywords

    except (OSError, KeyError, json.JSONDecodeError) as error:
        print(
            f"Failed to load keyword config: {error}",
            file=sys.stderr
        )
        sys.exit(1)


def tokenize_tweet(tweet):
    return set(re.findall(r"[a-zA-Z']+", tweet.lower()))


def classify_sentiment(tweet, positive_keywords, negative_keywords):
    words = tokenize_tweet(tweet)

    has_positive = bool(words & positive_keywords)
    has_negative = bool(words & negative_keywords)

    if has_positive and not has_negative:
        return "positive"

    if has_negative and not has_positive:
        return "negative"

    return "neutral"


def process_lines(positive_keywords, negative_keywords):
    reader = csv.reader(sys.stdin)

    for row in reader:
        if len(row) < 6:
            continue

        tweet = row[5].strip()

        if not tweet:
            continue

        sentiment = classify_sentiment(
            tweet,
            positive_keywords,
            negative_keywords
        )

        print(f"{sentiment}\t1")


def main():
    positive_keywords, negative_keywords = load_keywords()
    process_lines(positive_keywords, negative_keywords)


if __name__ == "__main__":
    main()