# src/visualize_car_type.py

import os
import sys

import pandas as pd
import matplotlib.pyplot as plt

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import LABELED_CSV_PATH


OUTPUT_PATH = "output/car_type_sentiment_comparison.png"


def normalize_sentiment_label(value):
    """
    Convert sentiment values to sentiment names.

    Supported formats:
    - 0, 2, 4
    - "0", "2", "4"
    - "negative", "neutral", "positive"
    """
    value = str(value).strip().lower()

    if value in ["0", "0.0", "negative"]:
        return "negative"
    if value in ["2", "2.0", "neutral"]:
        return "neutral"
    if value in ["4", "4.0", "positive"]:
        return "positive"

    return "unknown"


def main():
    df = pd.read_csv(LABELED_CSV_PATH)

    print("Loaded file:")
    print(LABELED_CSV_PATH)

    print("\nColumns:")
    print(df.columns.tolist())

    if "car_type" not in df.columns:
        raise ValueError("car_type column does not exist.")

    if "sentiment" not in df.columns:
        if "sentiment" not in df.columns:
            raise ValueError("sentiment or sentiment_name column is required.")
        df["sentiment"] = df["sentiment"].apply(normalize_sentiment_label)

    print("\nCar type counts:")
    print(df["car_type"].value_counts())

    print("\nSentiment counts by car_type:")
    count_df = (
        df.groupby(["car_name","car_type", "sentiment"])
        .size()
        .reset_index(name="count")
    )
    print(count_df)

    pivot_df = count_df.pivot(
        index="car_type",
        columns="sentiment",
        values="count",
    ).fillna(0)

    # Make column order consistent
    sentiment_order = ["negative", "neutral", "positive"]
    pivot_df = pivot_df.reindex(columns=sentiment_order, fill_value=0)

    print("\nPivot table:")
    print(pivot_df)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    ax = pivot_df.plot(kind="bar", figsize=(9, 6))

    ax.set_title("Sentiment Comparison by Car Type")
    ax.set_xlabel("Car Type")
    ax.set_ylabel("Number of Comments")
    ax.tick_params(axis="x", rotation=0)

    plt.tight_layout()
    plt.savefig(OUTPUT_PATH)
    plt.close()

    print(f"\nGraph saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()