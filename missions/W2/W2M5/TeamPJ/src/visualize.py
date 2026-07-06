# src/visualize.py

import os
import sys

import pandas as pd
import matplotlib.pyplot as plt

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    LABELED_CSV_FILES,
    SENTIMENT_COMPARISON_PATH,
)
from src.preprocess import clean_text


def load_labeled_comments(path):
    df = pd.read_csv(path)
    return df


def check_required_columns(df):
    required_columns = ["sentiment"]

    for column in required_columns:
        if column not in df.columns:
            raise ValueError(
                f"Required column '{column}' does not exist. "
                f"Available columns: {df.columns.tolist()}"
            )

    if "cleaned_text" not in df.columns and "text" not in df.columns:
        raise ValueError(
            "Either 'cleaned_text' or 'text' column is required. "
            f"Available columns: {df.columns.tolist()}"
        )


def ensure_cleaned_text(df):
    if "cleaned_text" not in df.columns:
        print("cleaned_text column not found. Creating cleaned_text from text column...")
        df["cleaned_text"] = df["text"].apply(clean_text)

    return df

def normalize_sentiment_label(value):
    """
    Normalize sentiment labels from different CSV formats.

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

def ensure_sentiment_name(df):
    """
    Create sentiment_name column for consistent visualization.
    """
    df["sentiment_name"] = df["sentiment"].apply(normalize_sentiment_label)

    unknown_count = (df["sentiment_name"] == "unknown").sum()

    if unknown_count > 0:
        print(f"Warning: {unknown_count} rows have unknown sentiment labels.")

    return df


def print_basic_info(df):
    print("Data shape:")
    print(df.shape)

    print("\nColumns:")
    print(df.columns.tolist())

    print("\nSentiment counts:")
    print(df["sentiment_name"].value_counts())

    print("\nSample rows:")
    print(df[["text", "cleaned_text", "sentiment", "sentiment_name"]].head())

def collect_sentiment_counts(dataset_name, df):
    """
    Collect negative / neutral / positive counts for one dataset.
    """
    counts = df["sentiment_name"].value_counts()

    result = []

    for sentiment in ["negative", "neutral", "positive"]:
        result.append(
            {
                "dataset": dataset_name,
                "sentiment": sentiment,
                "count": counts.get(sentiment, 0),
            }
        )

    return result

def save_sentiment_comparison_plot(comparison_df, output_path):
    """
    Save sentiment count comparison bar chart.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    pivot_df = comparison_df.pivot(
        index="dataset",
        columns="sentiment",
        values="count",
    ).fillna(0)

    print("\nSentiment comparison table:")
    print(pivot_df)

    ax = pivot_df.plot(
        kind="bar",
        figsize=(10, 6),
    )

    ax.set_title("Sentiment Comparison by Dataset")
    ax.set_xlabel("Dataset")
    ax.set_ylabel("Number of Comments")
    ax.tick_params(axis="x", rotation=0)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"\nSentiment comparison plot saved to: {output_path}")

def main():
    all_sentiment_counts = []

    for dataset_name, csv_path in LABELED_CSV_FILES.items():
        print("=" * 50)
        print(f"Dataset: {dataset_name}")
        print(f"CSV path: {csv_path}")

        df = load_labeled_comments(csv_path)

        print("Checking required columns...")
        check_required_columns(df)

        df = ensure_cleaned_text(df)
        df = ensure_sentiment_name(df)

        print("Printing basic information...")
        print_basic_info(df)

        sentiment_counts = collect_sentiment_counts(
            dataset_name=dataset_name,
            df=df,
        )
        all_sentiment_counts.extend(sentiment_counts)

    comparison_df = pd.DataFrame(all_sentiment_counts)

    save_sentiment_comparison_plot(
        comparison_df=comparison_df,
        output_path=SENTIMENT_COMPARISON_PATH,
    )

    print("\nStep 9-2 complete.")

if __name__ == "__main__":
    main()