# src/wordcloud_sampling.py

import os
import sys

import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import LABELED_CSV_PATH
from src.preprocess import clean_text


OUTPUT_DIR = "output/sampling_wordclouds"

# 감정별 최대 샘플 수
SAMPLE_SIZE_PER_GROUP = 300

# WordCloud에서 제외할 단어
CUSTOM_STOPWORDS = {
    "hyundai",
    "ioniq",
    "vehicle",
    "car",
    "cars",
    "look",
    "looks",
    "s",
    "great",
    "love",
    "thank",
    "thanks",
    "much",
    "better",
    "will",
    "good",
    "review",
    "t",
    "don",
    "think",
    "ev",
    "suv",
    "sedan",
    "video",
    "one",
    "like",
    "would",
    "really",
    "also",
    "get",
    "make",
    "new",
    "m",
    ".",
    "remind",
}


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


def load_labeled_comments():
    df = pd.read_csv(LABELED_CSV_PATH)

    print("Loaded file:")
    print(LABELED_CSV_PATH)

    print("\nColumns:")
    print(df.columns.tolist())

    if "text" not in df.columns:
        raise ValueError("text column does not exist.")

    if "car_type" not in df.columns:
        raise ValueError("car_type column does not exist.")

    # 중간에 잘못 섞인 헤더 행 제거
    df["car_type"] = df["car_type"].astype(str).str.strip()
    df = df[df["car_type"].isin(["SUV", "Sedan"])]

    if "sentiment_name" not in df.columns:
        if "sentiment" not in df.columns:
            raise ValueError("sentiment or sentiment_name column is required.")
        df["sentiment_name"] = df["sentiment"].apply(normalize_sentiment_label)

    df["sentiment_name"] = df["sentiment_name"].astype(str).str.strip().str.lower()
    df = df[df["sentiment_name"].isin(["negative", "neutral", "positive"])]

    if "cleaned_text" not in df.columns:
        df["cleaned_text"] = df["text"].apply(clean_text)

    df["cleaned_text"] = df["cleaned_text"].fillna("").astype(str)

    return df


def sample_comments(df):
    """
    Sample comments by car_type and sentiment_name.

    Example groups:
    - SUV negative
    - SUV positive
    - Sedan negative
    - Sedan positive
    """
    sampled_groups = []

    for (car_type, sentiment_name), group_df in df.groupby(
        ["car_type", "sentiment_name"]
    ):
        sample_size = min(len(group_df), SAMPLE_SIZE_PER_GROUP)

        sampled_df = group_df.sample(
            n=sample_size,
            random_state=42,
        )

        sampled_groups.append(sampled_df)

        print(
            f"{car_type} / {sentiment_name}: "
            f"original={len(group_df)}, sampled={sample_size}"
        )

    sampled_df = pd.concat(sampled_groups, ignore_index=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    sampled_path = os.path.join(OUTPUT_DIR, "sampled_comments.csv")
    sampled_df.to_csv(sampled_path, index=False, encoding="utf-8-sig")

    print(f"\nSampled comments saved to: {sampled_path}")

    return sampled_df


def create_wordcloud(text, title, output_path):
    if text.strip() == "":
        print(f"No text for {title}. Skipping.")
        return

    stopwords = set(STOPWORDS)
    stopwords.update(CUSTOM_STOPWORDS)

    wordcloud = WordCloud(
        width=1000,
        height=600,
        background_color="white",
        stopwords=stopwords,
        collocations=False,
    ).generate(text)

    plt.figure(figsize=(10, 6))
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"WordCloud saved to: {output_path}")

def create_group_wordclouds(sampled_df):
    """
    Create WordClouds only for positive and negative comments
    by car_type.
    """
    target_sentiments = ["negative", "positive"]

    filtered_df = sampled_df[
        sampled_df["sentiment_name"].isin(target_sentiments)
    ]

    for (car_type, sentiment_name), group_df in filtered_df.groupby(
        ["car_type", "sentiment_name"]
    ):
        text = " ".join(group_df["cleaned_text"].dropna().astype(str))

        filename = f"{car_type.lower()}_{sentiment_name}_wordcloud.png"
        output_path = os.path.join(OUTPUT_DIR, filename)

        title = f"{car_type} - {sentiment_name}"

        create_wordcloud(
            text=text,
            title=title,
            output_path=output_path,
        )

def create_car_type_wordclouds(sampled_df):
    """
    Create WordClouds by car_type only.
    """
    for car_type, group_df in sampled_df.groupby("car_type"):
        text = " ".join(group_df["cleaned_text"].dropna().astype(str))

        filename = f"{car_type.lower()}_all_wordcloud.png"
        output_path = os.path.join(OUTPUT_DIR, filename)

        title = f"{car_type} - All Comments"

        create_wordcloud(
            text=text,
            title=title,
            output_path=output_path,
        )


def main():
    df = load_labeled_comments()

    print("\nOriginal sentiment counts by car_type:")
    print(df.groupby(["car_type", "sentiment_name"]).size())

    sampled_df = sample_comments(df)

    print("\nSampled sentiment counts by car_type:")
    print(sampled_df.groupby(["car_type", "sentiment_name"]).size())

    create_group_wordclouds(sampled_df)

    print("\nSampling WordCloud complete.")


if __name__ == "__main__":
    main()