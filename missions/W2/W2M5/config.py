#1. 데이터 관련 설정
#2. 감성 라벨 설정
#3. 샘플링 설정
#4. WordCloud 설정
#5. 저장 경로 설정


# config.py

"""
Configuration file for W2M5 Personal Assignment.
This file stores constants used for sentiment word cloud visualization.
"""

# -----------------------------
# Dataset settings
# -----------------------------
DATA_PATH = "data/raw/training.1600000.processed.noemoticon.csv"

DATA_COLUMNS = [
    "target",
    "id",
    "date",
    "flag",
    "user",
    "text",
]


TARGET_COLUMN = "target"
TEXT_COLUMN = "text"
SENTIMENT_COLUMN = "sentiment"

SENTIMENT_LABELS = {
    0: "negative",
    4: "positive",
}

POSITIVE_LABEL = "positive"
NEGATIVE_LABEL = "negative"


# -----------------------------
# Sampling settings
# -----------------------------

SAMPLE_SIZE = 1000
RANDOM_STATE = 42


# -----------------------------
# WordCloud settings
# -----------------------------

WORDCLOUD_WIDTH = 800
WORDCLOUD_HEIGHT = 400
WORDCLOUD_BACKGROUND_COLOR = "white"
WORDCLOUD_MAX_WORDS = 200


# -----------------------------
# Output settings
# -----------------------------

OUTPUT_DIR = "output"
WORDCLOUD_OUTPUT_PATH = "output/sentiment_wordcloud.png"