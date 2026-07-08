"""
Configuration file for W2M5 Personal Assignment.

This file stores constants used for sentiment word cloud visualization.
"""

from pathlib import Path


# -----------------------------
# Path settings
# -----------------------------


BASE_DIR = Path(__file__).resolve().parent

OUTPUT_DIR = BASE_DIR / "output"

DATA_PATH = BASE_DIR.parent.parent / "data" / "raw" / "training.1600000.processed.noemoticon.csv"
WORDCLOUD_OUTPUT_PATH = OUTPUT_DIR / "sentiment_wordcloud.png"

# -----------------------------
# Dataset settings
# -----------------------------

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