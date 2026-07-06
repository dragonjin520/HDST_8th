# config.py

INPUT_CSV_PATH = "data/input/youtube_comments_english.csv"
LABELED_CSV_PATH = "data/output/youtube_comments_labeled.csv"

TEXT_COLUMN = "comment_text"

MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"

SENTIMENT_COUNT_PATH = "output/sentiment_count.png"
WORDCLOUD_POSITIVE_PATH = "output/wordcloud_positive.png"
WORDCLOUD_NEUTRAL_PATH = "output/wordcloud_neutral.png"
WORDCLOUD_NEGATIVE_PATH = "output/wordcloud_negative.png"

INFERENCE_ERROR_LOG_PATH = "logs/inference_errors.csv"