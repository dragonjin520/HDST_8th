# config.py

# Training data
TRAINING_CSV_PATH = "../data/raw/training.1600000.processed.noemoticon.csv"

# YouTube comment data
INPUT_CSV_PATH = "data/input/hyundai_youtube_comments.csv"
LABELED_CSV_PATH = "data/output/youtube_comments_labeled.csv"

# Columns
TEXT_COLUMN = "comment_text"

# Model outputs
MODEL_PATH = "models/sentiment_model.pkl"
VECTORIZER_PATH = "models/tfidf_vectorizer.pkl"

# Visualization outputs
SENTIMENT_COUNT_PATH = "output/sentiment_count.png"
WORDCLOUD_POSITIVE_PATH = "output/wordcloud_positive.png"
WORDCLOUD_NEGATIVE_PATH = "output/wordcloud_negative.png"

# Logs
PREDICTION_ERROR_LOG_PATH = "logs/prediction_errors.csv"