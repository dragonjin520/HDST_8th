# config.py

# =========================
# Data paths
# =========================

# Sentiment140 training data
# TeamPJ 기준으로 한 단계 위 W2M5/data/raw/에 있는 개인과제 CSV
TRAINING_CSV_PATH = "/Users/admin/Documents/GitHub/HDST_8th/missions/W2/data/raw/training.1600000.processed.noemoticon.csv"

# Hyundai YouTube comments input data
# 모델이 예측할 유튜브 댓글 CSV
YOUTUBE_INPUT_CSV_PATH = "/Users/admin/Documents/GitHub/HDST_8th/missions/W2/data/input/hyundai_youtube_comments_Final.csv"

# Prediction result
# 모델이 감정 라벨을 붙인 결과 CSV
LABELED_CSV_PATH = "/Users/admin/Documents/GitHub/HDST_8th/missions/W2/W2M5/TeamPJ/data/output/youtube_comments_labeled_Final_0.4_0.6.csv"


TRAINING_HISTORY_PATH = "logs/training_history_albert.csv"
MODEL_NAME = "albert_lstm"



# =========================
# Column names
# =========================

# Sentiment140 has no header, so we assign these names when loading
SENTIMENT140_COLUMNS = [
    "target",
    "ids",
    "date",
    "flag",
    "user",
    "text",
]

# Sentiment140 text column
TRAIN_TEXT_COLUMN = "text"

# Sentiment140 target column
TARGET_COLUMN = "target"

# YouTube comment text column
# 실제 hyundai_youtube_comments.csv의 댓글 컬럼명과 반드시 맞아야 함
YOUTUBE_TEXT_COLUMN = "text"


# =========================
# Label mapping
# =========================

# Sentiment140 original labels:
# 0 = negative
# 4 = positive
LABEL_MAP = {
    0: 0,
    4: 1,
}

# Model output labels:
# 0 = negative
# 1 = positive
LABEL_NAME_MAP = {
    0: "negative",
    1: "positive",
}


# =========================
# Model paths
# =========================

# Trained LSTM model
MODEL_PATH = "models/sentiment_lstm.pt"

# Saved tokenizer
TOKENIZER_PATH = "models/tokenizer.pkl"


# =========================
# Training settings
# =========================

# Sentiment140에서 클래스별로 몇 개씩 샘플링할지
# 25000이면 negative 25000 + positive 25000 = 총 50000개
SAMPLE_SIZE_PER_CLASS = 25000

# 단어 사전 크기
MAX_VOCAB_SIZE = 30000

# 한 댓글/트윗당 최대 단어 수
MAX_SEQUENCE_LENGTH = 100

# LSTM model settings
EMBEDDING_DIM = 100
HIDDEN_DIM = 128
NUM_LAYERS = 1
DROPOUT = 0.3

# Training hyperparameters
BATCH_SIZE = 64
EPOCHS = 5
LEARNING_RATE = 0.001

EARLY_STOPPING_PATIENCE = 2

# Data split ratio
TEST_SIZE = 0.1
VALIDATION_SIZE = 0.1

# Random seed
RANDOM_STATE = 42


# =========================
# Visualization output paths
# =========================

SENTIMENT_COUNT_PATH = "output/sentiment_count.png"
WORDCLOUD_POSITIVE_PATH = "output/wordcloud_positive.png"
WORDCLOUD_NEGATIVE_PATH = "output/wordcloud_negative.png"


# =========================
# Log paths
# =========================

TRAINING_LOG_PATH = "logs/training_log.txt"
PREDICTION_ERROR_LOG_PATH = "logs/prediction_errors.csv"

# =========================
# Comparison CSV files
# =========================

LABELED_CSV_FILES = {
    "Team": "data/output/youtube_comments_labeled_Final_0.4_0.6.csv",
}

COMPARISON_OUTPUT_DIR = "output/comparison_wordclouds"
SENTIMENT_COMPARISON_PATH = "output/sentiment_comparison.png"

# =========================

# Prediction thresholds

# =========================

NEGATIVE_THRESHOLD = 0.45

POSITIVE_THRESHOLD = 0.55

LABEL_NAME_MAP = {

    0: "negative",

    2: "neutral",

    4: "positive",

}