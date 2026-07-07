# src/predict_youtube.py

import os
import sys
import pickle

import pandas as pd
import torch

# Allow importing config.py from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    YOUTUBE_INPUT_CSV_PATH,
    LABELED_CSV_PATH,
    YOUTUBE_TEXT_COLUMN,
    MODEL_PATH,
    TOKENIZER_PATH,
    MAX_SEQUENCE_LENGTH,
    EMBEDDING_DIM,
    HIDDEN_DIM,
    NUM_LAYERS,
    DROPOUT,
    LABEL_NAME_MAP,
    NEGATIVE_THRESHOLD,
    POSITIVE_THRESHOLD,
)

from src.preprocess import clean_text, is_valid_text
from src.train_model import (
    SentimentLSTM,
    texts_to_padded_sequences,
    get_device,
)


def load_youtube_comments(path):
    """
    Load Hyundai YouTube comments CSV.
    """
    df = pd.read_csv(path)
    return df


def load_tokenizer(path):
    """
    Load saved tokenizer.
    """
    with open(path, "rb") as f:
        tokenizer = pickle.load(f)

    return tokenizer


def load_trained_model(model_path, tokenizer, device):
    """
    Load trained LSTM model.
    """
    vocab_size = len(tokenizer["word_to_index"])

    model = SentimentLSTM(
        vocab_size=vocab_size,
        embedding_dim=EMBEDDING_DIM,
        hidden_dim=HIDDEN_DIM,
        num_layers=NUM_LAYERS,
        dropout=DROPOUT,
    )

    model.load_state_dict(
        torch.load(
            model_path,
            map_location=device,
        )
    )

    model = model.to(device)
    model.eval()

    return model


def predict_sentiments(model, sequences, device, batch_size=64):
    """
    Predict positive probabilities.

    Returns:
        positive_probabilities: list of positive probabilities
    """
    all_probabilities = []

    tensor_sequences = torch.tensor(sequences, dtype=torch.long)

    with torch.no_grad():
        for start_idx in range(0, len(tensor_sequences), batch_size):
            batch_x = tensor_sequences[start_idx:start_idx + batch_size]
            batch_x = batch_x.to(device)

            logits = model(batch_x)
            probabilities = torch.sigmoid(logits)

            all_probabilities.extend(probabilities.cpu().numpy().tolist())

    return all_probabilities

def assign_sentiment_label(positive_probability):
    """
    Assign sentiment label using probability thresholds.

    0 = negative
    2 = neutral
    4 = positive
    """
    if positive_probability >= POSITIVE_THRESHOLD:
        return 4

    if positive_probability <= NEGATIVE_THRESHOLD:
        return 0

    return 2




def main():
    print("Loading YouTube comments...")
    df = load_youtube_comments(YOUTUBE_INPUT_CSV_PATH)

    print(f"Input data shape: {df.shape}")
    print("Columns:")
    print(df.columns.tolist())

    if YOUTUBE_TEXT_COLUMN not in df.columns:
        raise ValueError(
            f"Text column '{YOUTUBE_TEXT_COLUMN}' does not exist in CSV. "
            f"Available columns: {df.columns.tolist()}"
        )

    print("\nCleaning comments...")
    df["cleaned_text"] = df[YOUTUBE_TEXT_COLUMN].apply(clean_text)

    # Keep original invalid rows separately if needed
    invalid_df = df[~df["cleaned_text"].apply(is_valid_text)].copy()
    valid_df = df[df["cleaned_text"].apply(is_valid_text)].copy()

    print(f"Valid comments: {len(valid_df)}")
    print(f"Invalid comments removed: {len(invalid_df)}")

    print("\nLoading tokenizer...")
    tokenizer = load_tokenizer(TOKENIZER_PATH)

    print("Converting comments to padded sequences...")
    sequences = texts_to_padded_sequences(
        valid_df["cleaned_text"],
        tokenizer,
        MAX_SEQUENCE_LENGTH,
    )

    print(f"Sequence shape: {sequences.shape}")

    print("\nLoading trained model...")
    device = get_device()
    print(f"Using device: {device}")

    model = load_trained_model(
        model_path=MODEL_PATH,
        tokenizer=tokenizer,
        device=device,
    )

    print("\nPredicting sentiments...")
    positive_probabilities = predict_sentiments(
        model=model,
        sequences=sequences,
        device=device,
        batch_size=64,
    )

    valid_df["positive_probability"] = positive_probabilities
    valid_df["negative_probability"] = 1 - valid_df["positive_probability"]

    valid_df["sentiment"] = valid_df["positive_probability"].apply(
        assign_sentiment_label
    )

    valid_df["sentiment_name"] = valid_df["sentiment"].map(LABEL_NAME_MAP)
    print("\nPrediction result counts:")
    print(valid_df["sentiment"].value_counts())

    os.makedirs(os.path.dirname(LABELED_CSV_PATH), exist_ok=True)

    valid_df.to_csv(
        LABELED_CSV_PATH,
        index=False,
    )

    print(f"\nLabeled CSV saved to: {LABELED_CSV_PATH}")
    print("Step 8 complete.")


if __name__ == "__main__":
    main()