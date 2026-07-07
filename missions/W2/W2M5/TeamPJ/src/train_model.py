# src/train_model.py

import os
import sys
import pickle
from collections import Counter

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

import torch
from torch.utils.data import Dataset, DataLoader

# Allow importing config.py from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    TRAINING_CSV_PATH,
    SENTIMENT140_COLUMNS,
    TRAIN_TEXT_COLUMN,
    TARGET_COLUMN,
    LABEL_MAP,
    SAMPLE_SIZE_PER_CLASS,
    RANDOM_STATE,
    TEST_SIZE,
    VALIDATION_SIZE,
    MAX_VOCAB_SIZE,
    MAX_SEQUENCE_LENGTH,
    TOKENIZER_PATH,
    BATCH_SIZE,
    EMBEDDING_DIM,
    HIDDEN_DIM,
    NUM_LAYERS,
    DROPOUT,
    EPOCHS,
    LEARNING_RATE,
    MODEL_PATH,
    EARLY_STOPPING_PATIENCE
)

from src.preprocess import clean_text, is_valid_text


def load_sentiment140_data():
    """
    Load Sentiment140 CSV data.

    Sentiment140 has no header, so column names are assigned manually.
    """
    df = pd.read_csv(
        TRAINING_CSV_PATH,
        encoding="latin-1",
        names=SENTIMENT140_COLUMNS,
    )

    return df


def prepare_training_data(df):
    """
    Select needed columns, map labels, clean text,
    remove empty rows, and sample balanced data.
    """
    # Select only target and text columns
    df = df[[TARGET_COLUMN, TRAIN_TEXT_COLUMN]].copy()

    # Convert original labels:
    # 0 -> 0 negative
    # 4 -> 1 positive
    df["label"] = df[TARGET_COLUMN].map(LABEL_MAP)

    # Remove rows with unmapped labels
    df = df.dropna(subset=["label"]).copy()
    df["label"] = df["label"].astype(int)

    # Clean text
    df["cleaned_text"] = df[TRAIN_TEXT_COLUMN].apply(clean_text)

    # Remove empty cleaned text
    df = df[df["cleaned_text"].apply(is_valid_text)].copy()

    # Balanced sampling
    negative_df = df[df["label"] == 0].sample(
        n=SAMPLE_SIZE_PER_CLASS,
        random_state=RANDOM_STATE,
    )

    positive_df = df[df["label"] == 1].sample(
        n=SAMPLE_SIZE_PER_CLASS,
        random_state=RANDOM_STATE,
    )

    sampled_df = pd.concat([negative_df, positive_df])
    sampled_df = sampled_df.sample(
        frac=1,
        random_state=RANDOM_STATE,
    ).reset_index(drop=True)

    return sampled_df


def split_data(df):
    """
    Split data into train, validation, and test sets.

    Final ratio:
    - train: 80%
    - validation: 10%
    - test: 10%
    """
    X = df["cleaned_text"]
    y = df["label"]

    # First split: train+validation / test
    X_train_val, X_test, y_train_val, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=y,
    )

    # Convert validation size relative to train_val set
    # Example:
    # total validation = 0.1
    # remaining train_val = 0.9
    # validation ratio inside train_val = 0.1 / 0.9
    validation_ratio = VALIDATION_SIZE / (1 - TEST_SIZE)

    X_train, X_val, y_train, y_val = train_test_split(
        X_train_val,
        y_train_val,
        test_size=validation_ratio,
        random_state=RANDOM_STATE,
        stratify=y_train_val,
    )

    return X_train, X_val, X_test, y_train, y_val, y_test


def build_tokenizer(texts, max_vocab_size):
    """
    Build a simple word-level tokenizer from training texts.

    Index rules:
    - 0: padding
    - 1: unknown word
    - 2~: frequent words
    """
    word_counter = Counter()

    for text in texts:
        words = text.split()
        word_counter.update(words)

    most_common_words = word_counter.most_common(max_vocab_size - 2)

    word_to_index = {
        "<PAD>": 0,
        "<UNK>": 1,
    }

    for idx, (word, _) in enumerate(most_common_words, start=2):
        word_to_index[word] = idx

    tokenizer = {
        "word_to_index": word_to_index,
        "max_vocab_size": max_vocab_size,
        "max_sequence_length": MAX_SEQUENCE_LENGTH,
    }

    return tokenizer


def text_to_sequence(text, tokenizer):
    """
    Convert one text into a list of word indices.
    """
    word_to_index = tokenizer["word_to_index"]
    unk_index = word_to_index["<UNK>"]

    sequence = [
        word_to_index.get(word, unk_index)
        for word in text.split()
    ]

    return sequence


def pad_sequence(sequence, max_length):
    """
    Pad or truncate one sequence to fixed length.

    We use pre-padding so that the last LSTM hidden state
    is more likely to contain real word information.
    """
    if len(sequence) >= max_length:
        return sequence[:max_length]

    return [0] * (max_length - len(sequence)) + sequence


def texts_to_padded_sequences(texts, tokenizer, max_length):
    """
    Convert texts into padded integer sequences.
    """
    sequences = []

    for text in texts:
        sequence = text_to_sequence(text, tokenizer)
        padded_sequence = pad_sequence(sequence, max_length)
        sequences.append(padded_sequence)

    return np.array(sequences, dtype=np.int64)

class SentimentDataset(Dataset):
    """
    PyTorch Dataset for sentiment analysis.

    Args:
        sequences: Padded text sequences.
        labels: Sentiment labels.
    """

    def __init__(self, sequences, labels):
        self.sequences = torch.tensor(sequences, dtype=torch.long)
        self.labels = torch.tensor(labels, dtype=torch.float32)

    def __len__(self):
        return len(self.sequences)

    def __getitem__(self, index):
        return self.sequences[index], self.labels[index]

class SentimentLSTM(torch.nn.Module):
    """
    LSTM-based sentiment analysis model.

    Input:
        sequences: [batch_size, sequence_length]

    Output:
        logits: [batch_size]
    """

    def __init__(
        self,
        vocab_size,
        embedding_dim,
        hidden_dim,
        num_layers,
        dropout,
    ):
        super().__init__()

        self.embedding = torch.nn.Embedding(
            num_embeddings=vocab_size,
            embedding_dim=embedding_dim,
            padding_idx=0,
        )

        self.lstm = torch.nn.LSTM(
            input_size=embedding_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0,
        )

        self.dropout = torch.nn.Dropout(dropout)

        self.fc = torch.nn.Linear(
            in_features=hidden_dim,
            out_features=1,
        )

    def forward(self, sequences):
        embedded = self.embedding(sequences)

        _, (hidden, _) = self.lstm(embedded)

        # Last layer hidden state
        last_hidden = hidden[-1]

        dropped = self.dropout(last_hidden)

        logits = self.fc(dropped)

        return logits.squeeze(1)

class EarlyStopping:
    """
    Stop training when validation loss does not improve.
    """

    def __init__(self, patience=2):
        self.patience = patience
        self.best_loss = float("inf")
        self.counter = 0
        self.should_stop = False

    def step(self, validation_loss):
        if validation_loss < self.best_loss:
            self.best_loss = validation_loss
            self.counter = 0
            return True

        self.counter += 1

        if self.counter >= self.patience:
            self.should_stop = True

        return False


def get_device():
    """
    Select available device.

    Priority:
    1. CUDA for NVIDIA GPU
    2. MPS for Apple Silicon Mac
    3. CPU
    """
    if torch.cuda.is_available():
        return torch.device("cuda")

    if torch.backends.mps.is_available():
        return torch.device("mps")

    return torch.device("cpu")

def calculate_accuracy(logits, labels):
    """
    Calculate binary classification accuracy.

    logits:
        Raw model outputs before sigmoid.

    labels:
        Ground truth labels, 0 or 1.
    """
    probabilities = torch.sigmoid(logits)
    predictions = (probabilities >= 0.5).float()

    correct = (predictions == labels).sum().item()
    total = labels.size(0)

    return correct / total

def train_one_epoch(model, data_loader, criterion, optimizer, device):
    """
    Train model for one epoch.
    """
    model.train()

    total_loss = 0.0
    total_accuracy = 0.0

    for batch_x, batch_y in data_loader:
        batch_x = batch_x.to(device)
        batch_y = batch_y.to(device)

        optimizer.zero_grad()

        logits = model(batch_x)
        loss = criterion(logits, batch_y)

        loss.backward()
        optimizer.step()

        accuracy = calculate_accuracy(logits, batch_y)

        total_loss += loss.item()
        total_accuracy += accuracy

    avg_loss = total_loss / len(data_loader)
    avg_accuracy = total_accuracy / len(data_loader)

    return avg_loss, avg_accuracy

def evaluate(model, data_loader, criterion, device):
    """
    Evaluate model without updating weights.
    """
    model.eval()

    total_loss = 0.0
    total_accuracy = 0.0

    with torch.no_grad():
        for batch_x, batch_y in data_loader:
            batch_x = batch_x.to(device)
            batch_y = batch_y.to(device)

            logits = model(batch_x)
            loss = criterion(logits, batch_y)

            accuracy = calculate_accuracy(logits, batch_y)

            total_loss += loss.item()
            total_accuracy += accuracy

    avg_loss = total_loss / len(data_loader)
    avg_accuracy = total_accuracy / len(data_loader)

    return avg_loss, avg_accuracy

def save_model(model, path):
    """
    Save trained PyTorch model state_dict.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # torch.save(model.state_dict(), path)


def create_data_loaders(
    X_train,
    y_train,
    X_val,
    y_val,
    X_test,
    y_test,
    batch_size,
):
    """
    Create PyTorch DataLoaders for train, validation, and test data.
    """
    train_dataset = SentimentDataset(X_train, y_train)
    val_dataset = SentimentDataset(X_val, y_val)
    test_dataset = SentimentDataset(X_test, y_test)

    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=True,
    )

    val_loader = DataLoader(
        val_dataset,
        batch_size=batch_size,
        shuffle=False,
    )

    test_loader = DataLoader(
        test_dataset,
        batch_size=batch_size,
        shuffle=False,
    )

    return train_loader, val_loader, test_loader


def save_tokenizer(tokenizer, path):
    """
    Save tokenizer as pickle file.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "wb") as f:
        pickle.dump(tokenizer, f)


def main():
    print("Loading Sentiment140 data...")
    raw_df = load_sentiment140_data()
    print(f"Raw data shape: {raw_df.shape}")

    print("Preparing training data...")
    train_df = prepare_training_data(raw_df)

    print(f"Prepared data shape: {train_df.shape}")
    print("Label counts:")
    print(train_df["label"].value_counts())

    print("\nSample rows:")
    print(train_df[["cleaned_text", "label"]].head())

    print("\nSplitting data...")
    X_train, X_val, X_test, y_train, y_val, y_test = split_data(train_df)

    print(f"Train size: {len(X_train)}")
    print(f"Validation size: {len(X_val)}")
    print(f"Test size: {len(X_test)}")

    print("\nBuilding tokenizer...")
    tokenizer = build_tokenizer(
        texts=X_train,
        max_vocab_size=MAX_VOCAB_SIZE,
    )

    vocab_size = len(tokenizer["word_to_index"])
    print(f"Vocabulary size: {vocab_size}")

    print("\nConverting texts to padded sequences...")
    X_train_seq = texts_to_padded_sequences(
        X_train,
        tokenizer,
        MAX_SEQUENCE_LENGTH,
    )
    X_val_seq = texts_to_padded_sequences(
        X_val,
        tokenizer,
        MAX_SEQUENCE_LENGTH,
    )
    X_test_seq = texts_to_padded_sequences(
        X_test,
        tokenizer,
        MAX_SEQUENCE_LENGTH,
    )

    y_train_array = y_train.to_numpy(dtype=np.int64)
    y_val_array = y_val.to_numpy(dtype=np.int64)
    y_test_array = y_test.to_numpy(dtype=np.int64)

    print(f"X_train shape: {X_train_seq.shape}")
    print(f"X_val shape: {X_val_seq.shape}")
    print(f"X_test shape: {X_test_seq.shape}")

    print(f"y_train shape: {y_train_array.shape}")
    print(f"y_val shape: {y_val_array.shape}")
    print(f"y_test shape: {y_test_array.shape}")
    print("\nSaving tokenizer...")
    save_tokenizer(tokenizer, TOKENIZER_PATH)
    print(f"Tokenizer saved to: {TOKENIZER_PATH}")

    print("\nCreating DataLoaders...")
    train_loader, val_loader, test_loader = create_data_loaders(
        X_train_seq,
        y_train_array,
        X_val_seq,
        y_val_array,
        X_test_seq,
        y_test_array,
        BATCH_SIZE,
    )

    print(f"Number of train batches: {len(train_loader)}")
    print(f"Number of validation batches: {len(val_loader)}")
    print(f"Number of test batches: {len(test_loader)}")

    sample_batch_x, sample_batch_y = next(iter(train_loader))
    print(f"Sample batch X shape: {sample_batch_x.shape}")
    print(f"Sample batch y shape: {sample_batch_y.shape}")

    print("\nCreating LSTM model...")

    vocab_size = len(tokenizer["word_to_index"])

    model = SentimentLSTM(
        vocab_size=vocab_size,
        embedding_dim=EMBEDDING_DIM,
        hidden_dim=HIDDEN_DIM,
        num_layers=NUM_LAYERS,
        dropout=DROPOUT,
    )

    print(model)

    print("\nTesting model with one batch...")
    with torch.no_grad():
        sample_outputs = model(sample_batch_x)

    print(f"Model output shape: {sample_outputs.shape}")
    print(f"Sample output values: {sample_outputs[:5]}")

    print("\nStarting training...")

    device = get_device()
    print(f"Using device: {device}")

    model = model.to(device)

    criterion = torch.nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(
        model.parameters(),
        lr=LEARNING_RATE,
    )

    early_stopping = EarlyStopping(patience=EARLY_STOPPING_PATIENCE)

    for epoch in range(EPOCHS):
        train_loss, train_accuracy = train_one_epoch(
            model=model,
            data_loader=train_loader,
            criterion=criterion,
            optimizer=optimizer,
            device=device,
        )

        val_loss, val_accuracy = evaluate(
            model=model,
            data_loader=val_loader,
            criterion=criterion,
            device=device,
        )
        is_best_model = early_stopping.step(val_loss)

        if is_best_model:
            torch.save(model.state_dict(), MODEL_PATH)
            print(f"Best model saved. Validation loss: {val_loss:.4f}")

        if early_stopping.should_stop:
            print("Early stopping triggered.")
            break

        print(f"\nEpoch {epoch + 1}/{EPOCHS}")
        print(f"Train Loss: {train_loss:.4f}")
        print(f"Train Accuracy: {train_accuracy:.4f}")
        print(f"Validation Loss: {val_loss:.4f}")
        print(f"Validation Accuracy: {val_accuracy:.4f}")

    print("\nEvaluating on test set...")
    test_loss, test_accuracy = evaluate(
        model=model,
        data_loader=test_loader,
        criterion=criterion,
        device=device,
    )

    print(f"Test Loss: {test_loss:.4f}")
    print(f"Test Accuracy: {test_accuracy:.4f}")

    print("\nSaving model...")
    save_model(model, MODEL_PATH)
    print(f"Model saved to: {MODEL_PATH}")

    print("\nStep 7 complete.")

if __name__ == "__main__":
    main()