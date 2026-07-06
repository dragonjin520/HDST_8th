# src/preprocess.py

import re


def clean_text(text):
    """
    Clean English text for sentiment analysis.

    Steps:
    1. Convert text to lowercase
    2. Remove URLs
    3. Remove Twitter mentions
    4. Remove hashtag symbols
    5. Keep only English letters and spaces
    6. Normalize multiple spaces

    Args:
        text: Raw text data.

    Returns:
        Cleaned text string.
    """
    text = str(text).lower()

    # Remove URLs
    text = re.sub(r"http\S+|www\S+", "", text)

    # Remove Twitter mentions like @username
    text = re.sub(r"@\w+", "", text)

    # Remove only hashtag symbol, keep the word
    text = re.sub(r"#", "", text)

    # Keep only English letters and spaces
    text = re.sub(r"[^a-zA-Z\s]", " ", text)

    # Normalize spaces
    text = re.sub(r"\s+", " ", text).strip()

    return text


def is_valid_text(text):
    """
    Check whether cleaned text is valid for training or prediction.

    Args:
        text: Cleaned text string.

    Returns:
        True if text is not empty, otherwise False.
    """
    return isinstance(text, str) and text.strip() != ""