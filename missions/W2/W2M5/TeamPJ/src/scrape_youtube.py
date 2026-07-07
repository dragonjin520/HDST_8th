# src/scrape_youtube_comments.py

import os
import sys
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import SCRAPED_COMMENTS_PATH, YOUTUBE_VIDEO_TARGETS


SCROLL_PAUSE_SECONDS = 2
MAX_SCROLL_COUNT = 40


def create_driver():
    """
    Create Chrome driver for YouTube comment scraping.
    """
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_argument("--disable-notifications")

    driver = webdriver.Chrome(options=chrome_options)
    return driver


def scroll_to_comments(driver):
    """
    Scroll down to load the comments section.
    """
    driver.execute_script("window.scrollTo(0, 800);")
    time.sleep(3)


def scroll_down(driver, scroll_count):
    """
    Scroll down repeatedly to load more comments.
    """
    last_height = driver.execute_script("return document.documentElement.scrollHeight")

    for _ in range(scroll_count):
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(SCROLL_PAUSE_SECONDS)

        new_height = driver.execute_script("return document.documentElement.scrollHeight")

        if new_height == last_height:
            break

        last_height = new_height


def extract_comments(driver, car_name, car_type):
    """
    Extract visible YouTube comments from the current page.
    """
    comments = []

    comment_elements = driver.find_elements(By.CSS_SELECTOR, "ytd-comment-thread-renderer")

    for comment_element in comment_elements:
        try:
            author = comment_element.find_element(By.CSS_SELECTOR, "#author-text").text.strip()
        except Exception:
            author = ""

        try:
            text = comment_element.find_element(By.CSS_SELECTOR, "#content-text").text.strip()
        except Exception:
            text = ""

        try:
            like_count = comment_element.find_element(By.CSS_SELECTOR, "#vote-count-middle").text.strip()
        except Exception:
            like_count = ""

        try:
            published_at = comment_element.find_element(By.CSS_SELECTOR, ".published-time-text").text.strip()
        except Exception:
            published_at = ""

        if text:
            comments.append(
                {
                    "car_name": car_name,
                    "car_type": car_type,
                    "author": author,
                    "text": text,
                    "like_count": like_count,
                    "published_at": published_at,
                }
            )

    return comments


def scrape_video_comments(driver, target):
    car_name = target["car_name"]
    car_type = target["car_type"]
    url = target["url"]

    print("=" * 50)
    print(f"Scraping: {car_name} / {car_type}")
    print(url)

    driver.get(url)
    time.sleep(5)

    scroll_to_comments(driver)

    collected_comments = []
    seen_texts = set()

    for scroll_index in range(MAX_SCROLL_COUNT):
        driver.execute_script(
            "window.scrollTo(0, document.documentElement.scrollHeight);"
        )
        time.sleep(SCROLL_PAUSE_SECONDS)

        comments = extract_comments(
            driver=driver,
            car_name=car_name,
            car_type=car_type,
        )

        for comment in comments:
            key = (
                comment["car_name"],
                comment["car_type"],
                comment["author"],
                comment["text"],
            )

            if key not in seen_texts:
                seen_texts.add(key)
                collected_comments.append(comment)

        print(
            f"Scroll {scroll_index + 1}/{MAX_SCROLL_COUNT} - "
            f"Collected: {len(collected_comments)}"
        )

        if len(collected_comments) >= MAX_COMMENTS_PER_VIDEO:
            break

    return collected_comments[:MAX_COMMENTS_PER_VIDEO]

def main():
    all_comments = []

    driver = create_driver()

    try:
        for target in YOUTUBE_VIDEO_TARGETS:
            comments = scrape_video_comments(driver, target)
            all_comments.extend(comments)
    finally:
        driver.quit()

    df = pd.DataFrame(all_comments)

    if df.empty:
        print("No comments collected.")
        return

    df = df.drop_duplicates(subset=["car_name", "car_type", "author", "text"])

    os.makedirs(os.path.dirname(SCRAPED_COMMENTS_PATH), exist_ok=True)
    df.to_csv(SCRAPED_COMMENTS_PATH, index=False, encoding="utf-8-sig")

    print("=" * 50)
    print(f"Total comments saved: {len(df)}")
    print(f"Saved to: {SCRAPED_COMMENTS_PATH}")

    print("\nCar type counts:")
    print(df["car_type"].value_counts())


if __name__ == "__main__":
    main()