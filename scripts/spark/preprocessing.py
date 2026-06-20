#!/usr/bin/env python3
"""
STAGE 1: Text Preprocessing - PySpark
Sentiment140 Dataset Pipeline

Preprocesses raw tweets:
  - Remove URLs, mentions, hashtags
  - Normalize unicode (tiếng Việt)
  - Tokenize, remove stopwords
  - Stemming (Porter Stemmer)
  - Extract polarity label

Dataset format (CSV, no header):
  0=polarity (0=negative, 4=positive), 1=id, 2=date, 3=query, 4=user, 5=text
"""

import re
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, lower, trim, regexp_replace,
    split, length, when, concat_ws
)
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark import SparkConf

# ── STOPWORDS ────────────────────────────────────────────────────────────────
STOPWORDS = set([
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your",
    "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she",
    "her", "hers", "herself", "it", "its", "itself", "they", "them", "their",
    "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
    "these", "those", "am", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an",
    "the", "and", "but", "if", "or", "because", "as", "until", "while", "of",
    "at", "by", "for", "with", "about", "against", "between", "into", "through",
    "during", "before", "after", "above", "below", "to", "from", "up", "down",
    "in", "out", "on", "off", "over", "under", "again", "further", "then",
    "once", "here", "there", "when", "where", "why", "how", "all", "each",
    "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only",
    "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just",
    "don", "should", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren",
    "couldn", "didn", "doesn", "hadn", "hasn", "haven", "isn", "ma", "mightn",
    "mustn", "needn", "shan", "shouldn", "wasn", "weren", "won", "wouldn",
    "im", "ive", "id", "youre", "youve", "youd", "hes", "shes", "its", "were",
    "theyre", "theyve", "theyd", "weve", "wed", "would", "could", "should",
    "u", "ur", "rt", "dm", "fb", "tw", "twt", "lol", "haha", "omg", "wtf",
    "btw", "idk", "imo", "tbh", "afaik", "fyi", "smh", "tbf", "ngl", "ikr",
])


def clean_tweet(text):
    """Clean and normalize raw tweet text."""
    if not text:
        return ""
    text = str(text)
    # Remove URLs
    text = re.sub(r"http\S+|www\.\S+", "", text)
    # Remove @mentions
    text = re.sub(r"@\w+", "", text)
    # Remove hashtag symbols (keep words)
    text = re.sub(r"#(\w+)", r"\1", text)
    # Normalize repeated chars (loooove -> loove)
    text = re.sub(r"(.)\1{2,}", r"\1\1", text)
    # Normalize unicode tiếng Việt (simple NFC)
    import unicodedata
    text = unicodedata.normalize("NFKC", text)
    # Remove non-alphabetic (keep spaces)
    text = re.sub(r"[^a-zA-Z\s]", " ", text)
    # Lowercase
    text = text.lower()
    # Remove extra spaces
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize(text):
    """Simple whitespace tokenizer."""
    if not text:
        return []
    tokens = text.split()
    return [t for t in tokens if len(t) > 2 and t not in STOPWORDS]


def simple_stem(word):
    """Very lightweight Porter-like stemmer (Python port)."""
    # Simple suffix stripping
    suffixes = [
        ("ational", "ate"), ("tional", "tion"), ("enci", "ence"),
        ("anci", "ance"), ("izer", "ize"), ("alism", "al"),
        ("ation", "ate"), ("ator", "ate"), ("alism", "al"),
        ("iveness", "ive"), ("fulness", "ful"), ("ousness", "ous"),
        ("aliti", "al"), ("iviti", "ive"), ("biliti", "ble"),
        ("ies", "y"), ("ness", ""), ("ment", ""), ("ing", ""),
        ("ed", ""), ("ly", ""), ("er", ""), ("est", ""),
    ]
    for suffix, replacement in suffixes:
        if word.endswith(suffix):
            return word[:-len(suffix)] + replacement
    return word


# ── SPARK SETUP ───────────────────────────────────────────────────────────────
def create_spark_session(app_name="Preprocessing"):
    # Note: Memory settings are now primarily controlled by spark-defaults.conf
    # or command line arguments for better flexibility.
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()


def main():
    start = time.time()

    spark = create_spark_session("Sentiment-Preprocessing")

    # ── LOAD DATA ─────────────────────────────────────────────────────────────
    # Sentiment140 CSV: no header, 6 columns
    schema = "polarity INT, id STRING, date STRING, query STRING, user STRING, text STRING"
    hdfs_path = "hdfs://namenode:9000/sentiment/raw/sentiment140_full.csv"

    print(f"[{datetime.now()}] Loading data from HDFS...")
    df = spark.read.csv(hdfs_path, schema=schema, header=False)

    # Optimization: Remove df.count() for performance on weak machines
    # print(f"[{datetime.now()}] Loaded records")

    # ── FILTER INVALID ───────────────────────────────────────────────────────
    df = df.filter(col("text").isNotNull())
    df = df.filter(col("polarity").isin(0, 4))
    df = df.filter(length(col("text")) > 0)
    # print(f"[{datetime.now()}] After filter: valid records")

    # ── PREPROCESSING ────────────────────────────────────────────────────────
    print(f"[{datetime.now()}] Cleaning tweets...")
    clean_udf = udf(clean_tweet, StringType())
    df = df.withColumn("cleaned_text", clean_udf(col("text")))

    # Tokenize
    print(f"[{datetime.now()}] Tokenizing...")
    tokenize_udf = udf(tokenize, ArrayType(StringType()))
    df = df.withColumn("tokens", tokenize_udf(col("cleaned_text")))

    # Stemming (apply to each token)
    print(f"[{datetime.now()}] Stemming...")
    stem_udf = udf(
        lambda tokens: [simple_stem(t) for t in tokens] if tokens else [],
        ArrayType(StringType())
    )
    df = df.withColumn("stemmed_tokens", stem_udf(col("tokens")))

    # Join tokens back to string for feature extraction
    df = df.withColumn("processed_text", concat_ws(" ", col("stemmed_tokens")))

    # Sentiment label: 0=negative, 1=positive (binary)
    df = df.withColumn("label", when(col("polarity") == 4, 1).otherwise(0))

    # ── SAVE PROCESSED DATA ──────────────────────────────────────────────────
    output_path = "hdfs://namenode:9000/sentiment/processed/preprocessed"
    print(f"[{datetime.now()}] Saving to HDFS: {output_path}")

    # Save as Parquet (faster than CSV for Spark)
    df.select(
        "id", "date", "polarity", "label",
        "text", "cleaned_text", "processed_text", "stemmed_tokens"
    ).write.mode("overwrite").partitionBy("label").parquet(output_path)

    elapsed = time.time() - start
    print(f"[{datetime.now()}] PREPROCESSING COMPLETE in {elapsed:.1f}s")
    print(f"Output: {output_path}")

    # ── SAMPLE STATS ─────────────────────────────────────────────────────────
    label_counts = df.groupBy("label").count().collect()
    for row in label_counts:
        print(f"  Label {row['label']}: {row['count']:,}")

    spark.stop()
    return elapsed


if __name__ == "__main__":
    elapsed = main()
    sys.exit(0)
