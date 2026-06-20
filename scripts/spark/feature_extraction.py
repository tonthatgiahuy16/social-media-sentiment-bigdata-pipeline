#!/usr/bin/env python3
"""
STAGE 2: Feature Extraction - PySpark
TF-IDF, HashingTF, Word2Vec on preprocessed tweets

Features extracted:
  - TF-IDF (Term Frequency - Inverse Document Frequency)
  - HashingTF (fixed-size vector, memory efficient)
  - Word2Vec (dense embeddings, 100 dimensions)

Output: Feature vectors + label, saved to HDFS as Parquet
"""

import time
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.ml.feature import (
    HashingTF, IDF, Tokenizer, Word2Vec,
    CountVectorizer, NGram
)
from pyspark.ml import Pipeline
from pyspark import SparkConf


def create_spark_session(app_name="FeatureExtraction"):
    # Note: Memory settings are now primarily controlled by spark-defaults.conf
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()


def main():
    start = time.time()
    spark = create_spark_session("Sentiment-FeatureExtraction")

    # ── LOAD PREPROCESSED DATA ──────────────────────────────────────────────
    input_path = "hdfs://namenode:9000/sentiment/processed/preprocessed"
    print(f"[{datetime.now()}] Loading preprocessed data from {input_path}")

    df = spark.read.parquet(input_path)
    # print(f"[{datetime.now()}] Loaded records")

    # ── TRAIN/TEST SPLIT ─────────────────────────────────────────────────────
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    # print(f"[{datetime.now()}] Train/Test split complete")

    # ── FEATURE 1: TF-IDF ────────────────────────────────────────────────────
    print(f"[{datetime.now()}] Extracting TF-IDF features...")

    # HashingTF -> IDF pipeline
    hashing_tf = HashingTF(
        inputCol="stemmed_tokens",
        outputCol="raw_features",
        numFeatures=50000
    )
    idf = IDF(
        inputCol="raw_features",
        outputCol="tfidf_features",
        minDocFreq=5
    )

    idf_model = idf.fit(hashing_tf.transform(train_df))
    tfidf_train = idf_model.transform(hashing_tf.transform(train_df))
    tfidf_test = idf_model.transform(hashing_tf.transform(test_df))

    # ── FEATURE 2: Word2Vec Embeddings ──────────────────────────────────────
    print(f"[{datetime.now()}] Training Word2Vec embeddings...")

    word2vec = Word2Vec(
        vectorSize=100,
        minCount=5,
        inputCol="stemmed_tokens",
        outputCol="word2vec_features"
    )

    w2v_model = word2vec.fit(train_df)
    w2v_train = w2v_model.transform(tfidf_train)
    w2v_test = w2v_model.transform(tfidf_test)

    # ── FEATURE 3: N-gram Features (bigrams) ──────────────────────────────────
    print(f"[{datetime.now()}] Extracting N-gram features...")

    ngram = NGram(n=2, inputCol="stemmed_tokens", outputCol="bigrams")
    ngram_train = ngram.transform(w2v_train)
    ngram_test = ngram.transform(w2v_test)

    # CountVectorizer on bigrams
    cv_bigram = CountVectorizer(
        inputCol="bigrams",
        outputCol="bigram_features",
        vocabSize=20000,
        minDF=5.0
    )
    cv_bigram_model = cv_bigram.fit(ngram_train)
    cv_train = cv_bigram_model.transform(ngram_train)
    cv_test = cv_bigram_model.transform(ngram_test)

    # ── SAVE FEATURE-DATASET ─────────────────────────────────────────────────
    output_train = "hdfs://namenode:9000/sentiment/processed/features_train"
    output_test = "hdfs://namenode:9000/sentiment/processed/features_test"

    print(f"[{datetime.now()}] Saving feature datasets...")

    cols_to_save = [
        "id", "label", "text",
        "tfidf_features", "word2vec_features", "bigram_features"
    ]

    cv_train.select(cols_to_save).write.mode("overwrite").parquet(output_train)
    cv_test.select(cols_to_save).write.mode("overwrite").parquet(output_test)

    elapsed = time.time() - start
    print(f"[{datetime.now()}] FEATURE EXTRACTION COMPLETE in {elapsed:.1f}s")
    print(f"  Train output: {output_train}")
    print(f"  Test output:  {output_test}")

    spark.stop()
    return elapsed


if __name__ == "__main__":
    elapsed = main()
    sys.exit(0)
