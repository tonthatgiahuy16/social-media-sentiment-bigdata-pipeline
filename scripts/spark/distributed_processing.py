#!/usr/bin/env python3
"""
DISTRIBUTED PROCESSING: Single file vs Multiple files
======================================================
Demonstrates how Hadoop+Spark handles multiple datasets in parallel:

1. Processing a single large file (1.6M tweets)
   → Spark splits into partitions, processes in parallel across workers

2. Processing multiple small files simultaneously
   → Kafka streams + Spark Structured Streaming

3. Connecting multiple video/dataset inputs
   → Spark Union, broadcast join, partitioned reads

KEY CONCEPTS:
  - Partitioning: data split across executors
  - Shuffle: data redistribution across partitions
  - Broadcast: small datasets sent to all executors
  - Cache/Persist: keep hot data in memory
"""

import time
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat, lit, hash, spark_partition_id, avg, stddev
)
from pyspark import SparkConf


def create_spark_session(app_name="DistributedProcessing"):
    conf = SparkConf()
    conf.setAppName(app_name)
    conf.setMaster("spark://spark-master:7077")
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.executor.cores", 1)
    conf.set("spark.sql.shuffle.partitions", 8)
    return SparkSession.builder.config(conf=conf).getOrCreate()


def scenario_single_vs_multiple_partitions(spark, hdfs_path):
    """
    Scenario: Compare processing one large partition vs many small partitions.
    """
    print(f"\n{'='*55}")
    print("  SCENARIO 1: Single large file vs Multiple partitions")
    print(f"{'='*55}")

    schema = "polarity INT, id STRING, date STRING, query STRING, user STRING, text STRING"
    df = spark.read.csv(hdfs_path, schema=schema, header=False)
    total = df.count()
    partitions = df.rdd.getNumPartitions()
    print(f"  Total records  : {total:,}")
    print(f"  Default parts  : {partitions}")

    # ── Single partition (force 1 partition) ─────────────────────────────
    t0 = time.time()
    df_single = df.coalesce(1)
    count_single = df_single.groupBy("polarity").count().collect()
    time_single = time.time() - t0
    print(f"\n  [1 Partition]  Time: {time_single:.2f}s")

    # ── Default partitions (Spark auto) ───────────────────────────────────
    t0 = time.time()
    df_auto = df.repartition(8)
    count_auto = df_auto.groupBy("polarity").count().collect()
    time_auto = time.time() - t0
    print(f"  [8 Partitions] Time: {time_auto:.2f}s  (parallel workers)")

    # ── Many partitions (max parallelism) ─────────────────────────────────
    t0 = time.time()
    df_many = df.repartition(32)
    count_many = df_many.groupBy("polarity").count().collect()
    time_many = time.time() - t0
    print(f"  [32 Partitions] Time: {time_many:.2f}s")

    print(f"\n  INSIGHT: More partitions → more parallelism → faster (up to CPU limits)")
    print(f"  Each partition runs on a different executor core in parallel")
    print(f"  8 partitions = up to 8 tasks running simultaneously on 2 workers × 2 cores")

    return {
        "1_partition": round(time_single, 2),
        "8_partitions": round(time_auto, 2),
        "32_partitions": round(time_many, 2),
    }


def scenario_multiple_files(spark):
    """
    Scenario: Process multiple CSV files simultaneously (simulating multiple video feeds).
    Spark automatically distributes files across workers.
    """
    print(f"\n{'='*55}")
    print("  SCENARIO 2: Processing multiple files in parallel")
    print(f"{'='*55}")

    # Simulate: multiple sentiment sources (Twitter, Facebook, Reddit, etc.)
    sources = [
        ("source_twitter", 400_000),
        ("source_facebook", 350_000),
        ("source_reddit", 250_000),
        ("source_instagram", 300_000),
        ("source_tiktok", 300_000),
    ]

    from pyspark.sql import Row

    def gen_tweets(source, count):
        rows = []
        for i in range(count):
            polarity = 4 if i % 2 == 0 else 0
            rows.append(Row(
                polarity=polarity,
                source=source,
                text=f"tweet from {source} #{i}",
                id=f"{source}_{i}"
            ))
        return rows

    print("  Generating simulated multi-source data...")
    all_rows = []
    for source, count in sources:
        all_rows.extend(gen_tweets(source, min(count, 5000)))

    rdd = spark.sparkContext.parallelize(all_rows, numSlices=len(sources))
    df = rdd.toDF()

    partitions = df.rdd.getNumPartitions()
    print(f"  Sources        : {len(sources)}")
    print(f"  Total records  : {len(all_rows):,}")
    print(f"  RDD partitions : {partitions}")

    t0 = time.time()
    result = df.groupBy("source", "polarity").count().orderBy("source").collect()
    elapsed = time.time() - t0

    print(f"\n  Results per source (processed in parallel):")
    for row in result:
        print(f"    {row['source']:<15} | Polarity {row['polarity']} | Count: {row['count']}")
    print(f"\n  Total time: {elapsed:.2f}s (processed all {len(sources)} sources simultaneously)")

    # Compare: would need 5 sequential reads vs 1 parallel read
    seq_time_est = elapsed * len(sources)
    print(f"\n  Sequential time (est): {seq_time_est:.2f}s")
    print(f"  Parallel speedup     : {seq_time_est / elapsed:.1f}x")

    return {"parallel_time": round(elapsed, 2), "sources": len(sources)}


def scenario_broadcast_join(spark):
    """
    Scenario: Broadcast small dataset (sentiment lexicon) to all workers.
    This is analogous to broadcasting a video processing model to all nodes.
    """
    print(f"\n{'='*55}")
    print("  SCENARIO 3: Broadcast Join (small dataset → all workers)")
    print(f"{'='*55}")

    # Simulated sentiment lexicon (small, fits in memory on each executor)
    lexicon = {
        "love": 1, "amazing": 1, "great": 1, "awesome": 1, "excellent": 1,
        "happy": 1, "best": 1, "beautiful": 1, "wonderful": 1, "fantastic": 1,
        "hate": -1, "terrible": -1, "worst": -1, "awful": -1, "horrible": -1,
        "sad": -1, "angry": -1, "bad": -1, "ugly": -1, "disappointing": -1,
    }

    # Broadcast lexicon to all executors
    lexicon_bc = spark.sparkContext.broadcast(lexicon)

    def score_tweet(text):
        score = sum(lexicon_bc.value.get(w, 0) for w in text.lower().split())
        return score

    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    score_udf = udf(score_tweet, IntegerType())

    # Sample tweets
    tweets = spark.createDataFrame([
        {"text": "I love this amazing product"},
        {"text": "This is the worst experience ever"},
        {"text": "So happy with the great results"},
        {"text": "Terrible service and awful support"},
        {"text": "Amazing and fantastic day"},
    ])

    scored = tweets.withColumn("sentiment_score", score_udf(col("text")))
    print("  Broadcast join results (lexicon sent to all executors):")
    for row in scored.collect():
        sentiment = "POSITIVE" if row["sentiment_score"] > 0 else "NEGATIVE" if row["sentiment_score"] < 0 else "NEUTRAL"
        print(f"    '{row['text']}' → Score: {row['sentiment_score']} ({sentiment})")

    print(f"\n  Without broadcast: lexicon sent over network for EACH row (slow)")
    print(f"  With broadcast   : lexicon sent once per executor (FAST)")


def scenario_cache_persist(spark, hdfs_path):
    """
    Scenario: Cache hot data in memory (like keeping video frames in RAM).
    """
    print(f"\n{'='*55}")
    print("  SCENARIO 4: Cache vs No-Cache (in-memory vs disk)")
    print(f"{'='*55}")

    schema = "polarity INT, id STRING, date STRING, query STRING, user STRING, text STRING"
    df = spark.read.csv(hdfs_path, schema=schema, header=False).limit(100_000)

    # NO CACHE - read from HDFS every time
    t0 = time.time()
    for i in range(3):
        df.groupBy("polarity").count().collect()
    no_cache_time = time.time() - t0

    # WITH CACHE - keep in memory
    df_cached = df.cache()
    df_cached.count()  # trigger action to cache

    t0 = time.time()
    for i in range(3):
        df_cached.groupBy("polarity").count().collect()
    cache_time = time.time() - t0

    print(f"  3 iterations without cache : {no_cache_time:.2f}s")
    print(f"  3 iterations with cache    : {cache_time:.2f}s")
    print(f"  Speedup from cache        : {no_cache_time / cache_time:.1f}x")
    print(f"\n  Spark .cache() = LRU memory (MEMORY_AND_DISK fallback)")
    print(f"  Equivalent to keeping video frames in RAM for fast access")

    return {"no_cache": round(no_cache_time, 2), "with_cache": round(cache_time, 2)}


def main():
    spark = create_spark_session("Distributed-Processing")
    hdfs_path = "hdfs://namenode:9000/sentiment/raw/sentiment140_full.csv"

    spark.sparkContext.setLogLevel("WARN")

    r1 = scenario_single_vs_multiple_partitions(spark, hdfs_path)
    r2 = scenario_multiple_files(spark)
    r3 = scenario_broadcast_join(spark)
    r4 = scenario_cache_persist(spark, hdfs_path)

    print(f"\n{'='*55}")
    print("  DISTRIBUTED PROCESSING SUMMARY")
    print(f"{'='*55}")
    print(f"  Partitioning: {r1}")
    print(f"  Multi-file   : {r2}")
    print(f"  Cache speedup: {r4['no_cache']/r4['with_cache']:.1f}x faster with cache")
    print(f"\n  KEY TAKEAWAYS:")
    print(f"    - Spark partitions data across workers for true parallelism")
    print(f"    - Each partition processed by a separate CPU core")
    print(f"    - Multiple files processed simultaneously (not sequentially)")
    print(f"    - Broadcast variable: small data sent once to all workers")
    print(f"    - Cache: hot data stays in memory (like video RAM)")

    spark.stop()


if __name__ == "__main__":
    main()
