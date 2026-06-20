from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import time

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "social_media_tweets"

# Mock Sentiment Analysis Function (In production, load the saved Spark ML model)
def analyze_sentiment(text):
    text = text.lower()
    pos_words = ['love', 'great', 'fascinating', 'perfect', 'quick', 'good', 'reliable']
    neg_words = ['slow', 'bad', 'down', 'failing', 'why']
    
    score = 0
    for word in pos_words:
        if word in text: score += 1
    for word in neg_words:
        if word in text: score -= 1
        
    if score > 0: return "Positive"
    elif score < 0: return "Negative"
    else: return "Neutral"

sentiment_udf = udf(analyze_sentiment, StringType())

def main():
    spark = SparkSession.builder \
        .appName("SentimentStreaming") \
        .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/sentiment_db.predictions") \
        .getOrCreate()

    # Define Schema for Kafka messages
    schema = StructType([
        StructField("text", StringType()),
        StructField("user", StringType()),
        StructField("timestamp", StringType())
    ])

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Parse JSON data
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Apply Sentiment Analysis
    enriched_df = parsed_df.withColumn("sentiment", sentiment_udf(col("text")))

    # Output to Console (for debugging)
    # query = enriched_df.writeStream.outputMode("append").format("console").start()

    # Output to MongoDB (using foreachBatch for simplicity or a dedicated connector)
    def save_to_mongo(batch_df, batch_id):
        if batch_df.count() > 0:
            batch_df.write \
                .format("mongodb") \
                .mode("append") \
                .option("database", "sentiment_db") \
                .option("collection", "predictions") \
                .save()
            print(f"Batch {batch_id} saved to MongoDB.")

    # In production, we'd use the mongo-spark connector. 
    # For this demo, we can also use a simple python save inside foreachBatch.
    def save_to_mongo_simple(batch_df, batch_id):
        try:
            from pymongo import MongoClient
            client = MongoClient("mongodb://mongodb:27017/")
            db = client["sentiment_db"]
            records = batch_df.collect()
            if records:
                db.predictions.insert_many([row.asDict() for row in records])
                print(f"Batch {batch_id}: {len(records)} records saved to MongoDB.")
        except Exception as e:
            print(f"Error saving to MongoDB: {e}")

    query = enriched_df.writeStream \
        .foreachBatch(save_to_mongo_simple) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
