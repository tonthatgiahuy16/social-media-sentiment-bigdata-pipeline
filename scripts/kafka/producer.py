import time
import json
import random
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'social_media_tweets'

def get_random_tweet():
    tweets = [
        "I love this new spark pipeline! #bigdata",
        "Hadoop is so slow today, but HDFS is reliable.",
        "Sentiment analysis with machine learning is fascinating.",
        "Bad weather today, feeling a bit down. :( ",
        "Just finished my big data project! Feeling great!",
        "Kafka is great for real-time streaming.",
        "MongoDB makes it easy to store flexible data.",
        "Why is my spark job failing again? #debugging",
        "Airflow is perfect for orchestration.",
        "FastAPI is so quick to build!"
    ]
    return {
        "text": random.choice(tweets),
        "user": f"user_{random.randint(1, 100)}",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

def main():
    print(f"Starting Kafka Producer on {KAFKA_BROKER}...")
    producer = None
    
    # Wait for Kafka to be ready
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            break
        except Exception as e:
            print(f"Waiting for Kafka... ({i+1}/10)")
            time.sleep(5)
    
    if not producer:
        print("Could not connect to Kafka. Exiting.")
        return

    print(f"Connected! Sending tweets to topic: {TOPIC}")
    try:
        while True:
            tweet = get_random_tweet()
            producer.send(TOPIC, tweet)
            print(f"Sent: {tweet['text']}")
            time.sleep(random.uniform(1.0, 3.0))
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
