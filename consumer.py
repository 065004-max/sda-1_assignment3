"""
consumer.py
Kafka Consumer -> MongoDB Atlas
Streaming Data Analytics (SDA-1) - Assignment 2 (Dashboard)

Reads messages from a Kafka topic (produced by producer.py) and writes each
one as a document into a MongoDB Atlas collection, which MongoDB Atlas
Charts then reads to build the live dashboard.

Usage:
    pip install kafka-python pymongo

    python consumer.py --topic live-stock-ticks
    python consumer.py --topic synthetic-stock-ticks
"""

import argparse
import json

from kafka import KafkaConsumer
from pymongo import MongoClient

BOOTSTRAP_SERVERS = "localhost:9092"

# Replace with your own Atlas connection string (Atlas -> Connect -> Drivers -> Python)
MONGO_URI = "mongodb+srv://mongodbadmin:mongodbadmin004@cluster0.qphfbgh.mongodb.net/SDA?appName=Cluster0"
DB_NAME = "SDA"
COLLECTION_NAME = "stock_ticks"


def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]


def run_consumer(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="stock-dashboard-consumer",
    )
    collection = get_mongo_collection()

    print(f"Listening on topic '{topic}', writing to MongoDB '{DB_NAME}.{COLLECTION_NAME}' ...")
    count = 0
    for message in consumer:
        record = message.value
        collection.insert_one(record)
        count += 1
        print(f"[{count}] Inserted: {record}")


def main():
    parser = argparse.ArgumentParser(description="Kafka consumer writing to MongoDB Atlas.")
    parser.add_argument(
        "--topic",
        default="live-stock-ticks",
        help="Kafka topic to consume from (e.g. live-stock-ticks, synthetic-stock-ticks, historical-stock-data).",
    )
    args = parser.parse_args()
    run_consumer(args.topic)


if __name__ == "__main__":
    main()
