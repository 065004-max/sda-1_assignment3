"""
producer.py
Kafka Producer for Real-Time Equity Market Analytics Platform
Streaming Data Analytics (SDA-1) - Assignment 1

Publishes data from all three identified sources to their own Kafka topics:
  1. Synthetic data        -> topic: synthetic-stock-ticks
  2. Live/streaming data   -> topic: live-stock-ticks   (requires yfinance + internet)
  3. Static/historical CSV -> topic: historical-stock-data

Usage:
    pip install kafka-python pandas yfinance

    python producer.py synthetic
    python producer.py live
    python producer.py historical --file bhavcopy_sample.csv
    python producer.py all              # runs synthetic + historical, and live if available
"""

import argparse
import json
import random
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"


def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


# ---------------------------------------------------------------------------
# 1. SYNTHETIC DATA (random-walk tick generator)
# ---------------------------------------------------------------------------
SYNTHETIC_TOPIC = "synthetic-stock-ticks"
STOCKS_SEED = {"RELIANCE": 2938.0, "TCS": 4102.0, "INFY": 1587.0}


def generate_tick(symbol, last_price):
    change = random.uniform(-0.5, 0.5)
    new_price = round(last_price + change, 2)
    volume = random.randint(100, 1500)
    tick = {
        "symbol": symbol,
        "price": new_price,
        "volume": volume,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return tick, new_price


def run_synthetic_producer(producer, iterations=None):
    print(f"[synthetic] Publishing to Kafka topic '{SYNTHETIC_TOPIC}' ...")
    stocks = dict(STOCKS_SEED)
    count = 0
    while iterations is None or count < iterations:
        for symbol, price in stocks.items():
            tick, updated_price = generate_tick(symbol, price)
            stocks[symbol] = updated_price
            producer.send(SYNTHETIC_TOPIC, value=tick)
            print("[synthetic] Sent:", tick)
        producer.flush()
        time.sleep(1)
        count += 1


# ---------------------------------------------------------------------------
# 2. LIVE / STREAMING DATA (Yahoo Finance API)
# ---------------------------------------------------------------------------
LIVE_TOPIC = "live-stock-ticks"
LIVE_SYMBOLS = ["RELIANCE.NS", "TCS.NS", "INFY.NS"]


def fetch_live_quote(yf, symbol):
    data = yf.Ticker(symbol).history(period="1d", interval="1m").tail(1)
    row = data.iloc[-1]
    return {
        "symbol": symbol,
        "price": round(float(row["Close"]), 2),
        "volume": int(row["Volume"]),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def run_live_producer(producer, iterations=None, poll_seconds=15):
    try:
        import yfinance as yf
    except ImportError:
        print("[live] yfinance not installed. Run: pip install yfinance")
        return

    print(f"[live] Publishing to Kafka topic '{LIVE_TOPIC}' ...")
    count = 0
    while iterations is None or count < iterations:
        for sym in LIVE_SYMBOLS:
            try:
                quote = fetch_live_quote(yf, sym)
                producer.send(LIVE_TOPIC, value=quote)
                print("[live] Sent:", quote)
            except Exception as e:
                print(f"[live] Failed to fetch {sym}: {e}")
        producer.flush()
        time.sleep(poll_seconds)
        count += 1


# ---------------------------------------------------------------------------
# 3. STATIC / HISTORICAL DATA (CSV replay)
# ---------------------------------------------------------------------------
HISTORICAL_TOPIC = "historical-stock-data"


def run_historical_producer(producer, filepath, delay=0.5):
    print(f"[historical] Replaying '{filepath}' to Kafka topic '{HISTORICAL_TOPIC}' ...")
    df = pd.read_csv(filepath)
    for _, row in df.iterrows():
        record = row.to_dict()
        producer.send(HISTORICAL_TOPIC, value=record)
        print("[historical] Sent:", record)
        time.sleep(delay)
    producer.flush()


# ---------------------------------------------------------------------------
# Sample historical CSV generator (so historical mode works out of the box)
# ---------------------------------------------------------------------------
def ensure_sample_csv(filepath):
    import os

    if os.path.exists(filepath):
        return
    sample = pd.DataFrame(
        [
            {"date": "2026-09-01", "symbol": "RELIANCE", "open": 2910.00, "high": 2945.50,
             "low": 2905.00, "close": 2938.00, "volume": 5123400},
            {"date": "2026-09-02", "symbol": "RELIANCE", "open": 2938.00, "high": 2960.00,
             "low": 2930.00, "close": 2952.30, "volume": 4876200},
            {"date": "2026-09-01", "symbol": "TCS", "open": 4080.00, "high": 4115.00,
             "low": 4075.00, "close": 4102.00, "volume": 2145600},
        ]
    )
    sample.to_csv(filepath, index=False)
    print(f"[historical] Sample CSV created at '{filepath}'")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Kafka producer for equity market data sources.")
    parser.add_argument(
        "mode",
        choices=["synthetic", "live", "historical", "all"],
        help="Which data source to publish to Kafka.",
    )
    parser.add_argument("--file", default="bhavcopy_sample.csv", help="CSV file for historical mode.")
    parser.add_argument("--iterations", type=int, default=None, help="Limit number of loop iterations (default: run forever).")
    args = parser.parse_args()

    producer = get_producer()

    if args.mode == "synthetic":
        run_synthetic_producer(producer, iterations=args.iterations)
    elif args.mode == "live":
        run_live_producer(producer, iterations=args.iterations)
    elif args.mode == "historical":
        ensure_sample_csv(args.file)
        run_historical_producer(producer, args.file)
    elif args.mode == "all":
        ensure_sample_csv(args.file)
        run_historical_producer(producer, args.file)
        try:
            run_live_producer(producer, iterations=1)
        except Exception as e:
            print(f"[live] Skipped ({e})")
        run_synthetic_producer(producer, iterations=args.iterations or 5)

    producer.close()


if __name__ == "__main__":
    main()
