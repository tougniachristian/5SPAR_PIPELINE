from mastodon import Mastodon, StreamListener
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os
import json
import sys
import time

# Config depuis les variables d'environnement
MASTODON_BASE_URL = os.getenv("MASTODON_BASE_URL", "https://mastodon.social")
MASTODON_ACCESS_TOKEN = os.getenv("MASTODON_ACCESS_TOKEN")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mastodon_stream")

if not MASTODON_ACCESS_TOKEN:
    print(" ERROR: MASTODON_ACCESS_TOKEN not set")
    sys.exit(1)

# Init Mastodon API
mastodon = Mastodon(
    access_token=MASTODON_ACCESS_TOKEN,
    api_base_url=MASTODON_BASE_URL
)

# Init Kafka Producer avec retry
print("🔌 Connecting to Kafka...")
max_retries = 30
retry_delay = 2

for attempt in range(max_retries):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=10000,
            max_block_ms=10000
        )
        print("✅ Connected to Kafka successfully!")
        break
    except NoBrokersAvailable:
        if attempt < max_retries - 1:
            print(f" Kafka not ready yet (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        else:
            print(" Failed to connect to Kafka after multiple attempts")
            sys.exit(1)

class TootListener(StreamListener):
    def on_update(self, status):
        toot = {
            "id": status["id"],
            "content": status["content"],
            "account": status["account"]["acct"],
            "created_at": str(status["created_at"])
        }
        print(f" New toot from {toot['account']}")
        producer.send(KAFKA_TOPIC, toot)
    
    def on_abort(self, err):
        print(f" Stream aborted: {err}")
    
    def on_disconnect(self):
        print("🔌 Disconnected from Mastodon stream")

if __name__ == "__main__":
    print(" Starting Mastodon ingester...")
    try:
        mastodon.stream_public(TootListener())
    except KeyboardInterrupt:
        print("\n Shutting down gracefully...")
        producer.close()
    except Exception as e:
        print(f" Error: {e}")
        producer.close()
        sys.exit(1)