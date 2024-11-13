import finnhub
from confluent_kafka import Producer
import json
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
load_dotenv()
# Initialize the Finnhub client
apikey=os.getenv("finnhub_apikey")
finnhub_client = finnhub.Client(api_key=apikey)
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
# Initialize the Kafka producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092'  # Update with your Kafka server address
}
producer = Producer(producer_conf)


# Define the symbols and corresponding Kafka topics
symbols = ["AAPL", "TSLA", "MSFT", "AMZN", "GOOG", "META", "NVDA"]
topics = [
    "topic_AAPL",
    "topic_TSLA",
    "topic_MSFT",
    "topic_AMZN",
    "topic_GOOG",
    "topic_META",
    "topic_NVDA"
]

# Get the date range for yesterday
today = datetime.now()
yesterday = today - timedelta(days=1)
_from = yesterday.strftime('%Y-%m-%d')
_to = today.strftime('%Y-%m-%d')

# Loop through each symbol and topic
for symbol, topic in zip(symbols, topics):
    # Request company news for the previous day
    news = finnhub_client.company_news(symbol, _from=_from, to=_to)

    # Send each news item to the corresponding Kafka topic
    for article in news:
        # Serialize the message to JSON
        message = json.dumps(article).encode('utf-8')
        # Produce message to Kafka
        producer.produce(topic, value=message, callback=delivery_report)
        print(f"Sent to {topic}")

# Close the Kafka producer
producer.flush()
