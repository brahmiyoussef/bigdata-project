from confluent_kafka import Consumer
from pymongo import MongoClient, errors
import json
import time
import logging
import signal


# Kafka and MongoDB setup
KAFKA_SERVER = 'localhost:9092'
TOPICS = [
    "topic_AAPL",
    "topic_AMZN",
    "topic_GOOG",
    "topic_META",
    "topic_MSFT",
    "topic_NVDA",
    "topic_TSLA"
]
MONGO_URI = 'mongodb://localhost:27017/'
DATABASE_NAME = 'market_screener'
running = True

def signal_handler(sig, frame):
    global running
    logging.info("Signal received, shutting down...")
    running = False

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to consume messages from a Kafka topic and insert them into a MongoDB collection
def consume_and_store(topic_name, collection_name):
    # Initialize Kafka consumer
    global running

    consumer_conf = {
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': f'group_{topic_name}',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic_name])

    # Initialize MongoDB client
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DATABASE_NAME]
    collection = db[collection_name]

    logging.info(f"Starting consumption for topic '{topic_name}' and storing in collection '{collection_name}'...")

    # Read messages from Kafka and insert into MongoDB
    try:
        while True:
            message = consumer.poll(1.0)  # Poll for messages
            if message is None:
                if not running:
                    break
                continue
            if message.error():
                logging.error(f"Consumer error in {topic_name}: {message.error()}")
                continue

            # Insert message into MongoDB
            try:
                record = json.loads(message.value().decode('utf-8'))
                collection.insert_one(record)
                logging.info(f"Inserted message from topic '{topic_name}' into MongoDB collection '{collection_name}'.")
            except json.JSONDecodeError:
                logging.error(f"Failed to decode JSON from message in topic '{topic_name}'.")
            except errors.PyMongoError as e:
                logging.error(f"MongoDB insertion error for topic '{topic_name}': {e}")
    finally:
        consumer.close()
        mongo_client.close()
        logging.info(f"Stopped consumer for topic '{topic_name}'.")

# Sequentially process each topic
if __name__ == '__main__':
    
    
    try:
        for topic in TOPICS:
        collection_name = topic  # Use topic name as the collection name
        consume_and_store(topic, collection_name)
        logging.info("All news topics have been processed and consumers stopped.")
        consume_and_store("market_screen", "market_screen")
        logging.info("listenning to financial data topic.")
    except Exception as e:
        logging.error(f"Error in continuous listener: {e}")
        time.sleep(1)   
    
