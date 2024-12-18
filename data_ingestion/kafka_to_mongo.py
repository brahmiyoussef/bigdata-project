from confluent_kafka import Consumer
from pymongo import MongoClient, errors
import json
import time
import logging
import os
import signal
import dotenv
load_dotenv()
uri=os.getenv('mongo_uri')


# Kafka and MongoDB setup
KAFKA_SERVER = 'localhost:9092'
DATABASE_NAME = 'market_screener'
running = True

def signal_handler(sig, frame):
    global running
    logging.info("Signal received, shutting down...")
    running = False

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
topics=['news','market_screen']
# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to consume messages from a Kafka topic and insert them into a MongoDB collection
def consume_and_store(source, timeout=5):
    """
    Consume messages from a Kafka topic and insert them into a MongoDB collection.
    Stops if no message is processed within the timeout period.
    """
    global running

    # Kafka Consumer configuration
    consumer_conf = {
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': f'group_{source}',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([source])

    # MongoDB connection
    mongo_client = MongoClient(uri)
    db = mongo_client[DATABASE_NAME]
    collection = db[source]

    logging.info(f"Starting consumption for topic '{source}' and storing in collection '{source}'...")
    
    last_processed_time = time.time()  # Record the last message processed time

    try:
        while running:
            message = consumer.poll(1.0)  # Poll for messages (1-second timeout per poll)
            
            if message is None:
                # Check timeout
                if time.time() - last_processed_time > timeout:
                    logging.info(f"No messages processed in {timeout} seconds for topic '{source}'. Stopping consumer.")
                    break
                continue
            
            if message.error():
                logging.error(f"Consumer error in topic '{source}': {message.error()}")
                continue

            # Process the message
            try:
                record = json.loads(message.value().decode('utf-8'))
                collection.insert_one(record)
                last_processed_time = time.time()  # Reset timeout timer on successful message processing
                logging.info(f"Inserted message from topic '{source}' into MongoDB collection '{source}'.")
            except json.JSONDecodeError:
                logging.error(f"Failed to decode JSON from message in topic '{source}'.")
            except errors.PyMongoError as e:
                logging.error(f"MongoDB insertion error for topic '{source}': {e}")

    finally:
        consumer.close()
        mongo_client.close()
        logging.info(f"Stopped consumer for topic '{source}'.")

# Main function
if __name__ == '__main__':
    try:
        # Define topics and their corresponding collections

        for source in topics:
            logging.info(f"Starting to monitor topic '{source}'...")
            consume_and_store(source, timeout=5)  # Call with 5-second timeout
            logging.info(f"Finished monitoring topic '{source}'. Moving to the next topic.")

        logging.info("Finished processing all specified topics.")

    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        time.sleep(1)
