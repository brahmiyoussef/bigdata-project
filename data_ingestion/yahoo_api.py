import requests
import time
from kafka import KafkaProducer
import json

# Define the API endpoint and parameters
url = "https://yahoo-finance-api-data.p.rapidapi.com/summary/option-price"
querystring = {"symbol": "AAPL"}

headers = {
    "x-rapidapi-key": "e44d9f362bmsh68963b8873fe8a6p1b7348jsn8dd1f6dfefce",
    "x-rapidapi-host": "yahoo-finance-api-data.p.rapidapi.com"
}

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Update with your Kafka server address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka topic name
topic_name = 'aapple'

# Loop to send requests at a rate of 15 per second
while True:
    try:
        # Make the API request
        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()
        print('got data from api')

        # Send data to Kafka
        producer.send(topic_name, value=data)
        producer.flush()
        print("Sent data to Kafka:")

        # Wait to maintain 15 requests per second rate
        time.sleep(0.0667)  # 1/15 seconds

    except Exception as e:
        print("Error:", e)
        time.sleep(1)  # Wait a bit before retrying in case of error
