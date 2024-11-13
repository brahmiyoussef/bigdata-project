import requests
import time
from confluent_kafka import Producer
import json
import os
from dotenv import load_dotenv
load_dotenv()

url = "https://yahoo-finance15.p.rapidapi.com/api/v1/markets/screener"
querystring = {"list": "day_gainers"}
headers = {
    "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
    "x-rapidapi-host": "yahoo-finance15.p.rapidapi.com"  # double-check this value
}

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
topic_name = 'market_screen'
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

while True:
    try:
        response = requests.get(url, headers=headers, params=querystring)  # Removed verify=False for SSL check
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
        else:
            print('Got data from API')
            data = response.json()
            producer.produce(topic_name, value=json.dumps(data), callback=delivery_report)
            producer.flush()
            time.sleep(0.0667)
    except Exception as e:
        print("Error:", e)
        time.sleep(1)
