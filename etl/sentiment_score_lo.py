from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import unix_timestamp, current_timestamp
import pandas as pd
from pymongo import MongoClient
import dotenv
load_dotenv()






def load_to_mongodb(df, collection):
    uri=os.getenv('mongo_uri')

    mongo_uri = f"{uri}/market_screener.{collection}"
    df.write.format("mongo").option("uri", mongo_uri).mode("append").save()


def df_to_pandas(spark_df):
    try:
        pd_news = spark_df.toPandas()
        return pd_news
    except Exception as e:
        print(f"Error converting Spark DataFrame to Pandas DataFrame: {e}")
        return None


def pd_to_collection(df):
    try:
        uri=os.getenv('mongo_uri')
        db_name="market_screener"
        # Convert DataFrame to list of dictionaries and insert into MongoDB
        client = MongoClient(uri)
        db = client[db_name]
        db["sentimental_score"].insert_many(df.to_dict(orient='records'))
        print(f"Inserted {len(df)} records into sentimental_score.")
    except Exception as e:
        print(f"Error: {e}")
